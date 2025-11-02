// using https://github.com/knex/knex

import { DBOS, DBOSWorkflowConflictError, FunctionName } from '@dbos-inc/dbos-sdk';
import {
  type DataSourceTransactionHandler,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  DBOSDataSource,
  registerDataSource,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  CheckSchemaInstallationReturn,
  checkSchemaInstallationPG,
} from '@dbos-inc/dbos-sdk/datasource';
import { AsyncLocalStorage } from 'async_hooks';
import knex, { type Knex } from 'knex';
import { SuperJSON } from 'superjson';

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

interface KnexDataSourceContext {
  client: Knex.Transaction;
  owner: KnexTransactionHandler;
}

export type TransactionConfig = Pick<Knex.TransactionConfig, 'isolationLevel' | 'readOnly'> & {
  name?: string;
};

const asyncLocalCtx = new AsyncLocalStorage<KnexDataSourceContext>();

class KnexTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'KnexDataSource';
  #knexDBField: Knex | undefined;
  readonly schemaName: string;

  constructor(
    readonly name: string,
    private readonly config: Knex.Config,
    schemaName: string = 'dbos',
  ) {
    this.schemaName = schemaName;
  }

  async initialize(): Promise<void> {
    const knexDB = this.#knexDBField;
    this.#knexDBField = knex(this.config);
    await knexDB?.destroy();

    // Check for connectivity & the schema
    let installed = false;
    try {
      const { rows } = await this.#knexDBField.raw<{ rows: CheckSchemaInstallationReturn[] }>(
        checkSchemaInstallationPG(this.schemaName),
      );
      installed = !!rows[0].schema_exists && !!rows[0].table_exists;
    } catch (e) {
      DBOS.logger.error(e);
      throw new Error(
        `In initialization of 'KnexDataSource' ${this.name}: Database could not be reached: ${(e as Error).message}`,
      );
    }

    if (!installed) {
      try {
        await this.#knexDBField.raw(createTransactionCompletionSchemaPG(this.schemaName));
        await this.#knexDBField.raw(createTransactionCompletionTablePG(this.schemaName));
      } catch (err) {
        throw new Error(
          `In initialization of 'KnexDataSource' ${this.name}: The '${this.schemaName}.transaction_completion' table does not exist, and could not be created.  This should be added to your database migrations.
            See: https://docs.dbos.dev/typescript/tutorials/transaction-tutorial#installing-the-dbos-schema`,
        );
      }
    }
  }

  async destroy(): Promise<void> {
    const knexDB = this.#knexDBField;
    this.#knexDBField = undefined;
    await knexDB?.destroy();
  }

  get #knexDB() {
    if (!this.#knexDBField) {
      throw new Error(`DataSource ${this.name} is not initialized.`);
    }
    return this.#knexDBField;
  }

  async #checkExecution(
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    const result = await this.#knexDB<transaction_completion>('transaction_completion')
      .withSchema('dbos')
      .select('output', 'error')
      .where({
        workflow_id: workflowID,
        function_num: stepID,
      })
      .first();
    if (result === undefined) {
      return undefined;
    }
    const { output, error } = result;
    return error !== null ? { error } : { output };
  }

  async #recordError(workflowID: string, stepID: number, error: string): Promise<void> {
    try {
      await this.#knexDB<transaction_completion>('transaction_completion').withSchema('dbos').insert({
        workflow_id: workflowID,
        function_num: stepID,
        error,
      });
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  static async #recordOutput(
    client: Knex.Transaction,
    workflowID: string,
    stepID: number,
    output: string | null,
  ): Promise<void> {
    try {
      await client<transaction_completion>('transaction_completion').withSchema('dbos').insert({
        workflow_id: workflowID,
        function_num: stepID,
        output,
      });
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: TransactionConfig | undefined,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID;
    const stepID = DBOS.stepID;
    if (workflowID !== undefined && stepID === undefined) {
      throw new Error('DBOS.stepID is undefined inside a workflow.');
    }

    const readOnly = config?.readOnly ?? false;
    const saveResults = !readOnly && workflowID !== undefined;

    // Retry loop if appropriate
    let retryWaitMS = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMS = 2000; // Maximum wait 2 seconds.

    while (true) {
      // Check to see if this tx has already been executed
      const previousResult = saveResults ? await this.#checkExecution(workflowID, stepID!) : undefined;
      if (previousResult) {
        DBOS.span?.setAttribute('cached', true);

        if ('error' in previousResult) {
          throw SuperJSON.parse(previousResult.error);
        }
        return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
      }

      try {
        const result = await this.#knexDB.transaction<Return>(
          async (client) => {
            // execute user's transaction function
            const result = await asyncLocalCtx.run({ client, owner: this }, async () => {
              return (await func.call(target, ...args)) as Return;
            });

            // save the output of read/write transactions
            if (saveResults) {
              await KnexTransactionHandler.#recordOutput(client, workflowID, stepID!, SuperJSON.stringify(result));
            }

            return result;
          },
          { isolationLevel: config?.isolationLevel, readOnly: config?.readOnly },
        );

        return result;
      } catch (error) {
        if (isPGRetriableTransactionError(error)) {
          DBOS.span?.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMS }, performance.now());
          await new Promise((resolve) => setTimeout(resolve, retryWaitMS));
          retryWaitMS = Math.min(retryWaitMS * backoffFactor, maxRetryWaitMS);
          continue;
        } else {
          if (saveResults) {
            const message = SuperJSON.stringify(error);
            await this.#recordError(workflowID, stepID!, message);
          }

          throw error;
        }
      }
    }
  }
}

function isKnex(value: Knex | Knex.Config): value is Knex {
  return 'raw' in value;
}

export class KnexDataSource implements DBOSDataSource<TransactionConfig> {
  static #getClient(p?: KnexTransactionHandler) {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of KnexDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('invalid use of KnexDataSource.client outside of a DBOS transaction.');
    }
    if (p && p !== ctx.owner) throw new Error('Request of `KnexDataSource.client` from the wrong object.');
    return ctx.client;
  }

  static get client(): Knex.Transaction {
    return KnexDataSource.#getClient(undefined);
  }

  get client(): Knex.Transaction {
    return KnexDataSource.#getClient(this.#provider);
  }

  static async initializeDBOSSchema(knexOrConfig: Knex.Config, schemaName: string = 'dbos') {
    if (isKnex(knexOrConfig)) {
      await $initSchema(knexOrConfig);
    } else {
      const knexDB = knex(knexOrConfig);
      try {
        await $initSchema(knexDB);
      } finally {
        await knexDB.destroy();
      }
    }

    async function $initSchema(knexDB: Knex) {
      await knexDB.raw(createTransactionCompletionSchemaPG(schemaName));
      await knexDB.raw(createTransactionCompletionTablePG(schemaName));
    }
  }

  static async uninitializeDBOSSchema(knexOrConfig: Knex.Config, schemaName: string = 'dbos') {
    if (isKnex(knexOrConfig)) {
      await $uninitSchema(knexOrConfig);
    } else {
      const knexDB = knex(knexOrConfig);
      try {
        await $uninitSchema(knexDB);
      } finally {
        await knexDB.destroy();
      }
    }

    async function $uninitSchema(knexDB: Knex) {
      await knexDB.raw(
        `DROP TABLE IF EXISTS "${schemaName}".transaction_completion; DROP SCHEMA IF EXISTS "${schemaName}" CASCADE;`,
      );
    }
  }

  #provider: KnexTransactionHandler;

  constructor(
    readonly name: string,
    config: Knex.Config,
    schemaName: string = 'dbos',
  ) {
    this.#provider = new KnexTransactionHandler(name, config, schemaName);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(func: () => Promise<T>, config?: TransactionConfig) {
    return await runTransaction(func, config?.name ?? func.name, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: TransactionConfig & FunctionName,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, config);
  }

  transaction(config?: TransactionConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    return function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (!descriptor.value) {
        throw Error('Use of decorator when original method is undefined');
      }

      descriptor.value = ds.registerTransaction(descriptor.value, {
        ...config,
        name: config?.name ?? String(propertyKey),
        ctorOrProto: target,
      });

      return descriptor;
    };
  }
}
