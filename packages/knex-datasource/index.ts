// using https://github.com/knex/knex

import { DBOS, DBOSWorkflowConflictError } from '@dbos-inc/dbos-sdk';
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
}

export type TransactionConfig = Pick<Knex.TransactionConfig, 'isolationLevel' | 'readOnly'>;

const asyncLocalCtx = new AsyncLocalStorage<KnexDataSourceContext>();

class KnexTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'KnexDataSource';
  #knexDBField: Knex | undefined;

  constructor(
    readonly name: string,
    private readonly config: Knex.Config,
  ) {}

  async initialize(): Promise<void> {
    const knexDB = this.#knexDBField;
    this.#knexDBField = knex(this.config);
    await knexDB?.destroy();
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
            const result = await asyncLocalCtx.run({ client }, async () => {
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

export class KnexDataSource implements DBOSDataSource<TransactionConfig> {
  static get client(): Knex.Transaction {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of KnexDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('invalid use of KnexDataSource.client outside of a DBOS transaction.');
    }
    return ctx.client;
  }

  static async initializeInternalSchema(config: Knex.Config) {
    const knexDB = knex(config);
    try {
      await knexDB.raw(createTransactionCompletionSchemaPG);
      await knexDB.raw(createTransactionCompletionTablePG);
    } finally {
      await knexDB.destroy();
    }
  }

  #provider: KnexTransactionHandler;

  constructor(
    readonly name: string,
    config: Knex.Config,
  ) {
    this.#provider = new KnexTransactionHandler(name, config);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(callback: () => Promise<T>, funcName: string, config?: TransactionConfig) {
    return await runTransaction(callback, funcName, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: TransactionConfig,
    name?: string,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name: name ?? func.name }, config);
  }

  transaction(config?: TransactionConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    return function decorator<This, Args extends unknown[], Return>(
      _target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (!descriptor.value) {
        throw Error('Use of decorator when original method is undefined');
      }

      descriptor.value = ds.registerTransaction(descriptor.value, config, String(propertyKey));

      return descriptor;
    };
  }
}
