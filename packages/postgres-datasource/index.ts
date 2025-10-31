// using https://github.com/porsager/postgres

import postgres, { type Sql } from 'postgres';
import { DBOS, DBOSWorkflowConflictError, FunctionName } from '@dbos-inc/dbos-sdk';
import {
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  type DataSourceTransactionHandler,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  PGIsolationLevel as IsolationLevel,
  PGTransactionConfig,
  DBOSDataSource,
  registerDataSource,
  CheckSchemaInstallationReturn,
  checkSchemaInstallationPG,
} from '@dbos-inc/dbos-sdk/datasource';
import { AsyncLocalStorage } from 'node:async_hooks';
import { SuperJSON } from 'superjson';

interface PostgresTransactionOptions extends PGTransactionConfig {
  name?: string;
}

export { IsolationLevel, PostgresTransactionOptions };

interface PostgresDataSourceContext {
  client: postgres.TransactionSql;
  owner: PostgresTransactionHandler;
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
type Options = postgres.Options<{}>;

const asyncLocalCtx = new AsyncLocalStorage<PostgresDataSourceContext>();

class PostgresTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'PostgresDataSource';
  #dbField: Sql | undefined;
  readonly schemaName: string;

  constructor(
    readonly name: string,
    private readonly options: Options = {},
    schemaName: string = 'dbos',
  ) {
    this.schemaName = schemaName;
  }

  async initialize(): Promise<void> {
    const db = this.#dbField;
    this.#dbField = postgres(this.options);
    await db?.end();

    let installed = false;
    try {
      const rows = (await this.#dbField.unsafe(
        checkSchemaInstallationPG(this.schemaName),
      )) as CheckSchemaInstallationReturn[];
      installed = !!rows[0]?.schema_exists && !!rows[0]?.table_exists;
    } catch (e) {
      throw new Error(
        `In initialization of 'PostgresDataSource' ${this.name}: Database could not be reached: ${(e as Error).message}`,
      );
    }

    // Install
    if (!installed) {
      try {
        await this.#dbField.unsafe(createTransactionCompletionSchemaPG(this.schemaName));
        await this.#dbField.unsafe(createTransactionCompletionTablePG(this.schemaName));
      } catch (err) {
        throw new Error(
          `In initialization of 'PostgresDataSource' ${this.name}: The '${this.schemaName}.transaction_completion' table does not exist, and could not be created.  This should be added to your database migrations.
          See: https://docs.dbos.dev/typescript/tutorials/transaction-tutorial#installing-the-dbos-schema`,
        );
      }
    }
  }

  async destroy(): Promise<void> {
    const db = this.#dbField;
    this.#dbField = undefined;
    await db?.end();
  }

  get #db(): Sql {
    if (!this.#dbField) {
      throw new Error(`DataSource ${this.name} is not initialized.`);
    }
    return this.#dbField;
  }

  async #checkExecution(
    workflowID: string,
    stepID: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type Result = { output: string | null; error: string | null };
    const result = await this.#db<Result[]>/*sql*/ `
      SELECT output, error FROM "${this.schemaName}".transaction_completion
      WHERE workflow_id = ${workflowID} AND function_num = ${stepID}`;
    if (result.length === 0) {
      return undefined;
    }
    const { output, error } = result[0];
    return error !== null ? { error } : { output };
  }

  static async #recordOutput(
    client: postgres.TransactionSql,
    workflowID: string,
    stepID: number,
    output: string | null,
    schemaName: string,
  ): Promise<void> {
    try {
      await client/*sql*/ `
        INSERT INTO "${schemaName}".transaction_completion (workflow_id, function_num, output)
        VALUES (${workflowID}, ${stepID}, ${output})`;
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  async #recordError(workflowID: string, stepID: number, error: string): Promise<void> {
    try {
      await this.#db/*sql*/ `
        INSERT INTO "${this.schemaName}".transaction_completion (workflow_id, function_num, error)
        VALUES (${workflowID}, ${stepID}, ${error})`;
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: PostgresTransactionOptions | undefined,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID;
    const stepID = DBOS.stepID;
    if (workflowID !== undefined && stepID === undefined) {
      throw new Error('DBOS.stepID is undefined inside a workflow.');
    }

    const isolationLevel = config?.isolationLevel ? `ISOLATION LEVEL ${config.isolationLevel}` : '';
    const readOnly = config?.readOnly ?? false;
    const accessMode = config?.readOnly === undefined ? '' : readOnly ? 'READ ONLY' : 'READ WRITE';
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
        const result = await this.#db.begin<Return>(`${isolationLevel} ${accessMode}`, async (client) => {
          // execute user's transaction function
          const result = await asyncLocalCtx.run({ client, owner: this }, async () => {
            return (await func.call(target, ...args)) as Return;
          });

          // save the output of read/write transactions
          if (saveResults) {
            await PostgresTransactionHandler.#recordOutput(
              client,
              workflowID,
              stepID!,
              SuperJSON.stringify(result),
              this.schemaName,
            );
          }

          return result;
        });
        return result as Return;
      } catch (error) {
        if (isPGRetriableTransactionError(error)) {
          // 400001 is a serialization failure in PostgreSQL
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

export class PostgresDataSource implements DBOSDataSource<PostgresTransactionOptions> {
  static #getClient(p?: PostgresTransactionHandler): postgres.TransactionSql {
    if (!DBOS.isInTransaction()) {
      throw new Error('Invalid use of PostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('Invalid use of PostgresDataSource.client outside of a DBOS transaction.');
    }
    if (p && p !== ctx.owner) {
      throw new Error('Invalid retrieval of `PostgresDataSource.client` from the wrong instance.');
    }
    return ctx.client;
  }

  static get client() {
    return PostgresDataSource.#getClient(undefined);
  }

  get client() {
    return PostgresDataSource.#getClient(this.#provider);
  }

  static async initializeDBOSSchema(options: Options = {}, schemaName: string = 'dbos'): Promise<void> {
    const pg = postgres({ ...options, onnotice: () => {} });
    try {
      await pg.unsafe(createTransactionCompletionSchemaPG(schemaName));
      await pg.unsafe(createTransactionCompletionTablePG(schemaName));
    } finally {
      await pg.end();
    }
  }

  #provider: PostgresTransactionHandler;

  constructor(
    readonly name: string,
    options: Options = {},
    schemaName: string = 'dbos',
  ) {
    this.#provider = new PostgresTransactionHandler(name, options, schemaName);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(func: () => Promise<T>, config?: PostgresTransactionOptions) {
    return await runTransaction(func, config?.name ?? func.name, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: PostgresTransactionOptions & FunctionName,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, config);
  }

  transaction(config?: PostgresTransactionOptions) {
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
