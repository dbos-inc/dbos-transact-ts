// using https://github.com/porsager/postgres

import postgres, { type Sql } from 'postgres';
import { DBOS, DBOSWorkflowConflictError } from '@dbos-inc/dbos-sdk';
import {
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  type DataSourceTransactionHandler,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  PGIsolationLevel as IsolationLevel,
  PGTransactionConfig as PostgresTransactionOptions,
  DBOSDataSource,
  registerDataSource,
} from '@dbos-inc/dbos-sdk/datasource';
import { AsyncLocalStorage } from 'node:async_hooks';
import { SuperJSON } from 'superjson';

export { IsolationLevel, PostgresTransactionOptions };

interface PostgresDataSourceContext {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  client: postgres.TransactionSql<{}>;
}

const asyncLocalCtx = new AsyncLocalStorage<PostgresDataSourceContext>();

class PostgresTransactionHandler implements DataSourceTransactionHandler {
  readonly dsType = 'PostgresDataSource';
  readonly #db: Sql;

  constructor(
    readonly name: string,
    // eslint-disable-next-line @typescript-eslint/no-empty-object-type
    options: postgres.Options<{}> = {},
  ) {
    this.#db = postgres(options);
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return this.#db.end();
  }

  async #checkExecution(
    workflowID: string,
    functionNum: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type Result = { output: string | null; error: string | null };
    const result = await this.#db<Result[]>/*sql*/ `
      SELECT output, error FROM dbos.transaction_completion
      WHERE workflow_id = ${workflowID} AND function_num = ${functionNum}`;
    if (result.length === 0) {
      return undefined;
    }
    const { output, error } = result[0];
    return error !== null ? { error } : { output };
  }

  static async #recordOutput(
    // eslint-disable-next-line @typescript-eslint/no-empty-object-type
    client: postgres.TransactionSql<{}>,
    workflowID: string,
    functionNum: number,
    output: string | null,
  ): Promise<void> {
    try {
      await client/*sql*/ `
        INSERT INTO dbos.transaction_completion (workflow_id, function_num, output)
        VALUES (${workflowID}, ${functionNum}, ${output})`;
    } catch (error) {
      if (isPGKeyConflictError(error)) {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  async #recordError(workflowID: string, functionNum: number, error: string): Promise<void> {
    try {
      await this.#db/*sql*/ `
        INSERT INTO dbos.transaction_completion (workflow_id, function_num, error)
        VALUES (${workflowID}, ${functionNum}, ${error})`;
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
    if (workflowID === undefined) {
      throw new Error('Workflow ID is not set.');
    }
    const functionNum = DBOS.stepID;
    if (functionNum === undefined) {
      throw new Error('Function Number is not set.');
    }

    const isolationLevel = config?.isolationLevel ? `ISOLATION LEVEL ${config.isolationLevel}` : '';
    const readOnly = config?.readOnly ?? false;
    const accessMode = config?.readOnly === undefined ? '' : readOnly ? 'READ ONLY' : 'READ WRITE';
    const saveResults = !readOnly && workflowID;

    let retryWaitMS = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMS = 2000;

    while (true) {
      // Check to see if this tx has already been executed
      const previousResult = saveResults ? await this.#checkExecution(workflowID, functionNum) : undefined;
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
          const result = await asyncLocalCtx.run({ client }, async () => {
            return (await func.call(target, ...args)) as Return;
          });

          // save the output of read/write transactions
          if (saveResults) {
            await PostgresTransactionHandler.#recordOutput(
              client,
              workflowID,
              functionNum,
              SuperJSON.stringify(result),
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
            await this.#recordError(workflowID, functionNum, message);
          }

          throw error;
        }
      }
    }
  }
}

export class PostgresDataSource implements DBOSDataSource<PostgresTransactionOptions> {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  static get client(): postgres.TransactionSql<{}> {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of PostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('No async local context found.');
    }
    return ctx.client;
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  static async initializeInternalSchema(options: postgres.Options<{}> = {}): Promise<void> {
    const pg = postgres({ ...options, onnotice: () => {} });
    try {
      await pg.unsafe(createTransactionCompletionSchemaPG);
      await pg.unsafe(createTransactionCompletionTablePG);
    } finally {
      await pg.end();
    }
  }

  readonly name: string;
  #provider: PostgresTransactionHandler;

  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  constructor(name: string, options: postgres.Options<{}> = {}) {
    this.name = name;
    this.#provider = new PostgresTransactionHandler(name, options);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(callback: () => Promise<T>, name: string, config?: PostgresTransactionOptions) {
    return await runTransaction(callback, name, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: PostgresTransactionOptions,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name }, config);
  }

  transaction(config?: PostgresTransactionOptions) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    return function decorator<This, Args extends unknown[], Return>(
      _target: object,
      propertyKey: string,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (!descriptor.value) {
        throw Error('Use of decorator when original method is undefined');
      }

      descriptor.value = ds.registerTransaction(descriptor.value, propertyKey.toString(), config);

      return descriptor;
    };
  }
}
