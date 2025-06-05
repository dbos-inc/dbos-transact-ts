// using https://github.com/porsager/postgres

import postgres, { type Sql } from 'postgres';
import { DBOS, DBOSWorkflowConflictError } from '@dbos-inc/dbos-sdk';
import {
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  type DBOSTransactionalDataSource,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  PGIsolationLevel as IsolationLevel,
  PGTransactionConfig as PostgresTransactionOptions,
} from '@dbos-inc/dbos-sdk/datasource';
import { AsyncLocalStorage } from 'node:async_hooks';
import { SuperJSON } from 'superjson';

export { IsolationLevel, PostgresTransactionOptions };

interface PostgresDataSourceContext {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  client: postgres.TransactionSql<{}>;
}

export class PostgresDataSource implements DBOSTransactionalDataSource {
  static readonly #asyncLocalCtx = new AsyncLocalStorage<PostgresDataSourceContext>();

  static async runTxStep<T>(
    callback: () => Promise<T>,
    funcName: string,
    options: { dsName?: string; config?: PostgresTransactionOptions } = {},
  ) {
    return await runTransaction(callback, funcName, options);
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  static get client(): postgres.TransactionSql<{}> {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of PostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = PostgresDataSource.#asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('No async local context found.');
    }
    return ctx.client;
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  static async configure(options: postgres.Options<{}> = {}): Promise<void> {
    const pg = postgres({ ...options, onnotice: () => {} });
    try {
      await pg.unsafe(createTransactionCompletionSchemaPG);
      await pg.unsafe(createTransactionCompletionTablePG);
    } finally {
      await pg.end();
    }
  }

  readonly name: string;
  readonly dsType = 'PostgresDataSource';
  readonly #db: Sql;

  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  constructor(name: string, options: postgres.Options<{}> = {}) {
    this.name = name;
    this.#db = postgres(options);
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return this.#db.end();
  }

  async runTxStep<T>(callback: () => Promise<T>, funcName: string, config?: PostgresTransactionOptions) {
    return await runTransaction(callback, funcName, { dsName: this.name, config });
  }

  register<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: PostgresTransactionOptions,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name }, config);
  }

  static async #checkExecution(
    // eslint-disable-next-line @typescript-eslint/no-empty-object-type
    client: postgres.TransactionSql<{}>,
    workflowID: string,
    functionNum: number,
  ): Promise<{ output: string | null } | undefined> {
    type Result = { output: string };
    const result = await client<Result[]>/*sql*/ `
            SELECT output FROM dbos.transaction_completion
            WHERE workflow_id = ${workflowID} AND function_num = ${functionNum}`;

    return result.length > 0 ? { output: result[0].output } : undefined;
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

    let retryWaitMS = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMS = 2000;

    while (true) {
      try {
        const result = await this.#db.begin<Return>(`${isolationLevel} ${accessMode}`, async (client) => {
          // Check to see if this tx has already been executed
          const previousResult = readOnly
            ? undefined
            : await PostgresDataSource.#checkExecution(client, workflowID, functionNum);
          if (previousResult) {
            return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
          }

          // execute user's transaction function
          const result = await PostgresDataSource.#asyncLocalCtx.run({ client }, async () => {
            return (await func.call(target, ...args)) as Return;
          });

          // save the output of read/write transactions
          if (!readOnly) {
            await PostgresDataSource.#recordOutput(client, workflowID, functionNum, SuperJSON.stringify(result));

            // Note, existing code wraps #recordOutput call in a try/catch block that
            // converts DB error with code 25P02 to DBOSFailedSqlTransactionError.
            // However, existing code doesn't make any logic decisions based on that error type.
            // DBOSFailedSqlTransactionError does stored WF ID and function name, so I assume that info is logged out somewhere
          }

          return result;
        });
        // TODO: span.setStatus({ code: SpanStatusCode.OK });
        // TODO: this.tracer.endSpan(span);

        return result as Return;
      } catch (error) {
        if (isPGRetriableTransactionError(error)) {
          // 400001 is a serialization failure in PostgreSQL

          // TODO: span.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMillis }, performance.now());
          await new Promise((resolve) => setTimeout(resolve, retryWaitMS));
          retryWaitMS = Math.min(retryWaitMS * backoffFactor, maxRetryWaitMS);
          continue;
        } else {
          // TODO: span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
          // TODO: this.tracer.endSpan(span);

          // TODO: currently, we are *not* recording errors in the txOutput table.
          // For normal execution, this is fine because we also store tx step results (output and error) in the sysdb operation output table.
          // However, I'm concerned that we have a dueling execution hole where one tx fails while another succeeds.
          // This implies that we can end up in a situation where the step output records an error but the txOutput table records success.

          throw error;
        }
      }
    }
  }
}
