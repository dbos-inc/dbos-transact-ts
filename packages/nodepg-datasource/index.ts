// using https://github.com/brianc/node-postgres

import { DBOS, DBOSWorkflowConflictError, type DBOSTransactionalDataSource } from '@dbos-inc/dbos-sdk';
import { Client, type ClientBase, type ClientConfig, DatabaseError, Pool, type PoolConfig } from 'pg';
import { AsyncLocalStorage } from 'node:async_hooks';
import { SuperJSON } from 'superjson';

interface NodePostgresDataSourceContext {
  client: ClientBase;
}

export const IsolationLevel = Object.freeze({
  serializable: 'SERIALIZABLE',
  repeatableRead: 'REPEATABLE READ',
  readCommited: 'READ COMMITTED',
  readUncommitted: 'READ UNCOMMITTED',
});

type ValuesOf<T> = T[keyof T];

export interface NodePostgresTransactionOptions {
  isolationLevel?: ValuesOf<typeof IsolationLevel>;
  readOnly?: boolean;
}

function getErrorCode(error: unknown) {
  return error instanceof DatabaseError ? error.code : undefined;
}

export class NodePostgresDataSource implements DBOSTransactionalDataSource {
  static readonly #asyncLocalCtx = new AsyncLocalStorage<NodePostgresDataSourceContext>();

  static async runTxStep<T>(
    callback: () => Promise<T>,
    funcName: string,
    options: { dsName?: string; config?: NodePostgresTransactionOptions } = {},
  ) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, options);
  }

  static get client(): ClientBase {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of NodePostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = NodePostgresDataSource.#asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('No async local context found.');
    }
    return ctx.client;
  }

  static async configure(config: ClientConfig): Promise<void> {
    const client = new Client(config);
    try {
      await client.connect();
      await client.query(
        /*sql*/
        `CREATE SCHEMA IF NOT EXISTS dbos;
         CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
            workflow_id TEXT NOT NULL,
            function_num INT NOT NULL,
            output TEXT,
            created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
            PRIMARY KEY (workflow_id, function_num));`,
      );
    } finally {
      await client.end();
    }
  }

  readonly name: string;
  readonly dsType = 'NodePostgresDataSource';
  readonly #pool: Pool;

  constructor(name: string, config: PoolConfig) {
    this.name = name;
    this.#pool = new Pool(config);
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return this.#pool.end();
  }

  async runTxStep<T>(callback: () => Promise<T>, funcName: string, config?: NodePostgresTransactionOptions) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, { dsName: this.name, config });
  }

  register<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: NodePostgresTransactionOptions,
  ): (this: This, ...args: Args) => Promise<Return> {
    return DBOS.registerTransaction(this.name, func, { name }, config);
  }

  static async #checkExecution(
    client: ClientBase,
    workflowID: string,
    functionNum: number,
  ): Promise<{ output: string | null } | undefined> {
    const { rows } = await client.query<{ output: string }>(
      /*sql*/ `SELECT output FROM dbos.transaction_outputs
       WHERE workflow_id = $1 AND function_num = $2`,
      [workflowID, functionNum],
    );
    return rows.length > 0 ? { output: rows[0].output } : undefined;
  }

  static async #recordOutput(
    client: ClientBase,
    workflowID: string,
    functionNum: number,
    output: string | null,
  ): Promise<void> {
    try {
      await client.query(
        /*sql*/
        `INSERT INTO dbos.transaction_outputs (workflow_id, function_num, output) 
         VALUES ($1, $2, $3)`,
        [workflowID, functionNum, output],
      );
    } catch (error) {
      // 24505 is a duplicate key error in PostgreSQL
      if (getErrorCode(error) === '23505') {
        throw new DBOSWorkflowConflictError(workflowID);
      } else {
        throw error;
      }
    }
  }

  async #transaction<Return>(
    func: (client: ClientBase) => Promise<Return>,
    config: NodePostgresTransactionOptions = {},
  ): Promise<Return> {
    const isolationLevel = config?.isolationLevel ? `ISOLATION LEVEL ${config.isolationLevel}` : '';
    const readOnly = config?.readOnly ?? false;
    const accessMode = config?.readOnly === undefined ? '' : readOnly ? 'READ ONLY' : 'READ WRITE';

    const client = await this.#pool.connect();
    try {
      await client.query(/*sql*/ `BEGIN ${isolationLevel} ${accessMode}`);
      const result = await func(client);
      await client.query(/*sql*/ `COMMIT`);
      return result;
    } catch (error) {
      await client.query(/*sql*/ `ROLLBACK`);
      throw error;
    } finally {
      client.release();
    }
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: NodePostgresTransactionOptions | undefined,
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

    const readOnly = config?.readOnly ?? false;
    let retryWaitMS = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMS = 2000;

    while (true) {
      try {
        const result = await this.#transaction<Return>(
          async (client) => {
            // Check to see if this tx has already been executed
            const previousResult = readOnly
              ? undefined
              : await NodePostgresDataSource.#checkExecution(client, workflowID, functionNum);
            if (previousResult) {
              return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
            }

            // execute user's transaction function
            const result = await NodePostgresDataSource.#asyncLocalCtx.run({ client }, async () => {
              return (await func.call(target, ...args)) as Return;
            });

            // save the output of read/write transactions
            if (!readOnly) {
              await NodePostgresDataSource.#recordOutput(client, workflowID, functionNum, SuperJSON.stringify(result));

              // Note, existing code wraps #recordOutput call in a try/catch block that
              // converts DB error with code 25P02 to DBOSFailedSqlTransactionError.
              // However, existing code doesn't make any logic decisions based on that error type.
              // DBOSFailedSqlTransactionError does stored WF ID and function name, so I assume that info is logged out somewhere
            }

            return result;
          },
          { isolationLevel: config?.isolationLevel, readOnly: config?.readOnly },
        );
        // TODO: span.setStatus({ code: SpanStatusCode.OK });
        // TODO: this.tracer.endSpan(span);

        return result;
      } catch (error) {
        if (getErrorCode(error) === '40001') {
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
