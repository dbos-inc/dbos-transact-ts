// using https://github.com/brianc/node-postgres

import { DBOS, type DBOSTransactionalDataSource } from '@dbos-inc/dbos-sdk';
import { Client, type ClientBase, type ClientConfig, DatabaseError, Pool, type PoolConfig } from 'pg';
import { AsyncLocalStorage } from 'node:async_hooks';

export const IsolationLevel = Object.freeze({
  serializable: 'SERIALIZABLE',
  repeatableRead: 'REPEATABLE READ',
  readCommited: 'READ COMMITTED',
  readUncommitted: 'READ UNCOMMITTED',
});

type ValuesOf<T> = T[keyof T];

type IsolationLevel = ValuesOf<typeof IsolationLevel>;
export interface NodePostgresTransactionOptions {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
}

interface NodePostgresDataSourceContext {
  client: ClientBase;
}
const asyncLocalCtx = new AsyncLocalStorage<NodePostgresDataSourceContext>();

export class NodePostgresDataSource implements DBOSTransactionalDataSource {
  readonly name: string;
  readonly dsType = 'NodePostgresDataSource';
  readonly #pool: Pool;

  constructor(name: string, config: PoolConfig) {
    this.name = name;
    this.#pool = new Pool(config);
  }

  static get client(): ClientBase {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of PostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('No async local context found.');
    }
    return ctx.client;
  }

  static async runTxStep<T>(
    callback: () => Promise<T>,
    funcName: string,
    options: { dsName?: string; config?: NodePostgresTransactionOptions } = {},
  ) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, options);
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

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return this.#pool.end();
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

    return this.#runLocal(() => func.call(target, ...args), workflowID, functionNum, config);
  }

  async #runLocal<Return>(
    func: () => Promise<Return>,
    workflowID: string,
    functionNum: number,
    config: NodePostgresTransactionOptions | undefined,
  ): Promise<Return> {
    const isolationLevel = config?.isolationLevel ? `ISOLATION LEVEL ${config.isolationLevel}` : '';
    const readOnly = config?.readOnly ?? false;
    const accessMode = config?.readOnly === undefined ? '' : readOnly ? 'READ ONLY' : 'READ WRITE';

    while (true) {
      if (!readOnly) {
        const { rows } = await this.#pool.query<{ output: string }>(
          /*sql*/ `SELECT output FROM dbos.transaction_outputs
                 WHERE workflow_id = $1 AND function_num = $2`,
          [workflowID, functionNum],
        );
        if (rows.length > 0) {
          return JSON.parse(rows[0].output) as Return;
        }
      }

      const client = await this.#pool.connect();
      try {
        await client.query(/*sql*/ `BEGIN ${isolationLevel} ${accessMode}`);

        const output = await asyncLocalCtx.run({ client }, func);

        if (!readOnly) {
          try {
            await client.query(
              /*sql*/
              `INSERT INTO dbos.transaction_outputs (workflow_id, function_num, output) VALUES ($1, $2, $3)`,
              [workflowID, functionNum, JSON.stringify(output)],
            );
          } catch (error) {
            // 23505 is a duplicate key error
            if (error instanceof DatabaseError && error.code === '23505') {
              await client.query(/*sql*/ `ROLLBACK`);
              continue;
            } else {
              throw error;
            }
          }
        }

        await client.query(/*sql*/ `COMMIT`);
        return output;
      } catch (error) {
        await client.query(/*sql*/ `ROLLBACK`);
        throw error;
      } finally {
        client.release();
      }
    }
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
}
