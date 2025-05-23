// using https://github.com/porsager/postgres

import postgres, { type Sql } from 'postgres';
import { DBOS, type DBOSTransactionalDataSource } from '@dbos-inc/dbos-sdk';
import { AsyncLocalStorage } from 'node:async_hooks';

interface PostgresDataSourceContext {
  client: postgres.TransactionSql<{}>;
}
const asyncLocalCtx = new AsyncLocalStorage<PostgresDataSourceContext>();

class TxOutputDuplicateKeyError extends Error {}

export const IsolationLevel = Object.freeze({
  serializable: 'SERIALIZABLE',
  repeatableRead: 'REPEATABLE READ',
  readCommited: 'READ COMMITTED',
  readUncommitted: 'READ UNCOMMITTED',
});

type ValuesOf<T> = T[keyof T];

export interface PostgresTransactionOptions {
  isolationLevel?: ValuesOf<typeof IsolationLevel>;
  readOnly?: boolean;
}

export class PostgresDataSource implements DBOSTransactionalDataSource {
  readonly name: string;
  readonly dsType = 'PostgresDataSource';
  readonly #db: Sql;

  constructor(name: string, options: postgres.Options<{}> = {}) {
    this.name = name;
    this.#db = postgres(options);
  }

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

  static async runTxStep<T>(
    callback: () => Promise<T>,
    funcName: string,
    options: { dsName?: string; config?: PostgresTransactionOptions } = {},
  ) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, options);
  }

  async runTxStep<T>(callback: () => Promise<T>, funcName: string, config?: PostgresTransactionOptions) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, { dsName: this.name, config });
  }

  register<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: PostgresTransactionOptions,
  ): (this: This, ...args: Args) => Promise<Return> {
    return DBOS.registerTransaction(this.name, func, { name }, config);
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return this.#db.end();
  }

  async #getResult(workflowID: string, functionNum: number): Promise<string | undefined> {
    type Result = { output: string };
    const result = await this.#db<Result[]>/*sql*/ `
            SELECT output FROM dbos.transaction_outputs
            WHERE workflow_id = ${workflowID} AND function_num = ${functionNum}`;
    return result[0]?.output;
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: PostgresTransactionOptions,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID;
    const functionNum = DBOS.stepID;
    const isolationLevel = config.isolationLevel ? `ISOLATION LEVEL ${config.isolationLevel}` : '';
    const accessMode = config.readOnly === undefined ? '' : config.readOnly ? 'READ ONLY' : 'READ WRITE';

    if (!workflowID) {
      throw new Error('Workflow ID is not set.');
    }
    if (!functionNum) {
      throw new Error('Function Number is not set.');
    }

    while (true) {
      const result = await this.#getResult(workflowID, functionNum);
      if (result) {
        return JSON.parse(result) as Return;
      }

      try {
        const output = await this.#db.begin<Return>(`${isolationLevel} ${accessMode}`, async (client) => {
          const output = await asyncLocalCtx.run({ client }, async () => {
            return await func.call(target, ...args);
          });

          if (config.readOnly) {
            try {
              await client/*sql*/ `
                            INSERT INTO dbos.transaction_outputs (workflow_id, function_num, output)
                            VALUES (${workflowID}, ${functionNum}, ${JSON.stringify(output)})`;
            } catch (error) {
              // 23505 is a duplicate key error
              if (error instanceof postgres.PostgresError && error.code === '23505') {
                // I dislike using try/catch for flow control, but we need to throw an error here
                // in order to trigger transacation rollback
                throw new TxOutputDuplicateKeyError('Duplicate key error');
              } else {
                throw error;
              }
            }
          }

          return output;
        });

        return output as Return;
      } catch (e) {
        if (e instanceof TxOutputDuplicateKeyError) {
          continue;
        } else {
          throw e;
        }
      }
    }
  }

  static async ensureDatabase(name: string, options: postgres.Options<{}> = {}): Promise<void> {
    const pg = postgres({ ...options, onnotice: () => {} });
    try {
      await ensureDB(pg, name);
    } finally {
      await pg.end();
    }
  }

  static async configure(options: postgres.Options<{}> = {}): Promise<void> {
    const pg = postgres({ ...options, onnotice: () => {} });
    try {
      await pg/*sql*/ `
                CREATE SCHEMA IF NOT EXISTS dbos;
                CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
                    workflow_id TEXT NOT NULL,
                    function_num INT NOT NULL,
                    output TEXT,
                    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
                    PRIMARY KEY (workflow_id, function_num));`.simple();
    } finally {
      await pg.end();
    }
  }
}

// helper functions to create/drop the database
async function checkDB(sql: Sql, name: string) {
  const results = await sql/*sql*/ `SELECT 1 FROM pg_database WHERE datname = ${name}`.values();
  return results.length > 0;
}

export async function ensureDB(sql: Sql, name: string) {
  const exists = await checkDB(sql, name);
  if (!exists) {
    await sql.unsafe(/*sql*/ `CREATE DATABASE ${name}`).simple();
  }
}

export async function dropDB(sql: Sql, name: string) {
  const exists = await checkDB(sql, name);
  if (exists) {
    await sql.unsafe(/*sql*/ `DROP DATABASE ${name}`).simple();
  }
}
