// using https://github.com/brianc/node-postgres

import { DBOS, type DBOSTransactionalDataSource } from '@dbos-inc/dbos-sdk';
import {
  Client,
  type ClientBase,
  type ClientConfig,
  DatabaseError,
  Pool,
  type PoolConfig,
  type QueryResultRow,
} from 'pg';
import { AsyncLocalStorage } from 'node:async_hooks';
import { NoticeMessage } from 'pg-protocol/dist/messages.js';

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
  storedProc?: string;
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
    options: { dsName?: string; config?: Omit<NodePostgresTransactionOptions, 'storedProc'> } = {},
  ) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, options);
  }

  async runTxStep<T>(
    callback: () => Promise<T>,
    funcName: string,
    config?: Omit<NodePostgresTransactionOptions, 'storedProc'>,
  ) {
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
    config: NodePostgresTransactionOptions,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID;
    const functionNum = DBOS.stepID;

    if (!workflowID) {
      throw new Error('Workflow ID is not set.');
    }
    if (!functionNum) {
      throw new Error('Function Number is not set.');
    }

    if (config.storedProc !== undefined) {
      return this.#runRemote<Return>(config.storedProc, args, workflowID, functionNum);
    } else {
      return this.#runLocal(() => func.call(target, ...args), workflowID, functionNum, config);
    }
  }

  async #runLocal<Return>(
    func: () => Promise<Return>,
    workflowID: string,
    functionNum: number,
    config: NodePostgresTransactionOptions,
  ): Promise<Return> {
    const isolationLevel = config.isolationLevel ? `ISOLATION LEVEL ${config.isolationLevel}` : '';
    const accessMode = config.readOnly === undefined ? '' : config.readOnly ? 'READ ONLY' : 'READ WRITE';

    while (true) {
      const { rows } = await this.#pool.query<{ output: string }> /*sql*/(
        `SELECT output FROM dbos.transaction_outputs
                 WHERE workflow_id = $1 AND function_num = $2`,
        [workflowID, functionNum],
      );
      if (rows.length > 0) {
        return JSON.parse(rows[0].output) as Return;
      }

      const client = await this.#pool.connect();
      try {
        await client.query(/*sql*/ `BEGIN ${isolationLevel} ${accessMode}`);

        const output = await asyncLocalCtx.run({ client }, func);

        if (!config.readOnly) {
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

  async #runRemote<Return>(name: string, args: unknown[], workflowID: string, functionNum: number): Promise<Return> {
    const context = {};
    const $args = [workflowID, functionNum, JSON.stringify(args), context, null];
    const sql = `CALL "${name}_p"(${$args.map((_v, i) => `$${i + 1}`).join()});`;
    const client = await this.#pool.connect();
    try {
      client.on('notice', logNotice);

      type QueryResult = { return_value: { output?: Return; error?: Error } };
      const [{ return_value }] = await client.query<QueryResult>(sql, $args).then((value) => value.rows);
      const { output, error } = return_value;
      if (error) {
        throw new Error(error.message, { cause: error.cause });
      } else {
        return output!;
      }
    } finally {
      client.off('notice', logNotice);
      client.release();
    }
  }

  static async ensureDatabase(name: string, config: ClientConfig): Promise<void> {
    const client = new Client(config);
    try {
      await client.connect();
      await ensureDB(client, name);
    } finally {
      await client.end();
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

function logNotice(msg: NoticeMessage) {
  switch (msg.severity) {
    case 'INFO':
    case 'LOG':
    case 'NOTICE':
      DBOS.logger.info(msg.message);
      break;
    case 'WARNING':
      DBOS.logger.warn(msg.message);
      break;
    case 'DEBUG':
      DBOS.logger.debug(msg.message);
      break;
    case 'ERROR':
    case 'FATAL':
    case 'PANIC':
      DBOS.logger.error(msg.message);
      break;
    default:
      DBOS.logger.error(`Unknown notice severity: ${msg.severity} - ${msg.message}`);
  }
}

// helper functions to create/drop the database
async function checkDB(sql: ClientBase, name: string) {
  const results = await sql.query(/*sql*/ `SELECT 1 FROM pg_database WHERE datname = $1`, [name]);
  return results.rows.length > 0;
}

export async function ensureDB(sql: ClientBase, name: string) {
  const exists = await checkDB(sql, name);
  if (!exists) {
    await sql.query(/*sql*/ `CREATE DATABASE ${name}`);
  }
}

export async function dropDB(sql: ClientBase, name: string) {
  const exists = await checkDB(sql, name);
  if (exists) {
    await sql.query(/*sql*/ `DROP DATABASE ${name}`);
  }
}
