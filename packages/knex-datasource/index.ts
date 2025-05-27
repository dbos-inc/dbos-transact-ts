// using https://github.com/knex/knex

import { DBOS, DBOSTransactionalDataSource } from '@dbos-inc/dbos-sdk';
import { AsyncLocalStorage } from 'async_hooks';
import knex, { Knex } from 'knex';

interface transaction_outputs {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

interface KnexDataSourceContext {
  client: Knex.Transaction;
}

class KeyConflictError extends Error {}

export class KnexDataSource implements DBOSTransactionalDataSource {
  static readonly #asyncLocalCtx = new AsyncLocalStorage<KnexDataSourceContext>();

  static async runTxStep<T>(
    callback: () => Promise<T>,
    funcName: string,
    options: { dsName?: string; config?: Knex.TransactionConfig } = {},
  ) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, options);
  }

  static get client(): Knex.Transaction {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of PostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = KnexDataSource.#asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('No async local context found.');
    }
    return ctx.client;
  }

  static async configure(config: Knex.Config) {
    const schemaSupport = config.client === 'pg';
    const tableName = schemaSupport ? 'transaction_outputs' : 'dbos_transaction_outputs';
    const knexDB = knex(config);
    try {
      if (schemaSupport) {
        await knexDB.schema.createSchemaIfNotExists('dbos');
      }
      const schemaBuilder = schemaSupport ? knexDB.schema.withSchema('dbos') : knexDB.schema;
      if (!(await schemaBuilder.hasTable(tableName))) {
        await schemaBuilder.createTable(tableName, (table) => {
          table.string('workflow_id').notNullable();
          table.integer('function_num').notNullable();
          table.string('output').nullable();
          table.primary(['workflow_id', 'function_num']);
        });
      }
    } finally {
      await knexDB.destroy();
    }
  }

  readonly name: string;
  readonly dsType = 'KnexDataSource';
  readonly #knexDB: Knex;
  readonly #schemaSupport: boolean;

  constructor(name: string, config: Knex.Config) {
    this.name = name;
    this.#knexDB = knex(config);
    this.#schemaSupport = config.client === 'pg';
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return this.#knexDB.destroy();
  }

  async runTxStep<T>(callback: () => Promise<T>, funcName: string, config?: Knex.TransactionConfig) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, { dsName: this.name, config });
  }

  register<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: Knex.TransactionConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    return DBOS.registerTransaction(this.name, func, { name }, config);
  }

  async #getResult(workflowID: string, functionNum: number): Promise<string | undefined> {
    const txOutputs = this.#schemaSupport
      ? this.#knexDB<transaction_outputs>('transaction_outputs').withSchema('dbos')
      : this.#knexDB<transaction_outputs>('dbos_transaction_outputs');

    const result = await txOutputs
      .select('output')
      .where({
        workflow_id: workflowID,
        function_num: functionNum,
      })
      .first();
    return result?.output ?? undefined;
  }

  async #saveResult(client: Knex.Transaction, workflowID: string, functionNum: number, output: string): Promise<void> {
    try {
      const txOutputs = this.#schemaSupport
        ? client<transaction_outputs>('transaction_outputs').withSchema('dbos')
        : client<transaction_outputs>('dbos_transaction_outputs');

      await txOutputs.insert({
        workflow_id: workflowID,
        function_num: functionNum,
        output,
      });
    } catch (error) {
      if (error && typeof error === 'object' && 'code' in error && error.code === '23505') {
        throw new KeyConflictError('Key conflict error');
      } else {
        throw error;
      }
    }
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: Knex.TransactionConfig | undefined,
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

    while (true) {
      if (!readOnly) {
        const result = await this.#getResult(workflowID, functionNum);
        // TODO: DBOSJSON
        if (result) {
          return JSON.parse(result) as Return;
        }
      }

      try {
        return await this.#knexDB.transaction<Return>(
          async (client) => {
            const output = await KnexDataSource.#asyncLocalCtx.run({ client }, async () => {
              return (await func.call(target, ...args)) as Return;
            });

            if (!readOnly) {
              // TODO: DBOSJSON
              await this.#saveResult(client, workflowID, functionNum, JSON.stringify(output));
            }
            return output;
          },
          { isolationLevel: config?.isolationLevel, readOnly: config?.readOnly },
        );
      } catch (e) {
        if (e instanceof KeyConflictError) {
          continue;
        } else {
          throw e;
        }
      }
    }
  }
}
