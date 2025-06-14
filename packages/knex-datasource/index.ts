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
} from '@dbos-inc/dbos-sdk/datasource';
import { AsyncLocalStorage } from 'async_hooks';
import knex, { type Knex } from 'knex';
import { SuperJSON } from 'superjson';

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

interface KnexDataSourceContext {
  client: Knex.Transaction;
}

export type TransactionConfig = Pick<Knex.TransactionConfig, 'isolationLevel' | 'readOnly'>;

const asyncLocalCtx = new AsyncLocalStorage<KnexDataSourceContext>();

class KnexDSTH implements DataSourceTransactionHandler {
  readonly name: string;
  readonly dsType = 'KnexDataSource';
  readonly #knexDB: Knex;

  constructor(name: string, config: Knex.Config) {
    this.name = name;
    this.#knexDB = knex(config);
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  destroy(): Promise<void> {
    return this.#knexDB.destroy();
  }

  static async #checkExecution(
    client: Knex.Transaction,
    workflowID: string,
    functionNum: number,
  ): Promise<{ output: string | null } | undefined> {
    const result = await client<transaction_completion>('transaction_completion')
      .withSchema('dbos')
      .select('output')
      .where({
        workflow_id: workflowID,
        function_num: functionNum,
      })
      .first();
    return result === undefined ? undefined : { output: result.output };
  }

  static async #recordOutput(
    client: Knex.Transaction,
    workflowID: string,
    functionNum: number,
    output: string | null,
  ): Promise<void> {
    try {
      await client<transaction_completion>('transaction_completion').withSchema('dbos').insert({
        workflow_id: workflowID,
        function_num: functionNum,
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
        const result = await this.#knexDB.transaction<Return>(
          async (client) => {
            // Check to see if this tx has already been executed
            const previousResult =
              readOnly || !workflowID ? undefined : await KnexDSTH.#checkExecution(client, workflowID, functionNum);
            if (previousResult) {
              return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
            }

            // execute user's transaction function
            const result = await asyncLocalCtx.run({ client }, async () => {
              return (await func.call(target, ...args)) as Return;
            });

            // save the output of read/write transactions
            if (!readOnly && workflowID) {
              await KnexDSTH.#recordOutput(client, workflowID, functionNum, SuperJSON.stringify(result));

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
        if (isPGRetriableTransactionError(error)) {
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

export class KnexDataSource implements DBOSDataSource<TransactionConfig> {
  static get client(): Knex.Transaction {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of KnexDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('No async local context found.');
    }
    return ctx.client;
  }

  static async initializeInternalSchema(config: Knex.Config) {
    const knexDB = knex(config);
    try {
      await knexDB.schema.createSchemaIfNotExists('dbos');
      const exists = await knexDB.schema.withSchema('dbos').hasTable('transaction_completion');
      if (!exists) {
        await knexDB.schema.withSchema('dbos').createTable('transaction_completion', (table) => {
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
  #provider: KnexDSTH;

  constructor(name: string, config: Knex.Config) {
    this.name = name;
    this.#provider = new KnexDSTH(name, config);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(callback: () => Promise<T>, funcName: string, config?: TransactionConfig) {
    return await runTransaction(callback, funcName, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: TransactionConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name }, config);
  }

  transaction(config?: TransactionConfig) {
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
