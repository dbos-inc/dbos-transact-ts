// using https://github.com/brianc/node-postgres

import { DBOS, DBOSWorkflowConflictError } from '@dbos-inc/dbos-sdk';
import {
  type DataSourceTransactionHandler,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  registerTransaction,
  runTransaction,
  PGTransactionConfig as NodePostgresTransactionOptions,
  DBOSDataSource,
  registerDataSource,
} from '@dbos-inc/dbos-sdk/datasource';
import { Client, type ClientBase, type ClientConfig, Pool, type PoolConfig } from 'pg';
import { AsyncLocalStorage } from 'node:async_hooks';
import { SuperJSON } from 'superjson';

interface NodePostgresDataSourceContext {
  client: ClientBase;
}

export { NodePostgresTransactionOptions };

const asyncLocalCtx = new AsyncLocalStorage<NodePostgresDataSourceContext>();

class NodePGDSTH implements DataSourceTransactionHandler {
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

  static async #checkExecution(
    client: ClientBase,
    workflowID: string,
    functionNum: number,
  ): Promise<{ output: string | null } | undefined> {
    const { rows } = await client.query<{ output: string }>(
      /*sql*/ `SELECT output FROM dbos.transaction_completion
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
        `INSERT INTO dbos.transaction_completion (workflow_id, function_num, output) 
         VALUES ($1, $2, $3)`,
        [workflowID, functionNum, output],
      );
    } catch (error) {
      if (isPGKeyConflictError(error)) {
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
              : await NodePGDSTH.#checkExecution(client, workflowID, functionNum);
            if (previousResult) {
              return (previousResult.output ? SuperJSON.parse(previousResult.output) : null) as Return;
            }

            // execute user's transaction function
            const result = await asyncLocalCtx.run({ client }, async () => {
              return (await func.call(target, ...args)) as Return;
            });

            // save the output of read/write transactions
            if (!readOnly) {
              await NodePGDSTH.#recordOutput(client, workflowID, functionNum, SuperJSON.stringify(result));

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

export class NodePostgresDataSource implements DBOSDataSource<NodePostgresTransactionOptions> {
  static get client(): ClientBase {
    if (!DBOS.isInTransaction()) {
      throw new Error('invalid use of NodePostgresDataSource.client outside of a DBOS transaction.');
    }
    const ctx = asyncLocalCtx.getStore();
    if (!ctx) {
      throw new Error('No async local context found.');
    }
    return ctx.client;
  }

  static async initializeInternalSchema(config: ClientConfig): Promise<void> {
    const client = new Client(config);
    try {
      await client.connect();
      await client.query(createTransactionCompletionSchemaPG);
      await client.query(createTransactionCompletionTablePG);
    } finally {
      await client.end();
    }
  }

  readonly name: string;
  #provider: NodePGDSTH;

  constructor(name: string, config: PoolConfig) {
    this.name = name;
    this.#provider = new NodePGDSTH(name, config);
    registerDataSource(this.#provider);
  }

  async runTransaction<T>(callback: () => Promise<T>, funcName: string, config?: NodePostgresTransactionOptions) {
    return await runTransaction(callback, funcName, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: NodePostgresTransactionOptions,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name }, config);
  }

  transaction(config?: NodePostgresTransactionOptions) {
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
