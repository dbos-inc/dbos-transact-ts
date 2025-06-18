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

class NodePostgresTransactionHandler implements DataSourceTransactionHandler {
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

  async #checkExecution(
    workflowID: string,
    functionNum: number,
  ): Promise<{ output: string | null } | { error: string } | undefined> {
    type Result = { output: string | null; error: string | null };
    const { rows } = await this.#pool.query<Result>(
      /*sql*/
      `SELECT output, error FROM dbos.transaction_completion
       WHERE workflow_id = $1 AND function_num = $2`,
      [workflowID, functionNum],
    );
    if (rows.length === 0) {
      return undefined;
    }
    const { output, error } = rows[0];
    return error !== null ? { error } : { output };
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

  async #recordError(workflowID: string, functionNum: number, error: string): Promise<void> {
    try {
      await this.#pool.query(
        /*sql*/
        `INSERT INTO dbos.transaction_completion (workflow_id, function_num, error) 
         VALUES ($1, $2, $3)`,
        [workflowID, functionNum, error],
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
        const result = await this.#transaction<Return>(
          async (client) => {
            // execute user's transaction function
            const result = await asyncLocalCtx.run({ client }, async () => {
              return (await func.call(target, ...args)) as Return;
            });

            // save the output of read/write transactions
            if (saveResults) {
              await NodePostgresTransactionHandler.#recordOutput(
                client,
                workflowID,
                functionNum,
                SuperJSON.stringify(result),
              );
            }

            return result;
          },
          { isolationLevel: config?.isolationLevel, readOnly: config?.readOnly },
        );

        return result;
      } catch (error) {
        if (isPGRetriableTransactionError(error)) {
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

  #provider: NodePostgresTransactionHandler;

  constructor(
    readonly name: string,
    config: PoolConfig,
  ) {
    this.#provider = new NodePostgresTransactionHandler(name, config);
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
