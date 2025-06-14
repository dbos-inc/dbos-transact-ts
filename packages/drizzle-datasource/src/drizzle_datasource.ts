import { Pool, PoolConfig } from 'pg';
import { DBOS, Error } from '@dbos-inc/dbos-sdk';
import {
  type DataSourceTransactionHandler,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  isPGFailedSqlTransactionError,
  registerTransaction,
  runTransaction,
  PGIsolationLevel as IsolationLevel,
  PGTransactionConfig as DrizzleTransactionConfig,
  DBOSDataSource,
  registerDataSource,
} from '@dbos-inc/dbos-sdk/datasource';
import { drizzle, NodePgDatabase } from 'drizzle-orm/node-postgres';
import { pushSchema } from 'drizzle-kit/api';
import { AsyncLocalStorage } from 'async_hooks';
import { SuperJSON } from 'superjson';

export { IsolationLevel, DrizzleTransactionConfig };

interface DrizzleLocalCtx {
  drizzleClient: NodePgDatabase<{ [key: string]: object }>;
}
const asyncLocalCtx = new AsyncLocalStorage<DrizzleLocalCtx>();

function getCurrentDSContextStore(): DrizzleLocalCtx | undefined {
  return asyncLocalCtx.getStore();
}

function assertCurrentDSContextStore(): DrizzleLocalCtx {
  const ctx = getCurrentDSContextStore();
  if (!ctx) throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of TypeOrmDs outside of a `transaction`');
  return ctx;
}

export interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

class DrizzleDSTH implements DataSourceTransactionHandler {
  readonly dsType = 'drizzle';
  dataSource: NodePgDatabase<{ [key: string]: object }> | undefined;
  drizzlePool: Pool | undefined;

  constructor(
    readonly name: string,
    readonly config: PoolConfig,
    readonly entities: { [key: string]: object } = {},
  ) {}

  async initialize(): Promise<void> {
    this.drizzlePool = new Pool(this.config);
    this.dataSource = drizzle(this.drizzlePool, { schema: this.entities });

    return Promise.resolve();
  }

  async destroy(): Promise<void> {
    await this.drizzlePool?.end();
  }

  async #checkExecution<R>(
    client: Pool,
    workflowID: string,
    funcNum: number,
  ): Promise<
    | {
        res: R;
      }
    | undefined
  > {
    const result = await client.query<{
      output: string;
    }>(
      `SELECT output
       FROM dbos.transaction_completion
        WHERE workflow_id = $1 AND function_num = $2`,
      [workflowID, funcNum],
    );

    if (result.rows.length !== 1) {
      return undefined;
    }

    return { res: SuperJSON.parse(result.rows[0].output) };
  }

  async #recordOutput<R>(client: Pool, workflowID: string, funcNum: number, output: R): Promise<void> {
    const serialOutput = SuperJSON.stringify(output);
    await client.query<{ rows: transaction_completion[] }>(
      `INSERT INTO dbos.transaction_completion (
        workflow_id, 
        function_num,
        output,
        created_at
      ) VALUES ($1, $2, $3, $4)`,
      [workflowID, funcNum, serialOutput, Date.now()],
    );
  }

  async #recordError<R>(client: Pool, workflowID: string, funcNum: number, error: R): Promise<void> {
    const serialError = SuperJSON.stringify(error);
    await client.query<{ rows: transaction_completion[] }>(
      `INSERT INTO dbos.transaction_completion (
        workflow_id, 
        function_num,
        error,
        created_at
      ) VALUES ($1, $2, $3, $4)`,
      [workflowID, funcNum, serialError, Date.now()],
    );
  }

  /* Invoke a transaction function, called by the framework */
  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: DrizzleTransactionConfig,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    let isolationLevel: 'read uncommitted' | 'read committed' | 'repeatable read' | 'serializable';

    if (config === undefined || config.isolationLevel === undefined) {
      isolationLevel = 'serializable'; // Default isolation level
    } else if (config.isolationLevel === IsolationLevel.ReadUncommitted) {
      isolationLevel = 'read uncommitted';
    } else if (config.isolationLevel === IsolationLevel.ReadCommitted) {
      isolationLevel = 'read committed';
    } else if (config.isolationLevel === IsolationLevel.RepeatableRead) {
      isolationLevel = 'repeatable read';
    } else {
      isolationLevel = 'serializable';
    }

    const accessMode = 'read write';

    const readOnly = config?.readOnly ? true : false;

    const wfid = DBOS.workflowID!;
    const funcnum = DBOS.stepID!;
    const funcname = func.name;

    // Retry loop if appropriate
    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.
    const shouldCheckOutput = false;

    if (this.drizzlePool === undefined) {
      throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of Datasource');
    }

    if (this.dataSource === undefined) {
      throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of Datasource');
    }

    while (true) {
      let failedForRetriableReasons = false;

      try {
        const result = await this.dataSource.transaction(
          async (drizzleClient: NodePgDatabase<{ [key: string]: object }>) => {
            if (this.drizzlePool === undefined) {
              throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of Datasource');
            }

            if (shouldCheckOutput && !readOnly && wfid) {
              const executionResult = await this.#checkExecution<Return>(this.drizzlePool, wfid, funcnum);

              if (executionResult) {
                DBOS.span?.setAttribute('cached', true);
                return executionResult.res;
              }
            }

            const result = await asyncLocalCtx.run({ drizzleClient }, async () => {
              return await func.call(target, ...args);
            });

            // Save result
            try {
              if (!readOnly && wfid) {
                await this.#recordOutput(this.drizzlePool, wfid, funcnum, result);
              }
            } catch (e) {
              const error = e as Error;
              await this.#recordError(this.drizzlePool, wfid, funcnum, error);

              // Aside from a connectivity error, two kinds of error are anticipated here:
              //  1. The transaction is marked failed, but the user code did not throw.
              //      Bad on them.  We will throw an error (this will get recorded) and not retry.
              //  2. There was a key conflict in the statement, and we need to use the fetched output
              if (isPGFailedSqlTransactionError(error)) {
                DBOS.logger.error(
                  `In workflow ${wfid}, Postgres aborted a transaction but the function '${funcname}' did not raise an exception.  Please ensure that the transaction method raises an exception if the database transaction is aborted.`,
                );
                failedForRetriableReasons = false;
                throw new Error.DBOSFailedSqlTransactionError(wfid, funcname);
              } else if (isPGKeyConflictError(error)) {
                throw new Error.DBOSWorkflowConflictError(
                  `In workflow ${wfid}, Postgres raised a key conflict error in transaction '${funcname}'.  This is not retriable, but the output will be fetched from the database.`,
                );
              } else {
                DBOS.logger.error(`Unexpected error raised in transaction '${funcname}: ${error}`);
                failedForRetriableReasons = false;
                throw error;
              }
            }

            return result;
          },
          { isolationLevel, accessMode },
        );

        return result;
      } catch (e) {
        const err = e as Error;
        if (failedForRetriableReasons || isPGRetriableTransactionError(err)) {
          DBOS.span?.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMillis }, performance.now());
          // Retry serialization failures.
          await DBOS.sleepms(retryWaitMillis);
          retryWaitMillis *= backoffFactor;
          retryWaitMillis = retryWaitMillis < maxRetryWaitMs ? retryWaitMillis : maxRetryWaitMs;
          continue;
        } else {
          throw err;
        }
      }
    }
  }

  createInstance(): NodePgDatabase<{ [key: string]: object }> {
    const drizzlePool = new Pool(this.config);
    const ds = drizzle(drizzlePool, { schema: this.entities });
    return ds;
  }
}

export class DrizzleDataSource implements DBOSDataSource<DrizzleTransactionConfig> {
  #provider: DrizzleDSTH;

  constructor(
    readonly name: string,
    readonly config: PoolConfig,
    readonly entities: { [key: string]: object } = {},
  ) {
    this.#provider = new DrizzleDSTH(name, config, entities);
    registerDataSource(this.#provider);
  }

  // User calls this... DBOS not directly involved...
  static get drizzleClient(): NodePgDatabase<{ [key: string]: object }> {
    const ctx = assertCurrentDSContextStore();
    if (!DBOS.isInTransaction())
      throw new Error.DBOSInvalidWorkflowTransitionError(
        'Invalid use of `DrizzleDataSource.drizzleClient` outside of a `transaction`',
      );
    return ctx.drizzleClient;
  }

  get dataSource() {
    return this.#provider.dataSource;
  }

  async initializeInternalSchema(): Promise<void> {
    const drizzlePool = new Pool(this.config);
    const ds = drizzle(drizzlePool, { schema: this.entities });

    try {
      await ds.execute(createTransactionCompletionSchemaPG);
      await ds.execute(createTransactionCompletionTablePG);
    } catch (e) {
      const error = e as Error;
      throw new Error.DBOSError(`Unexpected error initializing schema: ${error.message}`);
    } finally {
      try {
        await drizzlePool.end();
      } catch (e) {}
    }
  }

  async runTransaction<T>(callback: () => Promise<T>, funcName: string, config?: DrizzleTransactionConfig) {
    return await runTransaction(callback, funcName, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: DrizzleTransactionConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name }, config);
  }

  // decorator
  transaction(config?: DrizzleTransactionConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ds = this;
    return function decorator<This, Args extends unknown[], Return>(
      _target: object,
      propertyKey: string,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      if (!descriptor.value) {
        throw new Error.DBOSError('Use of decorator when original method is undefined');
      }

      descriptor.value = ds.registerTransaction(descriptor.value, propertyKey.toString(), config);

      return descriptor;
    };
  }

  /**
   * Create user schema in database (for testing)
   */
  async createSchema() {
    const drizzlePool = new Pool(this.config);
    const db = drizzle(drizzlePool);
    try {
      const res = await pushSchema(this.entities, db);
      await res.apply();
    } finally {
      await drizzlePool.end();
    }
  }
}
