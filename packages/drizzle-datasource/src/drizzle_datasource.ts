import { Pool, PoolConfig, DatabaseError as PGDatabaseError } from 'pg';
import { DBOS, type DBOSTransactionalDataSource, Error } from '@dbos-inc/dbos-sdk';
import { drizzle, NodePgDatabase } from 'drizzle-orm/node-postgres';
import { AsyncLocalStorage } from 'async_hooks';
import { SuperJSON } from 'superjson';

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

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS dbos.transaction_completion (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    error TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num)
  );
`;

export const userDBIndex = `
  CREATE INDEX IF NOT EXISTS transaction_completion_created_at_index ON dbos.transaction_completion (created_at);
`;

/** Isolation typically supported by application databases */
export const IsolationLevel = {
  ReadUncommitted: 'READ UNCOMMITTED',
  ReadCommitted: 'READ COMMITTED',
  RepeatableRead: 'REPEATABLE READ',
  Serializable: 'SERIALIZABLE',
} as const;

type ValuesOf<T> = T[keyof T];
type IsolationLevel = ValuesOf<typeof IsolationLevel>;

export interface DrizzleTransactionConfig {
  /** Isolation level to request from underlying app database */
  isolationLevel?: IsolationLevel;
  /** If set, request read-only transaction from underlying app database */
  readOnly?: boolean;
}

export class DrizzleDS implements DBOSTransactionalDataSource {
  readonly dsType = 'drizzle';
  dataSource: NodePgDatabase<{ [key: string]: object }> | undefined;
  drizzlePool: Pool | undefined;

  constructor(
    readonly name: string,
    readonly config: PoolConfig,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    readonly entities: { [key: string]: object } = {},
  ) {}

  // User calls this... DBOS not directly involved...
  static get drizzleClient(): NodePgDatabase<{ [key: string]: object }> {
    const ctx = assertCurrentDSContextStore();
    if (!DBOS.isInTransaction())
      throw new Error.DBOSInvalidWorkflowTransitionError(
        'Invalid use of `DrizzleDS.drizzleClient` outside of a `transaction`',
      );
    return ctx.drizzleClient;
  }

  async initialize(): Promise<void> {
    // this.dataSource = this.createInstance();

    this.drizzlePool = new Pool(this.config);
    this.dataSource = drizzle(this.drizzlePool, { schema: this.entities });

    return Promise.resolve();
  }

  async initializeInternalSchema(): Promise<void> {
    const drizzlePool = new Pool(this.config);
    const ds = drizzle(drizzlePool, { schema: this.entities });

    try {
      await ds.execute(createUserDBSchema);
      await ds.execute(userDBSchema);
      await ds.execute(userDBIndex);
    } catch (e) {
      const error = e as Error;
      throw new Error.DBOSError(`Unexpected error initializing schema: ${error.message}`);
    } finally {
      try {
        await drizzlePool.end();
      } catch (e) {}
    }
  }

  /**
   * Will be called by DBOS during attempt at clean shutdown (generally in testing scenarios).
   */
  async destroy(): Promise<void> {
    await this.drizzlePool?.end();
  }

  async checkExecution<R>(
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

  /**
   * Invoke a transaction function
   */
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

            if (shouldCheckOutput && !readOnly) {
              const executionResult = await this.checkExecution<Return>(this.drizzlePool, wfid, funcnum);

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
              if (!readOnly) {
                await this.#recordOutput(this.drizzlePool, wfid, funcnum, result);
              }
            } catch (e) {
              const error = e as Error;
              await this.#recordError(this.drizzlePool, wfid, funcnum, error);

              // Aside from a connectivity error, two kinds of error are anticipated here:
              //  1. The transaction is marked failed, but the user code did not throw.
              //      Bad on them.  We will throw an error (this will get recorded) and not retry.
              //  2. There was a key conflict in the statement, and we need to use the fetched output
              if (this.isFailedSqlTransactionError(error)) {
                DBOS.logger.error(
                  `In workflow ${wfid}, Postgres aborted a transaction but the function '${funcname}' did not raise an exception.  Please ensure that the transaction method raises an exception if the database transaction is aborted.`,
                );
                failedForRetriableReasons = false;
                throw new Error.DBOSFailedSqlTransactionError(wfid, funcname);
              } else if (this.isKeyConflictError(error)) {
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
        if (failedForRetriableReasons || this.isRetriableTransactionError(err)) {
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

  getPostgresErrorCode(error: unknown): string | null {
    const dbErr: PGDatabaseError = error as PGDatabaseError;
    return dbErr.code ? dbErr.code : null;
  }

  isRetriableTransactionError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === '40001';
  }

  isKeyConflictError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === '23505';
  }

  isFailedSqlTransactionError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === '25P02';
  }

  async runTransaction<T>(callback: () => Promise<T>, funcName: string, config?: DrizzleTransactionConfig) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    target: {
      name: string;
    },
    config?: DrizzleTransactionConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    return DBOS.registerTransaction(this.name, func, target, config);
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

      descriptor.value = ds.registerTransaction(descriptor.value, { name: propertyKey.toString() }, config);

      return descriptor;
    };
  }

  async createSchema() {}
}
