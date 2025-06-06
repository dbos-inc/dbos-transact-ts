import { PoolConfig } from 'pg';
import { DBOS, Error } from '@dbos-inc/dbos-sdk';
import {
  type DBOSDataSourceTransactionHandler,
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  isPGRetriableTransactionError,
  isPGKeyConflictError,
  isPGFailedSqlTransactionError,
  registerTransaction,
  runTransaction,
  PGIsolationLevel as IsolationLevel,
  PGTransactionConfig as TypeOrmTransactionConfig,
  DBOSDataSource,
  registerDataSource,
} from '@dbos-inc/dbos-sdk/datasource';
import { DataSource, EntityManager } from 'typeorm';
import { AsyncLocalStorage } from 'async_hooks';
import { SuperJSON } from 'superjson';

interface DBOSTypeOrmLocalCtx {
  typeOrmEntityManager: EntityManager;
}
const asyncLocalCtx = new AsyncLocalStorage<DBOSTypeOrmLocalCtx>();

function getCurrentDSContextStore(): DBOSTypeOrmLocalCtx | undefined {
  return asyncLocalCtx.getStore();
}

function assertCurrentDSContextStore(): DBOSTypeOrmLocalCtx {
  const ctx = getCurrentDSContextStore();
  if (!ctx) throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of TypeOrmDs outside of a `transaction`');
  return ctx;
}

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

export { IsolationLevel, TypeOrmTransactionConfig };

class TypeOrmDSP implements DBOSDataSourceTransactionHandler {
  readonly dsType = 'TypeOrm';
  dataSource: DataSource | undefined;

  constructor(
    readonly name: string,
    readonly config: PoolConfig,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    readonly entities: Function[],
  ) {}

  async createInstance(): Promise<DataSource> {
    const ds = new DataSource({
      type: 'postgres',
      url: this.config.connectionString,
      connectTimeoutMS: this.config.connectionTimeoutMillis,
      entities: this.entities,
      poolSize: this.config.max,
    });
    await ds.initialize();
    return ds;
  }

  async initialize(): Promise<void> {
    this.dataSource = await this.createInstance();

    return Promise.resolve();
  }

  async destroy(): Promise<void> {
    await this.dataSource?.destroy();
  }

  async checkExecution<R>(
    client: DataSource,
    workflowID: string,
    funcNum: number,
  ): Promise<
    | {
        res: R;
      }
    | undefined
  > {
    type TxOutputRow = Pick<transaction_completion, 'output'> & {
      recorded: boolean;
    };

    const { rows } = await client.query<{ rows: TxOutputRow[] }>(
      `SELECT output
          FROM dbos.transaction_completion
          WHERE workflow_id=$1 AND function_num=$2;`,
      [workflowID, funcNum],
    );

    if (rows.length !== 1) {
      return undefined;
    }

    if (rows[0].output === null) {
      return undefined;
    }

    return { res: SuperJSON.parse(rows[0].output) };
  }

  async #recordOutput<R>(client: DataSource, workflowID: string, funcNum: number, output: R): Promise<void> {
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

  async #recordError<R>(client: DataSource, workflowID: string, funcNum: number, error: R): Promise<void> {
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

  /* Required by base class */
  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: TypeOrmTransactionConfig,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const isolationLevel = config?.isolationLevel ?? 'SERIALIZABLE';

    const readOnly = config?.readOnly ? true : false;

    const wfid = DBOS.workflowID!;
    const funcnum = DBOS.stepID!;
    const funcname = func.name;

    // Retry loop if appropriate
    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.
    const shouldCheckOutput = false;

    if (this.dataSource === undefined) {
      throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of Datasource');
    }

    while (true) {
      let failedForRetriableReasons = false;

      try {
        const result = this.dataSource.transaction(isolationLevel, async (transactionEntityManager: EntityManager) => {
          if (this.dataSource === undefined) {
            throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of Datasource');
          }

          if (shouldCheckOutput && !readOnly) {
            const executionResult = await this.checkExecution<Return>(this.dataSource, wfid, funcnum);

            if (executionResult) {
              DBOS.span?.setAttribute('cached', true);
              return executionResult.res;
            }
          }

          const result = await asyncLocalCtx.run({ typeOrmEntityManager: transactionEntityManager }, async () => {
            return await func.call(target, ...args);
          });

          // Save result
          try {
            if (!readOnly) {
              await this.#recordOutput(this.dataSource, wfid, funcnum, result);
            }
          } catch (e) {
            const error = e as Error;
            await this.#recordError(this.dataSource, wfid, funcnum, error);

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
        });

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
}

export class TypeOrmDS implements DBOSDataSource<TypeOrmTransactionConfig> {
  #provider: TypeOrmDSP;
  constructor(
    readonly name: string,
    readonly config: PoolConfig,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    readonly entities: Function[],
  ) {
    this.#provider = new TypeOrmDSP(name, config, entities);
    registerDataSource(this.#provider);
  }

  // User calls this... DBOS not directly involved...
  static get entityManager(): EntityManager {
    const ctx = assertCurrentDSContextStore();
    if (!DBOS.isInTransaction())
      throw new Error.DBOSInvalidWorkflowTransitionError(
        'Invalid use of `TypeOrmDS.entityManager` outside of a `transaction`',
      );
    return ctx.typeOrmEntityManager;
  }

  async initializeInternalSchema(): Promise<void> {
    const ds = await this.#provider.createInstance();

    try {
      await ds.query(createTransactionCompletionSchemaPG);
      await ds.query(createTransactionCompletionTablePG);
    } catch (e) {
      const error = e as Error;
      throw new Error.DBOSError(`Unexpected error initializing schema: ${error.message}`);
    } finally {
      try {
        await ds.destroy();
      } catch (e) {}
    }
  }

  /**
   * Run `callback` as a transaction against this DataSource
   * @param callback Function to run within a transactional context
   * @param funcName Name to record for the transaction
   * @param config Transaction configuration (isolation, etc)
   * @returns Return value from `callback`
   */
  async runTransaction<T>(callback: () => Promise<T>, funcName: string, config?: TypeOrmTransactionConfig) {
    return await runTransaction(callback, funcName, { dsName: this.name, config });
  }

  /**
   * Register function as DBOS transaction, to be called within the context
   *  of a transaction on this data source.
   *
   * @param func Function to wrap
   * @param target Name of function
   * @param config Transaction settings
   * @returns Wrapped function, to be called instead of `func`
   */
  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: TypeOrmTransactionConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    return registerTransaction(this.name, func, { name }, config);
  }

  /**
   * Decorator establishing function as a transaction
   */
  transaction(config?: TypeOrmTransactionConfig) {
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
   * For testing: Use DataSource.syncronize to install the user schema
   */
  async createSchema() {
    const ds = await this.#provider.createInstance();
    try {
      await ds.synchronize();
    } finally {
      await ds.destroy();
    }
  }
}
