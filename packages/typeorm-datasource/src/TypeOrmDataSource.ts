import { PoolConfig, DatabaseError as PGDatabaseError } from 'pg';
import { DBOS, type DBOSTransactionalDataSource, DBOSJSON, Error } from '@dbos-inc/dbos-sdk';
import { DataSource, EntityManager } from 'typeorm';
import { Type } from '@nestjs/common';
import { Data } from 'ws';
import { AsyncLocalStorage } from 'async_hooks';

interface DBOSTypeOrmLocalCtx {
  typeOrmEntityManager: EntityManager;
}
const asyncLocalCtx = new AsyncLocalStorage<DBOSTypeOrmLocalCtx>();

function getCurrentDSContextStore(): DBOSTypeOrmLocalCtx | undefined {
  return asyncLocalCtx.getStore();
}

function assertCurrentDSContextStore(): DBOSTypeOrmLocalCtx {
  const ctx = getCurrentDSContextStore();
  if (!ctx)
    throw new Error.DBOSInvalidWorkflowTransitionError(
      'Invalid use of `DBOSKnexDS.knexClient` outside of a `transaction`',
    );
  return ctx;
}

interface ExistenceCheck {
  exists: boolean;
}

export const schemaExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`;
export const txnOutputTableExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'transaction_outputs')`;
export const txnOutputIndexExistsQuery = `SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname='dbos' AND tablename = 'transaction_outputs' AND indexname = 'transaction_outputs_created_at_index')`;

export interface transaction_outputs {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS dbos.transaction_outputs (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num)
  );
`;

export const columnExistsQuery = `
  SELECT EXISTS (
    SELECT FROM information_schema.columns 
    WHERE table_schema = 'dbos' 
      AND table_name = 'transaction_outputs' 
      AND column_name = 'function_name'
  ) AS exists;
`;

export const addColumnQuery = `
  ALTER TABLE dbos.transaction_outputs 
    ADD COLUMN function_name TEXT NOT NULL DEFAULT '';
`;

export const userDBIndex = `
  CREATE INDEX IF NOT EXISTS transaction_outputs_created_at_index ON dbos.transaction_outputs (created_at);
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

export interface TypeOrmTransactionConfig {
  /** Isolation level to request from underlying app database */
  isolationLevel?: IsolationLevel;
  /** If set, request read-only transaction from underlying app database */
  readOnly?: boolean;
}

export class TypeOrmDS implements DBOSTransactionalDataSource {
  readonly dsType = 'TypeOrm';
  dataSource: DataSource | undefined;

  constructor(
    readonly name: string,
    readonly config: PoolConfig,
    readonly entities: Function[],
  ) {}

  // User calls this... DBOS not directly involved...
  static get entityManager(): EntityManager {
    const ctx = assertCurrentDSContextStore();
    if (!DBOS.isInTransaction())
      throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of `DBOS.sqlClient` outside of a `transaction`');
    return ctx.typeOrmEntityManager;
  }

  async initialize(): Promise<void> {
    this.dataSource = await this.createInstance();

    return Promise.resolve();
  }

  async InitializeSchema(): Promise<void> {
    const ds = await this.createInstance();

    console.log('created datasource');

    try {
      const schemaExists = await ds.query<{ rows: ExistenceCheck[] }>(schemaExistsQuery);
      console.log('checking schema exists', schemaExists);
      if (!schemaExists) {
        await ds.query(createUserDBSchema);
      }
      console.log('created schema');
      const txnOutputTableExists = await ds.query<{ rows: ExistenceCheck[] }>(txnOutputTableExistsQuery);
      console.log('checking txn output table exists', txnOutputTableExists);
      if (!txnOutputTableExists) {
        await ds.query(userDBSchema);
      } else {
        const columnExists = await ds.query<{ rows: ExistenceCheck[] }>(columnExistsQuery);
        console.log('checking column exists', columnExists);
        if (!columnExists) {
          await ds.query(addColumnQuery);
        }
      }
      console.log('created table');

      const txnOutputIndexExists = await ds.query<{ rows: ExistenceCheck[] }>(txnOutputIndexExistsQuery);
      if (!txnOutputIndexExists) {
        await ds.query(userDBIndex);
      }
      console.log('created index');
    } catch (e) {
      console.error(`Unexpected error initializing schema: ${e}`);
      throw new Error.DBOSError(`Unexpected error initializing schema: ${e}`);
    } finally {
      try {
        await ds.destroy();
      } catch (e) {}
    }

    return Promise.resolve();
  }

  /**
   * Will be called by DBOS during attempt at clean shutdown (generally in testing scenarios).
   */
  async destroy(): Promise<void> {
    await this.dataSource?.destroy();
    return Promise.resolve();
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
    type TxOutputRow = Pick<transaction_outputs, 'output'> & {
      recorded: boolean;
    };

    const { rows } = await client.query<{ rows: TxOutputRow[] }>(
      `SELECT output
          FROM dbos.knex_transaction_outputs
          WHERE workflow_id=? AND function_num=?;`,
      [workflowID, funcNum],
    );

    if (rows.length !== 1) {
      return undefined;
    }
    return { res: DBOSJSON.parse(rows[1].output) as R };
    // return rows[1].output as any;
  }

  async recordOutput<R>(client: DataSource, workflowID: string, funcNum: number, output: R): Promise<void> {
    const serialOutput = DBOSJSON.stringify(output);
    await client.query<{ rows: transaction_outputs[] }>(
      `INSERT INTO dbos.transaction_outputs (
        workflow_id, function_num,
        output,
        created_at
      ) VALUES (?, ?, ?, ?)`,
      [workflowID, funcNum, serialOutput, Date.now()],
    );
  }

  /**
   * Invoke a transaction function
   */
  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: TypeOrmTransactionConfig,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const isolationLevel = config.isolationLevel ? config.isolationLevel : 'SERIALIZABLE';

    const readOnly = config?.readOnly ? true : false;

    const wfid = DBOS.workflowID!;
    const funcnum = DBOS.stepID!;
    const funcname = func.name;

    // Retry loop if appropriate
    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.
    let shouldCheckOutput = false;

    if (this.dataSource === undefined) {
      throw new Error.DBOSInvalidWorkflowTransitionError('Invalid use of Datasource');
    }

    while (true) {
      let failedForRetriableReasons = false;

      if (shouldCheckOutput && !readOnly) {
        const executionResult = await this.checkExecution<Return>(this.dataSource, wfid, funcnum);

        if (executionResult) {
          DBOS.span?.setAttribute('cached', true);
          return executionResult.res;
        }
      }

      try {
        const result = this.dataSource?.transaction(isolationLevel, async (transactionEntityManager: EntityManager) => {
          const result = await asyncLocalCtx.run({ typeOrmEntityManager: transactionEntityManager }, async () => {
            return await func.call(target, ...args);
          });
          return result;
        });

        // Save result
        try {
          if (!readOnly) {
            await this.recordOutput(this.dataSource, wfid, funcnum, result);
          }
        } catch (e) {
          const error = e as Error;

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
            // Expected.  There is probably a result to return
            shouldCheckOutput = true;
            failedForRetriableReasons = true;
          } else {
            DBOS.logger.error(`Unexpected error raised in transaction '${funcname}: ${error}`);
            failedForRetriableReasons = false;
            throw error;
          }
        }

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

  async createInstance(): Promise<DataSource> {
    console.log(`Creating TypeORM DataSource for ${this.name} with config:`, this.config);
    console.log(`Entities:`, this.entities);

    let ds = new DataSource({
      type: 'postgres',
      host: this.config.host,
      port: this.config.port,
      username: this.config.user,
      password: 'postgres',
      connectTimeoutMS: this.config.connectionTimeoutMillis,
      entities: this.entities,
      poolSize: this.config.max,
    });
    await ds.initialize();
    console.log('TypeORM DataSource initialized:', ds.isInitialized);
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

  async runTransactionStep<T>(callback: () => Promise<T>, funcName: string, config?: TypeOrmTransactionConfig) {
    return await DBOS.runAsWorkflowTransaction(callback, funcName, { dsName: this.name, config });
  }

  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    target: {
      name: string;
    },
    config?: TypeOrmTransactionConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    return DBOS.registerTransaction(this.name, func, target, config);
  }

  // decorator
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

      descriptor.value = ds.registerTransaction(descriptor.value, { name: propertyKey.toString() }, config);

      return descriptor;
    };
  }
}
