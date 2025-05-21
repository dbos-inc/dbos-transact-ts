import { PoolConfig, DatabaseError as PGDatabaseError } from 'pg';
import { DBOS, type DBOSTransactionalDataSource } from '@dbos-inc/dbos-sdk';
import { DataSource } from 'typeorm';

interface ExistenceCheck {
  exists: boolean;
}

export const schemaExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`;
export const txnOutputTableExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'knex_transaction_outputs')`;
export const txnOutputIndexExistsQuery = `SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname='dbos' AND tablename = 'transaction_outputs' AND indexname = 'transaction_outputs_created_at_index')`;

export interface transaction_outputs {
  workflow_id: string;
  function_num: number;
  output: string | null;
}

export const createUserDBSchema = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const userDBSchema = `
  CREATE TABLE IF NOT EXISTS dbos.knex_transaction_outputs (
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

  async initialize(): Promise<void> {
    this.dataSource = this.createInstance();

    return Promise.resolve();
  }

  async InitializeSchema(): Promise<void> {
    const ds = this.createInstance();

    try {
      const schemaExists = await ds.query<{ rows: ExistenceCheck[] }>(schemaExistsQuery);
      if (!schemaExists.rows[0].exists) {
        await ds.query(createUserDBSchema);
      }
      const txnOutputTableExists = await ds.query<{ rows: ExistenceCheck[] }>(txnOutputTableExistsQuery);
      if (!txnOutputTableExists.rows[0].exists) {
        await ds.query(userDBSchema);
      } else {
        const columnExists = await ds.query<{ rows: ExistenceCheck[] }>(columnExistsQuery);
        if (!columnExists.rows[0].exists) {
          await ds.query(addColumnQuery);
        }
      }

      const txnOutputIndexExists = await ds.query<{ rows: ExistenceCheck[] }>(txnOutputIndexExistsQuery);
      if (!txnOutputIndexExists.rows[0].exists) {
        await ds.query(userDBIndex);
      }
    } catch (e) {
      console.error(`Unexpected error initializing schema: ${e}`);
      throw new Error(`Unexpected error initializing schema: ${e}`);
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

  /**
   * Invoke a transaction function
   */
  invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: TypeOrmTransactionConfig,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;

    while (true) {}

    return 'foo' as any; // TODO: Implement this method;
  }

  createInstance(): DataSource {
    return new DataSource({
      type: 'postgres',
      url: this.config.connectionString,
      connectTimeoutMS: this.config.connectionTimeoutMillis,
      entities: this.entities,
      poolSize: this.config.max,
    });
  }
}
