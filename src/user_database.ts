/* eslint-disable @typescript-eslint/no-explicit-any */
import { Pool, PoolConfig, PoolClient, DatabaseError as PGDatabaseError, QueryResultRow, Client } from 'pg';
import {
  createUserDBSchema,
  userDBIndex,
  userDBSchema,
  columnExistsQuery,
  addColumnQuery,
} from '../schemas/user_db_schema';
import { IsolationLevel, TransactionConfig } from './transaction';
import { ValuesOf } from './utils';
import { Knex } from 'knex';
import { GlobalLogger as Logger } from './telemetry/logs';
import { MikroORM, EntityManager } from '@mikro-orm/core';

export async function createDBIfDoesNotExist(poolConfig: PoolConfig, logger: Logger) {
  console.log('Creating database if it does not exist ');
  const pgUserClient = new Client(poolConfig);
  try {
    await pgUserClient.connect(); // Try to establish a connection
    await pgUserClient.end();
    return; // If successful, return
  } catch (error) {
    logger.info(`Database ${poolConfig.database} does not exist, creating...`);
  }
  // Craft a db string from the app db string, replacing the database name:
  const pgDbConnectionString = new URL(poolConfig.connectionString!);
  const app_database = pgDbConnectionString.pathname.substring(1);
  console;
  pgDbConnectionString.pathname = '/postgres';

  const postgresConfig = {
    ...poolConfig,
    connectionString: pgDbConnectionString.toString(),
  };
  const postgresClient = new Client(postgresConfig);
  let connection_failed = true;
  try {
    await postgresClient.connect();
    connection_failed = false;
    await postgresClient.query(`CREATE DATABASE ${app_database}`);
  } catch (e) {
    if (e instanceof Error) {
      if (connection_failed) {
        logger.error(
          `Error connecting to database ${postgresConfig.host}:${postgresConfig.port} with user ${postgresConfig.user}: ${e.message}`,
        );
      } else {
        logger.error(`Error creating database ${app_database}: ${e.message}`);
      }
    } else {
      logger.error(e);
    }
    throw e;
  } finally {
    await postgresClient.end();
  }
}

export interface UserDatabase {
  init(debugMode?: boolean): Promise<void>;
  destroy(): Promise<void>;
  getName(): UserDatabaseName;

  // Run transactionFunction as a database transaction with a given config and arguments.
  transaction<R, T extends unknown[]>(
    transactionFunction: UserDatabaseTransaction<R, T>,
    config: TransactionConfig,
    ...args: T
  ): Promise<R>;
  // Execute a query function
  queryFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    queryFunction: UserDatabaseQuery<C, R, T>,
    ...params: T
  ): Promise<R>;
  // Execute a raw SQL query.
  query<R, T extends unknown[]>(sql: string, ...params: T): Promise<R[]>;
  // Execute a raw SQL query in the session/transaction of a particular client.
  queryWithClient<R, T extends unknown[] = unknown[]>(
    client: UserDatabaseClient,
    sql: string,
    ...params: T
  ): Promise<R[]>;

  // Is a database error retriable?  Currently only serialization errors are retriable.
  isRetriableTransactionError(error: unknown): boolean;
  // Is a database error caused by a key conflict (key constraint violation or serialization error)?
  isKeyConflictError(error: unknown): boolean;
  // Is the database error caused by a failed or aported transaciton?
  isFailedSqlTransactionError(error: unknown): boolean;

  // Not all databases support this, TypeORM can.
  // Drop the user database tables (for testing)
  createSchema(): Promise<void>;
  // Drop the user database tables (for testing)
  dropSchema(): Promise<void>;
}

type UserDatabaseQuery<C extends UserDatabaseClient, R, T extends unknown[]> = (ctxt: C, ...args: T) => Promise<R>;
type UserDatabaseTransaction<R, T extends unknown[]> = (ctxt: UserDatabaseClient, ...args: T) => Promise<R>;

export type UserDatabaseClient =
  | PoolClient
  | PrismaClient
  | TypeORMEntityManager
  | Knex
  | DrizzleClient
  | EntityManager;

export const UserDatabaseName = {
  PGNODE: 'pg-node',
  PRISMA: 'prisma',
  TYPEORM: 'typeorm',
  KNEX: 'knex',
  DRIZZLE: 'drizzle',
  MIKROORM: 'mikroorm',
} as const;
export type UserDatabaseName = ValuesOf<typeof UserDatabaseName>;

export const schemaExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`;
export const txnOutputTableExistsQuery = `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'transaction_outputs')`;
export const txnOutputIndexExistsQuery = `SELECT EXISTS (SELECT FROM pg_indexes WHERE schemaname='dbos' AND tablename = 'transaction_outputs' AND indexname = 'transaction_outputs_created_at_index')`;

interface ExistenceCheck {
  exists: boolean;
}

export function pgNodeIsKeyConflictError(error: unknown): boolean {
  if (!(error instanceof PGDatabaseError)) {
    return false;
  }
  const pge = getPostgresErrorCode(error);
  return pge === '23505';

  function getPostgresErrorCode(error: unknown): string | null {
    const dbErr: PGDatabaseError = error as PGDatabaseError;
    return dbErr.code ? dbErr.code : null;
  }
}

/**
 * node-postgres user data access interface
 */
export class PGNodeUserDatabase implements UserDatabase {
  readonly pool: Pool;

  constructor(readonly poolConfig: PoolConfig) {
    this.pool = new Pool(poolConfig);
  }

  async init(debugMode: boolean = false): Promise<void> {
    if (!debugMode) {
      const schemaExists = await this.pool.query<ExistenceCheck>(schemaExistsQuery);
      if (!schemaExists.rows[0].exists) {
        await this.pool.query(createUserDBSchema);
      }
      const txnOutputTableExists = await this.pool.query<ExistenceCheck>(txnOutputTableExistsQuery);
      if (!txnOutputTableExists.rows[0].exists) {
        await this.pool.query(userDBSchema);
      } else {
        const columnExists = await this.pool.query<ExistenceCheck>(columnExistsQuery);
        if (!columnExists.rows[0].exists) {
          await this.pool.query(addColumnQuery);
        }
      }
      const txnIndexExists = await this.pool.query<ExistenceCheck>(txnOutputIndexExistsQuery);
      if (!txnIndexExists.rows[0].exists) {
        await this.pool.query(userDBIndex);
      }
    }
  }

  async destroy(): Promise<void> {
    await this.pool.end();
  }

  getName() {
    return UserDatabaseName.PGNODE;
  }

  async transaction<R, T extends unknown[]>(
    txn: UserDatabaseTransaction<R, T>,
    config: TransactionConfig,
    ...args: T
  ): Promise<R> {
    const client: PoolClient = await this.pool.connect();
    try {
      const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;
      await client.query(`BEGIN ISOLATION LEVEL ${isolationLevel}`);
      const result: R = await txn(client, ...args);
      await client.query(`COMMIT`);
      return result;
    } catch (err) {
      await client.query(`ROLLBACK`);
      throw err;
    } finally {
      client.release();
    }
  }

  async queryFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    func: UserDatabaseQuery<C, R, T>,
    ...args: T
  ): Promise<R> {
    const client: PoolClient = await this.pool.connect();
    try {
      return func(client as C, ...args);
    } finally {
      client.release();
    }
  }

  async query<R, T extends unknown[]>(sql: string, ...params: T): Promise<R[]> {
    return this.pool.query<QueryResultRow>(sql, params).then((value) => {
      return value.rows as R[];
    });
  }

  async queryWithClient<R, T extends unknown[]>(client: UserDatabaseClient, sql: string, ...params: T): Promise<R[]> {
    const pgClient: PoolClient = client as PoolClient;
    return pgClient.query<QueryResultRow>(sql, params).then((value) => {
      return value.rows as R[];
    });
  }

  getPostgresErrorCode(error: unknown): string | null {
    const dbErr: PGDatabaseError = error as PGDatabaseError;
    return dbErr.code ? dbErr.code : null;
  }

  isRetriableTransactionError(error: unknown): boolean {
    if (!(error instanceof PGDatabaseError)) {
      return false;
    }
    return this.getPostgresErrorCode(error) === '40001';
  }

  isKeyConflictError(error: unknown): boolean {
    if (!(error instanceof PGDatabaseError)) {
      return false;
    }
    const pge = this.getPostgresErrorCode(error);
    return pge === '23505';
  }

  isFailedSqlTransactionError(error: unknown): boolean {
    if (!(error instanceof PGDatabaseError)) {
      return false;
    }
    return this.getPostgresErrorCode(error) === '25P02';
  }

  async createSchema(): Promise<void> {
    return Promise.reject(new Error('createSchema() is not supported in PG user database.'));
  }

  async dropSchema(): Promise<void> {
    return Promise.reject(new Error('dropSchema() is not supported in PG user database.'));
  }
}

/**
 * Prisma user data access interface
 */
export interface PrismaClient {
  $queryRawUnsafe<R, T extends unknown[]>(query: string, ...params: T): Promise<R[]>;
  $transaction<R>(
    fn: (prisma: unknown) => Promise<R>,
    options?: { maxWait?: number; timeout?: number; isolationLevel?: unknown },
  ): Promise<R>;
  $disconnect(): Promise<void>;
}

interface PrismaError {
  code: string;
  meta: {
    code: string;
    message: string;
  };
}

const PrismaIsolationLevel = {
  ReadUncommitted: 'ReadUncommitted',
  ReadCommitted: 'ReadCommitted',
  RepeatableRead: 'RepeatableRead',
  Serializable: 'Serializable',
} as const;

export class PrismaUserDatabase implements UserDatabase {
  constructor(readonly prisma: PrismaClient) {}

  async init(debugMode: boolean = false): Promise<void> {
    if (!debugMode) {
      const schemaExists = await this.prisma.$queryRawUnsafe<ExistenceCheck, unknown[]>(schemaExistsQuery);
      if (!schemaExists[0].exists) {
        await this.prisma.$queryRawUnsafe(createUserDBSchema);
      }
      const txnOutputTableExists = await this.prisma.$queryRawUnsafe<ExistenceCheck, unknown[]>(
        txnOutputTableExistsQuery,
      );
      if (!txnOutputTableExists[0].exists) {
        await this.prisma.$queryRawUnsafe(userDBSchema);
      } else {
        const columnExists = await this.prisma.$queryRawUnsafe<ExistenceCheck, unknown[]>(columnExistsQuery);
        if (!columnExists[0].exists) {
          await this.prisma.$queryRawUnsafe(addColumnQuery);
        }
      }
      const txnIndexExists = await this.prisma.$queryRawUnsafe<ExistenceCheck, unknown[]>(txnOutputIndexExistsQuery);
      if (!txnIndexExists[0].exists) {
        await this.prisma.$queryRawUnsafe(userDBIndex);
      }
    }
  }

  async destroy(): Promise<void> {
    await this.prisma.$disconnect();
  }

  getName() {
    return UserDatabaseName.PRISMA;
  }

  async transaction<R, T extends unknown[]>(
    transaction: UserDatabaseTransaction<R, T>,
    config: TransactionConfig,
    ...args: T
  ): Promise<R> {
    let isolationLevel: string;
    if (config.isolationLevel === IsolationLevel.ReadUncommitted) {
      isolationLevel = PrismaIsolationLevel.ReadUncommitted;
    } else if (config.isolationLevel === IsolationLevel.ReadCommitted) {
      isolationLevel = PrismaIsolationLevel.ReadCommitted;
    } else if (config.isolationLevel === IsolationLevel.RepeatableRead) {
      isolationLevel = PrismaIsolationLevel.RepeatableRead;
    } else {
      isolationLevel = PrismaIsolationLevel.Serializable;
    }
    const result = await this.prisma.$transaction<R>(
      async (tx) => {
        return await transaction(tx as PrismaClient, ...args);
      },
      { isolationLevel: isolationLevel },
    );
    return result;
  }

  async queryFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    func: UserDatabaseQuery<C, R, T>,
    ...args: T
  ): Promise<R> {
    return func(this.prisma as C, ...args);
  }

  async query<R, T extends unknown[]>(sql: string, ...params: T): Promise<R[]> {
    return this.prisma.$queryRawUnsafe<R, T>(sql, ...params);
  }

  async queryWithClient<R, T extends unknown[]>(client: UserDatabaseClient, sql: string, ...params: T): Promise<R[]> {
    const prismaClient = client as PrismaClient;
    return prismaClient.$queryRawUnsafe<R, T>(sql, ...params);
  }

  getPostgresErrorCode(error: unknown): string | null {
    const dbErr: PrismaError = error as PrismaError;
    return dbErr.meta ? dbErr.meta.code : null;
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

  async createSchema(): Promise<void> {
    return Promise.reject(new Error('createSchema() is not supported in Prisma user database.'));
  }

  async dropSchema(): Promise<void> {
    return Promise.reject(new Error('dropSchema() is not supported in Prisma user database.'));
  }
}

interface TypeORMDataSource {
  readonly isInitialized: boolean;
  readonly manager: TypeORMEntityManager;
  initialize(): Promise<this>;
  query<R = unknown>(query: string): Promise<R>;
  destroy(): Promise<void>;

  synchronize(): Promise<void>;
  dropDatabase(): Promise<void>;
}

export interface TypeORMEntityManager {
  query<R, T extends unknown[]>(query: string, parameters?: T): Promise<R>;
  transaction<R>(
    isolationLevel: IsolationLevel,
    runinTransaction: (entityManager: TypeORMEntityManager) => Promise<R>,
  ): Promise<R>;
}

interface QueryFailedError<T> {
  driverError: T;
}

/**
 * TypeORM user data access interface
 */
export class TypeORMDatabase implements UserDatabase {
  readonly dataSource: TypeORMDataSource;

  constructor(readonly ds: TypeORMDataSource) {
    this.dataSource = ds;
  }

  async init(debugMode: boolean = false): Promise<void> {
    if (!this.dataSource.isInitialized) {
      await this.dataSource.initialize(); // Need to initialize datasource even in debug mode.
    }

    if (!debugMode) {
      const schemaExists = await this.dataSource.query<ExistenceCheck[]>(schemaExistsQuery);
      if (!schemaExists[0].exists) {
        await this.dataSource.query(createUserDBSchema);
      }
      const txnOutputTableExists = await this.dataSource.query<ExistenceCheck[]>(txnOutputTableExistsQuery);
      if (!txnOutputTableExists[0].exists) {
        await this.dataSource.query(userDBSchema);
      } else {
        const columnExists = await this.dataSource.query<ExistenceCheck[]>(columnExistsQuery);
        if (!columnExists[0].exists) {
          await this.dataSource.query(addColumnQuery);
        }
      }

      const txnIndexExists = await this.dataSource.query<ExistenceCheck[]>(txnOutputIndexExistsQuery);
      if (!txnIndexExists[0].exists) {
        await this.dataSource.query(userDBIndex);
      }
    }
  }

  async destroy(): Promise<void> {
    if (this.dataSource.isInitialized) {
      await this.dataSource.destroy();
    }
  }

  getName() {
    return UserDatabaseName.TYPEORM;
  }

  async transaction<R, T extends unknown[]>(
    txn: UserDatabaseTransaction<R, T>,
    config: TransactionConfig,
    ...args: T
  ): Promise<R> {
    const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;

    return this.dataSource.manager.transaction(
      isolationLevel,
      async (transactionEntityManager: TypeORMEntityManager) => {
        const result = await txn(transactionEntityManager, ...args);
        return result;
      },
    );
  }

  async queryFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    func: UserDatabaseQuery<C, R, T>,
    ...args: T
  ): Promise<R> {
    return func(this.dataSource.manager as C, ...args);
  }

  async query<R>(sql: string, ...params: unknown[]): Promise<R[]> {
    return this.dataSource.manager.query(sql, params).then((value) => {
      return value as R[];
    });
  }

  async queryWithClient<R, T extends unknown[]>(client: UserDatabaseClient, sql: string, ...params: T): Promise<R[]> {
    const tClient = client as TypeORMEntityManager;
    return tClient.query(sql, params).then((value) => {
      return value as R[];
    });
  }

  getPostgresErrorCode(error: unknown): string | null {
    const typeormErr = error as QueryFailedError<PGDatabaseError>;
    if (typeormErr.driverError) {
      const dbErr = typeormErr.driverError;
      return dbErr.code ? dbErr.code : null;
    } else {
      return null;
    }
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

  async createSchema(): Promise<void> {
    return this.dataSource.synchronize();
  }
  async dropSchema(): Promise<void> {
    return this.dataSource.dropDatabase();
  }
}

/**
 * Knex user data access interface
 */
export class KnexUserDatabase implements UserDatabase {
  constructor(readonly knex: Knex) {}

  async init(debugMode: boolean = false): Promise<void> {
    if (!debugMode) {
      const schemaExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(schemaExistsQuery);
      if (!schemaExists.rows[0].exists) {
        await this.knex.raw(createUserDBSchema);
      }
      const txnOutputTableExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(txnOutputTableExistsQuery);
      if (!txnOutputTableExists.rows[0].exists) {
        await this.knex.raw(userDBSchema);
      } else {
        const columnExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(columnExistsQuery);
        if (!columnExists.rows[0].exists) {
          await this.knex.raw(addColumnQuery);
        }
      }

      const txnIndexExists = await this.knex.raw<{ rows: ExistenceCheck[] }>(txnOutputIndexExistsQuery);
      if (!txnIndexExists.rows[0].exists) {
        await this.knex.raw(userDBIndex);
      }
    }
  }

  async destroy(): Promise<void> {
    await this.knex.destroy();
  }

  getName() {
    return UserDatabaseName.KNEX;
  }

  async transaction<R, T extends unknown[]>(
    transactionFunction: UserDatabaseTransaction<R, T>,
    config: TransactionConfig,
    ...args: T
  ): Promise<R> {
    let isolationLevel: Knex.IsolationLevels;
    if (config.isolationLevel === IsolationLevel.ReadUncommitted) {
      isolationLevel = 'read uncommitted';
    } else if (config.isolationLevel === IsolationLevel.ReadCommitted) {
      isolationLevel = 'read committed';
    } else if (config.isolationLevel === IsolationLevel.RepeatableRead) {
      isolationLevel = 'repeatable read';
    } else {
      isolationLevel = 'serializable';
    }
    const result = await this.knex.transaction<R>(
      async (transactionClient: Knex.Transaction) => {
        return await transactionFunction(transactionClient, ...args);
      },
      { isolationLevel: isolationLevel },
    );
    return result;
  }

  async queryFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    func: UserDatabaseQuery<C, R, T>,
    ...args: T
  ): Promise<R> {
    const result = await this.knex.transaction<R>(
      async (transactionClient: Knex.Transaction) => {
        return await func(transactionClient as unknown as C, ...args);
      },
      { isolationLevel: 'read committed', readOnly: true },
    );
    return result;
  }

  async query<R, T extends unknown[]>(sql: string, ...params: T): Promise<R[]> {
    return this.queryWithClient(this.knex, sql, ...params);
  }

  async queryWithClient<R, T extends unknown[]>(client: Knex, sql: string, ...uparams: T): Promise<R[]> {
    const knexSql = sql.replace(/\$\d+/g, '?'); // Replace $1, $2... with ?
    let params = uparams as any[];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    params = params.map((i) => (i === undefined ? null : i)); // Set undefined parameters to null.
    const rows = (await client.raw<R>(knexSql, params)) as { rows: R[] };
    return rows.rows;
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

  async createSchema(): Promise<void> {
    return Promise.reject(new Error('createSchema() is not supported in Knex user database.'));
  }

  async dropSchema(): Promise<void> {
    return Promise.reject(new Error('dropSchema() is not supported in Knex user database.'));
  }
}

export interface DrizzleClient {
  select(): unknown;
  selectDistinct(): unknown;
  update(table: unknown): unknown;
  insert(table: unknown): unknown;
  delete(table: unknown): unknown;
  execute(query: unknown): unknown;
  _: {
    session: any;
  };
  transaction<R>(fn: (tx: any) => Promise<R>, options: { isolationLevel: any; accessMode: any }): Promise<R>;
}

/**
 * Drizzle user data access interface
 */
export class DrizzleUserDatabase implements UserDatabase {
  constructor(
    readonly pool: Pool,
    readonly db: DrizzleClient,
  ) {}

  async init(debugMode: boolean = false): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const poolClient = this.db._.session.client as PoolClient;
    if (!debugMode) {
      const schemaExists = await poolClient.query<ExistenceCheck>(schemaExistsQuery);
      if (!schemaExists.rows[0].exists) {
        await poolClient.query(createUserDBSchema);
      }
      const txnOutputTableExists = await poolClient.query<ExistenceCheck>(txnOutputTableExistsQuery);
      if (!txnOutputTableExists.rows[0].exists) {
        await poolClient.query(userDBSchema);
      } else {
        const columnExists = await poolClient.query<ExistenceCheck>(columnExistsQuery);
        if (!columnExists.rows[0].exists) {
          await poolClient.query(addColumnQuery);
        }
      }

      const txnIndexExists = await poolClient.query<ExistenceCheck>(txnOutputIndexExistsQuery);
      if (!txnIndexExists.rows[0].exists) {
        await poolClient.query(userDBIndex);
      }
    }
  }

  async destroy(): Promise<void> {
    await this.pool.end();
  }

  getName() {
    return UserDatabaseName.DRIZZLE;
  }

  async transaction<R, T extends unknown[]>(
    transactionFunction: UserDatabaseTransaction<R, T>,
    config: TransactionConfig,
    ...args: T
  ): Promise<R> {
    let isolationLevel: 'read uncommitted' | 'read committed' | 'repeatable read' | 'serializable';
    if (config.isolationLevel === IsolationLevel.ReadUncommitted) {
      isolationLevel = 'read uncommitted';
    } else if (config.isolationLevel === IsolationLevel.ReadCommitted) {
      isolationLevel = 'read committed';
    } else if (config.isolationLevel === IsolationLevel.RepeatableRead) {
      isolationLevel = 'repeatable read';
    } else {
      isolationLevel = 'serializable';
    }
    const accessMode = 'read write';
    const result = await this.db.transaction<R>(
      async (tx: DrizzleClient) => {
        return await transactionFunction(tx, ...args);
      },
      { isolationLevel, accessMode },
    );
    return result;
  }

  async queryFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    func: UserDatabaseQuery<C, R, T>,
    ...args: T
  ): Promise<R> {
    return func(this.db as unknown as C, ...args);
  }

  async query<R, T extends unknown[]>(sql: string, ...params: T): Promise<R[]> {
    return this.queryWithClient(this.db, sql, ...params);
  }

  async queryWithClient<R, T extends unknown[]>(client: DrizzleClient, sqlString: string, ...params: T): Promise<R[]> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const sessionClient = client._.session.client as PoolClient;
    return sessionClient.query<QueryResultRow>(sqlString, params).then((value) => {
      return value.rows as R[];
    });
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

  async createSchema(): Promise<void> {
    return Promise.reject(new Error('createSchema() is not supported in Drizzle user database.'));
  }

  async dropSchema(): Promise<void> {
    return Promise.reject(new Error('dropSchema() is not supported in Drizzle user database.'));
  }
}

/**
 * TypeORM user data access interface
 */
export class MikroORMDatabase implements UserDatabase {
  readonly dataSource: MikroORM;
  readonly em: EntityManager;

  constructor(readonly ds: MikroORM) {
    this.dataSource = ds;
    this.em = ds.em.fork();
  }

  async init(debugMode: boolean = false): Promise<void> {
    if (!debugMode) {
      console.log('Mikro Orm init');

      const conn = this.em.getConnection();

      conn.execute('BEGIN');

      const schemaExists = await conn.execute(schemaExistsQuery);
      if (!schemaExists[0].exists) {
        await conn.execute(createUserDBSchema);
      }
      const txnOutputTableExists = await conn.execute<ExistenceCheck[]>(txnOutputTableExistsQuery);
      if (!txnOutputTableExists[0].exists) {
        await conn.execute(userDBSchema);
      } else {
        const columnExists = await conn.execute<ExistenceCheck[]>(columnExistsQuery);
        if (!columnExists[0].exists) {
          await conn.execute(addColumnQuery);
        }
      }

      const txnIndexExists = await conn.execute<ExistenceCheck[]>(txnOutputIndexExistsQuery);
      if (!txnIndexExists[0].exists) {
        await conn.execute(userDBIndex);
      }

      conn.execute('COMMIT');
    }
  }

  async destroy(): Promise<void> {
    await this.dataSource.close();
  }

  getName() {
    return UserDatabaseName.MIKROORM;
  }

  async transaction<R, T extends unknown[]>(
    txn: UserDatabaseTransaction<R, T>,
    config: TransactionConfig,
    ...args: T
  ): Promise<R> {
    const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;

    return this.em.transactional(async (em: EntityManager) => {
      return txn(em, ...args);
    }, {});
  }

  async queryFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    func: UserDatabaseQuery<C, R, T>,
    ...args: T
  ): Promise<R> {
    return func(this.em as C, ...args);
  }

  async query<R>(sql: string, ...params: unknown[]): Promise<R[]> {
    return this.em
      .getConnection()
      .execute(sql, params)
      .then((value) => {
        return value as R[];
      });
  }

  async queryWithClient<R, T extends unknown[]>(client: UserDatabaseClient, sql: string, ...params: T): Promise<R[]> {
    const tClient = client as EntityManager;
    console.log('Mikro Orm queryWithClient', sql, params);
    /* return tClient.getConnection().execute(sql, params).then((value) => {
      return value as R[];
    }); */
    return tClient
      .getConnection()
      .execute(sql, [...params])
      .then((value) => {
        return value as R[];
      });
  }

  getPostgresErrorCode(error: unknown): string | null {
    const typeormErr = error as QueryFailedError<PGDatabaseError>;
    if (typeormErr.driverError) {
      const dbErr = typeormErr.driverError;
      return dbErr.code ? dbErr.code : null;
    } else {
      return null;
    }
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

  async createSchema(): Promise<void> {
    await this.dataSource.getSchemaGenerator().refreshDatabase();
  }
  async dropSchema(): Promise<void> {
    console.log('Mikro Orm Dropping database');
    return this.dataSource.getSchemaGenerator().dropDatabase();
  }
}
