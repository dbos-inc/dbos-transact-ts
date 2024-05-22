import { Pool, PoolConfig, PoolClient, DatabaseError as PGDatabaseError, QueryResultRow } from "pg";
import { createUserDBSchema, userDBIndex, userDBSchema } from "../schemas/user_db_schema";
import { IsolationLevel, TransactionConfig } from "./transaction";
import { ValuesOf } from "./utils";
import { Knex } from "knex";

export interface UserDatabase {
  init(debugMode?: boolean): Promise<void>;
  destroy(): Promise<void>;
  getName(): UserDatabaseName;

  // Run transactionFunction as a database transaction with a given config and arguments.
  transaction<R>(transactionFunction: UserDatabaseTransaction<R>, config: TransactionConfig, ): Promise<R>;
  // Execute a query function
  queryFunction<C extends UserDatabaseClient, R>(queryFunction: UserDatabaseQuery<C, R>, ...args: unknown[]): Promise<R>;
  // Execute a raw SQL query.
  query<R>(sql: string, ...args: unknown[]): Promise<R[]>;
  // Execute a raw SQL query in the session/transaction of a particular client.
  queryWithClient<R = unknown[]>(client: UserDatabaseClient, sql: string, ...args: unknown[]): Promise<R[]>;

  // Is a database error retriable?  Currently only serialization errors are retriable.
  isRetriableTransactionError(error: unknown): boolean;
  // Is a database error caused by a key conflict (key constraint violation or serialization error)?
  isKeyConflictError(error: unknown): boolean;

  // Not all databases support this, TypeORM can.
  // Drop the user database tables (for testing)
  createSchema(): Promise<void>;
  // Drop the user database tables (for testing)
  dropSchema(): Promise<void>;
}

type UserDatabaseQuery<C extends UserDatabaseClient, R> = (ctxt: C, ...args: unknown[]) => Promise<R>;
type UserDatabaseTransaction<R> = (ctxt: UserDatabaseClient, ...args: unknown[]) => Promise<R>;

export type UserDatabaseClient = PoolClient | PrismaClient | TypeORMEntityManager | Knex;

export const UserDatabaseName = {
  PGNODE: "pg-node",
  PRISMA: "prisma",
  TYPEORM: "typeorm",
  KNEX: "knex",
} as const;
export type UserDatabaseName = ValuesOf<typeof UserDatabaseName>;

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
      await this.pool.query(createUserDBSchema);
      await this.pool.query(userDBSchema);
      await this.pool.query(userDBIndex);
    }
  }

  async destroy(): Promise<void> {
    await this.pool.end();
  }

  getName() {
    return UserDatabaseName.PGNODE;
  }

  async transaction<R>(txn: UserDatabaseTransaction<R>, config: TransactionConfig, ...args: unknown[]): Promise<R> {
    const client: PoolClient = await this.pool.connect();
    try {
      const readOnly = config.readOnly ?? false;
      const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;
      await client.query(`BEGIN ISOLATION LEVEL ${isolationLevel}`);
      if (readOnly) {
        await client.query(`SET TRANSACTION READ ONLY`);
      }
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

  async queryFunction<C extends UserDatabaseClient, R>(func: UserDatabaseQuery<C, R>, ...args: unknown[]): Promise<R> {
    const client: PoolClient = await this.pool.connect();
    try
    {
       return func(client as C, ...args);
    }
    finally
    {
      client.release();
    }
  }

  async query<R>(sql: string, ...args: unknown[]): Promise<R[]> {
    return this.pool.query<QueryResultRow>(sql, args).then((value) => {
      return value.rows as R[];
    });
  }

  async queryWithClient<R>(client: UserDatabaseClient, sql: string, ...args: unknown[]): Promise<R[]> {
    const pgClient: PoolClient = client as PoolClient;
    return pgClient.query<QueryResultRow>(sql, args).then((value) => {
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
    return this.getPostgresErrorCode(error) === "40001";
  }

  isKeyConflictError(error: unknown): boolean {
    if (!(error instanceof PGDatabaseError)) {
      return false;
    }
    const pge = this.getPostgresErrorCode(error);
    return pge === "23505";
  }

  async createSchema(): Promise<void> {
    return Promise.reject(new Error("createSchema() is not supported in PG user database."));
  }

  async dropSchema(): Promise<void> {
    return Promise.reject( new Error("dropSchema() is not supported in PG user database."));
  }
}

/**
 * Prisma user data access interface
 */
export interface PrismaClient {
  $queryRawUnsafe<R>(query: string, ...args: unknown[]): Promise<R[]>;
  $transaction<R>(fn: (prisma: unknown) => Promise<R>, options?: { maxWait?: number; timeout?: number; isolationLevel?: unknown }): Promise<R>;
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
  ReadUncommitted: "ReadUncommitted",
  ReadCommitted: "ReadCommitted",
  RepeatableRead: "RepeatableRead",
  Serializable: "Serializable",
} as const;

export class PrismaUserDatabase implements UserDatabase {
  constructor(readonly prisma: PrismaClient) { }

  async init(debugMode: boolean = false): Promise<void> {
    if (!debugMode) {
      await this.prisma.$queryRawUnsafe(createUserDBSchema);
      await this.prisma.$queryRawUnsafe(userDBSchema);
      await this.prisma.$queryRawUnsafe(userDBIndex);
    }
  }

  async destroy(): Promise<void> {
    await this.prisma.$disconnect();
  }

  getName() {
    return UserDatabaseName.PRISMA;
  }

  async transaction<R>(transaction: UserDatabaseTransaction<R>, config: TransactionConfig, ...args: unknown[]): Promise<R> {
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
      { isolationLevel: isolationLevel }
    );
    return result;
  }

  async queryFunction<C extends UserDatabaseClient, R>(func: UserDatabaseQuery<C, R>, ...args: unknown[]): Promise<R> {
    return func(this.prisma as C, ...args);
  }

  async query<R>(sql: string, ...args: unknown[]): Promise<R[]> {
    return this.prisma.$queryRawUnsafe<R>(sql, ...args);
  }

  async queryWithClient<R>(client: UserDatabaseClient, sql: string, ...args: unknown[]): Promise<R[]> {
    const prismaClient = client as PrismaClient;
    return prismaClient.$queryRawUnsafe<R>(sql, ...args);
  }

  getPostgresErrorCode(error: unknown): string | null {
    const dbErr: PrismaError = error as PrismaError;
    return dbErr.meta ? dbErr.meta.code : null;
  }

  isRetriableTransactionError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === "40001";
  }

  isKeyConflictError(error: unknown): boolean {
    const pge = this.getPostgresErrorCode(error);
    return pge === "23505";
  }

  async createSchema(): Promise<void> {
    return Promise.reject(new Error("createSchema() is not supported in Prisma user database."));
  }

  async dropSchema(): Promise<void> {
    return Promise.reject(new Error("dropSchema() is not supported in Prisma user database."));
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

interface TypeORMEntityManager {
  query<R>(query: string, parameters?: unknown[]): Promise<R>
  transaction<R>(isolationLevel: IsolationLevel, runinTransaction: (entityManager: TypeORMEntityManager) => Promise<R>): Promise<R>
}

interface QueryFailedError<T> {
  driverError: T
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
      await this.dataSource.query(createUserDBSchema);
      await this.dataSource.query(userDBSchema);
      await this.dataSource.query(userDBIndex);
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

  async transaction<R>(txn: UserDatabaseTransaction<R>, config: TransactionConfig, ...args: unknown[]): Promise<R> {
    const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;

    return this.dataSource.manager.transaction(isolationLevel,
      async (transactionEntityManager: TypeORMEntityManager) => {
        const result = await txn(transactionEntityManager, ...args);
        return result;
      },
    );
  }

  async queryFunction<C extends UserDatabaseClient, R>(func: UserDatabaseQuery<C, R>, ...args: unknown[]): Promise<R> {
    return func(this.dataSource.manager as C, ...args);
  }

  async query<R>(sql: string, ...params: unknown[]): Promise<R[]> {
    return this.dataSource.manager.query(sql, params).then((value) => {
      return value as R[];
    });
  }

  async queryWithClient<R>(client: UserDatabaseClient, sql: string, ...args: unknown[]): Promise<R[]> {
    const tClient = client as TypeORMEntityManager;
    return tClient.query(sql, args).then((value) => {
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
    return this.getPostgresErrorCode(error) === "40001";
  }

  isKeyConflictError(error: unknown): boolean {
    const pge = this.getPostgresErrorCode(error);
    return pge === "23505";
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

  constructor(readonly knex: Knex) { }

  async init(debugMode: boolean = false): Promise<void> {
    if (!debugMode) {
      await this.knex.raw(createUserDBSchema);
      await this.knex.raw(userDBSchema);
      await this.knex.raw(userDBIndex);
    }
  }

  async destroy(): Promise<void> {
    await this.knex.destroy();
  }

  getName() {
    return UserDatabaseName.KNEX;
  }

  async transaction<R>(transactionFunction: UserDatabaseTransaction<R>, config: TransactionConfig, ...args: unknown[]): Promise<R> {
    let isolationLevel: Knex.IsolationLevels;
    if (config.isolationLevel === IsolationLevel.ReadUncommitted) {
      isolationLevel = "read uncommitted";
    } else if (config.isolationLevel === IsolationLevel.ReadCommitted) {
      isolationLevel = "read committed";
    } else if (config.isolationLevel === IsolationLevel.RepeatableRead) {
      isolationLevel = "repeatable read";
    } else {
      isolationLevel = "serializable";
    }
    const result = await this.knex.transaction<R>(
      async (transactionClient: Knex.Transaction) => {
        return await transactionFunction(transactionClient, ...args);
      },
      { isolationLevel: isolationLevel, readOnly: config.readOnly ?? false }
    );
    return result;
  }

  async queryFunction<C extends UserDatabaseClient, R>(func: UserDatabaseQuery<C, R>, ...args: unknown[]): Promise<R> {
    const result = await this.knex.transaction<R>(
      async (transactionClient: Knex.Transaction) => {
        return await func(transactionClient as unknown as C, ...args);
      },
      { isolationLevel: "read committed", readOnly : true }
    );
    return result;
  }

  async query<R>(sql: string, ...args: unknown[]): Promise<R[]> {
    return this.queryWithClient(this.knex, sql, ...args);
  }

  async queryWithClient<R>(client: Knex, sql: string, ...args: unknown[]): Promise<R[]> {
    const knexSql = sql.replace(/\$\d+/g, '?'); // Replace $1, $2... with ?
    const params = args.map(i => i === undefined ? null : i); // Set undefined parameters to null.
    const rows = await client.raw<R>(knexSql, params) as { rows: R[] };
    return rows.rows;
  }

  getPostgresErrorCode(error: unknown): string | null {
    const dbErr: PGDatabaseError = error as PGDatabaseError;
    return dbErr.code ? dbErr.code : null;
  }

  isRetriableTransactionError(error: unknown): boolean {
    return this.getPostgresErrorCode(error) === "40001";
  }

  isKeyConflictError(error: unknown): boolean {
    const pge = this.getPostgresErrorCode(error);
    return pge === "23505";
  }

  async createSchema(): Promise<void> {
    return Promise.reject(new Error("createSchema() is not supported in Knex user database."));
  }

  async dropSchema(): Promise<void> {
    return Promise.reject(new Error("dropSchema() is not supported in Knex user database."));
  }
}
