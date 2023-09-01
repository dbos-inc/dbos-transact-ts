/* eslint-disable @typescript-eslint/no-explicit-any */
import { Pool, PoolConfig, PoolClient, DatabaseError as PGDatabaseError } from "pg";
import { createUserDBSchema, userDBSchema } from "../schemas/user_db_schema";
import { IsolationLevel, TransactionConfig } from "./transaction";
import { ValuesOf } from "./utils";
import { DataSource as TypeORMDataSource, EntityManager as TypeORMEntityManager, QueryFailedError } from "typeorm";

export interface UserDatabase {
  init(): Promise<void>;
  destroy(): Promise<void>;

  getName(): UserDatabaseName;

  transaction<T extends any[], R>(transaction: UserDatabaseTransaction<T, R>, config: TransactionConfig, ...args: T): Promise<R>;
  query<R>(sql: string, ...params: any[]): Promise<R[]>;
  queryWithClient<R>(client: UserDatabaseClient, sql: string, ...params: any[]): Promise<R[]>;

  isRetriableTransactionError(error: unknown): boolean;
  isKeyConflictError(error: unknown): boolean;
}

type UserDatabaseTransaction<T extends any[], R> = (ctxt: UserDatabaseClient, ...args: T) => Promise<R>;

export type UserDatabaseClient = PoolClient | PrismaClient | TypeORMEntityManager;

export const UserDatabaseName = {
  PGNODE: "pg-node",
  PRISMA: "prisma",
  TYPEORM: "typeorm",
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

  async init(): Promise<void> {
    await this.pool.query(createUserDBSchema);
    await this.pool.query(userDBSchema);
  }

  async destroy(): Promise<void> {
    await this.pool.end();
  }

  getName() {
    return UserDatabaseName.PGNODE;
  }

  async transaction<T extends any[], R>(txn: UserDatabaseTransaction<T, R>, config: TransactionConfig, ...args: T): Promise<R> {
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

  async query<R>(sql: string, ...params: any[]): Promise<R[]> {
    return this.pool.query(sql, params).then((value) => {
      return value.rows as R[];
    });
  }

  async queryWithClient<R>(client: UserDatabaseClient, sql: string, ...params: any[]): Promise<R[]> {
    const pgClient: PoolClient = client as PoolClient;
    return pgClient.query(sql, params).then((value) => {
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
    return pge === "40001" || pge === "23505";
  }
}

/**
 * Prisma user data access interface
 */
export interface PrismaClient {
  $queryRawUnsafe<R>(query: string, ...params: any[]): Promise<R[]>;
  $transaction<R>(fn: (prisma: any) => Promise<R>, options?: { maxWait?: number; timeout?: number; isolationLevel?: any }): Promise<R>;
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
  constructor(readonly prisma: PrismaClient) {}

  async init(): Promise<void> {
    await this.prisma.$queryRawUnsafe(createUserDBSchema);
    await this.prisma.$queryRawUnsafe(userDBSchema);
  }

  async destroy(): Promise<void> {
    await this.prisma.$disconnect();
  }

  getName() {
    return UserDatabaseName.PRISMA;
  }

  async transaction<T extends any[], R>(transaction: UserDatabaseTransaction<T, R>, config: TransactionConfig, ...args: T): Promise<R> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
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

  async query<R>(sql: string, ...params: any[]): Promise<R[]> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return this.prisma.$queryRawUnsafe<R>(sql, ...params);
  }

  async queryWithClient<R>(client: UserDatabaseClient, sql: string, ...params: any[]): Promise<R[]> {
    const prismaClient = client as PrismaClient;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return prismaClient.$queryRawUnsafe<R>(sql, ...params);
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
    return pge === "40001" || pge === "23505";
  }
}

/**
 * TypeORM user data access interface
 */
export class TypeORMDatabase implements UserDatabase {
  readonly dataSource: TypeORMDataSource;

  constructor(readonly ds: TypeORMDataSource) {
    this.dataSource = ds;
  }

  async init(): Promise<void> {
    if (!this.dataSource.isInitialized) {
      await this.dataSource.initialize();
    }

    await this.dataSource.query(createUserDBSchema);
    await this.dataSource.query(userDBSchema);
  }

  async destroy(): Promise<void> {
    if (this.dataSource.isInitialized) {
      await this.dataSource.destroy();
    }
  }

  getName() {
    return UserDatabaseName.TYPEORM;
  }

  async transaction<T extends any[], R>(txn: UserDatabaseTransaction<T, R>, config: TransactionConfig, ...args: T): Promise<R> {
    const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;

    return this.dataSource.manager.transaction(isolationLevel,
      async (transactionEntityManager : TypeORMEntityManager) => {
      const result = await txn(transactionEntityManager, ...args);
      return result;
      },
    );
  }

  async query<R>(sql: string, ...params: any[]): Promise<R[]> {
    return this.dataSource.manager.query(sql, params).then((value) => {
      return value as R[];
    });
  }

  async queryWithClient<R>(client: UserDatabaseClient, sql: string, ...params: any[]): Promise<R[]> {
    const tClient = client as TypeORMEntityManager;
    return tClient.query(sql, params).then((value) => {
      return value as R[];
    });
  }

  getPostgresErrorCode(error: unknown): string | null {
    const typeormErr = error as QueryFailedError;
    const dbErr = typeormErr.driverError as PGDatabaseError;
    return dbErr.code ? dbErr.code : null;
  }

  isRetriableTransactionError(error: unknown): boolean {
    if (!(error instanceof QueryFailedError)) {
      return false;
    }
    return this.getPostgresErrorCode(error) === "40001";
  }

  isKeyConflictError(error: unknown): boolean {
    if (!(error instanceof QueryFailedError)) {
      return false;
    }
    const pge = this.getPostgresErrorCode(error);
    return pge === "40001" || pge === "23505";
  }
}
