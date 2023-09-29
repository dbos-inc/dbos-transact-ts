/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from "pg";
import { PrismaClient, UserDatabaseName, UserDatabaseClient, TypeORMEntityManager } from "./user_database";
import { WorkflowContext } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { OperonContext, OperonContextImpl } from "./context";
import { ValuesOf } from "./utils";
import { Logger } from "./telemetry/logs";

// Can we call it OperonTransactionFunction
export type OperonTransaction<T extends any[], R> = (ctxt: TransactionContext, ...args: T) => Promise<R>;

export interface TransactionConfig {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
}

export const IsolationLevel = {
  ReadUncommitted: "READ UNCOMMITTED",
  ReadCommitted: "READ COMMITTED",
  RepeatableRead: "REPEATABLE READ",
  Serializable: "SERIALIZABLE",
} as const;
export type IsolationLevel = ValuesOf<typeof IsolationLevel>;

export interface TransactionContext extends OperonContext {
  pgClient: PoolClient;
  prismaClient: PrismaClient;
  typeormEM: TypeORMEntityManager;
}

export class TransactionContextImpl extends OperonContextImpl implements TransactionContext  {
  readonly pgClient: PoolClient = null as unknown as PoolClient;
  readonly prismaClient: PrismaClient = null as unknown as PrismaClient;

  readonly typeormEM: TypeORMEntityManager = null as unknown as TypeORMEntityManager;

  constructor(
    userDatabaseName: UserDatabaseName,
    client: UserDatabaseClient,
    config: TransactionConfig,
    workflowContext: WorkflowContext,
    span: Span,
    logger: Logger,
    readonly functionID: number,
    operationName: string
  ) {
    super(operationName, span, logger, workflowContext);
    void config;
    if (userDatabaseName === UserDatabaseName.PGNODE) {
      this.pgClient = client as PoolClient;
    } else if (userDatabaseName === UserDatabaseName.PRISMA) {
      this.prismaClient = client as PrismaClient;
    } else if (userDatabaseName === UserDatabaseName.TYPEORM) {
      this.typeormEM = client as TypeORMEntityManager;
    }

    if (workflowContext.applicationConfig) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = workflowContext.applicationConfig;
    }
  }
}
