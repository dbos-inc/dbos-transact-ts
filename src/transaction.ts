/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from "pg";
import { PrismaClient, UserDatabaseName, UserDatabaseClient, TypeORMEntityManager } from "./user_database";
import { Logger } from "./telemetry";
import { ValuesOf } from "./utils";
import { WorkflowContext } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { OperonContext } from './context';
import { LogSeverity } from "./telemetry/signals";

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

export class TransactionContext extends OperonContext {
  readonly pgClient: PoolClient = null as unknown as PoolClient;
  readonly prismaClient: PrismaClient = null as unknown as PrismaClient;

  readonly typeormEM: TypeORMEntityManager = null as unknown as TypeORMEntityManager;

  constructor(
    userDatabaseName: UserDatabaseName,
    client: UserDatabaseClient,
    config: TransactionConfig,
    workflowContext: WorkflowContext,
    private readonly logger: Logger,
    span: Span,
    readonly functionID: number,
    operationName: string,
  ) {
    super(operationName, span, workflowContext);
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

  info(message: string): void {
    this.logger.log(this, LogSeverity.Info, message);
  }

  warn(message: string): void {
    this.logger.log(this, LogSeverity.Warn, message);
  }

  log(message: string): void {
    this.logger.log(this, LogSeverity.Log, message);
  }

  error(message: string): void {
    this.logger.log(this, LogSeverity.Error, message);
  }

  debug(message: string): void {
    this.logger.log(this, LogSeverity.Debug, message);
  }
}
