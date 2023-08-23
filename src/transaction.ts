/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from "pg";
import { PrismaClient, UserDatabaseName, UserDatabaseClient } from "./user_database";
import { Logger } from "./telemetry";
import { ValuesOf } from "./utils";
import { WorkflowContext } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { OperonContext } from './context';
import { DataSource } from "typeorm";

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
  readonly dataSource: DataSource = null as unknown as DataSource;

  readonly workflowUUID: string;
  readonly runAs: string;

  constructor(
    userDatabaseName: UserDatabaseName,
    client: UserDatabaseClient,
    config: TransactionConfig,
    workflowContext: WorkflowContext,
    private readonly logger: Logger,
    readonly span: Span,
    readonly functionID: number,
    readonly operationName: string
  ) {
    super();
    void config;
    if (userDatabaseName === UserDatabaseName.PGNODE) {
      this.pgClient = client as PoolClient;
    } else if (userDatabaseName === UserDatabaseName.PRISMA) {
      this.prismaClient = client as PrismaClient;
    } else if (userDatabaseName === UserDatabaseName.TYPEORM) {
      this.dataSource = client as DataSource;
    }
    this.workflowUUID = workflowContext.workflowUUID;
    this.runAs = workflowContext.runAs;
  }

  log(severity: string, message: string): void {
    this.logger.log(this, severity, message);
  }
}
