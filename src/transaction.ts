/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from "pg";
import { PrismaClient, UserDatabaseName, UserDatabaseClient, TypeORMEntityManager } from "./user_database";
import { Logger } from "./telemetry";
import { ValuesOf } from "./utils";
import { WorkflowContext } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { OperonContext } from './context';

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

  readonly workflowUUID: string;

  constructor(
    userDatabaseName: UserDatabaseName,
    client: UserDatabaseClient,
    config: TransactionConfig,
    workflowContext: WorkflowContext,
    private readonly logger: Logger,
    readonly span: Span,
    readonly functionID: number,
    readonly operationName: string,
  ) {
    super({parentCtx: workflowContext});
    void config;
    if (userDatabaseName === UserDatabaseName.PGNODE) {
      this.pgClient = client as PoolClient;
    } else if (userDatabaseName === UserDatabaseName.PRISMA) {
      this.prismaClient = client as PrismaClient;
    } else if (userDatabaseName === UserDatabaseName.TYPEORM) {
      this.typeormEM = client as TypeORMEntityManager;
    }
    this.workflowUUID = workflowContext.workflowUUID;

    if (workflowContext.applicationConfig) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = workflowContext.applicationConfig;
    }
  }

  log(severity: string, message: string): void {
    this.logger.log(this, severity, message);
  }
}
