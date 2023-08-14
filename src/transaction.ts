/* eslint-disable @typescript-eslint/no-explicit-any */
import { PoolClient } from 'pg';
import { PrismaClient, UserDatabaseName, UserDatabaseClient } from './user_database';
import { TelemetryCollector, TelemetrySignal} from "./telemetry";
import { ValuesOf } from './utils';
import { WorkflowContext} from "./workflow";

export type OperonTransaction<T extends any[], R> = (ctxt: TransactionContext, ...args: T) => Promise<R>;

export interface TransactionConfig {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
}

export const IsolationLevel = {
  ReadUncommitted: "READ UNCOMMITTED",
  ReadCommitted: "READ COMMITTED",
  RepeatableRead: "REPEATABLE READ",
  Serializable: "SERIALIZABLE"
} as const;
export type IsolationLevel = ValuesOf<typeof IsolationLevel>

export class TransactionContext {
  readonly pgClient: PoolClient = null as unknown as PoolClient;
  readonly prismaClient: PrismaClient = null as unknown as PrismaClient;

  constructor(userDatabaseName: UserDatabaseName,
    client: UserDatabaseClient,
    config: TransactionConfig,
    private readonly workflowContext: WorkflowContext,
    private readonly telemetryCollector: TelemetryCollector,
    readonly functionID: number,
    readonly functionName: string) {
    void config;
    if (userDatabaseName === UserDatabaseName.PGNODE) {
      this.pgClient = client as PoolClient;
    } else if (userDatabaseName === UserDatabaseName.PRISMA) {
      this.prismaClient = client as PrismaClient;
    }
  }

  log(severity: string, message: string): void {
    const workflowContext = this.workflowContext;
    const signal: TelemetrySignal = {
      workflowUUID: workflowContext.workflowUUID,
      functionID: this.functionID,
      functionName: this.functionName,
      runAs: workflowContext.runAs,
      timestamp: Date.now(),
      severity: severity,
      logMessage: message,
    };
    this.telemetryCollector.push(signal);
  }
}