/* eslint-disable @typescript-eslint/no-explicit-any */
import { UserDatabaseName, UserDatabaseClient } from "./user_database";
import { WorkflowContextImpl } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { DBOSContext, DBOSContextImpl } from "./context";
import { ValuesOf } from "./utils";
import { GlobalLogger as Logger } from "./telemetry/logs";
import { WorkflowContextDebug } from "./debugger/debug_workflow";

// Can we call it TransactionFunction
export type Transaction<T extends any[], R> = (ctxt: TransactionContext<any>, ...args: T) => Promise<R>;

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

export interface TransactionContext<T extends UserDatabaseClient> extends DBOSContext {
  readonly client: T;
}

export class TransactionContextImpl<T extends UserDatabaseClient> extends DBOSContextImpl implements TransactionContext<T> {
  constructor(
    readonly clientKind: UserDatabaseName,
    readonly client: T,
    workflowContext: WorkflowContextImpl | WorkflowContextDebug,
    span: Span,
    logger: Logger,
    readonly functionID: number,
    operationName: string
  ) {
    super(operationName, span, logger, workflowContext);
    if (workflowContext.applicationConfig) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = workflowContext.applicationConfig;
    }
  }
}
