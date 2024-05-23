import { UserDatabaseName, UserDatabaseClient } from "./user_database";
import { WorkflowContextImpl } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { DBOSContext, DBOSContextImpl } from "./context";
import { ValuesOf } from "./utils";
import { GlobalLogger as Logger } from "./telemetry/logs";
import { WorkflowContextDebug } from "./debugger/debug_workflow";
import { ConfiguredClass } from "./decorators";
import { DBOSError } from "./error";

// Can we call it TransactionFunction
export type Transaction<R> = (ctxt: TransactionContext<UserDatabaseClient>, ...args: unknown[]) => Promise<R>;

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
  getConfiguredClass(): ConfiguredClass<unknown> | null;
  getClassConfig<T>(): T;
}

export class TransactionContextImpl<T extends UserDatabaseClient> extends DBOSContextImpl implements TransactionContext<T> {
  constructor(
    readonly clientKind: UserDatabaseName,
    readonly client: T,
    workflowContext: WorkflowContextImpl | WorkflowContextDebug,
    span: Span,
    logger: Logger,
    readonly functionID: number,
    operationName: string,
    readonly configuredClass: ConfiguredClass<unknown> | null
  ) {
    super(operationName, span, logger, workflowContext);
    this.applicationConfig = workflowContext.applicationConfig;
  }

  getClassConfig<T>(): T {
    if (!this.configuredClass) throw new DBOSError(`Configuration is required for ${this.operationName} but was not provided.  Was the method invoked with 'invoke' instead of 'invokeOnConfig'?`);
    return this.configuredClass.arg as T;
  }
  getConfiguredClass(): ConfiguredClass<unknown> {
    if (!this.configuredClass) throw new DBOSError(`Configuration is required for ${this.operationName} but was not provided.  Was the method invoked with 'invoke' instead of 'invokeOnConfig'?`);
    return this.configuredClass;
  }
}
