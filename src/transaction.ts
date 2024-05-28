/* eslint-disable @typescript-eslint/no-explicit-any */
import { UserDatabaseName, UserDatabaseClient } from "./user_database";
import { WorkflowContextImpl } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { DBOSContext, DBOSContextImpl } from "./context";
import { ValuesOf } from "./utils";
import { GlobalLogger as Logger } from "./telemetry/logs";
import { WorkflowContextDebug } from "./debugger/debug_workflow";
import { ConfiguredClass, InitConfigMethod } from "./decorators";
import { DBOSError } from "./error";

// Can we call it TransactionFunction
export type Transaction<T extends unknown[], R> = (ctxt: TransactionContext<any>, ...args: T) => Promise<R>;

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
  getConfiguredClass<C extends InitConfigMethod>(cls: C): ConfiguredClass<C, Parameters<C['initConfiguration']>[1]>;
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

  getConfiguredClass<C extends InitConfigMethod>(cls: C): ConfiguredClass<C, Parameters<C['initConfiguration']>[1]> {
    if (!this.configuredClass) throw new DBOSError(`Configuration is required for ${this.operationName} but was not provided.`);
    const cc = this.configuredClass as ConfiguredClass<C, Parameters<C['initConfiguration']>[1]>;
    if (cc.classCtor !== cls) throw new DBOSError(`Configration retrieval was attempted for class '${cls.name}' but saved for class '${cc.classCtor.name}'`);
    return cc;
  }
}
