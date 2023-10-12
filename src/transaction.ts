/* eslint-disable @typescript-eslint/no-explicit-any */
import { UserDatabaseName, UserDatabaseClient } from "./user_database";
import { WorkflowContextImpl } from "./workflow";
import { Span } from "@opentelemetry/sdk-trace-base";
import { OperonContext, OperonContextImpl } from "./context";
import { ValuesOf } from "./utils";
import { Logger } from "winston";

// Can we call it OperonTransactionFunction
export type OperonTransaction<T extends any[], R> = (ctxt: TransactionContext<any>, ...args: T) => Promise<R>;

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

export interface TransactionContext<T extends UserDatabaseClient> extends OperonContext {
  readonly client: T;
}

export class TransactionContextImpl<T extends UserDatabaseClient> extends OperonContextImpl implements TransactionContext<T>  {
  constructor(
    readonly clientKind: UserDatabaseName,
    readonly client: T,
    workflowContext: WorkflowContextImpl,
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
