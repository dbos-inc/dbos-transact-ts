import { DBOSContext, DBOSContextImpl } from "./context";
import { Span } from "@opentelemetry/sdk-trace-base";
import { GlobalLogger as Logger } from "./telemetry/logs";
import { WorkflowContextImpl } from "./workflow";
import { WorkflowContextDebug } from "./debugger/debug_workflow";
import { UserDatabase } from "./user_database";

export type StoredProcedure<R> = (ctxt: StoredProcedureContext, ...args: unknown[]) => Promise<R>;

export interface StoredProcedureContext extends Pick<DBOSContext, 'logger' | 'workflowUUID'> {
  query<R>(sql: string, ...params: unknown[]): Promise<R[]>;
}

export class StoredProcedureContextImpl extends DBOSContextImpl implements StoredProcedureContext {
  constructor(
    readonly client: UserDatabase,
    workflowContext: WorkflowContextImpl | WorkflowContextDebug,
    span: Span,
    logger: Logger,
    operationName: string
  ) {
    super(operationName, span, logger, workflowContext);
  }
  query<R>(sql: string, ...params: unknown[]): Promise<R[]> {
     
    return this.client.query<R, unknown[]>(sql, ...params);
  }
}
