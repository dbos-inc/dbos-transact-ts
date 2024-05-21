import { DBOSContext, DBOSContextImpl } from "./context";
import { Span } from "@opentelemetry/sdk-trace-base";
import { GlobalLogger as Logger } from "./telemetry/logs";
import { WorkflowContextImpl } from "./workflow";
import { WorkflowContextDebug } from "./debugger/debug_workflow";
import { UserDatabase } from "./user_database";

export type Procedure<R> = (ctxt: ProcedureContext, ...args: unknown[]) => Promise<R>;

export interface ProcedureContext extends Pick<DBOSContext, 'logger' | 'workflowUUID'> {
  query<R>(sql: string, ...params: unknown[]): Promise<R[]>;
}

export class ProcedureContextImpl extends DBOSContextImpl implements ProcedureContext {
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
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return this.client.query<R, unknown[]>(sql, ...params);
  }
}
