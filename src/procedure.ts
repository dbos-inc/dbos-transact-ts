import { DBOSContext, DBOSContextImpl } from "./context";
import { Span } from "@opentelemetry/sdk-trace-base";
import { GlobalLogger as Logger } from "./telemetry/logs";
import { WorkflowContextImpl } from "./workflow";
import { WorkflowContextDebug } from "./debugger/debug_workflow";
import { PoolClient } from "pg";
import { TransactionConfig } from "./transaction";

export interface QueryResultBase {
  rowCount: number;
}

export interface StoredProcedureConfig extends TransactionConfig {
  executeLocally?: boolean
}

export interface QueryResultRow {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [column: string]: any;
}

export interface QueryResult<R extends QueryResultRow> extends QueryResultBase {
  rows: R[];
}

export type StoredProcedure<R> = (ctxt: StoredProcedureContext, ...args: unknown[]) => Promise<R>;

export interface StoredProcedureContext extends Pick<DBOSContext, 'request' | 'workflowUUID' | 'authenticatedUser' | 'assumedRole' | 'authenticatedRoles' | 'logger'> {
  query<R extends QueryResultRow>(sql: string, ...params: unknown[]): Promise<QueryResult<R>>;
}

export class StoredProcedureContextImpl extends DBOSContextImpl implements StoredProcedureContext {
  constructor(
    readonly client: PoolClient,
    workflowContext: WorkflowContextImpl | WorkflowContextDebug,
    span: Span,
    logger: Logger,
    operationName: string
  ) {
    super(operationName, span, logger, workflowContext);
  }
  async query<R extends QueryResultRow>(sql: string, params: unknown[]): Promise<QueryResult<R>> {
    const { rowCount, rows } = await this.client.query<R>(sql, params);
    return { rowCount: rowCount ?? rows.length, rows };
  }
}
