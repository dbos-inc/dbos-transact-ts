import { DBOSContext, DBOSContextImpl } from './context';
import { Span } from '@opentelemetry/sdk-trace-base';
import { GlobalLogger as Logger } from './telemetry/logs';
import { WorkflowContextImpl } from './workflow';
import { PoolClient } from 'pg';
import { TransactionConfig } from './transaction';

export interface QueryResultBase {
  rowCount: number;
}

/**
 * Configuration for `@DBOS.storedProcedure` functions
 */
export interface StoredProcedureConfig extends TransactionConfig {
  /** If true, execute locally rather than using in-database stored procedure */
  executeLocally?: boolean;
}

export interface QueryResultRow {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [column: string]: any;
}

export interface QueryResult<R extends QueryResultRow> extends QueryResultBase {
  rows: R[];
}

export type StoredProcedure<T extends unknown[], R> = (ctxt: StoredProcedureContext, ...args: T) => Promise<R>;

/**
 * @deprecated This class is no longer necessary
 * To update to Transact 2.0+
 *   Remove `StoredProcedureContext` from function parameter lists
 *   Use `DBOS.` to access DBOS context within affected functions
 *   Adjust callers to call the function directly
 */
export interface StoredProcedureContext
  extends Pick<
    DBOSContext,
    'request' | 'workflowUUID' | 'authenticatedUser' | 'assumedRole' | 'authenticatedRoles' | 'logger'
  > {
  query<R extends QueryResultRow>(sql: string, ...params: unknown[]): Promise<QueryResult<R>>;
}

export class StoredProcedureContextImpl extends DBOSContextImpl implements StoredProcedureContext {
  constructor(
    readonly client: PoolClient,
    workflowContext: WorkflowContextImpl,
    span: Span,
    logger: Logger,
    readonly functionID: number,
    operationName: string,
  ) {
    super(operationName, span, logger, workflowContext);
  }
  async query<R extends QueryResultRow>(sql: string, params: unknown[]): Promise<QueryResult<R>> {
    const { rowCount, rows } = await this.client.query<R>(sql, params);
    return { rowCount: rowCount ?? rows.length, rows };
  }
}
