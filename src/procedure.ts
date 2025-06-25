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
