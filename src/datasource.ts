/**
 * This interface is to be used for implementers of transactional data sources
 */
export interface DBOSTransactionalDataSource {
  readonly name: string;
  readonly dsType: string;

  /**
   * Will be called by DBOS during launch.
   * This may be a no-op if the DS is initialized before telling DBOS about the DS at all.
   */
  initialize(): Promise<void>;

  /**
   * Will be called by DBOS during attempt at clean shutdown (generally in testing scenarios).
   */
  destroy(): Promise<void>;

  /**
   * Invoke a transaction function
   */
  invokeTransactionFunction<This, Args extends unknown[], Return>(
    config: unknown,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return>;
}

/// Calling into DBOS

/// Postgres helper routines

export const createTransactionCompletionSchemaPG = `CREATE SCHEMA IF NOT EXISTS dbos;`;

export const createTransactionCompletionTablePG = `
  CREATE TABLE IF NOT EXISTS dbos.transaction_completion (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    error TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num)
  );
`;

export function getPGErrorCode(error: unknown): string | undefined {
  return error && typeof error === 'object' && 'code' in error ? (error.code as string) : undefined;
}

export function isPGRetriableTransactionError(error: unknown): boolean {
  return getPGErrorCode(error) === '40001';
}

export function isPGKeyConflictError(error: unknown): boolean {
  return getPGErrorCode(error) === '23505';
}

export function isPGFailedSqlTransactionError(error: unknown): boolean {
  return getPGErrorCode(error) === '25P02';
}
