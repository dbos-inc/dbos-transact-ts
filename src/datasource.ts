import { SpanStatusCode } from '@opentelemetry/api';
import { Span } from '@opentelemetry/sdk-trace-base';
import { assertCurrentWorkflowContext, runWithDSContext } from './context';
import { DBOS } from './dbos';
import { DBOSExecutor, OperationType } from './dbos-executor';
import { getTransactionalDataSource } from './decorators';
import { DBOSInvalidWorkflowTransitionError } from './error';

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

/**
 * This function is to be called by `DBOSTransactionalDataSource` instances,
 *   with bits of user code to be run as transactions.
 * 1. The DS validates the type of config and provides the name
 * 2. The transaction will be started inside here, with a durable sysdb checkpoint.
 * 3. The DS will in turn be called upon to run the callback in a transaction context
 * @param callback - User callback function
 * @param funcName - Function name, for recording in system DB
 * @param options - Data source name and configuration
 * @returns the return from `callback`
 */
export async function runTransaction<T>(
  callback: () => Promise<T>,
  funcName: string,
  options: { dsName?: string; config?: unknown } = {},
) {
  if (!DBOS.isWithinWorkflow) {
    throw new DBOSInvalidWorkflowTransitionError(`Invalid call to \`${funcName}\` outside of a workflow`);
  }
  if (!DBOS.isInWorkflow()) {
    throw new DBOSInvalidWorkflowTransitionError(
      `Invalid call to \`${funcName}\` inside a \`step\`, \`transaction\`, or \`procedure\``,
    );
  }
  const dsn = options.dsName ?? '<default>';
  const ds = getTransactionalDataSource(dsn);

  const wfctx = assertCurrentWorkflowContext();
  const callnum = wfctx.functionIDGetIncrement();

  const span: Span = DBOSExecutor.globalInstance!.tracer.startSpan(
    funcName,
    {
      operationUUID: wfctx.workflowUUID,
      operationType: OperationType.TRANSACTION,
      authenticatedUser: wfctx.authenticatedUser,
      assumedRole: wfctx.assumedRole,
      authenticatedRoles: wfctx.authenticatedRoles,
      // isolationLevel: txnInfo.config.isolationLevel, // TODO: Pluggable
    },
    wfctx.span,
  );

  try {
    const res = DBOSExecutor.globalInstance!.runAsStep<T>(
      async () => {
        return await runWithDSContext(callnum, async () => {
          return await ds.invokeTransactionFunction(options.config ?? {}, undefined, callback);
        });
      },
      funcName,
      DBOS.workflowID,
      callnum,
    );

    span.setStatus({ code: SpanStatusCode.OK });
    DBOSExecutor.globalInstance!.tracer.endSpan(span);
    return res;
  } catch (err) {
    const e = err as Error;
    span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
    DBOSExecutor.globalInstance!.tracer.endSpan(span);
    throw err;
  }
}

// Transaction wrapper
export function registerTransaction<This, Args extends unknown[], Return>(
  dsName: string,
  func: (this: This, ...args: Args) => Promise<Return>,
  options: {
    name: string;
  },
  config?: unknown,
): (this: This, ...args: Args) => Promise<Return> {
  const dsn = dsName ?? '<default>';

  const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
    if (!DBOS.isWithinWorkflow()) {
      throw new DBOSInvalidWorkflowTransitionError(`Call to transaction '${options.name}' outside of a workflow`);
    }

    if (DBOS.isInTransaction() || DBOS.isInStep()) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Invalid call to a `trasaction` function from within a `step` or `transaction`',
      );
    }

    const ds = getTransactionalDataSource(dsn);

    const wfctx = assertCurrentWorkflowContext();
    const callnum = wfctx.functionIDGetIncrement();
    return DBOSExecutor.globalInstance!.runAsStep<Return>(
      async () => {
        return await runWithDSContext(callnum, async () => {
          return await ds.invokeTransactionFunction(config, this, func, ...rawArgs);
        });
      },
      options.name,
      DBOS.workflowID,
      callnum,
    );
  };

  Object.defineProperty(invokeWrapper, 'name', {
    value: options.name,
  });
  return invokeWrapper;
}

/// Postgres helper routines

/** Isolation typically supported by application databases */
export const PGIsolationLevel = Object.freeze({
  ReadUncommitted: 'READ UNCOMMITTED',
  ReadCommitted: 'READ COMMITTED',
  RepeatableRead: 'REPEATABLE READ',
  Serializable: 'SERIALIZABLE',
} as const);

type ValuesOf<T> = T[keyof T];
export type PGIsolationLevel = ValuesOf<typeof PGIsolationLevel>;

/**
 * Configuration for Postgres-like transactions
 */
export interface PGTransactionConfig {
  /** Isolation level to request from underlying app database */
  isolationLevel?: PGIsolationLevel;
  /** If set, request read-only transaction from underlying app database */
  readOnly?: boolean;
}

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
