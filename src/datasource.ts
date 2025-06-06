import { SpanStatusCode } from '@opentelemetry/api';
import { Span } from '@opentelemetry/sdk-trace-base';
import { assertCurrentWorkflowContext, runWithDataSourceContext } from './context';
import { DBOS } from './dbos';
import { DBOSExecutor, OperationType } from './dbos-executor';
import { getTransactionalDataSource, registerTransactionalDataSource } from './decorators';
import { DBOSInvalidWorkflowTransitionError } from './error';

/**
 * This interface is to be used for implementers of transactional data sources
 *   This is what gets registered for the transaction control framework
 */
export interface DataSourceTransactionHandler {
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

/**
 * This is the suggested interface guideline for presenting to the end user, but not
 *   strictly required.
 */
export interface DBOSDataSource<Config> {
  readonly name: string;

  /**
   * Run the code transactionally within this data source
   *   Implementers should strongly type the config
   * @param callback - Function to run within a transactional context
   * @param name - Step name to show in the system database, traces, etc.
   * @param config - Transaction configuration options
   */
  runTransaction<T>(callback: () => Promise<T>, name: string, config?: Config): Promise<T>;

  /**
   * Register function as DBOS transaction, to be called within the context
   *  of a transaction on this data source.
   *
   * Providing a static version of this functionality is optional.
   *
   * @param func - Function to wrap
   * @param name - Name of function
   * @param config - Transaction settings
   * @returns Wrapped function, to be called instead of `func`
   */
  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    name: string,
    config?: Config,
  ): (this: This, ...args: Args) => Promise<Return>;

  /**
   * Produce a Stage 2 method decorator
   * @param config - Configuration to apply to the decorated method
   */
  transaction(
    config?: Config,
  ): <This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) => TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>;

  // In addition to the named methods above, there should also be a way to get the
  //  strongly-typed transaction client object:
  //   `static get whateverClient(): WhateverClient;`

  // A way to initialize the internal schema used by the DS for transaction tracking
  //  This is only for testing, it should be documented how to create this entirely
  //  outside of DBOS.
  //   `async initializeInternalSchema(): Promise<void>;`
  //  or, it can be static, such as:
  //   `static async initializeInternalSchema(options: Config)`

  // A way to initialize the user's schema (for ORMs that are capable of this)
  //  Again, this is only for testing, it should be doable entirely outside of DBOS
  // `async createSchema(...)`
}

/// Calling into DBOS

/**
 * This function is to be called by `DataSourceTransactionHandler` instances,
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
    const res = await DBOSExecutor.globalInstance!.runInternalStep<T>(
      async () => {
        return await runWithDataSourceContext(callnum, async () => {
          return await ds.invokeTransactionFunction(options.config ?? {}, undefined, callback);
        });
      },
      funcName,
      // we can be sure workflowID is set because of previous call to assertCurrentWorkflowContext
      DBOS.workflowID!,
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
    return DBOSExecutor.globalInstance!.runInternalStep<Return>(
      async () => {
        return await runWithDataSourceContext(callnum, async () => {
          return await ds.invokeTransactionFunction(config, this, func, ...rawArgs);
        });
      },
      options.name,
      DBOS.workflowID!,
      callnum,
    );
  };

  Object.defineProperty(invokeWrapper, 'name', {
    value: options.name,
  });
  return invokeWrapper;
}

/**
 * Register a transactional data source, that helps DBOS provide
 *  transactional access to user databases
 * @param name - Registered name for the data source
 * @param ds - Transactional data source provider
 */
export function registerDataSource(ds: DataSourceTransactionHandler) {
  registerTransactionalDataSource(ds.name, ds);
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
