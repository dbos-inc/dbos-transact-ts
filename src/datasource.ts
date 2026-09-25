import { currentOwnerXid, functionIDGetIncrement, getCurrentContextStore, runWithDataSourceContext } from './context';
import { DBOS } from './dbos';
import { DBOSExecutor, OperationType } from './dbos-executor';
import {
  ensureDBOSIsLaunched,
  FunctionName,
  getTransactionalDataSource,
  registerFunctionWrapper,
  registerTransactionalDataSource,
  wrapDBOSFunctionAndRegister,
} from './decorators';
import {
  DBOSError,
  DBOSInitializationError,
  DBOSInvalidWorkflowTransitionError,
  DBOSWorkflowConflictError,
} from './error';
import { advisoryLockKey } from './system_database';
import { runWithTrace, SpanStatusCode } from './telemetry/traces';
import { SuperJSON } from 'superjson';

/**
 * This interface is to be used for implementers of transactional data sources
 *   This is what gets registered for the transaction control framework
 */
export interface DataSourceTransactionHandler {
  readonly name: string;

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
   * Delete this data source's checkpoints for `workflowID` from `startStep` onwards.
   *
   * Used by rewind, which drops the workflow's history from that step, including datasources checkpoints,
   * and on workflow completion (from step 0), once the workflow's step checkpoints cover every transaction.
   *
   * When `beforeCommit` is given, run the delete in a transaction and await `beforeCommit` after
   * the delete but before committing; if it throws, roll back and rethrow. Completion passes one that
   * throws DBOSWorkflowConflictError once another execution owns the workflow, whose checkpoints
   * may be the new owner's only record of a transaction.
   */
  deleteCheckpoints?(workflowID: string, startStep: number, beforeCommit?: () => Promise<void>): Promise<void>;

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
export interface DBOSDataSource<Config extends { name?: string }> {
  readonly name: string;

  /**
   * Run the code transactionally within this data source
   *   Implementers should strongly type the config
   * @param callback - Function to run within a transactional context
   * @param name - Step name to show in the system database, traces, etc.
   * @param config - Transaction configuration options
   */
  runTransaction<T>(callback: () => Promise<T>, config?: Config): Promise<T>;

  /**
   * Register function as DBOS transaction, to be called within the context
   *  of a transaction on this data source.
   *
   * Providing a static version of this functionality is optional.
   *
   * @param func - Function to wrap
   * @param config - Transaction settings, including function `name`
   * @param target - Class name, or class ctor/prototype
   * @returns Wrapped function, to be called instead of `func`
   */
  registerTransaction<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: Config & FunctionName,
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
  //   `static get client(): WhateverClient;`
  //   `get client(): WhateverClient;`

  // A static way to create or migrate the DS's transaction schema out of band, for use with
  //  `runMigrations: false`, built on `initializeDataSourceSchemaPG`:
  //   `static async initializeDBOSSchema(c: Config | Connection, schemaName?: string, options?: { applicationRole?: string })`
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
  options: { dsName: string; config?: unknown },
) {
  ensureDBOSIsLaunched('transactions');
  const ds = getTransactionalDataSource(options.dsName);

  if (!DBOS.isWithinWorkflow()) {
    return await runWithDataSourceContext(0, async () => {
      return await ds.invokeTransactionFunction(options.config ?? {}, undefined, callback);
    });
  }
  if (!DBOS.isInWorkflow()) {
    throw new DBOSInvalidWorkflowTransitionError(
      `Invalid call to \`${funcName}\` inside a \`step\` or \`transaction\``,
    );
  }

  recordDataSourceUse(ds);
  const callnum = functionIDGetIncrement();

  const tracer = DBOSExecutor.globalInstance!.tracer;
  const span = tracer.startSpan(
    funcName,
    {
      operationUUID: DBOS.workflowID,
      operationType: OperationType.TRANSACTION,
      operationName: funcName,
      authenticatedUser: DBOS.authenticatedUser,
      assumedRole: DBOS.assumedRole,
      authenticatedRoles: DBOS.authenticatedRoles,
    },
    DBOS.span,
  );

  try {
    const res = await runWithTrace(span, async () => {
      return await DBOSExecutor.globalInstance!.runInternalStep<T>(
        async () => {
          return await runWithDataSourceContext(callnum, async () => {
            return await ds.invokeTransactionFunction(options.config ?? {}, undefined, callback);
          });
        },
        funcName,
        // The isInWorkflow check above guarantees workflowID is set.
        DBOS.workflowID!,
        callnum,
      );
    });

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

/** Note that the running workflow called `ds`, so its completion clears `ds`'s checkpoints. */
function recordDataSourceUse(ds: DataSourceTransactionHandler) {
  // Recorded before the step runs, so a replayed step still clears an earlier execution's rows.
  getCurrentContextStore()?.usedDataSources?.add(ds);
}

// Transaction wrapper
export function registerTransaction<This, Args extends unknown[], Return, Config extends FunctionName>(
  dsName: string,
  func: (this: This, ...args: Args) => Promise<Return>,
  config?: Config,
): (this: This, ...args: Args) => Promise<Return> {
  const funcName = config?.name ?? func.name;
  const reg = wrapDBOSFunctionAndRegister(config?.ctorOrProto, config?.className, funcName, funcName, func);

  const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
    ensureDBOSIsLaunched('transactions');
    const ds = getTransactionalDataSource(dsName);
    const callFunc = reg.registeredFunction ?? reg.origFunction;

    if (!DBOS.isWithinWorkflow()) {
      return await runWithDataSourceContext(0, async () => {
        return await ds.invokeTransactionFunction(config, this, callFunc, ...rawArgs);
      });
    }

    if (DBOS.isInTransaction() || DBOS.isInStep()) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Invalid call to a `transaction` function from within a `step` or `transaction`',
      );
    }

    recordDataSourceUse(ds);
    const tracer = DBOSExecutor.globalInstance!.tracer;
    const span = tracer.startSpan(
      funcName,
      {
        operationUUID: DBOS.workflowID,
        operationType: OperationType.TRANSACTION,
        operationName: funcName,
        authenticatedUser: DBOS.authenticatedUser,
        assumedRole: DBOS.assumedRole,
        authenticatedRoles: DBOS.authenticatedRoles,
      },
      DBOS.span,
    );

    const callnum = functionIDGetIncrement();
    try {
      const res = await runWithTrace(span, async () => {
        return await DBOSExecutor.globalInstance!.runInternalStep<Return>(
          async () => {
            return await runWithDataSourceContext(callnum, async () => {
              return await ds.invokeTransactionFunction(config, this, callFunc, ...rawArgs);
            });
          },
          funcName,
          DBOS.workflowID!,
          callnum,
        );
      });
      span.setStatus({ code: SpanStatusCode.OK });
      DBOSExecutor.globalInstance!.tracer.endSpan(span);
      return res;
    } catch (err) {
      const e = err as Error;
      span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
      DBOSExecutor.globalInstance!.tracer.endSpan(span);
      throw err;
    }
  };

  registerFunctionWrapper(invokeWrapper, reg);

  Object.defineProperty(invokeWrapper, 'name', {
    value: funcName,
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

/** Error types re-exported so data sources can raise and recognize DBOS-typed failures. */
export { DBOSError, DBOSWorkflowConflictError };

/**
 * Throw DBOSWorkflowConflictError if the calling execution no longer owns `workflowID`.
 *
 * Data sources call this inside their transaction, after inserting the step's checkpoint:
 * that row makes a later owner's insert wait on this commit, so a stale execution rolls
 * back instead of applying a step the new owner also runs. The error must be rethrown
 * as is, never recorded as the step's outcome.
 */
export async function assertStillOwnsWorkflow(workflowID: string): Promise<void> {
  const ownerXid = currentOwnerXid(workflowID);
  // No token means nothing to fence on, as in a transaction run outside the workflow's own context.
  if (ownerXid === undefined) return;
  const owner = await DBOSExecutor.globalInstance!.systemDatabase.getWorkflowOwner(workflowID);
  if (owner !== ownerXid) {
    throw new DBOSWorkflowConflictError(workflowID);
  }
}

/**
 * Internal signal that a concurrent duplicate execution recorded this step's outcome first.
 * Data sources throw this from their completion insert so the user transaction rolls back
 * and the recorded outcome is replayed instead; it is never surfaced to user code.
 */
export class DBOSStepAlreadyRecordedError extends Error {
  constructor(workflowID: string, stepID: number) {
    super(`Step ${stepID} of workflow ${workflowID} was already recorded by a concurrent execution`);
    this.name = 'DBOSStepAlreadyRecordedError';
  }
}

/**
 * Deliver a transaction outcome recorded by an earlier or concurrent execution.
 * Data sources share this between their pre-execution check and their conflict handler
 * so the two paths cannot drift apart.
 */
export function replayRecordedStep<Return>(recorded: { output?: string | null; error?: string | null }): Return {
  DBOS.span?.setAttribute('cached', true);
  // Discriminate on the value: a raw row carries both keys, with the unused one null.
  if (typeof recorded.error === 'string') {
    throw SuperJSON.parse(recorded.error);
  }
  return (recorded.output ? SuperJSON.parse(recorded.output) : null) as Return;
}

function createTransactionCompletionTablePG(schemaName: string): string {
  return `
  CREATE TABLE IF NOT EXISTS ${quoteIdent(schemaName)}.transaction_completion (
    workflow_id TEXT NOT NULL,
    function_num INT NOT NULL,
    output TEXT,
    error TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
    PRIMARY KEY (workflow_id, function_num)
  );
`;
}

/// Data source schema migrations

/** Records the data source schema version; separate from the system database's `dbos_migrations`, since the two may share a schema. */
export const DATASOURCE_MIGRATIONS_TABLE = 'dbos_transaction_completion_migrations';

/** Longest a migrating transaction may sit idle while holding the migration lock. */
const MIGRATION_IDLE_TIMEOUT = '30s';

/** Options shared by data source constructors. */
export interface DataSourceMigrationOptions {
  /**
   * Whether `initialize` creates and migrates the data source's DBOS schema. Defaults to true.
   * When false, it only verifies the schema is migrated, so the role needs no DDL privileges;
   * migrate out of band with the data source's static `initializeDBOSSchema`.
   */
  runMigrations?: boolean;
}

/** Runs one parameter-free SQL statement and returns its rows. */
export type DataSourceSQLExecutor = (sql: string) => Promise<ReadonlyArray<Record<string, unknown>>>;

/** The data source schema migrations, in order; migration N moves the schema to version N. */
export function getDataSourceMigrationsPG(schemaName: string = 'dbos'): string[] {
  // Migration 1 is IF NOT EXISTS so schemas created before versioning adopt it cleanly.
  return [createTransactionCompletionTablePG(schemaName)];
}

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

async function pgTableExists(exec: DataSourceSQLExecutor, schemaName: string, table: string): Promise<boolean> {
  // pg_catalog, not information_schema, which hides tables the role holds no grant on.
  const rows = await exec(
    `SELECT 1 AS present FROM pg_catalog.pg_tables WHERE schemaname = ${quoteLiteral(schemaName)} AND tablename = ${quoteLiteral(table)}`,
  );
  return rows.length > 0;
}

async function readDataSourceVersion(exec: DataSourceSQLExecutor, schemaName: string): Promise<number> {
  if (!(await pgTableExists(exec, schemaName, DATASOURCE_MIGRATIONS_TABLE))) return 0;
  const rows = await exec(`SELECT version FROM ${quoteIdent(schemaName)}.${DATASOURCE_MIGRATIONS_TABLE}`);
  return rows.length > 0 ? Number(rows[0].version) : 0;
}

/**
 * Bring the data source schema to the latest version. `exec` must run every statement in one
 * transaction, so the advisory lock serializes concurrent migrators until it commits.
 * Issues no DDL when the schema is already at or ahead of the latest version.
 */
export async function migrateDataSourcePG(exec: DataSourceSQLExecutor, schemaName: string = 'dbos'): Promise<void> {
  const migrations = getDataSourceMigrationsPG(schemaName);
  const latest = migrations.length;
  if ((await readDataSourceVersion(exec, schemaName)) >= latest) return;

  // A frozen or partitioned migrator's session is killed, rolling back and releasing the lock.
  await exec(`SET LOCAL idle_in_transaction_session_timeout = '${MIGRATION_IDLE_TIMEOUT}'`);
  const versionRows = await exec('SELECT version() AS version');
  const serverVersion = versionRows[0]?.version;
  // CockroachDB has no pg_advisory_xact_lock, so it migrates unserialized.
  if (!(typeof serverVersion === 'string' && /cockroachdb/i.test(serverVersion))) {
    // The function sits in FROM so no client has to decode its void result.
    await exec(
      `SELECT 1 AS locked FROM pg_advisory_xact_lock(${advisoryLockKey(`dbos.datasource_migrations.${schemaName}`)})`,
    );
  }
  const current = await readDataSourceVersion(exec, schemaName);
  if (current >= latest) return;

  const quotedSchema = quoteIdent(schemaName);
  const table = `${quotedSchema}.${DATASOURCE_MIGRATIONS_TABLE}`;
  // Check first: CREATE ... IF NOT EXISTS still demands the CREATE privilege.
  const schemaRows = await exec(
    `SELECT 1 AS present FROM pg_catalog.pg_namespace WHERE nspname = ${quoteLiteral(schemaName)}`,
  );
  if (schemaRows.length === 0) {
    await exec(`CREATE SCHEMA ${quotedSchema}`);
  }
  if (!(await pgTableExists(exec, schemaName, DATASOURCE_MIGRATIONS_TABLE))) {
    await exec(`CREATE TABLE ${table} (version BIGINT NOT NULL PRIMARY KEY)`);
  }

  for (let v = current + 1; v <= latest; v++) {
    await exec(migrations[v - 1]);
  }
  await exec(
    current === 0 ? `INSERT INTO ${table} (version) VALUES (${latest})` : `UPDATE ${table} SET version = ${latest}`,
  );
}

/** Throw unless the data source schema is migrated, creating and changing nothing. */
export async function verifyDataSourcePG(
  exec: DataSourceSQLExecutor,
  schemaName: string = 'dbos',
  dataSourceName?: string,
): Promise<void> {
  const current = await readDataSourceVersion(exec, schemaName);
  const latest = getDataSourceMigrationsPG(schemaName).length;
  // A schema ahead of this build belongs to a newer peer, which migration also tolerates.
  if (current < latest) {
    const which = dataSourceName ? `Data source '${dataSourceName}'` : 'Data source';
    throw new DBOSInitializationError(
      `${which} schema '${schemaName}' is at transaction schema version ${current}, but this version of DBOS ` +
        `requires ${latest}. This data source is configured with runMigrations disabled, so it will not migrate it: ` +
        `either migrate it out of band (the data source's static \`initializeDBOSSchema\`) or enable runMigrations.`,
    );
  }
}

/** The minimal grants a data source needs at runtime: read the version, read and write checkpoints. */
export function getDataSourcePermissionsSQL(schemaName: string, roleName: string): string[] {
  const quotedSchema = quoteIdent(schemaName);
  const quotedRole = quoteIdent(roleName);
  return [
    `GRANT USAGE ON SCHEMA ${quotedSchema} TO ${quotedRole}`,
    `GRANT SELECT, INSERT, DELETE ON ${quotedSchema}.transaction_completion TO ${quotedRole}`,
    `GRANT SELECT ON ${quotedSchema}.${DATASOURCE_MIGRATIONS_TABLE} TO ${quotedRole}`,
  ];
}

/** Migrate the data source schema in one transaction, then grant `applicationRole` its runtime permissions. */
export async function initializeDataSourceSchemaPG(
  exec: DataSourceSQLExecutor,
  schemaName: string = 'dbos',
  applicationRole?: string,
): Promise<void> {
  await migrateDataSourcePG(exec, schemaName);
  if (applicationRole) {
    for (const stmt of getDataSourcePermissionsSQL(schemaName, applicationRole)) {
      await exec(stmt);
    }
  }
}

const SQLSTATE_PATTERN = /^[0-9A-Z]{5}$/;

function getPGErrorCode(error: unknown): string | undefined {
  // Some clients wrap the driver's error, so follow the cause chain to find the code.
  for (let depth = 0, e = error; e && typeof e === 'object' && depth < 10; depth++) {
    // Only a SQLSTATE ends the search, so a wrapper's own code cannot shadow the driver's.
    const code = (e as { code?: unknown }).code;
    if (typeof code === 'string' && SQLSTATE_PATTERN.test(code)) {
      return code;
    }
    e = (e as { cause?: unknown }).cause;
  }
  return undefined;
}

export function isPGRetriableTransactionError(error: unknown): boolean {
  return getPGErrorCode(error) === '40001';
}

export function isPGKeyConflictError(error: unknown): boolean {
  return getPGErrorCode(error) === '23505';
}
