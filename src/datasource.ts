import { context, SpanStatusCode, trace } from '@opentelemetry/api';
import { Span } from '@opentelemetry/sdk-trace-base';
import { functionIDGetIncrement, getNextWFID, runWithDataSourceContext } from './context';
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
import { DBOSInvalidWorkflowTransitionError } from './error';
import { Client } from 'pg';

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

  // A way to initialize the internal schema used by the DS for transaction tracking
  //  This is only for testing, it should be documented how to create this entirely
  //  outside of DBOS.
  //   `async initializeDBOSSchema(): Promise<void>;`
  //  or, it can be static, such as:
  //   `static async initializeDBOSSchema(c: Config | Connection)`
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
  ensureDBOSIsLaunched('transactions');
  const dsn = options.dsName ?? '<default>';
  const ds = getTransactionalDataSource(dsn);

  if (!DBOS.isWithinWorkflow()) {
    if (getNextWFID(undefined)) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Invalid call to transaction '${funcName}' outside of a workflow; with directive to start a workflow.`,
      );
    }
    return await runWithDataSourceContext(0, async () => {
      return await ds.invokeTransactionFunction(options.config ?? {}, undefined, callback);
    });
  }
  if (!DBOS.isInWorkflow()) {
    throw new DBOSInvalidWorkflowTransitionError(
      `Invalid call to \`${funcName}\` inside a \`step\`, \`transaction\`, or \`procedure\``,
    );
  }

  const callnum = functionIDGetIncrement();

  const span: Span = DBOSExecutor.globalInstance!.tracer.startSpan(
    funcName,
    {
      operationUUID: DBOS.workflowID,
      operationType: OperationType.TRANSACTION,
      operationName: funcName,
      authenticatedUser: DBOS.authenticatedUser ?? '',
      assumedRole: DBOS.assumedRole ?? '',
      authenticatedRoles: DBOS.authenticatedRoles ?? [],
      // isolationLevel: txnInfo.config.isolationLevel, // TODO: Pluggable
    },
    DBOS.span,
  );

  try {
    const res = await context.with(trace.setSpan(context.active(), span), async () => {
      return await DBOSExecutor.globalInstance!.runInternalStep<T>(
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

// Transaction wrapper
export function registerTransaction<This, Args extends unknown[], Return, Config extends FunctionName>(
  dsName: string,
  func: (this: This, ...args: Args) => Promise<Return>,
  config?: Config,
): (this: This, ...args: Args) => Promise<Return> {
  const dsn = dsName ?? '<default>';

  const funcName = config?.name ?? func.name;
  const reg = wrapDBOSFunctionAndRegister(config?.ctorOrProto, config?.className, funcName, func);

  const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
    ensureDBOSIsLaunched('transactions');
    const ds = getTransactionalDataSource(dsn);
    const callFunc = reg.registeredFunction ?? reg.origFunction;

    if (!DBOS.isWithinWorkflow()) {
      if (getNextWFID(undefined)) {
        throw new DBOSInvalidWorkflowTransitionError(
          `Call to transaction '${funcName}' made without starting workflow`,
        );
      }

      return await runWithDataSourceContext(0, async () => {
        return await ds.invokeTransactionFunction(config, this, callFunc, ...rawArgs);
      });
    }

    if (DBOS.isInTransaction() || DBOS.isInStep()) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Invalid call to a `trasaction` function from within a `step` or `transaction`',
      );
    }

    const span: Span = DBOSExecutor.globalInstance!.tracer.startSpan(
      funcName,
      {
        operationUUID: DBOS.workflowID,
        operationType: OperationType.TRANSACTION,
        operationName: funcName,
        authenticatedUser: DBOS.authenticatedUser ?? '',
        assumedRole: DBOS.assumedRole ?? '',
        authenticatedRoles: DBOS.authenticatedRoles ?? [],
        // isolationLevel: txnInfo.config.isolationLevel, // TODO: Pluggable
      },
      DBOS.span,
    );

    const callnum = functionIDGetIncrement();
    try {
      const res = await context.with(trace.setSpan(context.active(), span), async () => {
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

export interface CheckSchemaInstallationReturn {
  schema_exists: number;
  table_exists: number;
}

export const checkSchemaInstallationPG = `
SELECT
  EXISTS (
    SELECT 1
    FROM information_schema.schemata
    WHERE schema_name = 'dbos'
  ) AS schema_exists,
  EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'dbos'
      AND table_name = 'transaction_completion'
  ) AS table_exists;
`;

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

function getPGErrorCode(error: unknown): string | undefined {
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

// The authoritative implementation for dropping and creating databases.

/**
 * The logical thing to provide is the name of the DB to drop (`dbToDrop`) and a connection string with permission (`adminUrl`)
 * However, you can specify `urlToDrop` and we will do our best to find a way to connect and drop it.
 */
export interface DropDatabaseOptions {
  /** Name of the database to drop */
  dbToDrop?: string;
  /** URL of the database to drop */
  urlToDrop?: string;
  /** Admin/alternate DB URL on the same server. If omitted, we'll try `<urlToDrop but with /postgres>` */
  adminUrl?: string;
  /** Optional logger (default: console.log) */
  logger?: (msg: string) => void;
  /** Also try /template1 if /postgres fails (default: true) */
  tryTemplate1Fallback?: boolean;
}

/**
 * Result of a `dropPostgresDatabase` call.
 * Status of `dropped` or `did_not_exist` is a "success",
 *  from the perspective that we are completely sure the DB is not there at the end.
 * Status of `failed` means that the DB still exists,  or we cannot say.
 *  This means "failure" in the sense that the postcondition of a nonexistent DB is not verified.
 */
export type DropDatabaseResult =
  | { status: 'dropped' | 'did_not_exist'; notes: string[]; message: string }
  | { status: 'failed'; notes: string[]; hint?: string; message: string };

/**
 * Drop a postgres database from a postgres server.  This requires a target DB name,
 *  and a way to connect to its server with privileges to issue the drop.  See `opts`.
 * Environment variables are not currently considered.
 *
 * @param opts - Options for connecting to DB and issuing the drop
 * @returns `DropDatabaseResult` indicating success, failures, and any notes or hints
 */
export async function dropPGDatabase(opts: DropDatabaseOptions = {}): Promise<DropDatabaseResult> {
  if (!opts.urlToDrop && !opts.dbToDrop) {
    throw new TypeError(`dropPGDatabase requires a target database name or URL to DROP`);
  }

  const notes: string[] = [];
  const log = (msg: string) => {
    notes.push(msg);
    (opts.logger ?? console.log)(msg);
  };

  function fail(msg: string, hint?: string): DropDatabaseResult {
    log(`FAIL: ${msg}${hint ? ` | HINT: ${hint}` : ''}`);
    return { message: msg, status: 'failed', notes, hint };
  }

  const adminUrl = opts.adminUrl ?? (opts.urlToDrop ? deriveDatabaseUrl(opts.urlToDrop, 'postgres') : undefined);

  if (!adminUrl) {
    // We could consider the environment, but let's not right now.
    throw new TypeError(
      `dropPostgresDatabase requires a connection string to a database with permission to perform the DROP`,
    );
  }

  const maybeTemplate1Url = deriveDatabaseUrl(opts.urlToDrop ?? adminUrl, 'template1');
  const tryTemplate1Fallback = opts.tryTemplate1Fallback ?? true;

  const targetDb = opts.dbToDrop ?? getDbNameFromUrl(opts.urlToDrop!);
  if (!targetDb) {
    return fail('Target URL has no database name in the path (e.g., /mydb).', 'Fix the target URL and retry.');
  }

  log(`Target DB to drop: ${targetDb}`);
  log(`Admin URL (planned): ${maskDatabaseUrl(adminUrl)}`);

  // 1) Try admin connection first (best detection for existence & privileges)
  let admin = await tryConnect(adminUrl, log);
  if (!admin && tryTemplate1Fallback) {
    log(`Admin connect failed. Trying template1 as a fallback...`);
    admin = await tryConnect(maybeTemplate1Url, log);
  }

  // Helper to check DB existence via catalog (requires admin connection)
  const checkExistsViaAdmin = async (): Promise<boolean | 'unknown'> => {
    if (!admin) return 'unknown';
    const { rows } = await admin.query<{ exists: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1) AS exists`,
      [targetDb],
    );
    return rows[0]?.exists ?? false;
  };

  try {
    // If admin connected, see if the DB exists; if not, this is an early success.
    let exists: boolean | 'unknown' = 'unknown';
    if (admin) {
      exists = await checkExistsViaAdmin();
      if (exists === false) {
        log(`DB "${targetDb}" does not exist (confirmed via catalog).`);
        return { status: 'did_not_exist', notes, message: 'Success (already dropped)' };
      }
    }

    // If we couldn't connect as admin, try connecting to target to distinguish "doesn't exist" from failure to connect.
    if (!admin) {
      const probe = await connectOutcome(
        opts.urlToDrop ?? deriveDatabaseUrl(adminUrl, targetDb),
        log,
        'probe target (existence test)',
      );
      if (probe.kind === 'ok') {
        // We can reach the target DB—so it exists—but we’re connected *to* it; we cannot DROP from within.
        await probe.client.end().catch(() => {});
        return fail(
          `Database "${targetDb}" exists, but we could not establish an admin connection to drop it.`,
          `Provide an admin/alternate DB URL (same server) with privileges to DROP DATABASE`,
        );
      } else if (probe.code === '3D000') {
        log(`DB "${targetDb}" does not exist (error 3D000 while connecting). Database already does not exist.`);
        return { status: 'did_not_exist', notes, message: 'Success (already dropped)' };
      } else {
        // Ambiguous: not proven missing, no admin path to check or drop.
        return fail(
          `Could not establish any admin connection, and target connect failed with ${probe.code ?? probe.kind}.`,
          networkOrAuthHint(probe.code),
        );
      }
    }

    // 2) We have an admin connection and the DB likely exists. Check privileges upfront (nice error).
    const who = await currentIdentity(admin);
    const owner = await dbOwner(admin, targetDb);

    if (!who.isSuperuser && owner && owner !== who.user) {
      log(`Ownership check: DB owned by "${owner}", current_user is "${who.user}" (superuser=${who.isSuperuser}).`);
      // We can still try (maybe you have sufficient rights via membership), but warn early.
      log(`Warning: You might lack privileges to DROP this database unless you are the owner or superuser.`);
    }

    // 3) Attempt the drop
    try {
      log(`Attempting DROP ... WITH (FORCE).`);
      await dropWithForce(admin, targetDb);
    } catch (err) {
      const e = err as Error & { code: string };
      // If FORCE path failed due to syntax (older server), fallback once.
      if (isForceSyntaxError(e)) {
        log(`WITH (FORCE) not supported by server (syntax error). Falling back to terminate-and-drop.`);
        try {
          await terminateAndDrop(admin, targetDb, 3000, log);
        } catch (err2) {
          const e2 = err2 as Error & { code: string };
          await admin.end().catch(() => {});
          return fail(`Drop failed even after fallback: ${shortErr(e2)}`, createDropHintFromSqlState(e2?.code));
        }
      } else {
        await admin.end().catch(() => {});
        return fail(`Drop failed: ${shortErr(e)}`, createDropHintFromSqlState(e?.code));
      }
    }

    // 4) Verify postcondition
    const finalExists = await checkExistsViaAdmin();
    if (finalExists === false) {
      log(`Verified: database "${targetDb}" is gone.`);
      return { status: 'dropped', notes, message: 'Success (dropped)' };
    } else if (finalExists === true) {
      return fail(`After drop attempt, database "${targetDb}" still exists.`, `Terminate all sessions and retry.`);
    } else {
      // Unknown (shouldn't happen with admin connected)
      log(`Could not verify drop due to unexpected state.`);
      return { status: 'dropped', notes, message: 'Success (dropped)' }; // we did our best; treat as success if we didn't see errors
    }
  } finally {
    await admin?.end().catch(() => {});
  }
}

/**
 * Result of a `ensurePGDatabase` call.
 * Status of `created` or `already_exists` is a "success",
 *  from the perspective that we are completely sure the DB is there at the end.
 * Status of `failed` means that the DB still doesn't exist, or we cannot say.
 *  This is a "failure" from the sense that the postcondition of an existing DB is not verified.
 */
export type EnsureDatabaseResult =
  | { status: 'created' | 'already_exists'; message: string; notes: string[] }
  | { status: 'failed'; notes: string[]; message: string; hint?: string };

/**
 * The logical thing to provide is the name of the DB to ensure (`dbToEnsure`) and a connection string with permission (`adminUrl`)
 * However, you can specify `urlToEnsure` and we will do our best to find a way to connect and ensure it.
 */
export interface EnsureDatabaseOptions {
  /** Name of the database to ensure */
  dbToEnsure?: string;
  /** URL of the database to ensure */
  urlToEnsure?: string;
  /** Admin/alternate DB URL on the same server. If omitted, we'll try `<urlToEnsure but with /postgres>` */
  adminUrl?: string;
  /** Optional logger (default: console.log) */
  logger?: (msg: string) => void;
  /** Also try /template1 if /postgres fails (default: true) */
  tryTemplate1Fallback?: boolean;
}
export async function ensurePGDatabase(opts: EnsureDatabaseOptions): Promise<EnsureDatabaseResult> {
  if (!opts.urlToEnsure && !opts.dbToEnsure) {
    throw new TypeError(`ensurePGDatabase requires a target database name or URL to check`);
  }

  const notes: string[] = [];
  const log = (msg: string) => {
    notes.push(msg);
    (opts.logger ?? console.log)(msg);
  };

  function fail(msg: string, hint?: string): EnsureDatabaseResult {
    log(`FAIL: ${msg}${hint ? ` | HINT: ${hint}` : ''}`);
    return { status: 'failed', notes, hint, message: msg };
  }

  const targetDb = opts.dbToEnsure ?? getDbNameFromUrl(opts.urlToEnsure!);
  if (!targetDb) {
    return fail('Target URL has no database name in the path (e.g., /mydb).', 'Fix the target URL and retry.');
  }

  // Try a quick connect attempt first; this requires the least assumptions and has the least chance of messing us up.
  if (opts.urlToEnsure) {
    try {
      const probe = await connectOutcome(opts.urlToEnsure, log, 'probe target (existence test)');
      if (probe.kind === 'ok') {
        // We can reach the target DB, do nothing
        await probe.client.end().catch(() => {});
        return { status: 'already_exists', notes, message: 'Success (already existed)' };
      }
    } catch (e) {
      log(`Caught error probing database: (e as Error).message; attempting create.`);
    }
  }

  // At this point, we know we need an admin URL, if only as a base of the real URL
  const adminUrl = opts.adminUrl ?? (opts.urlToEnsure ? deriveDatabaseUrl(opts.urlToEnsure, 'postgres') : undefined);

  if (!adminUrl) {
    // We could consider the environment, but let's not right now.
    throw new TypeError(
      `dropPostgresDatabase requires a connection string to a database with permission to perform the DROP`,
    );
  }

  const maybeTemplate1Url = deriveDatabaseUrl(opts.urlToEnsure ?? adminUrl, 'template1');
  const tryTemplate1Fallback = opts.tryTemplate1Fallback ?? true;

  // 1) Try admin connection first (best detection for existence & privileges)
  let admin = await tryConnect(adminUrl, log);
  if (!admin && tryTemplate1Fallback) {
    log(`Admin connect failed. Trying template1 as a fallback...`);
    admin = await tryConnect(maybeTemplate1Url, log);
  }

  // Helper to check DB existence via catalog (requires admin connection)
  const checkExistsViaAdmin = async (): Promise<boolean | 'unknown'> => {
    if (!admin) return 'unknown';
    const { rows } = await admin.query<{ exists: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1) AS exists`,
      [targetDb],
    );
    return rows[0]?.exists ?? false;
  };

  try {
    // If admin connected, see if the DB exists; if so, this is an early success.
    let exists: boolean | 'unknown' = 'unknown';
    if (admin) {
      exists = await checkExistsViaAdmin();
      if (exists === true) {
        log(`DB "${targetDb}" exists (confirmed via catalog).`);
        return { status: 'already_exists', notes, message: 'Success (already existed)' };
      }
    }

    // If we couldn't connect as admin, try connecting to target to distinguish "doesn't exist" from failure to connect.
    if (!admin) {
      const dbUrl = opts.dbToEnsure ?? deriveDatabaseUrl(adminUrl, targetDb);
      const probe = await connectOutcome(dbUrl, log, 'probe target (existence test)');
      if (probe.kind === 'ok') {
        // We can reach the target DB.... via a URL derived from admin
        await probe.client.end().catch(() => {});
        log(`Probe of database ${targetDb} via ${maskDatabaseUrl(dbUrl)} succeeds.`);
        return { status: 'already_exists', notes, message: 'Success (already existed)' };
      } else {
        // Ambiguous: We do not know it to be there, and we can't make an admin connection to proceed.
        return fail(
          `Could not establish any admin connection, and target connect failed with ${probe.code ?? probe.kind}.`,
          networkOrAuthHint(probe.code),
        );
      }
    }

    // 3) Attempt the CREATE
    try {
      log(`Attempting CREATE.`);
      await createDb(admin, targetDb);
    } catch (err) {
      const e = err as Error & { code: string };
      await admin.end().catch(() => {});
      return fail(`Create failed: ${shortErr(e)}`, createDropHintFromSqlState(e?.code));
    }

    // 4) Verify postcondition
    const finalExists = await checkExistsViaAdmin();
    if (finalExists === true) {
      log(`Verified: database "${targetDb}" exists.`);
      return { status: 'created', notes, message: 'Success (created)' };
    } else if (finalExists === false) {
      return fail(`After create attempt, database "${targetDb}" does not exist still.`);
    } else {
      // Unknown (shouldn't happen with admin connected)
      log(`Could not verify creation due to unexpected state.`);
      return { status: 'created', notes, message: 'Success (unverified)' };
    }
  } finally {
    await admin?.end().catch(() => {});
  }
}

export function deriveDatabaseUrl(urlStr: string, otherDbName: string): string {
  try {
    const u = new URL(urlStr);
    u.pathname = `/${otherDbName}`;
    return u.toString();
  } catch {
    return urlStr; // best effort; connect will fail with clear message
  }
}

// The `pg` package we use does not parse the connect_timeout parameter, so we need to handle it ourselves.
export function getPGClientConfig(databaseUrl: string | URL) {
  const connectionString = typeof databaseUrl === 'string' ? databaseUrl : databaseUrl.toString();
  const timeout = getTimeout(typeof databaseUrl === 'string' ? new URL(databaseUrl) : databaseUrl);
  return {
    connectionString,
    connectionTimeoutMillis: timeout ? timeout * 1000 : 10000,
  };

  function getTimeout(url: URL) {
    try {
      const $timeout = url.searchParams.get('connect_timeout');
      return $timeout ? parseInt($timeout, 10) : undefined;
    } catch {
      // Ignore errors in parsing the connect_timeout parameter
      return undefined;
    }
  }
}

function getDbNameFromUrl(urlStr: string) {
  const u = new URL(urlStr);
  return u.pathname?.replace(/^\//, '') || '';
}

export function maskDatabaseUrl(urlStr: string): string {
  try {
    const u = new URL(urlStr);
    if (u.password) {
      const p = decodeURIComponent(u.password);
      const masked = p.length <= 2 ? p : `${p[0]}${'*'.repeat(p.length - 2)}${p[p.length - 1]}`;
      u.password = encodeURIComponent(masked);
    }
    return u.toString();
  } catch {
    return urlStr;
  }
}

async function tryConnect(url: string, log: (m: string) => void): Promise<Client | null> {
  log(`Connecting: ${maskDatabaseUrl(url)}`);
  const client = new Client(getPGClientConfig(url));
  try {
    await client.connect();
    return client;
  } catch (err) {
    const e = err as Error & { code?: string };
    log(`Connect failed: ${shortErr(e)}${e?.code ? ` (code ${e.code})` : ''}`);
    try {
      await client.end();
    } catch {}
    return null;
  }
}

async function connectOutcome(
  url: string,
  log: (m: string) => void,
  label: string,
): Promise<{ kind: 'ok'; client: Client } | { kind: 'error'; code?: string; message: string }> {
  log(`Connecting to ${label}: ${maskDatabaseUrl(url)}`);
  const client = new Client(getPGClientConfig(url));
  try {
    await client.connect();
    return { kind: 'ok', client };
  } catch (err) {
    const e = err as Error & { code?: string };
    try {
      await client.end();
    } catch {}
    return { kind: 'error', code: e?.code, message: e?.message ?? String(e) };
  }
}

function shortErr(e: Error): string {
  const m = e?.message ?? String(e);
  return m.length > 500 ? `${m.slice(0, 500)}…` : m;
}

async function currentIdentity(admin: Client): Promise<{ user: string; isSuperuser: boolean }> {
  const { rows: userRows } = await admin.query<{ user: string }>(`SELECT current_user AS user`);
  const user = userRows[0]?.user ?? '';
  const { rows: roleRows } = await admin.query<{ rolsuper: boolean }>(
    `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`,
  );
  return { user, isSuperuser: !!roleRows[0]?.rolsuper };
}

async function dbOwner(admin: Client, dbName: string): Promise<string | null> {
  const { rows } = await admin.query<{ owner: string }>(
    `SELECT r.rolname AS owner
     FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba
     WHERE d.datname = $1`,
    [dbName],
  );
  return rows[0]?.owner ?? null;
}

function ident(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

async function dropWithForce(admin: Client, dbName: string): Promise<void> {
  await admin.query(`DROP DATABASE IF EXISTS ${ident(dbName)} WITH (FORCE)`);
}

async function createDb(admin: Client, dbName: string): Promise<void> {
  await admin.query(`CREATE DATABASE ${ident(dbName)}`);
}

function isForceSyntaxError(e: { code?: string; message?: string }): boolean {
  return e?.code === '42601' /* syntax_error */ || /WITH\s*\(\s*FORCE\s*\)/i.test(e?.message ?? '');
}

async function terminateAndDrop(
  admin: Client,
  dbName: string,
  settleMs: number,
  log: (m: string) => void,
): Promise<void> {
  // Prevent new connections (best-effort; ignore errors)
  try {
    await admin.query(`ALTER DATABASE ${ident(dbName)} WITH ALLOW_CONNECTIONS = false`);
  } catch (e) {
    log(`ALTER DATABASE ... ALLOW_CONNECTIONS=false failed (continuing): ${shortErr(e as Error)}`);
  }
  // Terminate existing sessions
  await admin.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [dbName],
  );
  if (settleMs > 0) {
    log(`Waiting ${settleMs}ms for backends to terminate...`);
    await new Promise((r) => setTimeout(r, settleMs));
  }
  // Try DROP, and if "being accessed" shows up, retry once after an extra wait
  try {
    await admin.query(`DROP DATABASE IF EXISTS ${ident(dbName)}`);
  } catch (err) {
    const e = err as Error & { code: string };
    if (e?.code === '55006') {
      log(`DB still "being accessed by other users"; retrying after extra wait...`);
      await new Promise((r) => setTimeout(r, Math.max(1000, settleMs)));
      await admin.query(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
        [dbName],
      );
      await admin.query(`DROP DATABASE IF EXISTS ${ident(dbName)}`);
    } else {
      throw e;
    }
  }
}

export function networkOrAuthHint(code?: string): string | undefined {
  if (!code) return;
  switch (code) {
    case 'ECONNREFUSED':
      return 'Server not reachable. Check host/port or firewall.';
    case 'ENOTFOUND':
      return 'Hostname not resolvable. Check DNS/host.';
    case 'ETIMEDOUT':
      return 'Connection timed out. Check network/firewall.';
    case '28P01':
      return 'Invalid password.';
    case '28000':
      return 'Authentication rejected (pg_hba.conf or method).';
    default: {
      if (code.substring(0, 2) === '28') return 'Other connection security error';
      return undefined;
    }
  }
}

export function createDropHintFromSqlState(code?: string): string | undefined {
  switch (code?.substring(0, 5)) {
    case '42501':
      return 'Insufficient privilege. You must be the owner or a superuser.';
    case '53300':
      return 'Too many connections to server; free some slots.';
    default:
      break;
  }
  switch (code?.substring(0, 2)) {
    case '42':
      return 'Syntax error or access rule violation.';
    case '3D':
      return 'Target database does not exist (already gone).';
    case '55':
      return 'Database is in use. Terminate sessions or use PG13+ WITH (FORCE).';
    case '53':
      return 'Insufficient resources.';
    default:
      return undefined;
  }
}
