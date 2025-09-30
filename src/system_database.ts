import { DBOSExecutor, DBOSExternalState } from './dbos-executor';
import { DatabaseError, Pool, PoolClient, Notification, Client, PoolConfig } from 'pg';
import {
  DBOSWorkflowConflictError,
  DBOSNonExistentWorkflowError,
  DBOSMaxRecoveryAttemptsExceededError,
  DBOSConflictingWorkflowError,
  DBOSUnexpectedStepError,
  DBOSWorkflowCancelledError,
  DBOSQueueDuplicatedError,
  DBOSInitializationError,
} from './error';
import { GetPendingWorkflowsOutput, GetQueuedWorkflowsInput, GetWorkflowsInput, StatusString } from './workflow';
import {
  notifications,
  operation_outputs,
  workflow_status,
  workflow_events,
  event_dispatch_kv,
} from '../schemas/system_db_schema';
import { globalParams, cancellableSleep, INTERNAL_QUEUE_NAME, sleepms } from './utils';
import { GlobalLogger } from './telemetry/logs';
import { WorkflowQueue } from './wfqueue';
import { randomUUID } from 'crypto';
import { getClientConfig } from './utils';
import { connectToPGAndReportOutcome, ensurePGDatabase, maskDatabaseUrl } from './database_utils';
import { runSysMigrationsPg } from './sysdb_migrations/migration_runner';
import { allMigrations } from './sysdb_migrations/internal/migrations';

/* Result from Sys DB */
export interface SystemDatabaseStoredResult {
  output?: string | null;
  error?: string | null;
  cancelled?: boolean;
  childWorkflowID?: string | null;
  functionName?: string;
}

export const DBOS_FUNCNAME_SEND = 'DBOS.send';
export const DBOS_FUNCNAME_RECV = 'DBOS.recv';
export const DBOS_FUNCNAME_SETEVENT = 'DBOS.setEvent';
export const DBOS_FUNCNAME_GETEVENT = 'DBOS.getEvent';
export const DBOS_FUNCNAME_SLEEP = 'DBOS.sleep';
export const DBOS_FUNCNAME_GETSTATUS = 'getStatus';
export const DBOS_FUNCNAME_WRITESTREAM = 'DBOS.writeStream';
export const DBOS_FUNCNAME_CLOSESTREAM = 'DBOS.closeStream';
export const DEFAULT_POOL_SIZE = 10;

export const DBOS_STREAM_CLOSED_SENTINEL = '__DBOS_STREAM_CLOSED__';

/**
 * General notes:
 *   The responsibilities of the `SystemDatabase` are to store data for workflows, and
 *     associated steps, transactions, messages, and events.  The system DB is
 *     also the IPC mechanism that performs notifications when things change, for
 *     example a receive is unblocked when a send occurs, or a cancel interrupts
 *     the receive.
 *   The `SystemDatabase` expects values in inputs/outputs/errors to be JSON.  However,
 *     the serialization process of turning data into JSON or converting it back, should
 *     be done elsewhere (executor), as it may require application-specific logic or extensions.
 */
export interface SystemDatabase {
  init(debugMode?: boolean): Promise<void>;
  destroy(): Promise<void>;

  initWorkflowStatus(
    initStatus: WorkflowStatusInternal,
    maxRetries?: number,
  ): Promise<{ status: string; deadlineEpochMS?: number }>;
  recordWorkflowOutput(workflowID: string, status: WorkflowStatusInternal): Promise<void>;
  recordWorkflowError(workflowID: string, status: WorkflowStatusInternal): Promise<void>;

  getPendingWorkflows(executorID: string, appVersion: string): Promise<GetPendingWorkflowsOutput[]>;

  // If there is no record, res will be undefined;
  //  otherwise will be defined (with potentially undefined contents)
  getOperationResultAndThrowIfCancelled(
    workflowID: string,
    functionID: number,
  ): Promise<SystemDatabaseStoredResult | undefined>;
  getAllOperationResults(workflowID: string): Promise<operation_outputs[]>;
  recordOperationResult(
    workflowID: string,
    functionID: number,
    functionName: string,
    checkConflict: boolean,
    options?: {
      childWorkflowID?: string | null;
      output?: string | null;
      error?: string | null;
    },
  ): Promise<void>;

  getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatusInternal | null>;
  awaitWorkflowResult(
    workflowID: string,
    timeoutSeconds?: number,
    callerID?: string,
    timerFuncID?: number,
  ): Promise<SystemDatabaseStoredResult | undefined>;

  // Workflow management
  setWorkflowStatus(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    resetRecoveryAttempts: boolean,
  ): Promise<void>;
  cancelWorkflow(workflowID: string): Promise<void>;
  resumeWorkflow(workflowID: string): Promise<void>;
  forkWorkflow(
    workflowID: string,
    startStep: number,
    options?: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number },
  ): Promise<string>;
  checkIfCanceled(workflowID: string): Promise<void>;
  registerRunningWorkflow(workflowID: string, workflowPromise: Promise<unknown>): void;
  awaitRunningWorkflows(): Promise<void>; // Use in clean shutdown

  // Queues
  clearQueueAssignment(workflowID: string): Promise<boolean>;
  getDeduplicatedWorkflow(queueName: string, deduplicationID: string): Promise<string | null>;

  findAndMarkStartableWorkflows(queue: WorkflowQueue, executorID: string, appVersion: string): Promise<string[]>;

  // Actions w/ durable records and notifications
  durableSleepms(workflowID: string, functionID: number, duration: number): Promise<void>;

  send(
    workflowID: string,
    functionID: number,
    destinationID: string,
    message: string | null,
    topic?: string,
  ): Promise<void>;
  recv(
    workflowID: string,
    functionID: number,
    timeoutFunctionID: number,
    topic?: string,
    timeoutSeconds?: number,
  ): Promise<string | null>;

  setEvent(workflowID: string, functionID: number, key: string, value: string | null): Promise<void>;
  getEvent(
    workflowID: string,
    key: string,
    timeoutSeconds: number,
    callerWorkflow?: {
      workflowID: string;
      functionID: number;
      timeoutFunctionID: number;
    },
  ): Promise<string | null>;

  // Event receiver state queries / updates
  // An event dispatcher may keep state in the system database
  //   The 'service' should be unique to the event receiver keeping state, to separate from others
  //   The 'workflowFnName' workflow function name should be the fully qualified / unique function name dispatched
  //   The 'key' field allows multiple records per service / workflow function
  //   The service+workflowFnName+key uniquely identifies the record, which is associated with:
  //     'value' - a value set by the event receiver service; this string may be a JSON to keep complex details
  //     A version, either as a sequence number (long integer), or as a time (high precision floating point).
  //       If versions are in use, any upsert is discarded if the version field is less than what is already stored.
  //       The upsert returns the current record, which is useful if it is more recent.
  getEventDispatchState(service: string, workflowFnName: string, key: string): Promise<DBOSExternalState | undefined>;
  upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState>;

  // Streaming
  writeStreamFromWorkflow(workflowID: string, functionID: number, key: string, value: unknown): Promise<void>;
  writeStreamFromStep(workflowID: string, key: string, value: unknown): Promise<void>;
  closeStream(workflowID: string, functionID: number, key: string): Promise<void>;
  readStream(workflowID: string, key: string, offset: number): Promise<unknown>;

  // Workflow management
  listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatusInternal[]>;
  listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatusInternal[]>;
  garbageCollect(cutoffEpochTimestampMs?: number, rowsThreshold?: number): Promise<void>;
}

// For internal use, not serialized status.
export interface WorkflowStatusInternal {
  workflowUUID: string;
  status: string;
  workflowName: string;
  workflowClassName: string;
  workflowConfigName: string;
  queueName?: string;
  authenticatedUser: string;
  output: string | null;
  error: string | null; // Serialized error
  input: string | null;
  assumedRole: string;
  authenticatedRoles: string[];
  request: object;
  executorId: string;
  applicationVersion?: string;
  applicationID: string;
  createdAt: number;
  updatedAt?: number;
  recoveryAttempts?: number;
  timeoutMS?: number | null;
  deadlineEpochMS?: number;
  deduplicationID?: string;
  priority: number;
}

export interface EnqueueOptions {
  // Unique ID for deduplication on a queue
  deduplicationID?: string;
  // Priority of the workflow on the queue, starting from 1 ~ 2,147,483,647. Default 0 (highest priority).
  priority?: number;
}

export interface ExistenceCheck {
  exists: boolean;
}

export async function grantDbosSchemaPermissions(
  databaseUrl: string,
  roleName: string,
  logger: GlobalLogger,
): Promise<void> {
  logger.info(`Granting permissions for DBOS schema to ${roleName}`);

  const client = new Client(getClientConfig(databaseUrl));
  await client.connect();

  try {
    // Grant usage on the dbos schema
    const grantUsageSql = `GRANT USAGE ON SCHEMA dbos TO "${roleName}"`;
    logger.info(grantUsageSql);
    await client.query(grantUsageSql);

    // Grant all privileges on all existing tables in dbos schema (includes views)
    const grantTablesSql = `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbos TO "${roleName}"`;
    logger.info(grantTablesSql);
    await client.query(grantTablesSql);

    // Grant all privileges on all sequences in dbos schema
    const grantSequencesSql = `GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbos TO "${roleName}"`;
    logger.info(grantSequencesSql);
    await client.query(grantSequencesSql);

    // Grant execute on all functions and procedures in dbos schema
    const grantFunctionsSql = `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbos TO "${roleName}"`;
    logger.info(grantFunctionsSql);
    await client.query(grantFunctionsSql);

    // Grant default privileges for future objects in dbos schema
    const alterTablesSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON TABLES TO "${roleName}"`;
    logger.info(alterTablesSql);
    await client.query(alterTablesSql);

    const alterSequencesSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT ALL ON SEQUENCES TO "${roleName}"`;
    logger.info(alterSequencesSql);
    await client.query(alterSequencesSql);

    const alterFunctionsSql = `ALTER DEFAULT PRIVILEGES IN SCHEMA dbos GRANT EXECUTE ON FUNCTIONS TO "${roleName}"`;
    logger.info(alterFunctionsSql);
    await client.query(alterFunctionsSql);
  } catch (e) {
    logger.error(`Failed to grant permissions to role ${roleName}: ${(e as Error).message}`);
    throw e;
  } finally {
    await client.end();
  }
}

export async function ensureSystemDatabase(sysDbUrl: string, logger: GlobalLogger, debugMode: boolean = false) {
  if (!debugMode) {
    const res = await ensurePGDatabase({
      urlToEnsure: sysDbUrl,
      logger: (msg: string) => logger.debug(msg),
    });
    if (res.status === 'failed') {
      logger.warn(
        `Database could not be verified / created: ${maskDatabaseUrl(sysDbUrl)}: ${res.message} ${res.hint ?? ''}\n  ${res.notes.join('\n')}`,
      );
    }
  }

  const cconnect = await connectToPGAndReportOutcome(sysDbUrl, () => {}, 'System Database');
  if (cconnect.result !== 'ok') {
    logger.warn(
      `Unable to connect to system database at ${maskDatabaseUrl(sysDbUrl)}${debugMode ? ' (debug mode)' : ''}
      ${cconnect.message}: (${cconnect.code ? cconnect.code : 'connectivity problem'})`,
    );
    throw new DBOSInitializationError(`Unable to connect to system database at ${maskDatabaseUrl(sysDbUrl)}`);
  }

  try {
    if (debugMode) {
      logger.info(`Skipping system database migration in debug mode.`);
      return;
    }

    await runSysMigrationsPg(cconnect.client, allMigrations, {
      onWarn: (e: string) => logger.info(e),
    });
  } finally {
    try {
      await cconnect.client.end();
    } catch (e) {}
  }
}

class NotificationMap<T> {
  map: Map<string, Map<number, (event?: T) => void>> = new Map();
  curCK: number = 0;

  registerCallback(key: string, cb: (event?: T) => void) {
    if (!this.map.has(key)) {
      this.map.set(key, new Map());
    }
    const ck = this.curCK++;
    this.map.get(key)!.set(ck, cb);
    return { key, ck };
  }

  deregisterCallback(k: { key: string; ck: number }) {
    if (!this.map.has(k.key)) return;
    const sm = this.map.get(k.key)!;
    if (!sm.has(k.ck)) return;
    sm.delete(k.ck);
    if (sm.size === 0) {
      this.map.delete(k.key);
    }
  }

  callCallbacks(key: string, event?: T) {
    if (!this.map.has(key)) return;
    const sm = this.map.get(key)!;
    for (const cb of sm.values()) {
      cb(event);
    }
  }
}

interface InsertWorkflowResult {
  recovery_attempts: number;
  status: string;
  name: string;
  class_name: string;
  config_name: string;
  queue_name: string | null;
  workflow_deadline_epoch_ms: number | null;
}

async function insertWorkflowStatus(
  client: PoolClient,
  initStatus: WorkflowStatusInternal,
): Promise<InsertWorkflowResult> {
  try {
    const { rows } = await client.query<InsertWorkflowResult>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_status (
        workflow_uuid,
        status,
        name,
        class_name,
        config_name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        request,
        executor_id,
        application_version,
        application_id,
        created_at,
        recovery_attempts,
        updated_at,
        workflow_timeout_ms,
        workflow_deadline_epoch_ms,
        inputs,
        deduplication_id,
        priority
      ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
      ON CONFLICT (workflow_uuid)
        DO UPDATE SET
          recovery_attempts = CASE 
            WHEN workflow_status.status != '${StatusString.ENQUEUED}' 
            THEN workflow_status.recovery_attempts + 1 
            ELSE workflow_status.recovery_attempts 
          END,
          updated_at = EXCLUDED.updated_at,
          executor_id = CASE 
            WHEN EXCLUDED.status != '${StatusString.ENQUEUED}' 
            THEN EXCLUDED.executor_id 
            ELSE workflow_status.executor_id 
          END
        RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_deadline_epoch_ms`,
      [
        initStatus.workflowUUID,
        initStatus.status,
        initStatus.workflowName,
        initStatus.workflowClassName,
        initStatus.workflowConfigName,
        initStatus.queueName ?? null,
        initStatus.authenticatedUser,
        initStatus.assumedRole,
        JSON.stringify(initStatus.authenticatedRoles),
        JSON.stringify(initStatus.request),
        initStatus.executorId,
        initStatus.applicationVersion ?? null,
        initStatus.applicationID,
        initStatus.createdAt,
        initStatus.status === StatusString.ENQUEUED ? 0 : 1,
        initStatus.updatedAt ?? Date.now(),
        initStatus.timeoutMS ?? null,
        initStatus.deadlineEpochMS ?? null,
        initStatus.input ?? null,
        initStatus.deduplicationID ?? null,
        initStatus.priority,
      ],
    );
    if (rows.length === 0) {
      throw new Error(`Attempt to insert workflow ${initStatus.workflowUUID} failed`);
    }
    return rows[0];
  } catch (error) {
    const err: DatabaseError = error as DatabaseError;
    if (err.code === '23505') {
      throw new DBOSQueueDuplicatedError(
        initStatus.workflowUUID,
        initStatus.queueName ?? '',
        initStatus.deduplicationID ?? '',
      );
    }
    throw error;
  }
}

async function getWorkflowStatusValue(client: PoolClient, workflowID: string): Promise<string | undefined> {
  const { rows } = await client.query<{ status: string }>(
    `SELECT status FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status WHERE workflow_uuid=$1`,
    [workflowID],
  );
  return rows.length === 0 ? undefined : rows[0].status;
}

async function updateWorkflowStatus(
  client: PoolClient,
  workflowID: string,
  status: (typeof StatusString)[keyof typeof StatusString],
  options: {
    update?: {
      output?: string | null;
      error?: string | null;
      resetRecoveryAttempts?: boolean;
      queueName?: string | null;
      resetDeadline?: boolean;
      resetDeduplicationID?: boolean;
      resetStartedAtEpochMs?: boolean;
    };
    where?: {
      status?: (typeof StatusString)[keyof typeof StatusString];
    };
    throwOnFailure?: boolean;
  } = {},
): Promise<void> {
  let setClause = `SET status=$2, updated_at=$3`;
  let whereClause = `WHERE workflow_uuid=$1`;
  const args: (string | number | undefined)[] = [workflowID, status, Date.now()];

  const update = options.update ?? {};
  if (update.output) {
    const param = args.push(update.output);
    setClause += `, output=$${param}`;
  }

  if (update.error) {
    const param = args.push(update.error);
    setClause += `, error=$${param}`;
  }

  if (update.resetRecoveryAttempts) {
    setClause += `, recovery_attempts = 0`;
  }

  if (update.resetDeadline) {
    setClause += `, workflow_deadline_epoch_ms = NULL`;
  }

  if (update.queueName !== undefined) {
    const param = args.push(update.queueName ?? undefined);
    setClause += `, queue_name=$${param}`;
  }

  if (update.resetDeduplicationID) {
    setClause += `, deduplication_id = NULL`;
  }

  if (update.resetStartedAtEpochMs) {
    setClause += `, started_at_epoch_ms = NULL`;
  }

  const where = options.where ?? {};
  if (where.status) {
    const param = args.push(where.status);
    whereClause += ` AND status=$${param}`;
  }

  const result = await client.query<workflow_status>(
    `UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status ${setClause} ${whereClause}`,
    args,
  );

  const throwOnFailure = options.throwOnFailure ?? true;
  if (throwOnFailure && result.rowCount !== 1) {
    throw new DBOSWorkflowConflictError(`Attempt to record transition of nonexistent workflow ${workflowID}`);
  }
}

async function recordOperationResult(
  client: PoolClient,
  workflowID: string,
  functionID: number,
  functionName: string,
  checkConflict: boolean,
  options: {
    childWorkflowID?: string | null;
    output?: string | null;
    error?: string | null;
  } = {},
): Promise<void> {
  try {
    await client.query<operation_outputs>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs
       (workflow_uuid, function_id, output, error, function_name, child_workflow_id)
       VALUES ($1, $2, $3, $4, $5, $6)
       ${checkConflict ? '' : ' ON CONFLICT DO NOTHING'};`,
      [
        workflowID,
        functionID,
        options.output ?? null,
        options.error ?? null,
        functionName,
        options.childWorkflowID ?? null,
      ],
    );
  } catch (error) {
    const err: DatabaseError = error as DatabaseError;
    if (err.code === '40001' || err.code === '23505') {
      // Serialization and primary key conflict (Postgres).
      throw new DBOSWorkflowConflictError(workflowID);
    } else {
      throw err;
    }
  }
}

function mapWorkflowStatus(row: workflow_status): WorkflowStatusInternal {
  return {
    workflowUUID: row.workflow_uuid,
    status: row.status,
    workflowName: row.name,
    output: row.output ? row.output : null,
    error: row.error ? row.error : null,
    workflowClassName: row.class_name ?? '',
    workflowConfigName: row.config_name ?? '',
    queueName: row.queue_name ?? undefined,
    authenticatedUser: row.authenticated_user,
    assumedRole: row.assumed_role,
    authenticatedRoles: JSON.parse(row.authenticated_roles) as string[],
    request: row.request ? (JSON.parse(row.request) as object) : {},
    executorId: row.executor_id,
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
    applicationVersion: row.application_version,
    applicationID: row.application_id,
    recoveryAttempts: Number(row.recovery_attempts),
    input: row.inputs ? row.inputs : null,
    timeoutMS: row.workflow_timeout_ms ? Number(row.workflow_timeout_ms) : undefined,
    deadlineEpochMS: row.workflow_deadline_epoch_ms ? Number(row.workflow_deadline_epoch_ms) : undefined,
    deduplicationID: row.deduplication_id ?? undefined,
    priority: row.priority ?? 0,
  };
}

type AnyErr = { code?: string; errno?: number; message?: string; stack?: string; cause?: unknown };

// SQLSTATE classes/codes that are generally safe to retry
// https://www.postgresql.org/docs/current/errcodes-appendix.html
const RETRY_SQLSTATE_PREFIXES = new Set([
  '08', // Connection Exception
  '53', // Insufficient Resources
  '57', // Operator Intervention (e.g. admin_shutdown, cannot_connect_now)
]);

const RETRY_SQLSTATE_CODES = new Set([
  '40003', // statement_completion_unknown
]);

// Node.js transient network error codes (system call level)
const RETRY_NODE_ERRNOS = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ETIMEDOUT',
  'ECONNABORTED',
]);

function isPgDatabaseError(e: AnyErr): e is DatabaseError & AnyErr {
  // DatabaseError has 'code' (SQLSTATE)
  return !!e && typeof e === 'object' && typeof e.code === 'string' && e.code.length === 5;
}

function sqlStateLooksRetryable(sqlstate: string | undefined): boolean {
  if (!sqlstate) return false;
  if (RETRY_SQLSTATE_CODES.has(sqlstate)) return true;
  const prefix = sqlstate.toString().slice(0, 2);
  return RETRY_SQLSTATE_PREFIXES.has(prefix);
}

function nodeErrnoLooksRetryable(e: AnyErr): boolean {
  const code = e.code;
  return !!code && RETRY_NODE_ERRNOS.has(code);
}

function messageLooksRetryable(msg: string): boolean {
  const m = msg.toLowerCase();
  return (
    msg.includes('ECONNREFUSED') ||
    msg.includes('ECONNRESET') ||
    m.includes('connection timeout') ||
    m.includes('server closed the connection') ||
    m.includes('connection terminated unexpectedly') ||
    m.includes('client has encountered a connection error') ||
    m.includes('timeout exceeded when trying to connect') ||
    m.includes('could not connect to server')
  );
}

function* unwrapErrors(e: unknown): Generator<unknown, void, void> {
  // Walk through AggregateError.errors and cause chains
  const queue: unknown[] = [e];
  const seen = new Set<unknown>();
  while (queue.length) {
    const cur = queue.shift()!;
    if (cur && typeof cur === 'object') {
      if (seen.has(cur)) continue;
      seen.add(cur);
      // AggregateError (native and some libs)
      const ae = cur as { errors?: unknown[] };
      if (Array.isArray(ae.errors)) queue.push(...ae.errors);
      // cause chain
      const withCause = cur as { cause?: unknown };
      if (withCause.cause) queue.push(withCause.cause);
      // some libs wrap in { error }
      const wrapped = cur as { error?: unknown };
      if (wrapped.error) queue.push(wrapped.error);
    }
    yield cur;
  }
}

// "What could possibly go wrong?"
function retriablePostgresException(err: unknown): boolean {
  // Dig into AggregateErrors of various types
  for (const e of unwrapErrors(err)) {
    const anyErr = e as AnyErr;

    // For Postgres errors, check the code
    if (isPgDatabaseError(anyErr) && sqlStateLooksRetryable(anyErr.code)) {
      return true;
    }

    // Look for node-like retriable errors
    if (nodeErrnoLooksRetryable(anyErr)) {
      return true;
    }

    // Also, check for network issues in the string
    if (e instanceof Error) {
      if (e.stack && messageLooksRetryable(e.stack)) return true;
      if (e.message && messageLooksRetryable(e.message)) return true;
    } else {
      if (messageLooksRetryable(String(e))) return true;
    }
  }
  return false;
}

/**
 * If a workflow encounters a database connection issue while performing an operation,
 * block the workflow and retry the operation until it reconnects and succeeds.
 * In other words, if DBOS loses its database connection, everything pauses until the connection is recovered,
 * trading off availability for correctness.
 */
function dbRetry(
  options: {
    initialBackoff?: number;
    maxBackoff?: number;
  } = {},
) {
  const { initialBackoff = 1.0, maxBackoff = 60.0 } = options;
  return function <T extends (...args: never[]) => Promise<unknown>>(
    target: unknown,
    propertyName: string,
    descriptor: TypedPropertyDescriptor<T>,
  ): TypedPropertyDescriptor<T> {
    const method = descriptor.value!;
    descriptor.value = async function (this: never, ...args: never): Promise<unknown> {
      let retries = 0;
      let backoff = initialBackoff;
      while (true) {
        try {
          return await method.apply(this, args);
        } catch (e) {
          if (retriablePostgresException(e)) {
            retries++;
            // Calculate backoff with jitter
            const actualBackoff = backoff * (0.5 + Math.random());
            DBOSExecutor.globalInstance?.logger.warn(
              `Database connection failed: ${e instanceof Error ? e.message : String(e)}. ` +
                `Retrying in ${actualBackoff.toFixed(2)}s (attempt ${retries})`,
            );
            // Sleep with backoff
            await sleepms(actualBackoff * 1000); // Convert to milliseconds
            // Increase backoff for next attempt (exponential)
            backoff = Math.min(backoff * 2, maxBackoff);
          } else {
            throw e;
          }
        }
      }
    } as T;
    return descriptor;
  };
}

export class PostgresSystemDatabase implements SystemDatabase {
  readonly pool: Pool;

  /*
   * Generally, notifications are asynchronous.  One should:
   *  Subscribe to updates
   *  Read the database item in question
   *  In response to updates, re-read the database item
   *  Unsubscribe at the end
   * The notification mechanism is reliable in the sense that it will eventually deliver updates
   *  or the DB connection will get dropped.  The right thing to do if you lose connectivity to
   *  the system DB is to exit the process and go through recovery... system DB writes, notifications,
   *  etc may not have completed correctly, and recovery is the way to rebuild in-memory state.
   *
   * NOTE:
   * PG Notifications are not fully reliable.
   *   Dropped connections are recoverable - you just need to restart and scan everything.
   *      (The whole VM being the logical choice, so workflows can recover from any write failures.)
   *   The real problem is, if the pipes out of the server are full... then notifications can be
   *     dropped, and only the PG server log may note it.  For those reasons, we do occasional polling
   */
  notificationsClient: PoolClient | null = null;
  dbPollingIntervalResultMs: number = 1000;
  dbPollingIntervalEventMs: number = 10000;
  shouldUseDBNotifications: boolean = true;
  readonly notificationsMap: NotificationMap<void> = new NotificationMap();
  readonly workflowEventsMap: NotificationMap<void> = new NotificationMap();
  readonly cancelWakeupMap: NotificationMap<void> = new NotificationMap();
  customPool: boolean = false;

  readonly runningWorkflowMap: Map<string, Promise<unknown>> = new Map(); // Map from workflowID to workflow promise
  readonly workflowCancellationMap: Map<string, boolean> = new Map(); // Map from workflowID to its cancellation status.

  constructor(
    readonly systemDatabaseUrl: string,
    readonly logger: GlobalLogger,
    sysDbPoolSize: number = DEFAULT_POOL_SIZE,
    systemDatabasePool?: Pool,
  ) {
    if (systemDatabasePool) {
      this.pool = systemDatabasePool;
      this.customPool = true;
    } else {
      const systemPoolConfig: PoolConfig = {
        ...getClientConfig(systemDatabaseUrl),
        // This sets the application_name column in pg_stat_activity
        application_name: `dbos_transact_${globalParams.executorID}_${globalParams.appVersion}`,
        max: sysDbPoolSize,
      };
      this.pool = new Pool(systemPoolConfig);
    }

    this.pool.on('error', (err: Error) => {
      this.logger.warn(`Unexpected error in pool: ${err}`);
    });
    this.pool.on('connect', (client: PoolClient) => {
      client.on('error', (err: Error) => {
        this.logger.warn(`Unexpected error in idle client: ${err}`);
      });
    });
  }

  async init(debugMode: boolean = false) {
    if (!this.customPool) {
      await ensureSystemDatabase(this.systemDatabaseUrl, this.logger, debugMode);
    }

    if (this.shouldUseDBNotifications) {
      await this.#listenForNotifications();
    }
  }

  async destroy() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
    }
    if (this.notificationsClient) {
      try {
        this.notificationsClient.release(true);
      } catch (e) {
        this.logger.warn(`Error ending notifications client: ${String(e)}`);
      }
    }
    await this.pool.end();
  }

  @dbRetry()
  async initWorkflowStatus(
    initStatus: WorkflowStatusInternal,
    maxRetries?: number,
  ): Promise<{ status: string; deadlineEpochMS?: number }> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');

      const resRow = await insertWorkflowStatus(client, initStatus);
      if (resRow.name !== initStatus.workflowName) {
        const msg = `Workflow already exists with a different function name: ${resRow.name}, but the provided function name is: ${initStatus.workflowName}`;
        throw new DBOSConflictingWorkflowError(initStatus.workflowUUID, msg);
      } else if (resRow.class_name !== initStatus.workflowClassName) {
        const msg = `Workflow already exists with a different class name: ${resRow.class_name}, but the provided class name is: ${initStatus.workflowClassName}`;
        throw new DBOSConflictingWorkflowError(initStatus.workflowUUID, msg);
      } else if ((resRow.config_name || '') !== (initStatus.workflowConfigName || '')) {
        const msg = `Workflow already exists with a different class configuration: ${resRow.config_name}, but the provided class configuration is: ${initStatus.workflowConfigName}`;
        throw new DBOSConflictingWorkflowError(initStatus.workflowUUID, msg);
      } else if ((resRow.queue_name ?? undefined) !== (initStatus.queueName ?? undefined)) {
        // This is a warning because a different queue name is not necessarily an error.
        this.logger.warn(
          `Workflow (${initStatus.workflowUUID}) already exists in queue: ${resRow.queue_name}, but the provided queue name is: ${initStatus.queueName}. The queue is not updated. ${new Error().stack}`,
        );
      }

      // recovery_attempt means "attempts" (we kept the name for backward compatibility). It's default value is 1.
      // Every time we init the status, we increment `recovery_attempts` by 1.
      // Thus, when this number becomes equal to `maxRetries + 1`, we should mark the workflow as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
      const attempts = resRow.recovery_attempts;
      if (maxRetries && attempts > maxRetries + 1) {
        await updateWorkflowStatus(client, initStatus.workflowUUID, StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED, {
          where: { status: StatusString.PENDING },
          throwOnFailure: false,
        });
        throw new DBOSMaxRecoveryAttemptsExceededError(initStatus.workflowUUID, maxRetries);
      }
      this.logger.debug(`Workflow ${initStatus.workflowUUID} attempt number: ${attempts}.`);
      const status = resRow.status;
      const deadlineEpochMS = resRow.workflow_deadline_epoch_ms ?? undefined;
      return { status, deadlineEpochMS };
    } finally {
      try {
        await client.query('COMMIT');
      } finally {
        client.release();
      }
    }
  }

  @dbRetry()
  async recordWorkflowOutput(workflowID: string, status: WorkflowStatusInternal): Promise<void> {
    const client = await this.pool.connect();
    try {
      await updateWorkflowStatus(client, workflowID, StatusString.SUCCESS, {
        update: { output: status.output, resetDeduplicationID: true },
      });
    } finally {
      client.release();
    }
  }

  @dbRetry()
  async recordWorkflowError(workflowID: string, status: WorkflowStatusInternal): Promise<void> {
    const client = await this.pool.connect();
    try {
      await updateWorkflowStatus(client, workflowID, StatusString.ERROR, {
        update: { error: status.error, resetDeduplicationID: true },
      });
    } finally {
      client.release();
    }
  }

  async getPendingWorkflows(executorID: string, appVersion: string): Promise<GetPendingWorkflowsOutput[]> {
    const getWorkflows = await this.pool.query<workflow_status>(
      `SELECT workflow_uuid, queue_name 
       FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status 
       WHERE status=$1 AND executor_id=$2 AND application_version=$3`,
      [StatusString.PENDING, executorID, appVersion],
    );
    return getWorkflows.rows.map(
      (i) =>
        <GetPendingWorkflowsOutput>{
          workflowUUID: i.workflow_uuid,
          queueName: i.queue_name,
        },
    );
  }

  async #getOperationResultAndThrowIfCancelled(
    client: PoolClient,
    workflowID: string,
    functionID: number,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    await this.#checkIfCanceled(client, workflowID);

    const { rows } = await client.query<operation_outputs>(
      `SELECT output, error, child_workflow_id, function_name
       FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowID, functionID],
    );
    if (rows.length === 0) {
      return undefined;
    } else {
      return {
        output: rows[0].output,
        error: rows[0].error,
        childWorkflowID: rows[0].child_workflow_id,
        functionName: rows[0].function_name,
      };
    }
  }

  @dbRetry()
  async getOperationResultAndThrowIfCancelled(
    workflowID: string,
    functionID: number,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    const client = await this.pool.connect();
    try {
      return await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
    } finally {
      client.release();
    }
  }

  async getAllOperationResults(workflowID: string): Promise<operation_outputs[]> {
    const { rows } = await this.pool.query<operation_outputs>(
      `SELECT * FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs WHERE workflow_uuid=$1`,
      [workflowID],
    );
    return rows;
  }

  @dbRetry()
  async recordOperationResult(
    workflowID: string,
    functionID: number,
    functionName: string,
    checkConflict: boolean,
    options: {
      childWorkflowID?: string | null;
      output?: string | null;
      error?: string | null;
    } = {},
  ): Promise<void> {
    const client = await this.pool.connect();
    try {
      await recordOperationResult(client, workflowID, functionID, functionName, checkConflict, options);
    } finally {
      client.release();
    }
  }

  async forkWorkflow(
    workflowID: string,
    startStep: number,
    options: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number } = {},
  ): Promise<string> {
    const newWorkflowID = options.newWorkflowID ?? randomUUID();
    const workflowStatus = await this.getWorkflowStatus(workflowID);

    if (workflowStatus === null) {
      throw new DBOSNonExistentWorkflowError(`Workflow ${workflowID} does not exist`);
    }

    if (!workflowStatus.input) {
      throw new DBOSNonExistentWorkflowError(`Workflow ${workflowID} has no input`);
    }

    const client = await this.pool.connect();

    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');

      const now = Date.now();
      await insertWorkflowStatus(client, {
        workflowUUID: newWorkflowID,
        status: StatusString.ENQUEUED,
        workflowName: workflowStatus.workflowName,
        workflowClassName: workflowStatus.workflowClassName,
        workflowConfigName: workflowStatus.workflowConfigName,
        queueName: INTERNAL_QUEUE_NAME,
        authenticatedUser: workflowStatus.authenticatedUser,
        assumedRole: workflowStatus.assumedRole,
        authenticatedRoles: workflowStatus.authenticatedRoles,
        output: null,
        error: null,
        request: workflowStatus.request,
        executorId: globalParams.executorID,
        applicationVersion: options.applicationVersion ?? workflowStatus.applicationVersion,
        applicationID: workflowStatus.applicationID,
        createdAt: now,
        recoveryAttempts: 0,
        updatedAt: now,
        timeoutMS: options.timeoutMS ?? workflowStatus.timeoutMS,
        input: workflowStatus.input,
        deduplicationID: undefined,
        priority: 0,
      });

      if (startStep > 0) {
        const query = `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.operation_outputs 
          (workflow_uuid, function_id, output, error, function_name, child_workflow_id )
          SELECT $1 AS workflow_uuid, function_id, output, error, function_name, child_workflow_id
          FROM ${DBOSExecutor.systemDBSchemaName}.operation_outputs
          WHERE workflow_uuid = $2 AND function_id < $3`;
        await client.query(query, [newWorkflowID, workflowID, startStep]);
      }

      await client.query('COMMIT');
      return newWorkflowID;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async #runAndRecordResult(
    client: PoolClient,
    functionName: string,
    workflowID: string,
    functionID: number,
    func: () => Promise<string | null | undefined>,
  ): Promise<string | null | undefined> {
    const result = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
    if (result !== undefined) {
      if (result.functionName !== functionName) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, functionName, result.functionName!);
      }
      return result.output;
    }
    const output = await func();
    await recordOperationResult(client, workflowID, functionID, functionName, true, { output });
    return output;
  }

  @dbRetry()
  async durableSleepms(workflowID: string, functionID: number, durationMS: number): Promise<void> {
    let resolveNotification: () => void;
    const cancelPromise = new Promise<void>((resolve) => {
      resolveNotification = resolve;
    });

    const cbr = this.cancelWakeupMap.registerCallback(workflowID, resolveNotification!);
    try {
      let timeoutPromise: Promise<void> = Promise.resolve();
      const { promise, cancel: timeoutCancel } = await this.#durableSleep(workflowID, functionID, durationMS);
      timeoutPromise = promise;

      try {
        await Promise.race([cancelPromise, timeoutPromise]);
      } finally {
        timeoutCancel();
      }
    } finally {
      this.cancelWakeupMap.deregisterCallback(cbr);
    }

    await this.checkIfCanceled(workflowID);
  }

  async #durableSleep(
    workflowID: string,
    functionID: number,
    durationMS: number,
    maxSleepPerIteration?: number,
  ): Promise<{ promise: Promise<void>; cancel: () => void; endTime: number }> {
    if (maxSleepPerIteration === undefined) maxSleepPerIteration = durationMS;

    const curTime = Date.now();
    let endTimeMs = curTime + durationMS;

    const client = await this.pool.connect();
    try {
      const res = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
      if (res) {
        if (res.functionName !== DBOS_FUNCNAME_SLEEP) {
          throw new DBOSUnexpectedStepError(workflowID, functionID, DBOS_FUNCNAME_SLEEP, res.functionName!);
        }
        endTimeMs = JSON.parse(res.output!) as number;
      } else {
        await recordOperationResult(client, workflowID, functionID, DBOS_FUNCNAME_SLEEP, false, {
          output: JSON.stringify(endTimeMs),
        });
      }
      return {
        ...cancellableSleep(Math.max(Math.min(maxSleepPerIteration, endTimeMs - curTime), 0)),
        endTime: endTimeMs,
      };
    } finally {
      client.release();
    }
  }

  readonly nullTopic = '__null__topic__';

  @dbRetry()
  async send(
    workflowID: string,
    functionID: number,
    destinationID: string,
    message: string | null,
    topic?: string,
  ): Promise<void> {
    topic = topic ?? this.nullTopic;
    const client: PoolClient = await this.pool.connect();

    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      await this.#runAndRecordResult(client, DBOS_FUNCNAME_SEND, workflowID, functionID, async () => {
        await client.query(
          `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.notifications (destination_uuid, topic, message) VALUES ($1, $2, $3);`,
          [destinationID, topic, message],
        );
        return undefined;
      });
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '23503') {
        // Foreign key constraint violation (only expected for the INSERT query)
        throw new DBOSNonExistentWorkflowError(`Sent to non-existent destination workflow UUID: ${destinationID}`);
      } else {
        throw err;
      }
    } finally {
      client.release();
    }
  }

  @dbRetry()
  async recv(
    workflowID: string,
    functionID: number,
    timeoutFunctionID: number,
    topic?: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
  ): Promise<string | null> {
    topic = topic ?? this.nullTopic;
    // First, check for previous executions.
    const res = await this.getOperationResultAndThrowIfCancelled(workflowID, functionID);
    if (res) {
      if (res.functionName !== DBOS_FUNCNAME_RECV) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, DBOS_FUNCNAME_RECV, res.functionName!);
      }
      return res.output!;
    }

    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;

    while (true) {
      // register the key with the global notifications listener.
      let resolveNotification: () => void;
      const messagePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const payload = `${workflowID}::${topic}`;
      const cbr = this.notificationsMap.registerCallback(payload, resolveNotification!);
      const crh = this.cancelWakeupMap.registerCallback(workflowID, (_res) => {
        resolveNotification();
      });

      try {
        await this.checkIfCanceled(workflowID);

        // Check if the key is already in the DB, then wait for the notification if it isn't.
        const initRecvRows = (
          await this.pool.query<notifications>(
            `SELECT topic FROM ${DBOSExecutor.systemDBSchemaName}.notifications WHERE destination_uuid=$1 AND topic=$2;`,
            [workflowID, topic],
          )
        ).rows;

        if (initRecvRows.length !== 0) break;

        const ct = Date.now();
        if (finishTime && ct > finishTime) break; // Time's up

        let timeoutPromise: Promise<void> = Promise.resolve();
        let timeoutCancel: () => void = () => {};
        if (timeoutms) {
          const { promise, cancel, endTime } = await this.#durableSleep(
            workflowID,
            timeoutFunctionID,
            timeoutms,
            this.dbPollingIntervalEventMs,
          );
          timeoutPromise = promise;
          timeoutCancel = cancel;
          finishTime = endTime;
        } else {
          let poll = finishTime ? finishTime - ct : this.dbPollingIntervalEventMs;
          poll = Math.min(this.dbPollingIntervalEventMs, poll);
          const { promise, cancel } = cancellableSleep(poll);
          timeoutPromise = promise;
          timeoutCancel = cancel;
        }
        try {
          await Promise.race([messagePromise, timeoutPromise]);
        } finally {
          timeoutCancel();
        }
      } finally {
        this.notificationsMap.deregisterCallback(cbr);
        this.cancelWakeupMap.deregisterCallback(crh);
      }
    }

    await this.checkIfCanceled(workflowID);

    // Transactionally consume and return the message if it's in the DB, otherwise return null.
    let message: string | null = null;
    const client = await this.pool.connect();
    try {
      await client.query(`BEGIN ISOLATION LEVEL READ COMMITTED`);
      const finalRecvRows = (
        await client.query<notifications>(
          `WITH oldest_entry AS (
        SELECT destination_uuid, topic, message, created_at_epoch_ms
        FROM ${DBOSExecutor.systemDBSchemaName}.notifications
        WHERE destination_uuid = $1
          AND topic = $2
        ORDER BY created_at_epoch_ms ASC
        LIMIT 1
       )

        DELETE FROM ${DBOSExecutor.systemDBSchemaName}.notifications
        USING oldest_entry
        WHERE notifications.destination_uuid = oldest_entry.destination_uuid
          AND notifications.topic = oldest_entry.topic
          AND notifications.created_at_epoch_ms = oldest_entry.created_at_epoch_ms
        RETURNING notifications.*;`,
          [workflowID, topic],
        )
      ).rows;
      if (finalRecvRows.length > 0) {
        message = finalRecvRows[0].message;
      }
      await recordOperationResult(client, workflowID, functionID, DBOS_FUNCNAME_RECV, true, { output: message });
      await client.query(`COMMIT`);
    } catch (e) {
      this.logger.error(e);
      await client.query(`ROLLBACK`);
      throw e;
    } finally {
      client.release();
    }

    return message;
  }

  // Only used in tests
  async setWorkflowStatus(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    resetRecoveryAttempts: boolean,
  ): Promise<void> {
    const client = await this.pool.connect();
    try {
      await updateWorkflowStatus(client, workflowID, status, { update: { resetRecoveryAttempts } });
    } finally {
      client.release();
    }
  }

  @dbRetry()
  async setEvent(workflowID: string, functionID: number, key: string, message: string | null): Promise<void> {
    const client: PoolClient = await this.pool.connect();

    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      await this.#runAndRecordResult(client, DBOS_FUNCNAME_SETEVENT, workflowID, functionID, async () => {
        await client.query(
          `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.workflow_events (workflow_uuid, key, value)
             VALUES ($1, $2, $3)
             ON CONFLICT (workflow_uuid, key)
             DO UPDATE SET value = $3
             RETURNING workflow_uuid;`,
          [workflowID, key, message],
        );
        return undefined;
      });
      await client.query('COMMIT');
    } catch (e) {
      this.logger.error(e);
      await client.query(`ROLLBACK`);
      throw e;
    } finally {
      client.release();
    }
  }

  @dbRetry()
  async getEvent(
    workflowID: string,
    key: string,
    timeoutSeconds: number,
    callerWorkflow?: {
      workflowID: string;
      functionID: number;
      timeoutFunctionID: number;
    },
  ): Promise<string | null> {
    // Check if the operation has been done before for OAOO (only do this inside a workflow).
    if (callerWorkflow) {
      const res = await this.getOperationResultAndThrowIfCancelled(
        callerWorkflow.workflowID,
        callerWorkflow.functionID,
      );
      if (res) {
        if (res.functionName !== DBOS_FUNCNAME_GETEVENT) {
          throw new DBOSUnexpectedStepError(
            callerWorkflow.workflowID,
            callerWorkflow.functionID,
            DBOS_FUNCNAME_GETEVENT,
            res.functionName!,
          );
        }
        return res.output!;
      }
    }

    // Get the return the value. if it's in the DB, otherwise return null.
    let value: string | null = null;
    const payloadKey = `${workflowID}::${key}`;
    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;

    // Register the key with the global notifications listener first... we do not want to look in the DB first
    //  or that would cause a timing hole.
    while (true) {
      let resolveNotification: () => void;
      const valuePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const cbr = this.workflowEventsMap.registerCallback(payloadKey, resolveNotification!);
      const crh = callerWorkflow?.workflowID
        ? this.cancelWakeupMap.registerCallback(callerWorkflow.workflowID, (_res) => {
            resolveNotification();
          })
        : undefined;

      try {
        if (callerWorkflow?.workflowID) await this.checkIfCanceled(callerWorkflow?.workflowID);
        // Check if the key is already in the DB, then wait for the notification if it isn't.
        const initRecvRows = (
          await this.pool.query<workflow_events>(
            `SELECT key, value
             FROM ${DBOSExecutor.systemDBSchemaName}.workflow_events
             WHERE workflow_uuid=$1 AND key=$2;`,
            [workflowID, key],
          )
        ).rows;

        if (initRecvRows.length > 0) {
          value = initRecvRows[0].value;
          break;
        }

        const ct = Date.now();
        if (finishTime && ct > finishTime) break; // Time's up

        // If we have a callerWorkflow, we want a durable sleep, otherwise, not
        let timeoutPromise: Promise<void> = Promise.resolve();
        let timeoutCancel: () => void = () => {};
        if (callerWorkflow && timeoutms) {
          const { promise, cancel, endTime } = await this.#durableSleep(
            callerWorkflow.workflowID,
            callerWorkflow.timeoutFunctionID ?? -1,
            timeoutms,
            this.dbPollingIntervalEventMs,
          );
          timeoutPromise = promise;
          timeoutCancel = cancel;
          finishTime = endTime;
        } else {
          let poll = finishTime ? finishTime - ct : this.dbPollingIntervalEventMs;
          poll = Math.min(this.dbPollingIntervalEventMs, poll);
          const { promise, cancel } = cancellableSleep(poll);
          timeoutPromise = promise;
          timeoutCancel = cancel;
        }

        try {
          await Promise.race([valuePromise, timeoutPromise]);
        } finally {
          timeoutCancel();
        }
      } finally {
        this.workflowEventsMap.deregisterCallback(cbr);
        if (crh) this.cancelWakeupMap.deregisterCallback(crh);
      }
    }

    // Record the output if it is inside a workflow.
    if (callerWorkflow) {
      await this.recordOperationResult(
        callerWorkflow.workflowID,
        callerWorkflow.functionID,
        DBOS_FUNCNAME_GETEVENT,
        true,
        { output: value },
      );
    }
    return value;
  }

  #setWFCancelMap(workflowID: string) {
    if (this.runningWorkflowMap.has(workflowID)) {
      this.workflowCancellationMap.set(workflowID, true);
    }
    this.cancelWakeupMap.callCallbacks(workflowID);
  }

  #clearWFCancelMap(workflowID: string) {
    if (this.workflowCancellationMap.has(workflowID)) {
      this.workflowCancellationMap.delete(workflowID);
    }
  }

  async cancelWorkflow(workflowID: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');

      const statusResult = await getWorkflowStatusValue(client, workflowID);
      if (!statusResult) {
        throw new DBOSNonExistentWorkflowError(`Workflow ${workflowID} does not exist`);
      }
      if (
        statusResult === StatusString.SUCCESS ||
        statusResult === StatusString.ERROR ||
        statusResult === StatusString.CANCELLED
      ) {
        await client.query('ROLLBACK');
        return;
      }

      // Set the workflow's status to CANCELLED and remove it from any queue it is on
      await updateWorkflowStatus(client, workflowID, StatusString.CANCELLED, {
        update: { queueName: null, resetDeduplicationID: true, resetStartedAtEpochMs: true },
      });

      await client.query('COMMIT');
    } catch (error) {
      this.logger.error(error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    this.#setWFCancelMap(workflowID);
  }

  async #checkIfCanceled(client: PoolClient, workflowID: string): Promise<void> {
    if (this.workflowCancellationMap.get(workflowID) === true) {
      throw new DBOSWorkflowCancelledError(workflowID);
    }
    const statusValue = await getWorkflowStatusValue(client, workflowID);
    if (statusValue === StatusString.CANCELLED) {
      throw new DBOSWorkflowCancelledError(workflowID);
    }
  }

  @dbRetry()
  async checkIfCanceled(workflowID: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await this.#checkIfCanceled(client, workflowID);
    } finally {
      client.release();
    }
  }

  async resumeWorkflow(workflowID: string): Promise<void> {
    this.#clearWFCancelMap(workflowID);
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');

      // Check workflow status. If it is complete, do nothing.
      const statusResult = await getWorkflowStatusValue(client, workflowID);
      if (!statusResult || statusResult === StatusString.SUCCESS || statusResult === StatusString.ERROR) {
        await client.query('ROLLBACK');
        if (!statusResult) {
          if (statusResult === undefined) {
            throw new DBOSNonExistentWorkflowError(`Workflow ${workflowID} does not exist`);
          }
        }
        return;
      }

      // Set the workflow's status to ENQUEUED and reset recovery attempts and deadline.
      await updateWorkflowStatus(client, workflowID, StatusString.ENQUEUED, {
        update: {
          queueName: INTERNAL_QUEUE_NAME,
          resetRecoveryAttempts: true,
          resetDeadline: true,
          resetDeduplicationID: true,
          resetStartedAtEpochMs: true,
        },
        throwOnFailure: false,
      });

      await client.query('COMMIT');
    } catch (error) {
      this.logger.error(error);
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  registerRunningWorkflow(workflowID: string, workflowPromise: Promise<unknown>) {
    // Need to await for the workflow and capture errors.
    const awaitWorkflowPromise = workflowPromise
      .catch((error) => {
        this.logger.debug('Captured error in awaitWorkflowPromise: ' + error);
      })
      .finally(() => {
        // Remove itself from pending workflow map.
        this.runningWorkflowMap.delete(workflowID);
        this.workflowCancellationMap.delete(workflowID);
      });
    this.runningWorkflowMap.set(workflowID, awaitWorkflowPromise);
  }

  async awaitRunningWorkflows(): Promise<void> {
    if (this.runningWorkflowMap.size > 0) {
      this.logger.info('Waiting for pending workflows to finish.');
      await Promise.allSettled(this.runningWorkflowMap.values());
    }
    if (this.workflowEventsMap.map.size > 0) {
      this.logger.warn('Workflow events map is not empty - shutdown is not clean.');
      //throw new Error('Workflow events map is not empty - shutdown is not clean.');
    }
    if (this.notificationsMap.map.size > 0) {
      this.logger.warn('Message notification map is not empty - shutdown is not clean.');
      //throw new Error('Message notification map is not empty - shutdown is not clean.');
    }
  }

  @dbRetry()
  async getWorkflowStatus(
    workflowID: string,
    callerID?: string,
    callerFN?: number,
  ): Promise<WorkflowStatusInternal | null> {
    const funcGetStatus = async () => {
      const statuses = await this.listWorkflows({ workflowIDs: [workflowID] });
      const status = statuses.find((s) => s.workflowUUID === workflowID);
      return status ? JSON.stringify(status) : null;
    };

    if (callerID && callerFN) {
      const client = await this.pool.connect();
      try {
        // Check if the operation has been done before for OAOO (only do this inside a workflow).
        const json = await this.#runAndRecordResult(client, DBOS_FUNCNAME_GETSTATUS, callerID, callerFN, funcGetStatus);
        return parseStatus(json);
      } finally {
        client.release();
      }
    } else {
      const json = await funcGetStatus();
      return parseStatus(json);
    }

    function parseStatus(json: string | null | undefined): WorkflowStatusInternal | null {
      return json ? (JSON.parse(json) as WorkflowStatusInternal) : null;
    }
  }

  @dbRetry()
  async awaitWorkflowResult(
    workflowID: string,
    timeoutSeconds?: number,
    callerID?: string,
    timerFuncID?: number,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;

    while (true) {
      let resolveNotification: () => void;
      const statusPromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const irh = this.cancelWakeupMap.registerCallback(workflowID, (_res) => {
        resolveNotification();
      });
      const crh = callerID
        ? this.cancelWakeupMap.registerCallback(callerID, (_res) => {
            resolveNotification();
          })
        : undefined;
      try {
        if (callerID) await this.checkIfCanceled(callerID);
        try {
          const { rows } = await this.pool.query<workflow_status>(
            `SELECT status, output, error FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status 
             WHERE workflow_uuid=$1`,
            [workflowID],
          );
          if (rows.length > 0) {
            const status = rows[0].status;
            if (status === StatusString.SUCCESS) {
              return { output: rows[0].output };
            } else if (status === StatusString.ERROR) {
              return { error: rows[0].error };
            } else if (status === StatusString.CANCELLED) {
              return { cancelled: true };
            } else {
              // Status is not actionable
            }
          }
        } catch (e) {
          const err = e as Error;
          this.logger.error(`Exception from system database: ${err}`);
          throw err;
        }

        const ct = Date.now();
        if (finishTime && ct > finishTime) return undefined; // Time's up

        let timeoutPromise: Promise<void> = Promise.resolve();
        let timeoutCancel: () => void = () => {};
        if (timerFuncID !== undefined && callerID !== undefined && timeoutms !== undefined) {
          const { promise, cancel, endTime } = await this.#durableSleep(
            callerID,
            timerFuncID,
            timeoutms,
            this.dbPollingIntervalResultMs,
          );
          finishTime = endTime;
          timeoutPromise = promise;
          timeoutCancel = cancel;
        } else {
          let poll = finishTime ? finishTime - ct : this.dbPollingIntervalResultMs;
          poll = Math.min(this.dbPollingIntervalResultMs, poll);
          const { promise, cancel } = cancellableSleep(poll);
          timeoutPromise = promise;
          timeoutCancel = cancel;
        }

        try {
          await Promise.race([statusPromise, timeoutPromise]);
        } finally {
          timeoutCancel();
        }
      } finally {
        this.cancelWakeupMap.deregisterCallback(irh);
        if (crh) this.cancelWakeupMap.deregisterCallback(crh);
      }
    }
  }

  /* BACKGROUND PROCESSES */
  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  reconnectTimeout: NodeJS.Timeout | null = null;

  async #listenForNotifications() {
    const connect = async () => {
      const reconnect = () => {
        if (this.reconnectTimeout) {
          return;
        }
        this.reconnectTimeout = setTimeout(async () => {
          this.reconnectTimeout = null;
          await connect();
        }, 1000);
      };

      let client: PoolClient | null = null;
      try {
        client = await this.pool.connect();
        await client.query('LISTEN dbos_notifications_channel;');
        await client.query('LISTEN dbos_workflow_events_channel;');

        const handler = (msg: Notification) => {
          if (!this.shouldUseDBNotifications) return;
          if (msg.channel === 'dbos_notifications_channel' && msg.payload) {
            this.notificationsMap.callCallbacks(msg.payload);
          } else if (msg.channel === 'dbos_workflow_events_channel' && msg.payload) {
            this.workflowEventsMap.callCallbacks(msg.payload);
          }
        };

        client.on('notification', handler);
        client.on('error', (err: Error) => {
          this.logger.warn(`Error in notifications client: ${err}`);
          if (client) {
            client.removeAllListeners();
            client.release(true);
          }
          reconnect();
        });
        this.notificationsClient = client;
      } catch (error) {
        this.logger.warn(`Error in notifications listener: ${String(error)}`);
        if (client) {
          client.removeAllListeners();
          client.release(true);
        }
        reconnect();
      }
    };

    await connect();
  }

  // Event dispatcher queries / updates
  @dbRetry()
  async getEventDispatchState(
    service: string,
    workflowName: string,
    key: string,
  ): Promise<DBOSExternalState | undefined> {
    const res = await this.pool.query<event_dispatch_kv>(
      `SELECT * FROM ${DBOSExecutor.systemDBSchemaName}.event_dispatch_kv
       WHERE workflow_fn_name = $1 AND service_name = $2 AND key = $3;`,
      [workflowName, service, key],
    );

    if (res.rows.length === 0) return undefined;

    return {
      service: res.rows[0].service_name,
      workflowFnName: res.rows[0].workflow_fn_name,
      key: res.rows[0].key,
      value: res.rows[0].value,
      updateTime: res.rows[0].update_time,
      updateSeq:
        res.rows[0].update_seq !== null && res.rows[0].update_seq !== undefined
          ? BigInt(res.rows[0].update_seq)
          : undefined,
    };
  }

  @dbRetry()
  async upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState> {
    const res = await this.pool.query<event_dispatch_kv>(
      `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.event_dispatch_kv (
        service_name, workflow_fn_name, key, value, update_time, update_seq)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (service_name, workflow_fn_name, key)
       DO UPDATE SET
         update_time = GREATEST(EXCLUDED.update_time, event_dispatch_kv.update_time),
         update_seq =  GREATEST(EXCLUDED.update_seq,  event_dispatch_kv.update_seq),
         value = CASE WHEN (EXCLUDED.update_time > event_dispatch_kv.update_time 
            OR EXCLUDED.update_seq > event_dispatch_kv.update_seq 
            OR (event_dispatch_kv.update_time IS NULL and event_dispatch_kv.update_seq IS NULL)
         ) THEN EXCLUDED.value ELSE event_dispatch_kv.value END
       RETURNING value, update_time, update_seq;`,
      [state.service, state.workflowFnName, state.key, state.value, state.updateTime, state.updateSeq],
    );

    return {
      service: state.service,
      workflowFnName: state.workflowFnName,
      key: state.key,
      value: res.rows[0].value,
      updateTime: res.rows[0].update_time,
      updateSeq:
        res.rows[0].update_seq !== undefined && res.rows[0].update_seq !== null
          ? BigInt(res.rows[0].update_seq)
          : undefined,
    };
  }

  async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatusInternal[]> {
    const schemaName = DBOSExecutor.systemDBSchemaName;
    const selectColumns = [
      'workflow_uuid',
      'status',
      'name',
      'recovery_attempts',
      'config_name',
      'class_name',
      'authenticated_user',
      'authenticated_roles',
      'assumed_role',
      'queue_name',
      'executor_id',
      'created_at',
      'updated_at',
      'application_version',
      'application_id',
      'workflow_deadline_epoch_ms',
      'workflow_timeout_ms',
    ];

    input.loadInput = input.loadInput ?? true;
    input.loadOutput = input.loadOutput ?? true;
    if (input.loadInput) {
      selectColumns.push('inputs', 'request');
    }

    if (input.loadOutput) {
      selectColumns.push('output', 'error');
    }

    input.sortDesc = input.sortDesc ?? false; // By default, sort in ascending order

    // Build WHERE clauses
    const whereClauses: string[] = [];
    const params: unknown[] = [];
    let paramCounter = 1;

    if (input.workflowName) {
      whereClauses.push(`name = $${paramCounter}`);
      params.push(input.workflowName);
      paramCounter++;
    }
    if (input.workflow_id_prefix) {
      whereClauses.push(`workflow_uuid LIKE $${paramCounter}`);
      params.push(`${input.workflow_id_prefix}%`);
      paramCounter++;
    }
    if (input.workflowIDs) {
      const placeholders = input.workflowIDs.map((_, i) => `$${paramCounter + i}`).join(', ');
      whereClauses.push(`workflow_uuid IN (${placeholders})`);
      params.push(...input.workflowIDs);
      paramCounter += input.workflowIDs.length;
    }
    if (input.authenticatedUser) {
      whereClauses.push(`authenticated_user = $${paramCounter}`);
      params.push(input.authenticatedUser);
      paramCounter++;
    }
    if (input.startTime) {
      whereClauses.push(`created_at >= $${paramCounter}`);
      params.push(new Date(input.startTime).getTime());
      paramCounter++;
    }
    if (input.endTime) {
      whereClauses.push(`created_at <= $${paramCounter}`);
      params.push(new Date(input.endTime).getTime());
      paramCounter++;
    }
    if (input.status) {
      whereClauses.push(`status = $${paramCounter}`);
      params.push(input.status);
      paramCounter++;
    }
    if (input.applicationVersion) {
      whereClauses.push(`application_version = $${paramCounter}`);
      params.push(input.applicationVersion);
      paramCounter++;
    }

    const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const orderClause = `ORDER BY created_at ${input.sortDesc ? 'DESC' : 'ASC'}`;
    const limitClause = input.limit ? `LIMIT ${input.limit}` : '';
    const offsetClause = input.offset ? `OFFSET ${input.offset}` : '';

    const query = `
      SELECT ${selectColumns.join(', ')}
      FROM ${schemaName}.workflow_status
      ${whereClause}
      ${orderClause}
      ${limitClause}
      ${offsetClause}
    `;

    const result = await this.pool.query<workflow_status>(query, params);
    return result.rows.map(mapWorkflowStatus);
  }

  async listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatusInternal[]> {
    const schemaName = DBOSExecutor.systemDBSchemaName;
    const selectColumns = [
      'workflow_uuid',
      'status',
      'name',
      'recovery_attempts',
      'config_name',
      'class_name',
      'authenticated_user',
      'authenticated_roles',
      'assumed_role',
      'queue_name',
      'executor_id',
      'created_at',
      'updated_at',
      'application_version',
      'application_id',
      'workflow_deadline_epoch_ms',
      'workflow_timeout_ms',
    ];

    input.loadInput = input.loadInput ?? true;
    if (input.loadInput) {
      selectColumns.push('inputs', 'request');
    }

    const sortDesc = input.sortDesc ?? false; // By default, sort in ascending order

    // Build WHERE clauses
    const whereClauses: string[] = [];
    const params: unknown[] = [];
    let paramCounter = 1;

    // Always filter for queued workflows
    whereClauses.push(`queue_name IS NOT NULL`);
    whereClauses.push(`status IN ($${paramCounter}, $${paramCounter + 1})`);
    params.push(StatusString.ENQUEUED, StatusString.PENDING);
    paramCounter += 2;

    if (input.workflowName) {
      whereClauses.push(`name = $${paramCounter}`);
      params.push(input.workflowName);
      paramCounter++;
    }
    if (input.queueName) {
      whereClauses.push(`queue_name = $${paramCounter}`);
      params.push(input.queueName);
      paramCounter++;
    }
    if (input.startTime) {
      whereClauses.push(`created_at >= $${paramCounter}`);
      params.push(new Date(input.startTime).getTime());
      paramCounter++;
    }
    if (input.endTime) {
      whereClauses.push(`created_at <= $${paramCounter}`);
      params.push(new Date(input.endTime).getTime());
      paramCounter++;
    }
    if (input.status) {
      whereClauses.push(`status = $${paramCounter}`);
      params.push(input.status);
      paramCounter++;
    }

    const whereClause = `WHERE ${whereClauses.join(' AND ')}`;
    const orderClause = `ORDER BY created_at ${sortDesc ? 'DESC' : 'ASC'}`;
    const limitClause = input.limit ? `LIMIT ${input.limit}` : '';
    const offsetClause = input.offset ? `OFFSET ${input.offset}` : '';

    const query = `
      SELECT ${selectColumns.join(', ')}
      FROM ${schemaName}.workflow_status
      ${whereClause}
      ${orderClause}
      ${limitClause}
      ${offsetClause}
    `;

    const result = await this.pool.query<workflow_status>(query, params);
    return result.rows.map(mapWorkflowStatus);
  }

  async clearQueueAssignment(workflowID: string): Promise<boolean> {
    // Reset the status of the task from "PENDING" to "ENQUEUED"
    const wqRes = await this.pool.query<workflow_status>(
      `UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status
        SET started_at_epoch_ms = NULL, status = $2
        WHERE workflow_uuid = $1 AND queue_name is NOT NULL AND status = $3`,
      [workflowID, StatusString.ENQUEUED, StatusString.PENDING],
    );
    // If no rows were affected, the workflow is not anymore in the queue or was already completed
    return (wqRes.rowCount ?? 0) > 0;
  }

  @dbRetry()
  async getDeduplicatedWorkflow(queueName: string, deduplicationID: string): Promise<string | null> {
    const { rows } = await this.pool.query<workflow_status>(
      `SELECT workflow_uuid FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
       WHERE queue_name = $1 AND deduplication_id = $2`,
      [queueName, deduplicationID],
    );

    if (rows.length === 0) {
      return null;
    }

    return rows[0].workflow_uuid;
  }

  async findAndMarkStartableWorkflows(queue: WorkflowQueue, executorID: string, appVersion: string): Promise<string[]> {
    const startTimeMs = Date.now();
    const limiterPeriodMS = queue.rateLimit ? queue.rateLimit.periodSec * 1000 : 0;
    const claimedIDs: string[] = [];

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');

      // If there is a rate limit, compute how many functions have started in its period.
      let numRecentQueries = 0;
      if (queue.rateLimit) {
        const countResult = await client.query<{ count: string }>(
          `SELECT COUNT(*) FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
           WHERE queue_name = $1
             AND status <> $2
             AND started_at_epoch_ms > $3`,
          [queue.name, StatusString.ENQUEUED, startTimeMs - limiterPeriodMS],
        );
        numRecentQueries = Number(countResult.rows[0].count);
        if (numRecentQueries >= queue.rateLimit.limitPerPeriod) {
          await client.query('COMMIT');
          return claimedIDs;
        }
      }

      // Dequeue functions eligible for this worker and ordered by the time at which they were enqueued.
      // If there is a global or local concurrency limit N, select only the N oldest enqueued
      // functions, else select all of them.

      let maxTasks = Infinity;

      if (queue.workerConcurrency !== undefined || queue.concurrency !== undefined) {
        // Count how many workflows on this queue are currently PENDING both locally and globally.
        const runningTasksResult = await client.query(
          `SELECT executor_id, COUNT(*) as task_count
           FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
           WHERE queue_name = $1 AND status = $2
           GROUP BY executor_id`,
          [queue.name, StatusString.PENDING],
        );
        const runningTasksResultDict: Record<string, number> = {};
        runningTasksResult.rows.forEach((row: { executor_id: string; task_count: string }) => {
          runningTasksResultDict[row.executor_id] = Number(row.task_count);
        });
        const runningTasksForThisWorker = runningTasksResultDict[executorID] || 0;

        if (queue.workerConcurrency !== undefined) {
          maxTasks = Math.max(0, queue.workerConcurrency - runningTasksForThisWorker);
        }

        if (queue.concurrency !== undefined) {
          const totalRunningTasks = Object.values(runningTasksResultDict).reduce((acc, val) => acc + val, 0);
          if (totalRunningTasks > queue.concurrency) {
            this.logger.warn(
              `Total running tasks (${totalRunningTasks}) exceeds the global concurrency limit (${queue.concurrency})`,
            );
          }
          const availableTasks = Math.max(0, queue.concurrency - totalRunningTasks);
          maxTasks = Math.min(maxTasks, availableTasks);
        }
      }

      // Return immediately if there are no available tasks due to flow control limits
      if (maxTasks <= 0) {
        await client.query('COMMIT');
        return claimedIDs;
      }

      // Retrieve the first max_tasks workflows in the queue.
      // Only retrieve workflows of the local version (or without version set)
      const lockMode = queue.concurrency ? 'FOR UPDATE NOWAIT' : 'FOR UPDATE SKIP LOCKED';
      const orderClause = queue.priorityEnabled ? 'ORDER BY priority ASC, created_at ASC' : 'ORDER BY created_at ASC';
      const limitClause = maxTasks !== Infinity ? `LIMIT ${maxTasks}` : '';

      const selectQuery = `
        SELECT workflow_uuid
        FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
        WHERE status = $1
          AND queue_name = $2
          AND (application_version IS NULL OR application_version = $3)
        ${orderClause}
        ${limitClause}
        ${lockMode}
      `;

      const { rows } = await client.query<{ workflow_uuid: string }>(selectQuery, [
        StatusString.ENQUEUED,
        queue.name,
        appVersion,
      ]);

      // Start the workflows
      const workflowIDs = rows.map((row) => row.workflow_uuid);
      for (const id of workflowIDs) {
        // If we have a rate limit, stop starting functions when the number
        //   of functions started this period exceeds the limit.
        if (queue.rateLimit && claimedIDs.length + numRecentQueries >= queue.rateLimit.limitPerPeriod) {
          break;
        }

        // Start the functions by marking them as pending and updating their executor IDs
        await client.query(
          `UPDATE ${DBOSExecutor.systemDBSchemaName}.workflow_status
           SET status = $1,
               executor_id = $2,
               application_version = $3,
               started_at_epoch_ms = $4,
               workflow_deadline_epoch_ms = CASE
                 WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                 THEN (EXTRACT(epoch FROM now()) * 1000)::bigint + workflow_timeout_ms
                 ELSE workflow_deadline_epoch_ms
               END
           WHERE workflow_uuid = $5`,
          [StatusString.PENDING, executorID, appVersion, startTimeMs, id],
        );
        claimedIDs.push(id);
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    // Return the IDs of all functions we marked started
    return claimedIDs;
  }

  @dbRetry()
  async writeStreamFromStep(workflowID: string, key: string, value: unknown): Promise<void> {
    const client: PoolClient = await this.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');

      // Find the maximum offset for this workflow_uuid and key combination
      const maxOffsetResult = await client.query(
        `SELECT MAX("offset") FROM ${DBOSExecutor.systemDBSchemaName}.streams 
         WHERE workflow_uuid = $1 AND key = $2`,
        [workflowID, key],
      );

      // Next offset is max + 1, or 0 if no records exist
      const maxOffset = (maxOffsetResult.rows[0] as { max: number | null }).max;
      const nextOffset = maxOffset !== null ? maxOffset + 1 : 0;

      // Serialize the value before storing
      const serializedValue = JSON.stringify(value);

      // Insert the new stream entry
      await client.query(
        `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.streams (workflow_uuid, key, value, "offset")
         VALUES ($1, $2, $3, $4)`,
        [workflowID, key, serializedValue, nextOffset],
      );

      await client.query('COMMIT');
    } catch (e) {
      this.logger.error(e);
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  @dbRetry()
  async writeStreamFromWorkflow(workflowID: string, functionID: number, key: string, value: unknown): Promise<void> {
    const client: PoolClient = await this.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');

      const functionName =
        value === DBOS_STREAM_CLOSED_SENTINEL ? DBOS_FUNCNAME_CLOSESTREAM : DBOS_FUNCNAME_WRITESTREAM;

      await this.#runAndRecordResult(client, functionName, workflowID, functionID, async () => {
        // Find the maximum offset for this workflow_uuid and key combination
        const maxOffsetResult = await client.query(
          `SELECT MAX("offset") FROM ${DBOSExecutor.systemDBSchemaName}.streams 
           WHERE workflow_uuid = $1 AND key = $2`,
          [workflowID, key],
        );

        // Next offset is max + 1, or 0 if no records exist
        const maxOffset = (maxOffsetResult.rows[0] as { max: number | null }).max;
        const nextOffset = maxOffset !== null ? maxOffset + 1 : 0;

        // Serialize the value before storing
        const serializedValue = JSON.stringify(value);

        // Insert the new stream entry
        await client.query(
          `INSERT INTO ${DBOSExecutor.systemDBSchemaName}.streams (workflow_uuid, key, value, "offset")
           VALUES ($1, $2, $3, $4)`,
          [workflowID, key, serializedValue, nextOffset],
        );

        return undefined;
      });

      await client.query('COMMIT');
    } catch (e) {
      this.logger.error(e);
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async closeStream(workflowID: string, functionID: number, key: string): Promise<void> {
    await this.writeStreamFromWorkflow(workflowID, functionID, key, DBOS_STREAM_CLOSED_SENTINEL);
  }

  @dbRetry()
  async readStream(workflowID: string, key: string, offset: number): Promise<unknown> {
    const client: PoolClient = await this.pool.connect();
    try {
      const result = await client.query(
        `SELECT value FROM ${DBOSExecutor.systemDBSchemaName}.streams 
         WHERE workflow_uuid = $1 AND key = $2 AND "offset" = $3`,
        [workflowID, key, offset],
      );

      if (result.rows.length === 0) {
        throw new Error(`No value found for workflow_uuid=${workflowID}, key=${key}, offset=${offset}`);
      }

      // Deserialize the value before returning
      const row = result.rows[0] as { value: string };
      return JSON.parse(row.value);
    } finally {
      client.release();
    }
  }

  async garbageCollect(cutoffEpochTimestampMs?: number, rowsThreshold?: number): Promise<void> {
    if (rowsThreshold !== undefined) {
      // Get the created_at timestamp of the rows_threshold newest row
      const result = await this.pool.query<{ created_at: number }>(
        `SELECT created_at
         FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
         ORDER BY created_at DESC
         LIMIT 1 OFFSET $1`,
        [rowsThreshold - 1],
      );

      if (result.rows.length > 0) {
        const rowsBasedCutoff = result.rows[0].created_at;
        // Use the more restrictive cutoff (higher timestamp = more recent = more deletion)
        if (cutoffEpochTimestampMs === undefined || rowsBasedCutoff > cutoffEpochTimestampMs) {
          cutoffEpochTimestampMs = rowsBasedCutoff;
        }
      }
    }

    if (cutoffEpochTimestampMs === undefined) {
      return;
    }

    // Delete all workflows older than cutoff that are NOT PENDING or ENQUEUED
    await this.pool.query(
      `DELETE FROM ${DBOSExecutor.systemDBSchemaName}.workflow_status
       WHERE created_at < $1
         AND status NOT IN ($2, $3)`,
      [cutoffEpochTimestampMs, StatusString.PENDING, StatusString.ENQUEUED],
    );

    return;
  }
}
