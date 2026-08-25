import { DBOSExecutor, DBOSExternalState } from './dbos-executor';
import { DatabaseError, Pool, PoolClient, Notification, Client, PoolConfig, ClientBase } from 'pg';
import {
  DBOSWorkflowConflictError,
  DBOSNonExistentWorkflowError,
  DBOSConflictingWorkflowError,
  DBOSUnexpectedStepError,
  DBOSWorkflowCancelledError,
  DBOSQueueDuplicatedError,
  DBOSInitializationError,
  DBOSError,
} from './error';
import { GetPendingWorkflowsOutput, GetWorkflowsInput, StatusString } from './workflow';
import {
  notifications,
  operation_outputs,
  workflow_status,
  workflow_events,
  workflow_events_history,
  streams,
  event_dispatch_kv,
  workflow_schedules,
  application_versions,
  queues,
  SysDBSerializationFormat,
} from '../schemas/system_db_schema';
import {
  globalParams,
  cancellableSleep,
  dbRetryConfig,
  INTERNAL_QUEUE_NAME,
  Semaphore,
  sleepConfig,
  sleepms,
} from './utils';
import { GlobalLogger } from './telemetry/logs';
import { QueueRateLimit, resolveQueueLimits, WorkflowQueue } from './wfqueue';
import { randomUUID } from 'crypto';
import { getClientConfig } from './utils';
import { ensurePGDatabase, maskDatabaseUrl } from './database_utils';
import { getCurrentSysDBVersion, runSysMigrationsPg } from './sysdb_migrations/migration_runner';
import { allMigrations } from './sysdb_migrations/internal/migrations';
import {
  DEBUG_TRIGGER_STEP_COMMIT,
  DEBUG_TRIGGER_INITWF_COMMIT,
  DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT,
  DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES,
  debugTriggerPoint,
} from './debugpoint';
import { DBOSPortableJSON, DBOSSerializer, safeParse } from './serialization';

/* Result from Sys DB */
export interface SystemDatabaseStoredResult {
  output?: string | null;
  error?: string | null;
  cancelled?: boolean;
  maxRecoveryAttemptsExceeded?: boolean;
  childWorkflowID?: string | null;
  functionName?: string;
  serialization?: string | null; // For WF result, and for steps that persist a format (recv/getEvent)
}

/* Exported workflow format for import/export */
export interface ExportedWorkflow {
  workflow_status: workflow_status;
  operation_outputs: operation_outputs[];
  workflow_events: workflow_events[];
  workflow_events_history: workflow_events_history[];
  streams: streams[];
}

export const DBOS_FUNCNAME_SEND = 'DBOS.send';
export const DBOS_FUNCNAME_RECV = 'DBOS.recv';
export const DBOS_FUNCNAME_SETEVENT = 'DBOS.setEvent';
export const DBOS_FUNCNAME_GETEVENT = 'DBOS.getEvent';
export const DBOS_FUNCNAME_SLEEP = 'DBOS.sleep';
export const DBOS_FUNCNAME_GETSTATUS = 'getStatus';
export const DBOS_FUNCNAME_WRITESTREAM = 'DBOS.writeStream';
export const DBOS_FUNCNAME_CLOSESTREAM = 'DBOS.closeStream';
export const DBOS_FUNCNAME_READSTREAM = 'DBOS.readStream';
export const DBOS_FUNCNAME_READSTREAMOFFSET = 'DBOS.readStreamOffset';
export const DEFAULT_POOL_SIZE = 10;

export const DBOS_STREAM_CLOSED_SENTINEL = '__DBOS_STREAM_CLOSED__';
// The sentinel as it is stored: portable JSON, the same bytes every language writes and reads.
export const DBOS_STREAM_CLOSED_SENTINEL_SERIALIZED = DBOSPortableJSON.stringify(DBOS_STREAM_CLOSED_SENTINEL);

/** Whether a stream value is the marker a closed stream ends with. Takes the deserialized value. */
export function isStreamClosedSentinel(value: unknown): boolean {
  return typeof value === 'string' && value === DBOS_STREAM_CLOSED_SENTINEL;
}

/**
 * Whether a stored value is the marker as releases before the portable form wrote it: unserialized,
 * so no deserializer parses it. Callers must test this before deserializing.
 */
export function isLegacyClosedSentinel(serializedValue: string): boolean {
  return serializedValue === DBOS_STREAM_CLOSED_SENTINEL;
}

// LISTEN/NOTIFY channels. Streams and workflow_events are pushed by the notifier loop off the write path; notifications fires from an in-transaction DB trigger so recv is never woken before its row commits.
export const DBOS_NOTIFICATIONS_CHANNEL = 'dbos_notifications_channel';
export const DBOS_WORKFLOW_EVENTS_CHANNEL = 'dbos_workflow_events_channel';
export const DBOS_STREAMS_CHANNEL = 'dbos_streams_channel';

// Interval for coalescing LISTEN/NOTIFY notifications off the write path; caps the rate of notifying commits regardless of write throughput.
export const DEFAULT_NOTIFICATION_COALESCE_MS = 10;

export interface WorkflowScheduleInternal {
  scheduleId: string;
  scheduleName: string;
  workflowName: string;
  workflowClassName: string;
  schedule: string;
  status: string;
  context: string; // JSON-serialized
  lastFiredAt: string | null;
  automaticBackfill: boolean;
  cronTimezone: string | null;
  queueName: string | null;
  // Owning application; undefined leaves it unclaimed. Writers may name another.
  applicationName?: string;
}

// Definition fields updateSchedule can change in place. Only the keys present are updated; runtime state (schedule_id, status, last_fired_at) is left untouched.
export interface WorkflowScheduleUpdate {
  schedule?: string;
  context?: string; // JSON-serialized
  automaticBackfill?: boolean;
  cronTimezone?: string | null;
  queueName?: string | null;
}

export interface VersionInfo {
  versionId: string;
  versionName: string;
  versionTimestamp: number;
  createdAt: number;
  // Owning application; undefined if unclaimed.
  applicationName?: string;
}

/** Rows a rename moved, by table. */
export interface ApplicationRowCounts {
  queues: number;
  schedules: number;
  versions: number;
  workflows: number;
  steps: number;
}

// Workflows re-owned per transaction by a rename. Matches the GC default.
export const DEFAULT_RENAME_BATCH_SIZE = 10_000;

// Workflows deleted per transaction by garbage collection.
export const DEFAULT_GC_BATCH_SIZE = 10_000;

export interface QueueRecord {
  name: string;
  concurrency: number | null;
  workerConcurrency: number | null;
  rateLimitMax: number | null;
  rateLimitPeriodSec: number | null;
  priorityEnabled: boolean;
  partitionQueue: boolean;
  // Any of these being set partitions the queue; each applies per partition.
  partitionConcurrency: number | null;
  partitionWorkerConcurrency: number | null;
  partitionRateLimitMax: number | null;
  partitionRateLimitPeriodSec: number | null;
  pollingIntervalSec: number;
  // Owner from the queues table; undefined for in-memory and pre-upgrade queues.
  applicationName?: string;
}

/** The subset of a queue record that may be changed after creation. Ownership moves only by rename. */
export type QueueRecordUpdate = Partial<Omit<QueueRecord, 'name' | 'applicationName'>>;

const QUEUE_COLUMN_BY_FIELD: Record<keyof QueueRecordUpdate, string> = {
  concurrency: 'concurrency',
  workerConcurrency: 'worker_concurrency',
  rateLimitMax: 'rate_limit_max',
  rateLimitPeriodSec: 'rate_limit_period_sec',
  priorityEnabled: 'priority_enabled',
  partitionQueue: 'partition_queue',
  partitionConcurrency: 'partition_concurrency',
  partitionWorkerConcurrency: 'partition_worker_concurrency',
  partitionRateLimitMax: 'partition_rate_limit_max',
  partitionRateLimitPeriodSec: 'partition_rate_limit_period_sec',
  pollingIntervalSec: 'polling_interval_sec',
};

const QUEUE_COLUMNS =
  'name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec, priority_enabled, partition_queue, ' +
  'partition_concurrency, partition_worker_concurrency, partition_rate_limit_max, partition_rate_limit_period_sec, ' +
  'polling_interval_sec, application_name';

function queueRecordFromRow(row: queues): QueueRecord {
  return {
    name: row.name,
    concurrency: row.concurrency,
    workerConcurrency: row.worker_concurrency,
    rateLimitMax: row.rate_limit_max,
    rateLimitPeriodSec: row.rate_limit_period_sec,
    priorityEnabled: row.priority_enabled,
    partitionQueue: row.partition_queue,
    partitionConcurrency: row.partition_concurrency,
    partitionWorkerConcurrency: row.partition_worker_concurrency,
    partitionRateLimitMax: row.partition_rate_limit_max,
    partitionRateLimitPeriodSec: row.partition_rate_limit_period_sec,
    pollingIntervalSec: row.polling_interval_sec,
    applicationName: row.application_name ?? undefined,
  };
}

export interface WorkflowAggregateRow {
  group: Record<string, string | null>;
  count: number | null;
  minCreatedAt: number | null;
  maxQueueWaitMs: number | null;
  maxTotalLatencyMs: number | null;
}

export interface StepAggregateRow {
  group: Record<string, string | null>;
  count: number | null;
  maxDurationMs: number | null;
}

export interface GetWorkflowAggregatesInput {
  groupByStatus?: boolean;
  groupByName?: boolean;
  groupByQueueName?: boolean;
  groupByExecutorId?: boolean;
  groupByApplicationVersion?: boolean;
  groupByApplicationName?: boolean;
  selectCount?: boolean;
  selectMinCreatedAt?: boolean;
  selectMaxQueueWaitMs?: boolean;
  selectMaxTotalLatencyMs?: boolean;
  timeBucketSizeMs?: number;
  status?: string[];
  startTime?: string;
  endTime?: string;
  completedAfter?: string;
  completedBefore?: string;
  dequeuedAfter?: string;
  dequeuedBefore?: string;
  name?: string[];
  appVersion?: string[];
  executorId?: string[];
  queueName?: string[];
  workflowIdPrefix?: string[];
  workflowIDs?: string[];
  authenticatedUser?: string[];
  forkedFrom?: string[];
  wasForkedFrom?: boolean;
  parentWorkflowID?: string[];
  hasParent?: boolean;
  queuesOnly?: boolean;
  attributes?: Record<string, unknown>;
  scheduleName?: string[];
  // Count only these owning applications'. By default, only this application's.
  applicationName?: string[];
}

export interface GetStepAggregatesInput {
  groupByFunctionName?: boolean;
  groupByStatus?: boolean;
  selectCount?: boolean;
  selectMaxDurationMs?: boolean;
  timeBucketSizeMs?: number;
  status?: string[];
  functionName?: string[];
  workflowIdPrefix?: string[];
  completedAfter?: string;
  completedBefore?: string;
  // Count only these owning applications'. By default, only this application's.
  applicationName?: string[];
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
  createdAt?: number;
  updatedAt?: number;
  recoveryAttempts?: number;
  timeoutMS?: number;
  deadlineEpochMS?: number;
  deduplicationID?: string;
  priority: number;
  queuePartitionKey?: string;
  startedAtEpochMs?: number;
  forkedFrom?: string;
  wasForkedFrom?: boolean;
  parentWorkflowID?: string;
  serialization: string | null;
  delayUntilEpochMS?: number;
  completedAt?: number;
  // Custom key-value attributes attached to the workflow at creation. Not inherited by child workflows.
  attributes?: Record<string, unknown>;
  // If this workflow was enqueued by a named schedule, that schedule's name. Only set by the persistent scheduler.
  scheduleName?: string;
  // Absolute cap (epoch ms) beyond which bounces may not extend the delay; unset if not debounced or no timeout.
  debounceDeadlineEpochMS?: number;
  // True if this workflow's deduplication ID is a debounce key to clear on the DELAYED->ENQUEUED transition.
  isDebounced?: boolean;
  // Owning application; undefined writes an unclaimed row.
  applicationName?: string;
}

export interface EnqueueOptions {
  // Unique ID for deduplication on a queue
  deduplicationID?: string;
  // Priority of the workflow on the queue, starting from 1 ~ 2,147,483,647. Default 0 (highest priority).
  priority?: number;
  // Partition key for partitioned queues
  queuePartitionKey?: string;
  // Application version to set on the enqueued workflow (overrides the current app version)
  applicationVersion?: string;
  // Number of seconds to delay the workflow before it starts executing. The workflow will be in DELAYED status until the delay expires.
  delaySeconds?: number;
  // Internal, set only by the debouncer: absolute cap (epoch ms) on how far the delay may extend.
  debounceDeadlineEpochMS?: number;
  // Internal, set only by the debouncer: marks the deduplication ID as a debounce key.
  isDebounced?: boolean;
  // The application the workflow is enqueued for; undefined means the enqueuer's own.
  applicationName?: string;
}

// Arguments to debounceDelayedWorkflow: identify the debounced workflow by
// (name, class, queue, debounce key) and carry the new delay and inputs.
export interface DebounceParams {
  workflowName: string;
  workflowClassName: string;
  queueName: string;
  deduplicationID: string;
  delayUntilEpochMS: number;
  input: string | null;
  serialization: string | null;
  // The application the bounce acts for; undefined means the enqueuer's own.
  applicationName?: string;
}

export interface DebounceResult {
  // The extended workflow's ID if an existing debounced DELAYED workflow was bounced; null if no bounce occurred.
  bouncedWorkflowID: string | null;
  // The current holder of (queue_name, deduplication_id) when no bounce occurred, or null if the key is unheld.
  holderWorkflowID: string | null;
  // Whether the holder is itself a debounced workflow.
  holderIsDebounced: boolean;
  // The holder's workflow name and class; a mismatch with the caller's means a debounce-key collision between workflows.
  holderWorkflowName: string | null;
  holderWorkflowClassName: string | null;
  // The holder's owning application; a mismatch means the collision is across applications.
  holderApplicationName: string | null;
}

// How to handle a collision with another workflow that has the same `enqueueOptions.deduplicationID`
// on the same queue.
//   'reject' (default): throw `DBOSQueueDuplicatedError`.
//   'return-existing': return a handle to the existing workflow; arguments passed by the colliding
//     caller are discarded and the handle resolves with the original workflow's result.
export type DuplicationPolicy = 'reject' | 'return-existing';

export interface ExistenceCheck {
  exists: boolean;
}

export interface MetricData {
  metricType: string;
  metricName: string;
  value: number;
}

/** The statements granting permissions on all entities in the system schema to a role. */
export function getDbosSchemaPermissionsSql(schemaName: string, roleName: string): string[] {
  return [
    // Grant usage on the system schema
    `GRANT USAGE ON SCHEMA "${schemaName}" TO "${roleName}"`,
    // Grant all privileges on all existing tables in the system schema (includes views)
    `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "${schemaName}" TO "${roleName}"`,
    // Grant all privileges on all sequences in the system schema
    `GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "${schemaName}" TO "${roleName}"`,
    // Grant execute on all functions and procedures in the system schema
    `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "${schemaName}" TO "${roleName}"`,
    // Grant default privileges for future objects in the system schema
    `ALTER DEFAULT PRIVILEGES IN SCHEMA "${schemaName}" GRANT ALL ON TABLES TO "${roleName}"`,
    `ALTER DEFAULT PRIVILEGES IN SCHEMA "${schemaName}" GRANT ALL ON SEQUENCES TO "${roleName}"`,
    `ALTER DEFAULT PRIVILEGES IN SCHEMA "${schemaName}" GRANT EXECUTE ON FUNCTIONS TO "${roleName}"`,
  ];
}

export async function grantDbosSchemaPermissions(
  databaseUrl: string,
  roleName: string,
  logger: GlobalLogger,
  schemaName: string = 'dbos',
): Promise<void> {
  logger.info(`Granting permissions for ${schemaName} schema to ${roleName}`);

  const client = new Client(getClientConfig(databaseUrl));
  await client.connect();

  try {
    for (const sql of getDbosSchemaPermissionsSql(schemaName, roleName)) {
      logger.info(sql);
      await client.query(sql);
    }
  } catch (e) {
    logger.error(`Failed to grant permissions to role ${roleName}: ${(e as Error).message}`);
    throw e;
  } finally {
    await client.end();
  }
}

/**
 * Check out a connection carrying our own error handler, so a socket death while we hold it is
 * logged instead of thrown at an emitter with no listener. `release()` takes the handler back off,
 * so nothing we attach outlives the checkout and the pool itself is never touched.
 */
async function borrowClient(pool: Pool, onError: (err: Error) => void): Promise<PoolClient> {
  const client = await pool.connect();
  // Nothing awaits between the checkout and this attach, nor between the removal and release()
  // below, so there is no turn of the event loop in which we hold the connection unguarded.
  client.on('error', onError);
  const release = client.release.bind(client);
  client.release = (err?: Error | boolean) => {
    client.removeListener('error', onError);
    release(err);
  };
  return client;
}

/** Connect to the system database without creating it. The caller releases the client. */
async function connectToSystemDatabase(sysDbUrl: string, logger: GlobalLogger, customPool?: Pool): Promise<ClientBase> {
  if (customPool) {
    return await borrowClient(customPool, (err: Error) =>
      logger.warn(`Unexpected error in system database client: ${err}`),
    );
  }
  const sysClient = new Client(getClientConfig(sysDbUrl));
  // An 'error' event with no listener would take down the process.
  sysClient.on('error', (err: Error) => logger.warn(`Unexpected error in system database client: ${err}`));
  try {
    await sysClient.connect();
  } catch (e) {
    await sysClient.end().catch(() => {});
    throw new DBOSInitializationError(
      `Unable to connect to system database at ${maskDatabaseUrl(sysDbUrl)}: ${(e as Error).message}`,
      e instanceof Error ? e : undefined,
    );
  }
  return sysClient;
}

async function releaseSystemDatabaseClient(client: ClientBase, customPool?: Pool): Promise<void> {
  try {
    if (customPool) {
      (client as PoolClient).release();
    } else {
      await (client as Client).end();
    }
  } catch (e) {}
}

async function isCockroachDB(client: ClientBase): Promise<boolean> {
  const versionRes = await client.query<{ version: string }>('SELECT version() AS version');
  return /cockroachdb/i.test(versionRes.rows[0]?.version ?? '');
}

export async function ensureSystemDatabase(
  sysDbUrl: string,
  logger: GlobalLogger,
  customPool?: Pool,
  schemaName: string = 'dbos',
  useListenNotify: boolean = true,
) {
  if (!customPool) {
    // A custom pool means the database already exists; otherwise, create it if it does not.
    await ensurePGDatabase(sysDbUrl, logger);
  }
  const client = await connectToSystemDatabase(sysDbUrl, logger, customPool);

  try {
    const isCockroach = await isCockroachDB(client);
    await runSysMigrationsPg(client, allMigrations(schemaName, { useListenNotify, isCockroach }), schemaName, {
      onWarn: (e: string) => logger.info(e),
      isCockroach,
    });
  } finally {
    await releaseSystemDatabaseClient(client, customPool);
  }
}

/** Check the system database is migrated to the version this build requires, creating and changing nothing. */
export async function verifySystemDatabase(
  sysDbUrl: string,
  logger: GlobalLogger,
  customPool?: Pool,
  schemaName: string = 'dbos',
  useListenNotify: boolean = true,
) {
  const client = await connectToSystemDatabase(sysDbUrl, logger, customPool);

  try {
    const isCockroach = await isCockroachDB(client);
    const requiredVersion = allMigrations(schemaName, { useListenNotify, isCockroach }).length;
    const currentVersion = await getCurrentSysDBVersion(client, schemaName);
    // A database ahead of this build belongs to a newer peer, which the migration runner also tolerates.
    if (currentVersion < requiredVersion) {
      throw new DBOSInitializationError(
        `System database ${maskDatabaseUrl(sysDbUrl)} is at schema version ${currentVersion}, but this version ` +
          `of DBOS requires ${requiredVersion}. This process is configured with runMigrations disabled, so it ` +
          `will not migrate it: either migrate the system database out of band (\`npx dbos schema\`) or launch ` +
          `with runMigrations enabled.`,
      );
    }
    logger.debug(`System database schema version ${currentVersion} satisfies the required version ${requiredVersion}`);
  } finally {
    await releaseSystemDatabaseClient(client, customPool);
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
  status: string;
  name: string;
  class_name: string;
  config_name: string;
  queue_name: string | null;
  workflow_deadline_epoch_ms: number | null;
  executor_id: string | null;
  owner_xid: string | null;
  serialization: string | null;
}

function mapVersionInfo(row: application_versions): VersionInfo {
  return {
    versionId: row.version_id,
    versionName: row.version_name,
    versionTimestamp: Number(row.version_timestamp),
    createdAt: Number(row.created_at),
    applicationName: row.application_name ?? undefined,
  };
}

const SCHEDULE_COLUMNS =
  'schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name, application_name';

function mapWorkflowSchedule(row: workflow_schedules): WorkflowScheduleInternal {
  return {
    scheduleId: row.schedule_id,
    scheduleName: row.schedule_name,
    workflowName: row.workflow_name,
    workflowClassName: row.workflow_class_name,
    schedule: row.schedule,
    status: row.status,
    context: row.context,
    lastFiredAt: row.last_fired_at ?? null,
    automaticBackfill: !!row.automatic_backfill,
    cronTimezone: row.cron_timezone ?? null,
    queueName: row.queue_name ?? null,
    applicationName: row.application_name ?? undefined,
  };
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
    queuePartitionKey: row.queue_partition_key ?? undefined,
    startedAtEpochMs: row.started_at_epoch_ms ? Number(row.started_at_epoch_ms) : undefined,
    forkedFrom: row.forked_from ?? undefined,
    wasForkedFrom: row.was_forked_from ?? false,
    parentWorkflowID: row.parent_workflow_id ?? undefined,
    serialization: row.serialization,
    delayUntilEpochMS: row.delay_until_epoch_ms ? Number(row.delay_until_epoch_ms) : undefined,
    completedAt: row.completed_at ? Number(row.completed_at) : undefined,
    attributes: row.attributes ?? undefined,
    scheduleName: row.schedule_name ?? undefined,
    debounceDeadlineEpochMS: row.debounce_deadline_epoch_ms ? Number(row.debounce_deadline_epoch_ms) : undefined,
    isDebounced: row.is_debounced ?? false,
    applicationName: row.application_name ?? undefined,
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

/**
 * Kept out of the sets above: those feed `dbRetry`, which retries forever, and the step-recording
 * path maps 40001 to a workflow conflict. Only bulk maintenance work retries on these.
 */
const SERIALIZATION_SQLSTATE_CODES = new Set([
  '40001', // serialization_failure (MVCC conflict)
  '40P01', // deadlock_detected
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

function isSerializationError(err: unknown): boolean {
  for (const e of unwrapErrors(err)) {
    const anyErr = e as AnyErr;
    if (isPgDatabaseError(anyErr) && !!anyErr.code && SERIALIZATION_SQLSTATE_CODES.has(anyErr.code)) {
      return true;
    }
  }
  return false;
}

/**
 * Re-run a batch that lost a deadlock or serialization race. The database already rolled it
 * back, so replaying it is safe.
 */
async function retryOnSerializationError<T>(operation: () => Promise<T>): Promise<T> {
  const maxAttempts = 10;
  const maxBackoff = 2.0;
  let backoff = 0.05;
  for (let attempt = 1; ; attempt++) {
    try {
      return await operation();
    } catch (e) {
      if (attempt === maxAttempts || !isSerializationError(e)) {
        throw e;
      }
      // Jittered backoff, so peers that collided do not collide again
      const actualBackoff = backoff * (0.5 + Math.random());
      DBOSExecutor.globalInstance?.logger.debug(
        `Garbage collection lost a concurrency race: ${e instanceof Error ? e.message : String(e)}. ` +
          `Retrying in ${actualBackoff.toFixed(2)}s (attempt ${attempt})`,
      );
      await sleepms(actualBackoff * 1000);
      backoff = Math.min(backoff * 2, maxBackoff);
    }
  }
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
  return function <T extends (...args: never[]) => Promise<unknown>>(
    target: unknown,
    propertyName: string,
    descriptor: TypedPropertyDescriptor<T>,
  ): TypedPropertyDescriptor<T> {
    const method = descriptor.value!;
    descriptor.value = async function (this: never, ...args: never): Promise<unknown> {
      // Read the defaults per call so the backoff stays tunable after the decorator is applied.
      const maxBackoff = options.maxBackoff ?? dbRetryConfig.maxBackoffSec;
      let retries = 0;
      let backoff = options.initialBackoff ?? dbRetryConfig.initialBackoffSec;
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
export class SystemDatabase {
  // ==================== Lifecycle ====================
  readonly pool: Pool;
  readonly schemaName: string;

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
  dbPollingIntervalStreamMs: number = 1000;
  shouldUseDBNotifications: boolean = true;
  readonly notificationsMap: NotificationMap<void> = new NotificationMap();
  readonly workflowEventsMap: NotificationMap<void> = new NotificationMap();
  readonly streamsMap: NotificationMap<void> = new NotificationMap();
  customPool: boolean = false;

  // Interval for coalescing LISTEN/NOTIFY notifications pushed off the write path (Postgres + L/N only).
  readonly notificationCoalesceMs: number = DEFAULT_NOTIFICATION_COALESCE_MS;
  // Coalesced NOTIFY payloads keyed by channel, flushed by the notifier loop; soft-private so tests can drive a flush.
  private pendingNotifications: Map<string, Set<string>> = new Map();
  #notifierActive: boolean = false;
  // Wakes the notifier out of its coalescing sleep so shutdown flushes promptly.
  #notifierWake: (() => void) | null = null;
  // The notifier loop's completion, awaited on destroy so a final flush precedes closing the pool.
  #notifierLoop: Promise<void> | undefined = undefined;

  /**
   * Caps how many DB-backed polling reads (from wait operations) may run
   * concurrently against the pool, so a polling storm cannot check out every
   * client and starve control-plane operations. See {@link #pollWithLimiter}.
   */
  readonly pollLimiter: Semaphore;

  readonly runningWorkflowMap: Map<
    string,
    { promise: Promise<unknown>; queueName?: string; queuePartitionKey?: string }
  > = new Map(); // Map from workflowID to workflow promise, queue name and partition key

  // Per-partition-key created_at cursors: keep per-key queue order monotonic across batches
  readonly #batchCreatedAtCursors: Map<string, number> = new Map();

  // Set by destroy(), so polling waits end instead of running on against a pool that outlives this handle.
  #destroyed: boolean = false;

  constructor(
    readonly systemDatabaseUrl: string,
    readonly logger: GlobalLogger,
    readonly serializer: DBOSSerializer,
    sysDbPoolSize: number = DEFAULT_POOL_SIZE,
    systemDatabasePool?: Pool,
    schemaName: string = 'dbos',
    useListenNotify: boolean = true,
    pollingConcurrency?: number,
    notificationCoalesceMs: number = DEFAULT_NOTIFICATION_COALESCE_MS,
    // The application this handle acts for; undefined writes unclaimed rows.
    readonly appName?: string,
  ) {
    this.schemaName = schemaName;
    this.shouldUseDBNotifications = useListenNotify;
    this.notificationCoalesceMs = notificationCoalesceMs;

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

    // Default the polling limit to half the pool (minimum 1), reserving the rest
    // of the pool for control-plane operations.
    const effectivePoolSize = this.pool.options.max ?? sysDbPoolSize;
    const pollingLimit = pollingConcurrency ?? Math.max(1, Math.floor(effectivePoolSize / 2));
    this.pollLimiter = new Semaphore(pollingLimit);

    // Only ever attach listeners to a pool we own; a caller's pool is theirs to instrument. Idle
    // connections are all this covers, since #connect guards the ones we are holding.
    if (!this.customPool) {
      this.pool.on('error', (err: Error) => {
        this.logger.warn(`Unexpected error in pool: ${err}`);
      });
    }
  }

  readonly #onClientError = (err: Error) => {
    this.logger.warn(`Unexpected error on a system database connection: ${err}`);
  };

  /** Check out a pool connection guarded for as long as we hold it. See {@link borrowClient}. */
  #connect(): Promise<PoolClient> {
    return borrowClient(this.pool, this.#onClientError);
  }
  getSerializer(): DBOSSerializer {
    return this.serializer;
  }

  // ==================== Application Ownership ====================

  /**
   * A predicate matching rows owned by these applications plus unclaimed ones, which belong to
   * every application; unset or empty matches everything. Appends its bind parameter to `params`.
   */
  #appNameFilter(column: string, value: string | string[] | null | undefined, params: unknown[]): string {
    // An empty name is no name: it is not a value any application could be configured with.
    const names = !value ? [] : Array.isArray(value) ? value : [value];
    if (names.length === 0) return 'TRUE';
    params.push(names);
    return `(${column} = ANY($${params.length}) OR ${column} IS NULL)`;
  }

  /**
   * The filter above defaulted to this handle's own application: an unset filter scopes to what
   * this application owns, not to every application's rows. A handle with no application of its
   * own still matches every one.
   */
  #observabilityFilter(column: string, value: string | string[] | null | undefined, params: unknown[]): string {
    return this.#appNameFilter(column, value ?? this.appName, params);
  }

  /**
   * The version name a dequeue treats as latest: this application's own plus unclaimed
   * ones, so a named peer's deploy does not demote this one.
   */
  async #latestApplicationVersionName(client: PoolClient | Pool): Promise<string | undefined> {
    const params: unknown[] = [];
    const scope = this.#appNameFilter('application_name', this.appName, params);
    const { rows } = await client.query<{ version_name: string }>(
      `SELECT version_name
         FROM "${this.schemaName}".application_versions
        WHERE ${scope}
        ORDER BY version_timestamp DESC LIMIT 1`,
      params,
    );
    return rows[0]?.version_name;
  }

  /**
   * The owner to persist when writing a row that may already exist. A nameless writer
   * leaves the owner intact; a named one collides only with a different name.
   */
  async #resolveRowOwner(
    client: PoolClient | Pool,
    table: string,
    keyColumn: string,
    name: string,
    owner: string | undefined,
    kind: string,
  ): Promise<string | undefined> {
    const { rows } = await client.query<{ application_name: string | null }>(
      `SELECT application_name FROM "${this.schemaName}".${table} WHERE ${keyColumn} = $1`,
      [name],
    );
    const current = rows[0]?.application_name ?? null;
    if (current === null) return owner;
    if (owner === undefined || current === owner) return current;
    // A version name is computed or pinned, so "pick another" is config advice, not a rename.
    const takeANewName =
      kind === 'Application version'
        ? `set a distinct applicationVersion for '${owner}'`
        : `give '${owner}' a different ${kind.toLowerCase()} name`;
    throw new DBOSError(
      `${kind} '${name}' is already registered by application '${current}' in this system database. ` +
        `${kind} names must be unique across applications sharing a system database. ` +
        `Either ${takeANewName}, or, if '${current}' was renamed to '${owner}', ` +
        `re-own its rows first with dbos rename-application`,
    );
  }

  /** Migrates the system database, or, when `runMigrations` is false, verifies it is already migrated. */
  async init(runMigrations: boolean = true) {
    const migrateOrVerify = runMigrations ? ensureSystemDatabase : verifySystemDatabase;
    await migrateOrVerify(
      this.systemDatabaseUrl,
      this.logger,
      this.customPool ? this.pool : undefined,
      this.schemaName,
      this.shouldUseDBNotifications,
    );

    if (this.shouldUseDBNotifications) {
      await this.#listenForNotifications();
      // Push coalesced stream and event notifications off the write path.
      this.#notifierActive = true;
      this.#notifierLoop = this.#runNotifier();
    }
  }

  async destroy() {
    // Set synchronously, before any await, so no reconnect is scheduled or published after this point.
    this.#notificationsStopped = true;
    this.#destroyed = true;
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
    // Stop the notifier and await its final flush before the pool closes.
    this.#notifierActive = false;
    this.#notifierWake?.();
    if (this.#notifierLoop) {
      await this.#notifierLoop;
      this.#notifierLoop = undefined;
    }
    if (this.notificationsClient) {
      this.#retireNotificationsClient(this.notificationsClient);
    }
    // We attached nothing to the pool object itself, so there is nothing to unpick; only close one we own.
    if (!this.customPool) {
      await this.pool.end();
    }
  }

  // ==================== Workflow Status ====================
  /** Runs on `client` if given, joining its transaction; otherwise in its own retried transaction. */
  async initWorkflowStatus(
    initStatus: WorkflowStatusInternal,
    ownerXid: string | null,
    client?: ClientBase,
  ): Promise<{
    status: string;
    shouldExecuteOnThisExecutor: boolean;
    deadlineEpochMS?: number;
    serialization: SysDBSerializationFormat | null;
  }> {
    if (client !== undefined) {
      return await this.#initWorkflowStatusInternal(client, initStatus, ownerXid);
    }
    return await this.initWorkflowStatusStandalone(initStatus, ownerXid);
  }

  @dbRetry()
  private async initWorkflowStatusStandalone(
    initStatus: WorkflowStatusInternal,
    ownerXid: string | null,
  ): Promise<{
    status: string;
    shouldExecuteOnThisExecutor: boolean;
    deadlineEpochMS?: number;
    serialization: SysDBSerializationFormat | null;
  }> {
    const client = await this.#connect();
    let shouldCommit = false;
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const result = await this.#initWorkflowStatusInternal(client, initStatus, ownerXid);
      // If there is an existing DB record and we aren't here to recover it, leave it be.
      shouldCommit = result.shouldExecuteOnThisExecutor;
      return result;
    } finally {
      try {
        if (shouldCommit) {
          await client.query('COMMIT');
          await debugTriggerPoint(DEBUG_TRIGGER_INITWF_COMMIT);
        } else {
          await client.query('ROLLBACK');
        }
      } finally {
        client.release();
      }
    }
  }

  async #initWorkflowStatusInternal(
    client: ClientBase,
    initStatus: WorkflowStatusInternal,
    ownerXid: string | null,
  ): Promise<{
    status: string;
    shouldExecuteOnThisExecutor: boolean;
    deadlineEpochMS?: number;
    serialization: SysDBSerializationFormat | null;
  }> {
    const resRow = await this.insertWorkflowStatus(client, initStatus, ownerXid);
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

    const status = resRow.status;
    const deadlineEpochMS = resRow.workflow_deadline_epoch_ms ?? undefined;

    // The upsert above already set executor assignment for a row we own.
    return {
      status,
      deadlineEpochMS,
      shouldExecuteOnThisExecutor: ownerXid === resRow.owner_xid,
      serialization: resRow.serialization,
    };
  }

  /** Move claimed workflows that exhausted their attempts off the queue, leaving rows others have moved on alone. */
  @dbRetry()
  async deadLetterWorkflows(workflowIDs: string[], minRecoveryAttempts: number): Promise<void> {
    if (workflowIDs.length === 0) return;
    await this.pool.query(
      `UPDATE "${this.schemaName}".workflow_status
       SET status = $1,
           deduplication_id = NULL,
           started_at_epoch_ms = NULL,
           queue_name = NULL,
           updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           completed_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
       WHERE workflow_uuid = ANY($2::text[]) AND status = $3 AND recovery_attempts >= $4`,
      [StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED, workflowIDs, StatusString.PENDING, minRecoveryAttempts],
    );
  }

  /** Highest created_at among still-active rows per partition key, used to seed the in-memory cursor. */
  async #maxPartitionKeyCreatedAt(keys: string[]): Promise<Map<string, number>> {
    const maxima = new Map<string, number>();
    if (keys.length === 0) return maxima;
    const { rows } = await this.pool.query<{ queue_partition_key: string; max_created_at: string | null }>(
      `SELECT queue_partition_key, MAX(created_at) AS max_created_at
       FROM "${this.schemaName}".workflow_status
       WHERE queue_partition_key = ANY($1) AND status = ANY($2)
       GROUP BY queue_partition_key`,
      [keys, [StatusString.ENQUEUED, StatusString.PENDING]],
    );
    for (const row of rows) {
      if (row.max_created_at !== null) {
        maxima.set(row.queue_partition_key, Number(row.max_created_at));
      }
    }
    return maxima;
  }

  /**
   * Stamp created_at monotonic within each partition key so per-key order holds across batches.
   * Unordered rows (no partition key) get wall-clock time and never touch the cursors.
   */
  async #assignBatchCreatedAt(statuses: WorkflowStatusInternal[]): Promise<number[]> {
    const nowMS = Date.now();
    const batchKeys = new Set<string>();
    for (const status of statuses) {
      if (status.queuePartitionKey !== undefined) {
        batchKeys.add(status.queuePartitionKey);
      }
    }
    // On first sight of a key, seed its cursor from the DB high-water mark so per-key order
    // survives a restart or rebalance instead of resetting to wall-clock.
    const unseen = Array.from(batchKeys).filter((key) => !this.#batchCreatedAtCursors.has(key));
    const seeds = await this.#maxPartitionKeyCreatedAt(unseen);
    // Synchronous from here, so a concurrent batch cannot interleave with these cursor updates.
    for (const [key, seededMax] of seeds) {
      // max() guards against a concurrent batch that already advanced this key.
      this.#batchCreatedAtCursors.set(key, Math.max(this.#batchCreatedAtCursors.get(key) ?? 0, seededMax + 1));
    }
    const createdAts: number[] = [];
    const nextForKey = new Map<string, number>();
    for (const status of statuses) {
      const key = status.queuePartitionKey;
      if (key === undefined) {
        createdAts.push(nowMS);
        continue;
      }
      const value = nextForKey.get(key) ?? Math.max(nowMS, this.#batchCreatedAtCursors.get(key) ?? 0);
      createdAts.push(value);
      nextForKey.set(key, value + 1);
    }
    for (const [key, next] of nextForKey) {
      this.#batchCreatedAtCursors.set(key, next);
    }
    return createdAts;
  }

  /**
   * Batch-insert ENQUEUED workflow status rows in a single transaction.
   *
   * Rows whose workflow_uuid already exists are skipped rather than updated, making this
   * idempotent under redelivery (e.g. Kafka). Returns the IDs of the rows actually inserted.
   *
   * Deliberately not `@dbRetry()`-decorated, unlike its neighbours: that loop is unabortable, so a
   * connection outage would trap the caller in it rather than let it back off and observe a
   * shutdown. Callers retry this themselves.
   */
  async enqueueWorkflows(statuses: WorkflowStatusInternal[]): Promise<Set<string>> {
    const inserted = new Set<string>();
    if (statuses.length === 0) return inserted;
    for (const status of statuses) {
      if (status.status !== StatusString.ENQUEUED) {
        throw new DBOSError(
          `enqueueWorkflows only accepts ${StatusString.ENQUEUED} workflows, but ${status.workflowUUID} is ${status.status}`,
        );
      }
      if (status.deduplicationID !== undefined) {
        throw new DBOSError(`enqueueWorkflows does not support deduplication IDs, but ${status.workflowUUID} has one`);
      }
    }
    const createdAts = await this.#assignBatchCreatedAt(statuses);
    const columns = [
      'workflow_uuid',
      'status',
      'name',
      'class_name',
      'config_name',
      'queue_name',
      'authenticated_user',
      'assumed_role',
      'authenticated_roles',
      'request',
      'executor_id',
      'application_version',
      'application_id',
      'created_at',
      'recovery_attempts',
      'updated_at',
      'workflow_timeout_ms',
      'workflow_deadline_epoch_ms',
      'inputs',
      'deduplication_id',
      'priority',
      'queue_partition_key',
      'parent_workflow_id',
      'serialization',
      'owner_xid',
      'delay_until_epoch_ms',
      'attributes',
      'schedule_name',
      'application_name',
    ];
    const client = await this.#connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      // Chunk to stay well under the bind-parameter limit.
      const chunkSize = 500;
      for (let start = 0; start < statuses.length; start += chunkSize) {
        const chunk = statuses.slice(start, start + chunkSize);
        const tuples: string[] = [];
        const params: unknown[] = [];
        let paramIdx = 1;
        for (let i = 0; i < chunk.length; i++) {
          const status = chunk[i];
          const createdAt = createdAts[start + i];
          tuples.push(`(${columns.map(() => `$${paramIdx++}`).join(', ')})`);
          params.push(
            status.workflowUUID,
            status.status,
            status.workflowName,
            // For cross-language compatibility, these MUST be NULL in the database when not set
            status.workflowClassName === '' ? null : status.workflowClassName,
            status.workflowConfigName === '' ? null : status.workflowConfigName,
            status.queueName ?? null,
            status.authenticatedUser,
            status.assumedRole,
            JSON.stringify(status.authenticatedRoles),
            JSON.stringify(status.request),
            status.executorId,
            status.applicationVersion ?? null,
            status.applicationID,
            createdAt,
            0,
            createdAt,
            status.timeoutMS ?? null,
            status.deadlineEpochMS ?? null,
            status.input,
            null,
            status.priority,
            status.queuePartitionKey ?? null,
            status.parentWorkflowID ?? null,
            status.serialization,
            null,
            status.delayUntilEpochMS ?? null,
            status.attributes ? JSON.stringify(status.attributes) : null,
            status.scheduleName ?? null,
            status.applicationName ?? null,
          );
        }
        const { rows } = await client.query<{ workflow_uuid: string }>(
          `INSERT INTO "${this.schemaName}".workflow_status (${columns.join(', ')})
           VALUES ${tuples.join(', ')}
           ON CONFLICT (workflow_uuid) DO NOTHING
           RETURNING workflow_uuid`,
          params,
        );
        for (const row of rows) {
          inserted.add(row.workflow_uuid);
        }
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
    return inserted;
  }

  @dbRetry()
  async recordWorkflowOutput(workflowID: string, status: WorkflowStatusInternal): Promise<boolean> {
    const client = await this.#connect();
    try {
      return await this.#recordWorkflowOutcome(client, workflowID, StatusString.SUCCESS, { output: status.output });
    } finally {
      client.release();
    }
  }

  @dbRetry()
  async recordWorkflowError(workflowID: string, status: WorkflowStatusInternal): Promise<boolean> {
    const client = await this.#connect();
    try {
      return await this.#recordWorkflowOutcome(client, workflowID, StatusString.ERROR, { error: status.error });
    } finally {
      client.release();
    }
  }

  // Record a workflow's terminal outcome (SUCCESS or ERROR), reporting whether
  // the write landed. The write applies only to a PENDING row: a run owns its
  // workflow's outcome exactly as long as the row says that run is what the
  // workflow is doing. (Note: this does not prevent a write when another
  // concurrent execution is already running and the status is PENDING. However,
  // both executions should be deterministic and idempotent.)
  //
  // Returning false means the row was CANCELLED, dead-lettered, already
  // terminal, handed to another execution (ENQUEUED/DELAYED, e.g. by a
  // concurrent resume), or deleted; the caller resolves which by awaiting the
  // recorded outcome.
  async #recordWorkflowOutcome(
    client: PoolClient,
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    outcome: { output?: string | null; error?: string | null },
  ): Promise<boolean> {
    const rowCount = await this.updateWorkflowStatus(client, workflowID, status, {
      update: { ...outcome, resetDeduplicationID: true, setCompletedAt: true },
      where: { status: StatusString.PENDING },
      throwOnFailure: false,
    });
    return rowCount > 0;
  }

  async getPendingWorkflows(executorID: string, appVersion: string): Promise<GetPendingWorkflowsOutput[]> {
    const params: unknown[] = [StatusString.PENDING, executorID, appVersion];
    // executor_id defaults to "local", so it collides across applications.
    const scope = this.#appNameFilter('application_name', this.appName, params);
    const getWorkflows = await this.pool.query<workflow_status>(
      `SELECT workflow_uuid
       FROM "${this.schemaName}".workflow_status
       WHERE status=$1 AND executor_id=$2 AND application_version=$3 AND ${scope}`,
      params,
    );
    return getWorkflows.rows.map(
      (i) =>
        <GetPendingWorkflowsOutput>{
          workflowUUID: i.workflow_uuid,
        },
    );
  }

  // Recovery re-enqueues rather than executing directly so the queue's atomic dequeue admits exactly one runner, and the executor ID predicate rejects sweeps for rows a live executor has already claimed.
  async reenqueueWorkflowsForRecovery(
    executorID: string,
    appVersion: string,
    recoveryQueueName: string,
  ): Promise<string[]> {
    const params: unknown[] = [StatusString.ENQUEUED, recoveryQueueName, StatusString.PENDING, executorID, appVersion];
    // executor_id defaults to "local", so it collides across applications.
    const scope = this.#appNameFilter('application_name', this.appName, params);
    const result = await this.pool.query<{ workflow_uuid: string }>(
      `UPDATE "${this.schemaName}".workflow_status
       SET started_at_epoch_ms = NULL,
           status = $1,
           updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           queue_name = COALESCE(queue_name, $2)
       WHERE status = $3
         AND executor_id = $4
         AND application_version = $5
         AND ${scope}
       RETURNING workflow_uuid`,
      params,
    );
    return result.rows.map((row) => row.workflow_uuid);
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
      const client = await this.#connect();
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

  /** Max IDs per {@link getWorkflowStatuses} fetch: listWorkflows binds one parameter per ID. */
  statusFetchChunkSize: number = 500;

  // Retried per chunk so a reconnect refetches one chunk, not every chunk before it.
  @dbRetry()
  private async fetchWorkflowStatusChunk(workflowIDs: string[]): Promise<WorkflowStatusInternal[]> {
    return await this.listWorkflows({ workflowIDs, loadInput: true, loadOutput: false });
  }

  /** Fetch many statuses in as few round trips as possible. IDs with no row are omitted. */
  async getWorkflowStatuses(workflowIDs: string[]): Promise<Map<string, WorkflowStatusInternal>> {
    const statuses = new Map<string, WorkflowStatusInternal>();
    for (let start = 0; start < workflowIDs.length; start += this.statusFetchChunkSize) {
      for (const status of await this.fetchWorkflowStatusChunk(
        workflowIDs.slice(start, start + this.statusFetchChunkSize),
      )) {
        statuses.set(status.workflowUUID, status);
      }
    }
    return statuses;
  }

  // Only used in tests
  async setWorkflowStatus(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    resetRecoveryAttempts: boolean,
    internalOptions?: {
      updateName?: string;
      queueName?: string;
      resetStartedAtEpochMs?: boolean;
    },
  ): Promise<void> {
    const client = await this.#connect();
    try {
      await this.updateWorkflowStatus(client, workflowID, status, {
        update: {
          resetRecoveryAttempts,
          resetNameTo: internalOptions?.updateName,
          queueName: internalOptions?.queueName,
          resetStartedAtEpochMs: internalOptions?.resetStartedAtEpochMs,
        },
      });
    } finally {
      client.release();
    }
  }

  // ==================== Step Results ====================
  @dbRetry()
  async getOperationResultAndThrowIfCancelled(
    workflowID: string,
    functionID: number,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    const client = await this.#connect();
    try {
      return await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
    } finally {
      client.release();
    }
  }

  async getAllOperationResults(workflowID: string, limit?: number, offset?: number): Promise<operation_outputs[]> {
    let query = `SELECT * FROM "${this.schemaName}".operation_outputs WHERE workflow_uuid=$1 ORDER BY function_id`;
    const params: unknown[] = [workflowID];
    if (limit !== undefined) {
      params.push(limit);
      query += ` LIMIT $${params.length}`;
    }
    if (offset !== undefined) {
      params.push(offset);
      query += ` OFFSET $${params.length}`;
    }
    const { rows } = await this.pool.query<operation_outputs>(query, params);
    return rows;
  }

  @dbRetry()
  async recordOperationResult(
    workflowID: string,
    functionID: number,
    functionName: string,
    checkConflict: boolean,
    startTimeEpochMs: number,
    endTimeEpochMs: number,
    options: {
      childWorkflowID?: string | null;
      output?: string | null;
      error?: string | null;
      serialization?: string | null;
    } = {},
  ): Promise<void> {
    const client = await this.#connect();
    try {
      await this.recordOperationResultInternal(
        client,
        workflowID,
        functionID,
        functionName,
        checkConflict,
        startTimeEpochMs,
        endTimeEpochMs,
        options,
      );
    } finally {
      client.release();
      await debugTriggerPoint(DEBUG_TRIGGER_STEP_COMMIT);
    }
  }

  async runTransactionalStep(
    workflowID: string,
    functionID: number,
    functionName: string,
    callback: (client: PoolClient) => Promise<string | null>,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    const client = await this.#connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const existing = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
      if (existing !== undefined) {
        await client.query('ROLLBACK');
        return existing;
      }
      const startTime = Date.now();
      const output = await callback(client);
      await this.recordOperationResultInternal(
        client,
        workflowID,
        functionID,
        functionName,
        true,
        startTime,
        Date.now(),
        {
          output,
        },
      );
      await client.query('COMMIT');
      await debugTriggerPoint(DEBUG_TRIGGER_STEP_COMMIT);
      return undefined;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  @dbRetry()
  async checkPatch(
    workflowID: string,
    functionID: number,
    patchName: string,
    deprecated: boolean,
  ): Promise<{ isPatched: boolean; hasEntry: boolean }> {
    // Not doing a cancel check at this point.
    if (functionID === undefined) throw new TypeError('functionID must be defined');

    patchName = `DBOS.patch-${patchName}`;

    const { rows } = await this.pool.query<operation_outputs>(
      `SELECT function_name
       FROM "${this.schemaName}".operation_outputs
      WHERE workflow_uuid=$1 AND function_id=$2`,
      [workflowID, functionID],
    );

    if (deprecated) {
      // Deprecated does not write anything.  We skip any existing matching patch marker if it matches
      if (rows.length === 0) {
        return { isPatched: true, hasEntry: false };
      }
      return { isPatched: true, hasEntry: rows[0].function_name === patchName };
    }

    // Nondeprecated - skip matching entry, unpatched if nonmatching entry,
    //  If there is no entry, we insert one that indicates it is patched.
    if (rows.length !== 0) {
      if (rows[0].function_name === patchName) {
        return { isPatched: true, hasEntry: true };
      }
      return { isPatched: false, hasEntry: false };
    }

    // Insert a patchmarker
    const dn = Date.now();
    await this.pool.query<operation_outputs>(
      `INSERT INTO ${this.schemaName}.operation_outputs
       (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, application_name)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT DO NOTHING;`,
      [workflowID, functionID, null, null, patchName, null, dn, dn, this.appName ?? null],
    );

    return { isPatched: true, hasEntry: true };
  }

  // ==================== Workflow Management ====================
  async cancelWorkflows(workflowIDs: string[], cancelChildren: boolean = false): Promise<void> {
    if (!cancelChildren) {
      await this.#cancelWorkflows(workflowIDs);
      return;
    }

    // Cascade cancellation to child workflows level by level.
    const visited = new Set<string>(workflowIDs);
    let frontier = workflowIDs;
    while (frontier.length > 0) {
      await this.#cancelWorkflows(frontier);
      const children = await this.#getDirectChildren(frontier);
      frontier = children.filter((id) => !visited.has(id));
      for (const id of frontier) {
        visited.add(id);
      }
    }
  }

  async #cancelWorkflows(workflowIDs: string[]): Promise<void> {
    await this.pool.query(
      `UPDATE "${this.schemaName}".workflow_status
       SET status = $1, queue_name = NULL, deduplication_id = NULL, started_at_epoch_ms = NULL,
           updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           completed_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
       WHERE workflow_uuid = ANY($2)
         AND status NOT IN ($3, $4)`,
      [StatusString.CANCELLED, workflowIDs, StatusString.SUCCESS, StatusString.ERROR],
    );
  }

  @dbRetry()
  async checkIfCanceled(workflowID: string): Promise<void> {
    await this.#checkIfCanceled(this.pool, workflowID);
  }

  async resumeWorkflows(workflowIDs: string[], queueName?: string): Promise<void> {
    await this.pool.query(
      `UPDATE "${this.schemaName}".workflow_status
       SET status = $1, queue_name = $2, recovery_attempts = 0,
           workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
           started_at_epoch_ms = NULL,
           updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           completed_at = NULL
       WHERE workflow_uuid = ANY($3)
         AND status NOT IN ($4, $5)`,
      [StatusString.ENQUEUED, queueName ?? INTERNAL_QUEUE_NAME, workflowIDs, StatusString.SUCCESS, StatusString.ERROR],
    );
  }

  async setWorkflowPriority(workflowID: string, priority: number): Promise<void> {
    await this.pool.query(
      `UPDATE "${this.schemaName}".workflow_status
       SET priority = $1, updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
       WHERE workflow_uuid = $2
         AND status IN ($3, $4)`,
      [priority, workflowID, StatusString.ENQUEUED, StatusString.DELAYED],
    );
  }

  async setWorkflowDelay(workflowID: string, delayUntilEpochMS: number): Promise<void> {
    await this.pool.query(
      `UPDATE "${this.schemaName}".workflow_status
       SET delay_until_epoch_ms = $1, updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
       WHERE workflow_uuid = $2
         AND status = $3`,
      [delayUntilEpochMS, workflowID, StatusString.DELAYED],
    );
  }

  /**
   * Extend an existing debounced DELAYED workflow's delay and update its inputs, atomically.
   * The new delay is capped at the workflow's debounce_deadline_epoch_ms, if one is set.
   * Matching on workflow name and class ensures a debounce-key collision between different
   * workflows never overwrites another workflow's inputs. The bounce acts for `params.applicationName`:
   * it extends only that application's holders plus unclaimed ones, claiming those. If nothing matched,
   * returns the current holder (or that the key is unheld) so the caller can start fresh or surface a conflict.
   * Runs on `client` if given, joining its transaction (e.g. a transactional step's);
   * otherwise in its own retried transaction.
   */
  async debounceDelayedWorkflow(params: DebounceParams, client?: PoolClient): Promise<DebounceResult> {
    if (client !== undefined) {
      return await this.#debounceDelayedWorkflowInternal(client, params);
    }
    return await this.debounceDelayedWorkflowStandalone(params);
  }

  @dbRetry()
  private async debounceDelayedWorkflowStandalone(params: DebounceParams): Promise<DebounceResult> {
    const client = await this.#connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const result = await this.#debounceDelayedWorkflowInternal(client, params);
      await client.query('COMMIT');
      return result;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async #debounceDelayedWorkflowInternal(client: PoolClient, params: DebounceParams): Promise<DebounceResult> {
    const classNameOrNull = params.workflowClassName === '' ? null : params.workflowClassName;
    const updateParams: unknown[] = [
      params.delayUntilEpochMS,
      params.input,
      params.serialization,
      params.workflowName,
      classNameOrNull,
      params.queueName,
      params.deduplicationID,
      StatusString.DELAYED,
      params.applicationName ?? null,
    ];
    // Never extend a workflow the target application doesn't own; falls through to the holder below.
    const ownScope = this.#appNameFilter('application_name', params.applicationName, updateParams);
    const updated = await client.query<{ workflow_uuid: string }>(
      `UPDATE "${this.schemaName}".workflow_status
       SET delay_until_epoch_ms = CASE
             WHEN debounce_deadline_epoch_ms IS NOT NULL AND debounce_deadline_epoch_ms < $1
             THEN debounce_deadline_epoch_ms
             ELSE $1
           END,
           inputs = $2, serialization = $3,
           updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           -- Claim it for the target, as its dequeue would: left unclaimed, every peer coalesces onto the one workflow and the last inputs win.
           application_name = COALESCE(application_name, $9)
       WHERE name = $4 AND class_name IS NOT DISTINCT FROM $5
         AND queue_name = $6 AND deduplication_id = $7
         AND status = $8 AND is_debounced = TRUE
         AND ${ownScope}
       RETURNING workflow_uuid`,
      updateParams,
    );
    if (updated.rows.length > 0) {
      return {
        bouncedWorkflowID: updated.rows[0].workflow_uuid,
        holderWorkflowID: null,
        holderIsDebounced: false,
        holderWorkflowName: null,
        holderWorkflowClassName: null,
        holderApplicationName: null,
      };
    }
    // Unscoped, so a holder that blocked the update above is reportable.
    const holder = await client.query<workflow_status>(
      `SELECT workflow_uuid, is_debounced, name, class_name, application_name
       FROM "${this.schemaName}".workflow_status
       WHERE queue_name = $1 AND deduplication_id = $2`,
      [params.queueName, params.deduplicationID],
    );
    if (holder.rows.length === 0) {
      return {
        bouncedWorkflowID: null,
        holderWorkflowID: null,
        holderIsDebounced: false,
        holderWorkflowName: null,
        holderWorkflowClassName: null,
        holderApplicationName: null,
      };
    }
    return {
      bouncedWorkflowID: null,
      holderWorkflowID: holder.rows[0].workflow_uuid,
      holderIsDebounced: holder.rows[0].is_debounced ?? false,
      holderWorkflowName: holder.rows[0].name,
      holderWorkflowClassName: holder.rows[0].class_name ?? null,
      holderApplicationName: holder.rows[0].application_name ?? null,
    };
  }

  // Get the immediate (one-level) child workflow IDs for a set of workflows.
  async #getDirectChildren(workflowIDs: string[]): Promise<string[]> {
    if (workflowIDs.length === 0) {
      return [];
    }
    const result = await this.pool.query<{ workflow_uuid: string }>(
      `SELECT workflow_uuid
       FROM "${this.schemaName}".workflow_status
       WHERE parent_workflow_id = ANY($1)`,
      [workflowIDs],
    );
    return result.rows.map((row) => row.workflow_uuid);
  }

  async getWorkflowChildren(workflowID: string): Promise<string[]> {
    // BFS to find all descendant workflows
    const descendants = new Set<string>();
    let frontier = [workflowID];
    while (frontier.length > 0) {
      const children = await this.#getDirectChildren(frontier);
      frontier = children.filter((id) => !descendants.has(id));
      for (const id of frontier) {
        descendants.add(id);
      }
    }
    return [...descendants];
  }

  async deleteWorkflows(workflowIDs: string[], deleteChildren: boolean = false): Promise<void> {
    const allIds = [...workflowIDs];
    if (deleteChildren) {
      for (const wfid of workflowIDs) {
        allIds.push(...(await this.getWorkflowChildren(wfid)));
      }
    }

    await this.pool.query(`DELETE FROM "${this.schemaName}".workflow_status WHERE workflow_uuid = ANY($1)`, [allIds]);

    for (const wfid of allIds) {
      this.runningWorkflowMap.delete(wfid);
    }
  }

  async forkWorkflow(
    workflowID: string,
    startStep: number,
    options: {
      newWorkflowID?: string;
      applicationVersion?: string;
      timeoutMS?: number;
      queueName?: string;
      queuePartitionKey?: string;
      replacementChildren?: Record<string, string>;
    } = {},
  ): Promise<string> {
    const newWorkflowID = options.newWorkflowID ?? randomUUID();
    const result = await this.bulkForkWorkflows([workflowID], [newWorkflowID], [startStep], options);
    return result[0];
  }

  async forkFromFailure(
    workflowIDs: string[],
    options: {
      applicationVersion?: string;
      queueName?: string;
      queuePartitionKey?: string;
      fromLastFailure?: boolean;
      fromLastStep?: boolean;
      fromStep?: number;
      fromStepName?: string;
    } = {},
  ): Promise<string[]> {
    const modes = [
      options.fromLastFailure ?? false,
      options.fromLastStep ?? false,
      options.fromStep !== undefined,
      options.fromStepName !== undefined,
    ].filter(Boolean).length;
    if (modes !== 1) {
      throw new Error('Exactly one of fromLastFailure, fromLastStep, fromStep, or fromStepName must be specified');
    }

    let startSteps: number[];

    if (options.fromStep !== undefined) {
      startSteps = Array(workflowIDs.length).fill(options.fromStep) as number[];
    } else {
      let query: string;
      const params: unknown[] = [workflowIDs];

      if (options.fromLastFailure) {
        query = `SELECT workflow_uuid,
                        COALESCE(
                          MAX(function_id) FILTER (WHERE error IS NOT NULL),
                          MAX(function_id)
                        ) AS start_step
                 FROM "${this.schemaName}".operation_outputs
                 WHERE workflow_uuid = ANY($1)
                 GROUP BY workflow_uuid`;
      } else if (options.fromLastStep) {
        query = `SELECT workflow_uuid, MAX(function_id) AS start_step
                 FROM "${this.schemaName}".operation_outputs
                 WHERE workflow_uuid = ANY($1)
                 GROUP BY workflow_uuid`;
      } else {
        // fromStepName
        query = `SELECT workflow_uuid, MAX(function_id) AS start_step
                 FROM "${this.schemaName}".operation_outputs
                 WHERE workflow_uuid = ANY($1) AND function_name = $2
                 GROUP BY workflow_uuid`;
        params.push(options.fromStepName);
      }

      const result = await this.pool.query<{ workflow_uuid: string; start_step: number }>(query, params);
      const startStepByID = new Map(result.rows.map((r) => [r.workflow_uuid, Number(r.start_step)]));
      if (options.fromStepName !== undefined) {
        for (const wid of workflowIDs) {
          if (!startStepByID.has(wid)) {
            throw new Error(`Workflow ${wid} has no step named '${options.fromStepName}'`);
          }
        }
      }
      // A workflow with no recorded steps has nothing to resume from, so restart it from the beginning.
      startSteps = workflowIDs.map((wid) => startStepByID.get(wid) ?? 0);
    }

    const forkedIDs = workflowIDs.map(() => randomUUID());
    return this.bulkForkWorkflows(workflowIDs, forkedIDs, startSteps, options);
  }

  private async bulkForkWorkflows(
    originalWorkflowIDs: string[],
    forkedWorkflowIDs: string[],
    startSteps: number[],
    options: {
      applicationVersion?: string;
      timeoutMS?: number;
      queueName?: string;
      queuePartitionKey?: string;
      replacementChildren?: Record<string, string>;
    } = {},
  ): Promise<string[]> {
    if (originalWorkflowIDs.length === 0) {
      return [];
    }
    if (originalWorkflowIDs.length !== forkedWorkflowIDs.length || originalWorkflowIDs.length !== startSteps.length) {
      throw new Error('originalWorkflowIDs, forkedWorkflowIDs, and startSteps must have the same length');
    }

    const client = await this.#connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');

      // Fetch the status of all original workflows inside the transaction.
      const { rows: statusRows } = await client.query<workflow_status>(
        `SELECT workflow_uuid, name, class_name, config_name, application_id,
                authenticated_user, authenticated_roles, assumed_role, inputs, serialization,
                request, application_version, attributes, application_name
         FROM "${this.schemaName}".workflow_status
         WHERE workflow_uuid = ANY($1)`,
        [originalWorkflowIDs],
      );

      const statusByID = new Map(statusRows.map((r) => [r.workflow_uuid, r]));
      for (const wid of originalWorkflowIDs) {
        if (!statusByID.has(wid)) {
          throw new DBOSNonExistentWorkflowError(`Workflow ${wid} does not exist`);
        }
      }

      const queueName = options.queueName ?? INTERNAL_QUEUE_NAME;

      // Bulk insert all forked workflow status rows.
      const insertCols = [
        'workflow_uuid',
        'status',
        'name',
        'class_name',
        'config_name',
        'queue_name',
        'authenticated_user',
        'assumed_role',
        'authenticated_roles',
        'request',
        'application_version',
        'application_id',
        'inputs',
        'queue_partition_key',
        'forked_from',
        'serialization',
        'attributes',
        'application_name',
      ];
      if (options.timeoutMS !== undefined) {
        insertCols.push('workflow_timeout_ms');
      }
      // One owner per fork, shared by its status row and its copied steps: the source's, or this application claiming an unclaimed one.
      const forkOwners = new Map<string, string | null>(
        originalWorkflowIDs.map((origID, i) => [
          forkedWorkflowIDs[i],
          statusByID.get(origID)!.application_name ?? this.appName ?? null,
        ]),
      );

      const valuesPlaceholders: string[] = [];
      const params: unknown[] = [];
      let paramIdx = 1;
      for (let i = 0; i < originalWorkflowIDs.length; i++) {
        const origID = originalWorkflowIDs[i];
        const forkID = forkedWorkflowIDs[i];
        const ws = statusByID.get(origID)!;
        const placeholders = insertCols.map(() => `$${paramIdx++}`).join(', ');
        valuesPlaceholders.push(`(${placeholders})`);
        params.push(
          forkID,
          StatusString.ENQUEUED,
          ws.name,
          ws.class_name ?? null,
          ws.config_name ?? null,
          queueName,
          ws.authenticated_user,
          ws.assumed_role,
          ws.authenticated_roles,
          ws.request,
          options.applicationVersion ?? ws.application_version ?? null,
          ws.application_id,
          ws.inputs,
          options.queuePartitionKey ?? null,
          origID,
          ws.serialization,
          ws.attributes ? JSON.stringify(ws.attributes) : null,
          forkOwners.get(forkID) ?? null,
        );
        if (options.timeoutMS !== undefined) {
          params.push(options.timeoutMS);
        }
      }
      await client.query(
        `INSERT INTO "${this.schemaName}".workflow_status (${insertCols.join(', ')})
         VALUES ${valuesPlaceholders.join(', ')}`,
        params,
      );

      // Mark all original workflows as having been forked from.
      await client.query(
        `UPDATE "${this.schemaName}".workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = ANY($1)`,
        [originalWorkflowIDs],
      );

      // For workflows with start_step > 0, copy checkpoints/events/streams.
      // Build a mapping CTE so each copy is a single SQL statement.
      const forkMappings = originalWorkflowIDs
        .map((origID, i) => ({ origID, forkID: forkedWorkflowIDs[i], startStep: startSteps[i] }))
        .filter((m) => m.startStep > 0);

      if (forkMappings.length > 0) {
        const mappingValues: string[] = [];
        const mappingParams: unknown[] = [];
        let mIdx = 1;
        for (const m of forkMappings) {
          // Cast: an unclaimed fork makes owner a bare NULL the VALUES list cannot type.
          mappingValues.push(`($${mIdx}::text, $${mIdx + 1}::text, $${mIdx + 2}::int, $${mIdx + 3}::text)`);
          mappingParams.push(m.origID, m.forkID, m.startStep, forkOwners.get(m.forkID) ?? null);
          mIdx += 4;
        }
        const mappingCTE = `WITH mapping(orig_id, fork_id, start_step, owner) AS (VALUES ${mappingValues.join(', ')})`;

        // Build the child_workflow_id expression, applying replacements if provided.
        let childWfExpr = 'oo.child_workflow_id';
        const ooParams = [...mappingParams];
        if (options.replacementChildren && Object.keys(options.replacementChildren).length > 0) {
          const whenClauses: string[] = [];
          for (const [oldId, newId] of Object.entries(options.replacementChildren)) {
            whenClauses.push(`WHEN oo.child_workflow_id = $${mIdx} THEN $${mIdx + 1}::text`);
            ooParams.push(oldId, newId);
            mIdx += 2;
          }
          childWfExpr = `CASE ${whenClauses.join(' ')} ELSE oo.child_workflow_id END`;
        }

        // Copy operation outputs
        await client.query(
          `${mappingCTE}
           INSERT INTO "${this.schemaName}".operation_outputs
             (workflow_uuid, function_id, output, error, serialization, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, application_name)
           SELECT m.fork_id, oo.function_id, oo.output, oo.error, oo.serialization, oo.function_name, ${childWfExpr}, oo.started_at_epoch_ms, oo.completed_at_epoch_ms, m.owner
           FROM mapping m
           JOIN "${this.schemaName}".operation_outputs oo
             ON oo.workflow_uuid = m.orig_id AND oo.function_id < m.start_step`,
          ooParams,
        );

        // Copy streams
        await client.query(
          `${mappingCTE}
           INSERT INTO "${this.schemaName}".streams
             (workflow_uuid, key, value, serialization, "offset", function_id)
           SELECT m.fork_id, s.key, s.value, s.serialization, s."offset", s.function_id
           FROM mapping m
           JOIN "${this.schemaName}".streams s
             ON s.workflow_uuid = m.orig_id AND s.function_id < m.start_step`,
          mappingParams,
        );

        // Copy events history
        await client.query(
          `${mappingCTE}
           INSERT INTO "${this.schemaName}".workflow_events_history
             (workflow_uuid, function_id, key, value, serialization)
           SELECT m.fork_id, weh.function_id, weh.key, weh.value, weh.serialization
           FROM mapping m
           JOIN "${this.schemaName}".workflow_events_history weh
             ON weh.workflow_uuid = m.orig_id AND weh.function_id < m.start_step`,
          mappingParams,
        );

        // Copy only the latest version of each event using a window function
        await client.query(
          `${mappingCTE}
           INSERT INTO "${this.schemaName}".workflow_events
             (workflow_uuid, key, value, serialization)
           SELECT ranked.workflow_uuid, ranked.key, ranked.value, ranked.serialization
           FROM (
             SELECT m.fork_id AS workflow_uuid, weh.key, weh.value, weh.serialization,
                    ROW_NUMBER() OVER (PARTITION BY weh.workflow_uuid, weh.key ORDER BY weh.function_id DESC) AS rn
             FROM mapping m
             JOIN "${this.schemaName}".workflow_events_history weh
               ON weh.workflow_uuid = m.orig_id AND weh.function_id < m.start_step
           ) ranked
           WHERE ranked.rn = 1`,
          mappingParams,
        );
      }

      await client.query('COMMIT');
      return forkedWorkflowIDs;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async exportWorkflow(workflowID: string, exportChildren: boolean = false): Promise<ExportedWorkflow[]> {
    const workflowIDs = [workflowID];
    if (exportChildren) {
      workflowIDs.push(...(await this.getWorkflowChildren(workflowID)));
    }

    const exportedWorkflows: ExportedWorkflow[] = [];

    const client = await this.#connect();
    try {
      for (const wfID of workflowIDs) {
        // Export workflow_status
        const statusResult = await client.query<workflow_status>(
          // owner_xid is intentionally omitted: it is a transient transaction-ownership
          // token, not logical workflow state, and a source database's xid is
          // meaningless in the target.
          `SELECT
            workflow_uuid, status, name, authenticated_user, assumed_role,
            authenticated_roles, request, output, error, executor_id,
            created_at, updated_at, application_version, application_id,
            class_name, config_name, recovery_attempts, queue_name,
            workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
            deduplication_id, inputs, priority, queue_partition_key, forked_from,
            parent_workflow_id, serialization, delay_until_epoch_ms,
            was_forked_from, rate_limited, completed_at, attributes, schedule_name,
            debounce_deadline_epoch_ms, is_debounced, application_name
          FROM "${this.schemaName}".workflow_status
          WHERE workflow_uuid = $1`,
          [wfID],
        );

        if (statusResult.rows.length === 0) {
          throw new DBOSNonExistentWorkflowError(`Workflow ${wfID} does not exist`);
        }

        const workflowStatus = statusResult.rows[0];

        // Export operation_outputs
        const outputsResult = await client.query<operation_outputs>(
          `SELECT
            workflow_uuid, function_id, function_name, output, error,
            child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms,
            serialization, application_name
          FROM "${this.schemaName}".operation_outputs
          WHERE workflow_uuid = $1`,
          [wfID],
        );

        // Export workflow_events
        const eventsResult = await client.query<workflow_events>(
          `SELECT workflow_uuid, key, value, serialization
          FROM "${this.schemaName}".workflow_events
          WHERE workflow_uuid = $1`,
          [wfID],
        );

        // Export workflow_events_history
        const historyResult = await client.query<workflow_events_history>(
          `SELECT workflow_uuid, function_id, key, value, serialization
          FROM "${this.schemaName}".workflow_events_history
          WHERE workflow_uuid = $1`,
          [wfID],
        );

        // Export streams
        const streamsResult = await client.query<streams>(
          `SELECT workflow_uuid, key, value, "offset", function_id, serialization
          FROM "${this.schemaName}".streams
          WHERE workflow_uuid = $1`,
          [wfID],
        );

        exportedWorkflows.push({
          workflow_status: workflowStatus,
          operation_outputs: outputsResult.rows,
          workflow_events: eventsResult.rows,
          workflow_events_history: historyResult.rows,
          streams: streamsResult.rows,
        });
      }
    } finally {
      client.release();
    }

    return exportedWorkflows;
  }

  async importWorkflow(workflows: ExportedWorkflow[]): Promise<void> {
    const client = await this.#connect();
    try {
      await client.query('BEGIN');

      for (const workflow of workflows) {
        const status = workflow.workflow_status;

        // Import workflow_status
        await client.query(
          `INSERT INTO "${this.schemaName}".workflow_status (
            workflow_uuid, status, name, authenticated_user, assumed_role,
            authenticated_roles, request, output, error, executor_id,
            created_at, updated_at, application_version, application_id,
            class_name, config_name, recovery_attempts, queue_name,
            workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
            deduplication_id, inputs, priority, queue_partition_key, forked_from,
            parent_workflow_id, serialization, delay_until_epoch_ms,
            was_forked_from, rate_limited, completed_at, attributes, schedule_name,
            debounce_deadline_epoch_ms, is_debounced, application_name
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37)`,
          [
            status.workflow_uuid,
            status.status,
            status.name,
            status.authenticated_user,
            status.assumed_role,
            status.authenticated_roles,
            status.request,
            status.output,
            status.error,
            status.executor_id,
            status.created_at,
            status.updated_at,
            status.application_version,
            status.application_id,
            status.class_name,
            status.config_name,
            status.recovery_attempts,
            status.queue_name,
            status.workflow_timeout_ms,
            status.workflow_deadline_epoch_ms,
            status.started_at_epoch_ms,
            status.deduplication_id,
            status.inputs,
            status.priority,
            status.queue_partition_key,
            status.forked_from,
            status.parent_workflow_id,
            status.serialization,
            status.delay_until_epoch_ms ?? null,
            // NOT NULL columns: fall back to FALSE for payloads exported before
            // these fields were included.
            status.was_forked_from ?? false,
            status.rate_limited ?? false,
            status.completed_at ?? null,
            status.attributes ? JSON.stringify(status.attributes) : null,
            status.schedule_name ?? null,
            status.debounce_deadline_epoch_ms ?? null,
            status.is_debounced ?? false,
            status.application_name ?? null,
          ],
        );

        // Import operation_outputs
        for (const output of workflow.operation_outputs) {
          await client.query(
            `INSERT INTO "${this.schemaName}".operation_outputs (
              workflow_uuid, function_id, function_name, output, error,
              child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms,
              serialization, application_name
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
            [
              output.workflow_uuid,
              output.function_id,
              output.function_name,
              output.output,
              output.error,
              output.child_workflow_id,
              output.started_at_epoch_ms,
              output.completed_at_epoch_ms,
              output.serialization,
              output.application_name ?? null,
            ],
          );
        }

        // Import workflow_events
        for (const event of workflow.workflow_events) {
          await client.query(
            `INSERT INTO "${this.schemaName}".workflow_events (
              workflow_uuid, key, value, serialization
            ) VALUES ($1, $2, $3, $4)`,
            [event.workflow_uuid, event.key, event.value, event.serialization],
          );
        }

        // Import workflow_events_history
        for (const history of workflow.workflow_events_history) {
          await client.query(
            `INSERT INTO "${this.schemaName}".workflow_events_history (
              workflow_uuid, function_id, key, value, serialization
            ) VALUES ($1, $2, $3, $4, $5)`,
            [history.workflow_uuid, history.function_id, history.key, history.value, history.serialization],
          );
        }

        // Import streams
        for (const stream of workflow.streams) {
          await client.query(
            `INSERT INTO "${this.schemaName}".streams (
              workflow_uuid, key, value, "offset", function_id, serialization
            ) VALUES ($1, $2, $3, $4, $5, $6)`,
            [stream.workflow_uuid, stream.key, stream.value, stream.offset, stream.function_id, stream.serialization],
          );
        }
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // ==================== Awaiting Workflows ====================
  registerRunningWorkflow(
    workflowID: string,
    workflowPromise: Promise<unknown>,
    onSettled: () => void,
    queueName?: string,
    queuePartitionKey?: string,
  ) {
    // Need to await for the workflow and capture errors.
    const awaitWorkflowPromise = workflowPromise
      .catch((error) => {
        const outcome = this.#destroyed ? 'was abandoned by shutdown' : 'failed';
        this.logger.debug(`Workflow ${workflowID} ${outcome}: ${error}`);
      })
      .finally(() => {
        onSettled();
      });
    this.runningWorkflowMap.set(workflowID, {
      promise: awaitWorkflowPromise,
      queueName,
      queuePartitionKey,
    });
  }

  checkForRunningWorkflow(workflowID: string): boolean {
    return this.runningWorkflowMap.has(workflowID);
  }

  clearRunningWorkflow(workflowID: string): void {
    this.runningWorkflowMap.delete(workflowID);
  }

  /** Workflows this worker is running for a queue, across every partition of it. */
  countRunningWorkflowsForQueue(queueName: string): number {
    let count = 0;
    for (const entry of this.runningWorkflowMap.values()) {
      if (entry.queueName === queueName) count++;
    }
    return count;
  }

  /** Workflows this worker is running for one partition of a queue. */
  countRunningWorkflowsForPartition(queueName: string, queuePartitionKey?: string): number {
    let count = 0;
    for (const entry of this.runningWorkflowMap.values()) {
      if (entry.queueName === queueName && entry.queuePartitionKey === queuePartitionKey) count++;
    }
    return count;
  }

  /** Wait up to `timeoutMS` for locally-running workflows to finish. Without a timeout, do not wait at all. */
  async awaitRunningWorkflows(timeoutMS?: number): Promise<void> {
    if (timeoutMS !== undefined && timeoutMS > 0) {
      const deadline = Date.now() + timeoutMS;
      if (this.runningWorkflowMap.size > 0) {
        this.logger.info('Waiting for pending workflows to finish.');
      }
      // Each pass picks up workflows a draining workflow started, and awaits any given run only once.
      const awaited = new Set<Promise<unknown>>();
      for (;;) {
        const pending = Array.from(this.runningWorkflowMap.values(), (entry) => entry.promise).filter(
          (promise) => !awaited.has(promise),
        );
        if (pending.length === 0) break;
        for (const promise of pending) awaited.add(promise);
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timedOut = await Promise.race([
          Promise.allSettled(pending).then(() => false),
          new Promise<boolean>((resolve) => {
            timer = setTimeout(() => resolve(true), Math.max(0, deadline - Date.now()));
          }),
        ]);
        clearTimeout(timer);
        if (timedOut) break;
      }
    }
    if (this.runningWorkflowMap.size > 0) {
      this.logger.warn(
        `Shutting down while ${this.runningWorkflowMap.size} workflows are still running: ${Array.from(this.runningWorkflowMap.keys()).join(', ')}`,
      );
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

  /**
   * Run a DB-backed polling read under the polling concurrency limiter, so that
   * high-fan-out wait loops cannot check out every pool client and starve
   * control-plane operations. Only polling reads should go through here;
   * control-plane work hits the pool directly and bypasses the limiter.
   */
  #pollWithLimiter<T>(query: () => Promise<T>): Promise<T> {
    // Closing our own pool used to end these waits; a caller's pool stays open, so end them here.
    if (this.#destroyed) {
      return Promise.reject(new DBOSError('The system database has been shut down'));
    }
    return this.pollLimiter.runExclusive(query);
  }

  /**
   * Cancellation check for use inside polling wait loops: the status read runs
   * under the polling limiter so it counts against the same concurrency budget
   * as the rest of the loop's reads.
   */
  /** Cancellation check for polling waits: goes through the limiter so readers cannot starve the pool. */
  async checkIfCanceledLimited(workflowID: string): Promise<void> {
    await this.#pollWithLimiter(() => this.#checkIfCanceled(this.pool, workflowID));
  }

  @dbRetry()
  // A missing row normally means the workflow has not been inserted yet, so
  // polling for it is correct. Callers that know the row must already exist
  // (e.g. a run parking on an outcome it just failed to write) pass
  // `failIfMissing` to fail fast with DBOSNonExistentWorkflowError instead of
  // polling forever.
  async awaitWorkflowResult(
    workflowID: string,
    timeoutSeconds?: number,
    callerID?: string,
    timerFuncID?: number,
    pollingIntervalMs?: number,
    failIfMissing?: boolean,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;
    const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalResultMs;

    // Record the durable timeout deadline once before polling. #durableSleep persists it on the
    // first call and reads back the same value on recovery, so it never changes across iterations.
    if (timerFuncID !== undefined && callerID !== undefined && timeoutms !== undefined) {
      finishTime = await this.#durableSleep(callerID, timerFuncID, timeoutms);
    }

    while (true) {
      if (callerID) await this.checkIfCanceledLimited(callerID);
      let rows: workflow_status[];
      try {
        ({ rows } = await this.#pollWithLimiter(() =>
          this.pool.query<workflow_status>(
            `SELECT status, output, error, serialization FROM "${this.schemaName}".workflow_status
             WHERE workflow_uuid=$1`,
            [workflowID],
          ),
        ));
      } catch (e) {
        const err = e as Error;
        this.logger.error(`Exception from system database: ${err}`, err);
        throw err;
      }
      if (rows.length > 0) {
        const status = rows[0].status;
        if (status === StatusString.SUCCESS) {
          return { output: rows[0].output, serialization: rows[0].serialization };
        } else if (status === StatusString.ERROR) {
          return { error: rows[0].error, serialization: rows[0].serialization };
        } else if (status === StatusString.CANCELLED) {
          return { cancelled: true };
        } else if (status === StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED) {
          return { maxRecoveryAttemptsExceeded: true };
        } else {
          // Status is not actionable
        }
      } else if (failIfMissing) {
        throw new DBOSNonExistentWorkflowError(`Workflow ${workflowID} does not exist`);
      }

      const ct = Date.now();
      if (finishTime && ct > finishTime) return undefined; // Time's up

      let poll = finishTime ? finishTime - Date.now() : pollIntervalMs;
      poll = Math.min(pollIntervalMs, poll);
      await sleepms(poll);
    }
  }

  @dbRetry()
  async awaitFirstWorkflowId(workflowIds: string[], callerID?: string, pollingIntervalMs?: number): Promise<string> {
    const placeholders = workflowIds.map((_, i) => `$${i + 1}`).join(', ');
    const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalResultMs;

    while (true) {
      if (callerID) await this.checkIfCanceledLimited(callerID);

      const { rows } = await this.#pollWithLimiter(() =>
        this.pool.query<workflow_status>(
          `SELECT workflow_uuid FROM "${this.schemaName}".workflow_status
           WHERE workflow_uuid IN (${placeholders})
             AND status NOT IN ('${StatusString.PENDING}', '${StatusString.ENQUEUED}', '${StatusString.DELAYED}')
           LIMIT 1`,
          workflowIds,
        ),
      );

      if (rows.length > 0) {
        return rows[0].workflow_uuid;
      }

      await sleepms(pollIntervalMs);
    }
  }

  @dbRetry()
  async awaitWorkflowIds(workflowIds: string[], callerID?: string, pollingIntervalMs?: number): Promise<void> {
    const remainingWorkflowIds = new Set(workflowIds);
    const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalResultMs;

    while (remainingWorkflowIds.size > 0) {
      const currentWorkflowIds = [...remainingWorkflowIds];

      if (callerID) await this.checkIfCanceledLimited(callerID);

      const { rows } = await this.#pollWithLimiter(() =>
        this.pool.query<{ workflow_uuid: string }>(
          `SELECT workflow_uuid FROM "${this.schemaName}".workflow_status
           WHERE workflow_uuid = ANY($1::text[])
             AND status NOT IN ('${StatusString.PENDING}', '${StatusString.ENQUEUED}', '${StatusString.DELAYED}')`,
          [currentWorkflowIds],
        ),
      );

      for (const row of rows) {
        remainingWorkflowIds.delete(row.workflow_uuid);
      }
      if (remainingWorkflowIds.size === 0) {
        return;
      }

      await sleepms(pollIntervalMs);
    }
  }

  // ==================== Sleep ====================
  @dbRetry()
  async durableSleepms(workflowID: string, functionID: number, durationMS: number): Promise<void> {
    const endTime = await this.#durableSleep(workflowID, functionID, durationMS, true);

    while (Date.now() < endTime) {
      await sleepms(Math.min(endTime - Date.now(), sleepConfig.maxTimeoutMS));
    }

    await this.checkIfCanceled(workflowID);
  }

  // ==================== Messaging ====================
  readonly nullTopic = '__null__topic__';

  @dbRetry()
  async send(
    workflowID: string,
    functionID: number,
    destinationID: string,
    message: string | null,
    topic: string | undefined,
    serialization: string | null,
    idempotencyKey?: string,
  ): Promise<void> {
    topic = topic ?? this.nullTopic;
    const messageUUID = idempotencyKey ? `${idempotencyKey}::${destinationID}` : randomUUID();
    const client: PoolClient = await this.#connect();

    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      await this.#runAndRecordResult(client, DBOS_FUNCNAME_SEND, workflowID, functionID, async () => {
        await client.query(
          `INSERT INTO "${this.schemaName}".notifications (destination_uuid, topic, message, serialization, message_uuid)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (message_uuid) DO NOTHING;`,
          [destinationID, topic, message, serialization, messageUUID],
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

  /** Runs on `client` if given, joining its transaction; otherwise on the pool with retries. */
  async sendDirect(
    destinationID: string,
    message: string | null,
    topic: string | undefined,
    serialization: string | null,
    idempotencyKey?: string,
    client?: ClientBase,
  ): Promise<void> {
    if (client !== undefined) {
      return await this.#sendDirectInternal(client, destinationID, message, topic, serialization, idempotencyKey);
    }
    return await this.sendDirectStandalone(destinationID, message, topic, serialization, idempotencyKey);
  }

  @dbRetry()
  private async sendDirectStandalone(
    destinationID: string,
    message: string | null,
    topic: string | undefined,
    serialization: string | null,
    idempotencyKey?: string,
  ): Promise<void> {
    return await this.#sendDirectInternal(this.pool, destinationID, message, topic, serialization, idempotencyKey);
  }

  async #sendDirectInternal(
    db: ClientBase | Pool,
    destinationID: string,
    message: string | null,
    topic: string | undefined,
    serialization: string | null,
    idempotencyKey?: string,
  ): Promise<void> {
    topic = topic ?? this.nullTopic;
    // Same per-destination scoping as send() above.
    const messageUUID = idempotencyKey ? `${idempotencyKey}::${destinationID}` : randomUUID();
    try {
      await db.query(
        `INSERT INTO "${this.schemaName}".notifications (destination_uuid, topic, message, serialization, message_uuid)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (message_uuid) DO NOTHING;`,
        [destinationID, topic, message, serialization, messageUUID],
      );
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '23503') {
        throw new DBOSNonExistentWorkflowError(`Sent to non-existent destination workflow UUID: ${destinationID}`);
      }
      throw err;
    }
  }

  @dbRetry()
  async recv(
    workflowID: string,
    functionID: number,
    timeoutFunctionID: number,
    topic?: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
    pollingIntervalMs?: number,
  ): Promise<{ serializedValue: string | null; serialization: string | null }> {
    topic = topic ?? this.nullTopic;
    const startTime = Date.now();
    // First, check for previous executions.
    const res = await this.getOperationResultAndThrowIfCancelled(workflowID, functionID);
    if (res) {
      if (res.functionName !== DBOS_FUNCNAME_RECV) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, DBOS_FUNCNAME_RECV, res.functionName!);
      }
      return { serializedValue: res.output!, serialization: res.serialization ?? null };
    }

    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;
    const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalEventMs;

    // Record the durable timeout deadline once before polling. #durableSleep persists it on the
    // first call and reads back the same value on recovery, so it never changes across iterations.
    if (timeoutms) {
      finishTime = await this.#durableSleep(workflowID, timeoutFunctionID, timeoutms);
    }

    while (true) {
      // register the key with the global notifications listener.
      let resolveNotification: () => void;
      const messagePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const payload = `${workflowID}::${topic}`;
      const cbr = this.notificationsMap.registerCallback(payload, resolveNotification!);

      try {
        await this.checkIfCanceledLimited(workflowID);

        // Check if the key is already in the DB, then wait for the notification if it isn't.
        const initRecvRows = (
          await this.#pollWithLimiter(() =>
            this.pool.query<notifications>(
              `SELECT topic FROM "${this.schemaName}".notifications WHERE destination_uuid=$1 AND topic=$2 AND consumed = false;`,
              [workflowID, topic],
            ),
          )
        ).rows;

        if (initRecvRows.length !== 0) break;

        const ct = Date.now();
        if (finishTime && ct > finishTime) break; // Time's up

        let poll = finishTime ? finishTime - Date.now() : pollIntervalMs;
        poll = Math.min(pollIntervalMs, poll);
        const { promise, cancel } = cancellableSleep(poll);
        try {
          await Promise.race([messagePromise, promise]);
        } finally {
          cancel();
        }
      } finally {
        this.notificationsMap.deregisterCallback(cbr);
      }
    }

    await this.checkIfCanceled(workflowID);

    // Transactionally consume and return the message if it's in the DB, otherwise return null.
    let message: string | null = null;
    let serialization: string | null = null;
    const client = await this.#connect();
    try {
      await client.query(`BEGIN ISOLATION LEVEL READ COMMITTED`);
      const finalRecvRows = (
        await client.query<notifications>(
          `UPDATE "${this.schemaName}".notifications
        SET consumed = true
        WHERE destination_uuid = $1
          AND topic = $2
          AND consumed = false
          AND message_uuid = (
            SELECT message_uuid
            FROM "${this.schemaName}".notifications
            WHERE destination_uuid = $1
              AND topic = $2
              AND consumed = false
            ORDER BY created_at_epoch_ms ASC
            LIMIT 1
          )
        RETURNING notifications.message, notifications.serialization;`,
          [workflowID, topic],
        )
      ).rows;
      if (finalRecvRows.length > 0) {
        message = finalRecvRows[0].message;
        serialization = finalRecvRows[0].serialization;
      }
      await this.recordOperationResultInternal(
        client,
        workflowID,
        functionID,
        DBOS_FUNCNAME_RECV,
        true,
        startTime,
        Date.now(),
        {
          output: message,
          serialization,
        },
      );
      await client.query(`COMMIT`);
    } catch (e) {
      this.logger.error(e);
      await client.query(`ROLLBACK`);
      throw e;
    } finally {
      client.release();
    }

    return { serializedValue: message, serialization };
  }

  // ==================== Events ====================
  @dbRetry()
  async setEvent(
    workflowID: string,
    functionID: number,
    key: string,
    message: string | null,
    serialization: string | null,
  ): Promise<void> {
    const client: PoolClient = await this.#connect();

    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      // Only a real write (not a replay) should wake readers.
      let didWrite = false;
      await this.#runAndRecordResult(client, DBOS_FUNCNAME_SETEVENT, workflowID, functionID, async () => {
        await client.query(
          `INSERT INTO "${this.schemaName}".workflow_events (workflow_uuid, key, value, serialization)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, key)
             DO UPDATE SET value = $3, serialization = $4
             RETURNING workflow_uuid;`,
          [workflowID, key, message, serialization],
        );
        // Also write to the immutable history table for fork support
        await client.query(
          `INSERT INTO "${this.schemaName}".workflow_events_history (workflow_uuid, function_id, key, value, serialization)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (workflow_uuid, function_id, key)
             DO UPDATE SET value = $4, serialization = $5;`,
          [workflowID, functionID, key, message, serialization],
        );
        didWrite = true;
        return undefined;
      });
      await client.query('COMMIT');
      // Notify only after commit, so a woken getEvent sees the value.
      if (didWrite) {
        this.#signalNotification(DBOS_WORKFLOW_EVENTS_CHANNEL, `${workflowID}::${key}`);
      }
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
    pollingIntervalMs?: number,
  ): Promise<{ serializedValue: string | null; serialization: string | null }> {
    const startTime = Date.now();
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
        return { serializedValue: res.output!, serialization: res.serialization ?? null };
      }
    }

    // Get the return the value. if it's in the DB, otherwise return null.
    let value: string | null = null;
    let valueSer: string | null = null;
    const payloadKey = `${workflowID}::${key}`;
    const timeoutms = timeoutSeconds !== undefined ? timeoutSeconds * 1000 : undefined;
    let finishTime = timeoutms !== undefined ? Date.now() + timeoutms : undefined;
    const pollIntervalMs = pollingIntervalMs ?? this.dbPollingIntervalEventMs;

    // If we have a callerWorkflow, we want a durable sleep, otherwise, not. Record the durable
    // timeout deadline once before polling. #durableSleep persists it on the first call and reads
    // back the same value on recovery, so it never changes across iterations.
    if (callerWorkflow && timeoutms) {
      finishTime = await this.#durableSleep(
        callerWorkflow.workflowID,
        callerWorkflow.timeoutFunctionID ?? -1,
        timeoutms,
      );
    }

    // Register the key with the global notifications listener first... we do not want to look in the DB first
    //  or that would cause a timing hole.
    while (true) {
      let resolveNotification: () => void;
      const valuePromise = new Promise<void>((resolve) => {
        resolveNotification = resolve;
      });
      const cbr = this.workflowEventsMap.registerCallback(payloadKey, resolveNotification!);

      try {
        if (callerWorkflow?.workflowID) await this.checkIfCanceledLimited(callerWorkflow?.workflowID);
        // Check if the key is already in the DB, then wait for the notification if it isn't.
        const initRecvRows = (
          await this.#pollWithLimiter(() =>
            this.pool.query<workflow_events>(
              `SELECT key, value, serialization
             FROM "${this.schemaName}".workflow_events
             WHERE workflow_uuid=$1 AND key=$2;`,
              [workflowID, key],
            ),
          )
        ).rows;

        if (initRecvRows.length > 0) {
          value = initRecvRows[0].value;
          valueSer = initRecvRows[0].serialization;
          break;
        }

        const ct = Date.now();
        if (finishTime && ct > finishTime) break; // Time's up

        let poll = finishTime ? finishTime - Date.now() : pollIntervalMs;
        poll = Math.min(pollIntervalMs, poll);
        const { promise, cancel } = cancellableSleep(poll);

        try {
          await Promise.race([valuePromise, promise]);
        } finally {
          cancel();
        }
      } finally {
        this.workflowEventsMap.deregisterCallback(cbr);
      }
    }

    // Record the output if it is inside a workflow.
    if (callerWorkflow) {
      await this.recordOperationResult(
        callerWorkflow.workflowID,
        callerWorkflow.functionID,
        DBOS_FUNCNAME_GETEVENT,
        true,
        startTime,
        Date.now(),
        {
          output: value,
          serialization: valueSer,
        },
      );
    }
    return { serializedValue: value, serialization: valueSer };
  }

  // Event dispatcher queries / updates
  @dbRetry()
  async getEventDispatchState(
    service: string,
    workflowName: string,
    key: string,
  ): Promise<DBOSExternalState | undefined> {
    const res = await this.pool.query<event_dispatch_kv>(
      `SELECT * FROM "${this.schemaName}".event_dispatch_kv
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
      `INSERT INTO "${this.schemaName}".event_dispatch_kv (
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

  // ==================== Streams ====================
  @dbRetry()
  async writeStreamFromStep(
    workflowID: string,
    functionID: number,
    key: string,
    serializedValue: string,
    serialization: string | null,
  ): Promise<void> {
    while (true) {
      try {
        // Derives the first unused offset inside the insert; two writers can still pick the same one.
        await this.pool.query(
          `INSERT INTO "${this.schemaName}".streams (workflow_uuid, key, value, "offset", function_id, serialization)
           SELECT $1::text, $2::text, $3::text, COALESCE(MAX(s."offset"), -1) + 1, $4::int, $5::text
           FROM "${this.schemaName}".streams s
           WHERE s.workflow_uuid = $1 AND s.key = $2`,
          [workflowID, key, serializedValue, functionID, serialization],
        );
      } catch (e) {
        // Only an offset conflict resolves on retry; anything else would spin forever.
        if (e instanceof DatabaseError && e.code === '23505') {
          this.logger.warn(`Stream offset conflict for workflow ${workflowID}, key ${key}; retrying`);
          await sleepms(100);
          continue;
        }
        this.logger.error(e);
        throw e;
      }
      // Notify only after commit, so a woken reader sees the value.
      this.#signalNotification(DBOS_STREAMS_CHANNEL, `${workflowID}::${key}`);
      return;
    }
  }

  @dbRetry()
  async writeStreamFromWorkflow(
    workflowID: string,
    functionID: number,
    key: string,
    serializedValue: string,
    serialization: string | null,
    functionName: string,
  ): Promise<void> {
    const client: PoolClient = await this.#connect();
    try {
      while (true) {
        // Only a real insert (not a replay) should wake readers.
        let didWrite = false;
        try {
          await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
          await this.#runAndRecordResult(client, functionName, workflowID, functionID, async () => {
            // Derives the first unused offset inside the insert; two writers can still pick the same one.
            await client.query(
              `INSERT INTO "${this.schemaName}".streams (workflow_uuid, key, value, "offset", function_id, serialization)
               SELECT $1::text, $2::text, $3::text, COALESCE(MAX(s."offset"), -1) + 1, $4::int, $5::text
               FROM "${this.schemaName}".streams s
               WHERE s.workflow_uuid = $1 AND s.key = $2`,
              [workflowID, key, serializedValue, functionID, serialization],
            );
            didWrite = true;
            return undefined;
          });
          await client.query('COMMIT');
        } catch (e) {
          // Only an offset conflict resolves on retry; anything else would spin forever.
          const offsetConflict = e instanceof DatabaseError && e.code === '23505';
          // Log before touching the connection again: a failing ROLLBACK is what would propagate.
          if (!offsetConflict) this.logger.error(e);
          // Roll back before waiting, so a retry does not hold an aborted transaction open.
          await client.query('ROLLBACK');
          if (offsetConflict) {
            this.logger.warn(`Stream offset conflict for workflow ${workflowID}, key ${key}; retrying`);
            await sleepms(100);
            continue;
          }
          throw e;
        }
        // Notify only after commit, so a woken reader sees the value.
        if (didWrite) {
          this.#signalNotification(DBOS_STREAMS_CHANNEL, `${workflowID}::${key}`);
        }
        return;
      }
    } finally {
      client.release();
    }
  }

  async closeStreamFromWorkflow(workflowID: string, functionID: number, key: string): Promise<void> {
    await this.writeStreamFromWorkflow(
      workflowID,
      functionID,
      key,
      DBOS_STREAM_CLOSED_SENTINEL_SERIALIZED,
      DBOSPortableJSON.name(),
      DBOS_FUNCNAME_CLOSESTREAM,
    );
  }

  async closeStreamFromStep(workflowID: string, stepID: number, key: string): Promise<void> {
    await this.writeStreamFromStep(
      workflowID,
      stepID,
      key,
      DBOS_STREAM_CLOSED_SENTINEL_SERIALIZED,
      DBOSPortableJSON.name(),
    );
  }

  // Read the value at `offset` and the workflow's status in one query: status null = no such workflow, value undefined = nothing at that offset.
  @dbRetry()
  async readStreamValue(
    workflowID: string,
    key: string,
    offset: number,
  ): Promise<{ status: string | null; value: { serializedValue: string; serialization: string | null } | undefined }> {
    // LEFT JOIN so a workflow with nothing at offset still reports its status (single PK lookup); under the poll limiter, inside @dbRetry so the permit frees across backoff.
    const result = await this.#pollWithLimiter(() =>
      this.pool.query<{
        status: string;
        value: string | null;
        serialization: string | null;
        stream_offset: number | null;
      }>(
        // "offset" is a reserved word, so alias it (stream_offset) to read it back plainly.
        `SELECT ws.status AS status, s.value AS value, s.serialization AS serialization, s."offset" AS stream_offset
         FROM "${this.schemaName}".workflow_status ws
         LEFT OUTER JOIN "${this.schemaName}".streams s
           ON s.workflow_uuid = ws.workflow_uuid AND s.key = $2 AND s."offset" = $3
         WHERE ws.workflow_uuid = $1`,
        [workflowID, key, offset],
      ),
    );

    if (result.rows.length === 0) {
      return { status: null, value: undefined };
    }
    const row = result.rows[0];
    // streams.offset is non-nullable, so a NULL here means the join matched nothing at offset.
    if (row.stream_offset === null) {
      return { status: row.status, value: undefined };
    }
    return { status: row.status, value: { serializedValue: row.value as string, serialization: row.serialization } };
  }

  #signalNotification(channel: string, payload: string): void {
    // Coalesce a wakeup on `channel` for `payload`; no-op without LISTEN/NOTIFY (clients, CockroachDB), which poll.
    if (!this.shouldUseDBNotifications) {
      return;
    }
    let batch = this.pendingNotifications.get(channel);
    if (batch === undefined) {
      batch = new Set();
      this.pendingNotifications.set(channel, batch);
    }
    batch.add(payload);
  }

  // Periodically flush coalesced notifications across all channels, keeping the notifying commit off the write path.
  async #runNotifier(): Promise<void> {
    while (this.#notifierActive) {
      const { promise, cancel } = cancellableSleep(this.notificationCoalesceMs);
      this.#notifierWake = cancel;
      await promise;
      this.#notifierWake = null;
      if (!this.#notifierActive) {
        break;
      }
      try {
        await this.flushNotifications();
      } catch (e) {
        // Last resort: the flush drops its own failed batch, so this catches only unexpected errors that must not kill the push path.
        if (this.#notifierActive) {
          this.logger.warn(`Notifier error: ${String(e)}`);
          const { promise: backoff } = cancellableSleep(1000);
          await backoff;
        }
      }
    }
    // Final flush so values written just before shutdown still wake readers promptly.
    try {
      await this.flushNotifications();
    } catch (e) {
      this.logger.warn(`Notifier final flush error: ${String(e)}`);
    }
  }

  // Emit one notifying transaction per channel for all pending payloads; drop a channel's batch on failure. Soft-private so tests can drive it.
  async flushNotifications(): Promise<void> {
    let hasPending = false;
    for (const batch of this.pendingNotifications.values()) {
      if (batch.size > 0) {
        hasPending = true;
        break;
      }
    }
    if (!hasPending) {
      return;
    }
    // Grab and clear atomically (no await between), so writes during the flush start the next batch.
    const batches = this.pendingNotifications;
    this.pendingNotifications = new Map();
    // One transaction per channel so an unsendable payload on one channel drops only its own batch.
    for (const [channel, batch] of batches) {
      if (batch.size === 0) {
        continue;
      }
      try {
        // One statement: one round trip, one async-notify queue-lock acquisition; unnest emits one notification per payload.
        const client = await this.#connect();
        try {
          await client.query(`SELECT pg_notify($1, p) FROM unnest($2::text[]) AS p`, [channel, Array.from(batch)]);
        } finally {
          client.release();
        }
      } catch (e) {
        // Drop the batch (don't requeue) on failure, e.g. a payload over pg_notify's 8000-byte limit; polling still delivers those values.
        this.logger.warn(`Notifier flush error on ${channel}: ${String(e)}`);
      }
    }
  }

  // ==================== Observability: Workflow Communications ====================

  async getAllEvents(workflowID: string): Promise<Record<string, unknown>> {
    const client = await this.#connect();
    try {
      const result = await client.query<{ key: string; value: string; serialization: string | null }>(
        `SELECT key, value, serialization FROM "${this.schemaName}".workflow_events
         WHERE workflow_uuid = $1`,
        [workflowID],
      );
      const events: Record<string, unknown> = {};
      for (const row of result.rows) {
        events[row.key] = await safeParse(this.serializer, row.value, row.serialization);
      }
      return events;
    } finally {
      client.release();
    }
  }

  async getAllNotifications(
    workflowID: string,
  ): Promise<{ topic: string | null; message: unknown; createdAtEpochMs: number; consumed: boolean }[]> {
    const client = await this.#connect();
    try {
      const result = await client.query<{
        topic: string;
        message: string;
        serialization: string | null;
        created_at_epoch_ms: string;
        consumed: boolean;
      }>(
        `SELECT topic, message, serialization, created_at_epoch_ms, consumed
         FROM "${this.schemaName}".notifications
         WHERE destination_uuid = $1
         ORDER BY created_at_epoch_ms`,
        [workflowID],
      );
      return await Promise.all(
        result.rows.map(async (row) => ({
          topic: row.topic === this.nullTopic ? null : row.topic,
          message: await safeParse(this.serializer, row.message, row.serialization),
          createdAtEpochMs: Number(row.created_at_epoch_ms),
          consumed: row.consumed,
        })),
      );
    } finally {
      client.release();
    }
  }

  async getAllStreamEntries(workflowID: string): Promise<Record<string, unknown[]>> {
    const client = await this.#connect();
    try {
      const result = await client.query<{ key: string; value: string; serialization: string | null }>(
        `SELECT key, value, serialization FROM "${this.schemaName}".streams
         WHERE workflow_uuid = $1
         ORDER BY key, "offset"`,
        [workflowID],
      );
      const streams: Record<string, unknown[]> = {};
      const closed = new Set<string>();
      for (const row of result.rows) {
        if (closed.has(row.key)) {
          continue;
        }
        // safeParse yields the raw string for the legacy unserialized marker, which does not parse.
        const value = await safeParse(this.serializer, row.value, row.serialization);
        if (isStreamClosedSentinel(value)) {
          // End the stream where readStream does, so the two never disagree.
          closed.add(row.key);
          streams[row.key] ??= [];
          continue;
        }
        (streams[row.key] ??= []).push(value);
      }
      return streams;
    } finally {
      client.release();
    }
  }

  // ==================== Queues ====================
  async transitionDelayedWorkflows(): Promise<void> {
    // Transition workflows from DELAYED to ENQUEUED when their delay has expired.
    // For debounced workflows, clear the deduplication ID in the same atomic update: it is a
    // debounce key held only while DELAYED, so a later same-key debounce starts a fresh workflow.
    const params: unknown[] = [StatusString.ENQUEUED, Date.now(), StatusString.DELAYED];
    // Only what this application would dequeue: a peer's debounce key is not ours to clear.
    const scope = this.#appNameFilter('application_name', this.appName, params);
    await this.pool.query(
      `UPDATE "${this.schemaName}".workflow_status
       SET status = $1, updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
           deduplication_id = CASE WHEN is_debounced THEN NULL ELSE deduplication_id END
       WHERE status = $3 AND delay_until_epoch_ms <= $2 AND ${scope}`,
      params,
    );
  }

  @dbRetry()
  async getDeduplicatedWorkflow(queueName: string, deduplicationID: string): Promise<string | null> {
    const { rows } = await this.pool.query<workflow_status>(
      `SELECT workflow_uuid FROM "${this.schemaName}".workflow_status
       WHERE queue_name = $1 AND deduplication_id = $2`,
      [queueName, deduplicationID],
    );

    if (rows.length === 0) {
      return null;
    }

    return rows[0].workflow_uuid;
  }

  @dbRetry()
  async getQueuePartitions(queueName: string): Promise<string[]> {
    // Recursive-CTE loose index scan: SELECT DISTINCT would scan every ENQUEUED row, whereas each iteration here is one seek on idx_workflow_status_partition_dequeue_v2, so cost scales with the number of partitions rather than the backlog depth.
    const params: unknown[] = [queueName, StatusString.ENQUEUED];
    // Only partitions this application can actually dequeue from.
    const scope = this.#appNameFilter('application_name', this.appName, params);
    const { rows } = await this.pool.query<{ pk: string }>(
      `WITH RECURSIVE partitions AS (
         (SELECT MIN(queue_partition_key) AS pk
          FROM "${this.schemaName}".workflow_status
          WHERE queue_name = $1 AND status = $2 AND queue_partition_key IS NOT NULL AND ${scope})
         UNION ALL
         (SELECT (SELECT MIN(queue_partition_key)
                  FROM "${this.schemaName}".workflow_status
                  WHERE queue_name = $1 AND status = $2 AND queue_partition_key > partitions.pk AND ${scope})
          FROM partitions
          WHERE partitions.pk IS NOT NULL)
       )
       SELECT pk FROM partitions WHERE pk IS NOT NULL`,
      params,
    );

    return rows.map((row) => row.pk);
  }

  async findAndMarkStartableWorkflows(
    queue: WorkflowQueue,
    executorID: string,
    appVersion: string,
    queuePartitionKey?: string,
    localRunningCount: number = 0,
    partitionLocalRunningCount: number = 0,
  ): Promise<string[]> {
    const claimedIDs: string[] = [];
    const limits = resolveQueueLimits(queue);
    const partitionParams: string[] = queuePartitionKey !== undefined ? [queuePartitionKey] : [];
    // Shares a concurrency or rate limit budget with other executors.
    const hasSharedBudget =
      limits.globalConcurrency !== undefined ||
      limits.partitionConcurrency !== undefined ||
      limits.rateLimit !== undefined ||
      limits.partitionRateLimit !== undefined;
    // Shares that budget across partitions too, so sweeps of different partitions read disjoint rows and could each spend it.
    const hasWriteSkew =
      queuePartitionKey !== undefined && (limits.globalConcurrency !== undefined || limits.rateLimit !== undefined);

    const client = await this.#connect();
    try {
      // Default to READ COMMITTED except with a budget shared across executors
      if (hasSharedBudget) {
        await client.query(`BEGIN ISOLATION LEVEL ${hasWriteSkew ? 'SERIALIZABLE' : 'REPEATABLE READ'}`);
      } else {
        await client.query('BEGIN');
      }

      /** Slots left in a rate limit's rolling window, at the scope that limit applies to. */
      const rateLimitRemaining = async (rateLimit: QueueRateLimit, partitionScoped: boolean): Promise<number> => {
        const params: unknown[] = [queue.name, StatusString.ENQUEUED, StatusString.DELAYED, rateLimit.periodSec * 1000];
        // Count only what this application would dequeue, matching the select below.
        const scope = this.#appNameFilter('application_name', this.appName, params);
        const partitionFilter = partitionScoped ? `AND queue_partition_key = $${params.push(queuePartitionKey)}` : '';
        const { rows } = await client.query<{ count: string }>(
          `SELECT COUNT(*) FROM "${this.schemaName}".workflow_status
           WHERE queue_name = $1
             AND rate_limited = TRUE
             AND status NOT IN ($2, $3)
             -- Database clock on both sides, as the claim stamps started_at_epoch_ms with it.
             AND started_at_epoch_ms > (EXTRACT(epoch FROM now()) * 1000)::bigint - $4
             AND ${scope}
             ${partitionFilter}`,
          params,
        );
        return rateLimit.limitPerPeriod - Number(rows[0].count);
      };

      /**
       * Workflows already running, which peer workers count against too. Kept as its own query per
       * scope: the partition-scoped predicate rides idx_workflow_status_partition_dequeue_v2, which
       * a queue-wide scan loses.
       */
      const pendingCount = async (partitionScoped: boolean): Promise<number> => {
        const params: unknown[] = [queue.name, StatusString.PENDING];
        const scope = this.#appNameFilter('application_name', this.appName, params);
        const partitionFilter = partitionScoped ? `AND queue_partition_key = $${params.push(queuePartitionKey)}` : '';
        const { rows } = await client.query<{ count: string }>(
          `SELECT COUNT(*) FROM "${this.schemaName}".workflow_status
           WHERE queue_name = $1 AND status = $2 AND ${scope} ${partitionFilter}`,
          params,
        );
        return Number(rows[0]?.count ?? 0);
      };

      // Compute maxTasks, the number of workflows startable under every flow control limit on this queue.
      let maxTasks = Infinity;

      if (limits.workerConcurrency !== undefined) {
        // Use the in-memory registry for this worker's running count — avoids a DB round trip.
        maxTasks = Math.min(maxTasks, Math.max(0, limits.workerConcurrency - localRunningCount));
      }
      if (limits.partitionWorkerConcurrency !== undefined) {
        maxTasks = Math.min(maxTasks, Math.max(0, limits.partitionWorkerConcurrency - partitionLocalRunningCount));
      }
      if (maxTasks <= 0) {
        await client.query('COMMIT');
        return claimedIDs;
      }

      if (limits.rateLimit !== undefined) {
        // Bound the claim by the limiter's remaining slots so a backlogged queue locks only what it can start.
        maxTasks = Math.min(maxTasks, await rateLimitRemaining(limits.rateLimit, false));
      }
      if (limits.partitionRateLimit !== undefined) {
        maxTasks = Math.min(maxTasks, await rateLimitRemaining(limits.partitionRateLimit, true));
      }
      if (maxTasks <= 0) {
        await client.query('COMMIT');
        return claimedIDs;
      }

      if (limits.globalConcurrency !== undefined) {
        // Global concurrency still requires a DB query since other workers may be running workflows too.
        const totalRunningTasks = await pendingCount(false);
        if (totalRunningTasks > limits.globalConcurrency) {
          this.logger.warn(
            `Total running tasks (${totalRunningTasks}) exceeds the global concurrency limit (${limits.globalConcurrency})`,
          );
        }
        maxTasks = Math.min(maxTasks, Math.max(0, limits.globalConcurrency - totalRunningTasks));
      }
      if (limits.partitionConcurrency !== undefined) {
        const partitionRunningTasks = await pendingCount(true);
        if (partitionRunningTasks > limits.partitionConcurrency) {
          this.logger.warn(
            `Total running tasks (${partitionRunningTasks}) on partition ${queuePartitionKey} of queue ${queue.name} exceeds the partition concurrency limit (${limits.partitionConcurrency})`,
          );
        }
        maxTasks = Math.min(maxTasks, Math.max(0, limits.partitionConcurrency - partitionRunningTasks));
      }
      // Return immediately if there are no available tasks due to flow control limits
      if (maxTasks <= 0) {
        await client.query('COMMIT');
        return claimedIDs;
      }

      // Retrieve the first max_tasks workflows in the queue.
      const latestVersion = await this.#latestApplicationVersionName(client);
      const isLatestVersion = latestVersion === undefined || latestVersion === appVersion;
      const versionClause = isLatestVersion
        ? '(application_version = $3 OR application_version IS NULL)'
        : 'application_version = $3';

      // A limit shared across processes needs a consistent view of the table: NOWAIT makes an
      // overlapping dequeuer abort rather than claim the next rows and spend the same budget twice.
      const lockMode = hasSharedBudget ? 'FOR UPDATE NOWAIT' : 'FOR UPDATE SKIP LOCKED';
      const limitClause = maxTasks !== Infinity ? `LIMIT ${maxTasks}` : '';

      const selectParams: unknown[] = [StatusString.ENQUEUED, queue.name, appVersion, ...partitionParams];
      const selectScope = this.#appNameFilter('application_name', this.appName, selectParams);
      const selectQuery = `
        SELECT workflow_uuid
        FROM "${this.schemaName}".workflow_status
        WHERE status = $1
          AND queue_name = $2
          AND ${versionClause}
          AND ${selectScope}
          ${queuePartitionKey !== undefined ? 'AND queue_partition_key = $4' : ''}
        ORDER BY priority ASC, created_at ASC
        ${limitClause}
        ${lockMode}
      `;

      const { rows } = await client.query<{ workflow_uuid: string }>(selectQuery, selectParams);
      // Fires while the SELECT FOR UPDATE lock is held — tests can throw a
      // synthetic 55P03 here to simulate a concurrent executor winning the race.
      await debugTriggerPoint(DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT);

      // Start the workflows
      const workflowIDs = rows.map((row) => row.workflow_uuid);
      if (workflowIDs.length > 0) {
        // Start the functions by marking them as pending and updating their executor IDs.
        const updateParams: unknown[] = [
          StatusString.PENDING,
          executorID,
          appVersion,
          limits.rateLimit !== undefined || limits.partitionRateLimit !== undefined,
          workflowIDs,
          StatusString.ENQUEUED,
          // Claim an unclaimed row for this application; a nameless dequeuer leaves ownership untouched.
          this.appName ?? null,
        ];
        // Re-check ownership alongside status, as the partitioned claim guard does.
        const claimScope = this.#appNameFilter('application_name', this.appName, updateParams);
        // RETURNING reports exactly the rows this statement flipped, so a row another worker won is absent.
        const flippedResult = await client.query<{ workflow_uuid: string }>(
          `UPDATE "${this.schemaName}".workflow_status
           SET status = $1,
               executor_id = $2,
               application_version = $3,
               started_at_epoch_ms = (EXTRACT(epoch FROM now()) * 1000)::bigint,
               rate_limited = $4,
               application_name = COALESCE(application_name, $7),
               recovery_attempts = recovery_attempts + 1,
               updated_at = (EXTRACT(epoch FROM now()) * 1000)::bigint,
               workflow_deadline_epoch_ms = CASE
                 WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                 THEN (EXTRACT(epoch FROM now()) * 1000)::bigint + workflow_timeout_ms
                 ELSE workflow_deadline_epoch_ms
               END
           WHERE workflow_uuid = ANY($5::text[]) AND status = $6 AND ${claimScope}
           RETURNING workflow_uuid`,
          updateParams,
        );
        const flippedIDs = new Set(flippedResult.rows.map((row) => row.workflow_uuid));
        for (const id of workflowIDs) {
          if (flippedIDs.has(id)) claimedIDs.push(id);
        }
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

  /** Max heads admitted per sweep: bounds dispatch, not the partition walk; an unconstrained sweep takes the lowest keys, so higher keys can wait under sustained load. */
  partitionedDequeueSweepCap: number = 8192;

  /** Dequeue each partition's head-of-line workflow in one transaction, at most {@link partitionedDequeueSweepCap} per sweep; only valid for partition-concurrency-1 queues with no queue-wide limit. */
  async findAndMarkStartablePartitionedWorkflows(
    queue: WorkflowQueue,
    executorID: string,
    appVersion: string,
    maxTasks: number = Infinity,
  ): Promise<string[]> {
    const limits = resolveQueueLimits(queue);
    if (
      limits.partitionConcurrency !== 1 ||
      limits.globalConcurrency !== undefined ||
      limits.rateLimit !== undefined ||
      limits.partitionRateLimit !== undefined
    ) {
      throw new DBOSError(
        `Batched partitioned dequeue requires a queue with partition concurrency 1 and no queue-wide concurrency or rate limit: ${queue.name}`,
      );
    }
    // partitionWorkerConcurrency needs no handling here: any value above 0 is capped at partition concurrency 1, which the PENDING gate already enforces globally, and 0 makes the caller's maxTasks 0.
    const client = await this.#connect();
    try {
      await client.query('BEGIN');

      const latestVersion = await this.#latestApplicationVersionName(client);
      const isLatestVersion = latestVersion === undefined || latestVersion === appVersion;
      const versionClause = (n: number) =>
        isLatestVersion
          ? `(application_version = $${n} OR application_version IS NULL)`
          : `application_version = $${n}`;

      // This worker's own budget bounds the sweep alongside the cap.
      const sweepLimit = Math.min(this.partitionedDequeueSweepCap, maxTasks);
      // When the worker's budget is the binding constraint, probe partitions in random order to prevent starvation.
      const sweepOrder = sweepLimit < this.partitionedDequeueSweepCap ? 'random()' : 'partitions.pk ASC';
      const candidateParams: unknown[] = [
        queue.name,
        StatusString.ENQUEUED,
        appVersion,
        StatusString.PENDING,
        sweepLimit,
      ];
      const candidateScope = this.#appNameFilter('application_name', this.appName, candidateParams);

      // Walk distinct partition keys with a recursive-CTE loose index scan (one seek per key, mirroring getQueuePartitions) so sweep cost scales with partition count, not backlog depth.
      const candidateResult = await client.query<{ workflow_uuid: string }>(
        `WITH RECURSIVE partitions AS (
           (SELECT MIN(queue_partition_key) AS pk
            FROM "${this.schemaName}".workflow_status
            WHERE queue_name = $1 AND status = $2 AND queue_partition_key IS NOT NULL AND ${candidateScope})
           UNION ALL
           (SELECT (SELECT MIN(queue_partition_key)
                    FROM "${this.schemaName}".workflow_status
                    WHERE queue_name = $1 AND status = $2 AND queue_partition_key > partitions.pk AND ${candidateScope})
            FROM partitions
            WHERE partitions.pk IS NOT NULL)
         )
         , chosen AS (
           SELECT partitions.pk
           FROM partitions
           WHERE partitions.pk IS NOT NULL
             -- Unscoped by design: a mutual-exclusion probe must block on any owner's row.
             AND NOT EXISTS (
               SELECT 1
               FROM "${this.schemaName}".workflow_status
               WHERE queue_name = $1 AND status = $4
                 AND queue_partition_key IS NOT NULL AND queue_partition_key = partitions.pk
             )
           ORDER BY ${sweepOrder}
           LIMIT $5
         )
         SELECT head.workflow_uuid
         FROM chosen
         -- LATERAL plans as a tight nested loop; a correlated scalar subquery runs as a slower per-row SubPlan.
         JOIN LATERAL (
           SELECT workflow_uuid
           FROM "${this.schemaName}".workflow_status
           WHERE queue_name = $1 AND status = $2
             AND queue_partition_key = chosen.pk
             AND ${versionClause(3)}
             AND ${candidateScope}
           -- workflow_uuid totalizes the head order (same head for every worker under created_at ties) and the index's trailing workflow_uuid keeps this a pure top-1 probe.
           ORDER BY priority ASC, created_at ASC, workflow_uuid ASC
           LIMIT 1
         ) head ON TRUE
         -- Which partitions were chosen is settled above; order the result so the claim, and the dispatch it feeds, are deterministic.
         ORDER BY chosen.pk ASC`,
        candidateParams,
      );
      const candidateIDs = candidateResult.rows.map((row) => row.workflow_uuid);
      if (candidateIDs.length === 0) {
        await client.query('COMMIT');
        return [];
      }
      await debugTriggerPoint(DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES);

      // Re-check queue/partition/version alongside status so a row resumeWorkflows moved to another queue mid-sweep is dropped, not hijacked.
      const claimGuard = (ids: number, status: number, name: number, version: number, scope: string) =>
        `workflow_uuid = ANY($${ids}::text[])
           AND status = $${status}
           AND queue_name = $${name}
           AND queue_partition_key IS NOT NULL
           AND ${versionClause(version)}
           AND ${scope}`;

      const lockedParams: unknown[] = [candidateIDs, StatusString.ENQUEUED, queue.name, appVersion];
      const lockedScope = this.#appNameFilter('application_name', this.appName, lockedParams);
      // Lock the fixed candidate set — never a LIMIT query, whose SKIP LOCKED could slide past a locked head and admit out of order.
      const lockedResult = await client.query<{ workflow_uuid: string }>(
        `SELECT workflow_uuid
         FROM "${this.schemaName}".workflow_status
         WHERE ${claimGuard(1, 2, 3, 4, lockedScope)}
         FOR UPDATE SKIP LOCKED`,
        lockedParams,
      );
      const lockedIDs = new Set(lockedResult.rows.map((row) => row.workflow_uuid));
      // Preserve partition order for submission.
      const claimIDs = candidateIDs.filter((id) => lockedIDs.has(id));
      if (claimIDs.length === 0) {
        await client.query('COMMIT');
        return [];
      }

      const flipParams: unknown[] = [
        StatusString.PENDING,
        executorID,
        claimIDs,
        StatusString.ENQUEUED,
        queue.name,
        appVersion,
        // Claim the row, as the unpartitioned dequeue does.
        this.appName ?? null,
      ];
      const flipScope = this.#appNameFilter('application_name', this.appName, flipParams);
      // Start the workflows by marking them PENDING; RETURNING reports exactly the rows this statement flipped.
      const flippedResult = await client.query<{ workflow_uuid: string }>(
        `UPDATE "${this.schemaName}".workflow_status
         SET status = $1,
             executor_id = $2,
             application_version = $6,
             started_at_epoch_ms = (EXTRACT(epoch FROM now()) * 1000)::bigint,
             rate_limited = FALSE,
             application_name = COALESCE(application_name, $7),
             recovery_attempts = recovery_attempts + 1,
             updated_at = (EXTRACT(epoch FROM now()) * 1000)::bigint,
             workflow_deadline_epoch_ms = CASE
               WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
               THEN (EXTRACT(epoch FROM now()) * 1000)::bigint + workflow_timeout_ms
               ELSE workflow_deadline_epoch_ms
             END
         WHERE ${claimGuard(3, 4, 5, 6, flipScope)}
         RETURNING workflow_uuid`,
        flipParams,
      );

      await client.query('COMMIT');

      const flippedIDs = new Set(flippedResult.rows.map((row) => row.workflow_uuid));
      const claimedIDs = claimIDs.filter((id) => flippedIDs.has(id));
      if (claimedIDs.length > 0) {
        this.logger.debug(`[${queue.name}] dequeueing ${claimedIDs.length} task(s)`);
      }
      return claimedIDs;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  // ==================== Queries & Maintenance ====================
  async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatusInternal[]> {
    const schemaName = this.schemaName;
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
      'deduplication_id',
      'priority',
      'queue_partition_key',
      'started_at_epoch_ms',
      'forked_from',
      'was_forked_from',
      'parent_workflow_id',
      'delay_until_epoch_ms',
      'completed_at',
      'attributes',
      'schedule_name',
      'debounce_deadline_epoch_ms',
      'is_debounced',
      'application_name',
    ];

    input.loadInput = input.loadInput ?? true;
    input.loadOutput = input.loadOutput ?? true;
    if (input.loadInput) {
      selectColumns.push('inputs', 'request');
    }

    if (input.loadOutput) {
      selectColumns.push('output', 'error');
    }

    if (input.loadInput || input.loadOutput) {
      selectColumns.push('serialization');
    }

    input.sortDesc = input.sortDesc ?? false; // By default, sort in ascending order

    // Build WHERE clauses
    const whereClauses: string[] = [];
    const params: unknown[] = [];
    let paramCounter = 1;

    // Helper: add a filter for a field that may be a single value or an array.
    // Uses = for a single value, IN (...) for an array.
    const addFilter = (column: string, value: string | string[] | undefined) => {
      if (!value) return;
      if (Array.isArray(value)) {
        const placeholders = value.map((_, i) => `$${paramCounter + i}`).join(', ');
        whereClauses.push(`${column} IN (${placeholders})`);
        params.push(...value);
        paramCounter += value.length;
      } else {
        whereClauses.push(`${column} = $${paramCounter}`);
        params.push(value);
        paramCounter++;
      }
    };

    // If queuesOnly, filter for queued workflows
    if (input.queuesOnly) {
      whereClauses.push(`queue_name IS NOT NULL`);
      whereClauses.push(`status IN ($${paramCounter}, $${paramCounter + 1}, $${paramCounter + 2})`);
      params.push(StatusString.ENQUEUED, StatusString.PENDING, StatusString.DELAYED);
      paramCounter += 3;
    }

    addFilter('name', input.workflowName);
    addFilter('queue_name', input.queueName);
    addFilter('schedule_name', input.scheduleName);

    // A workflow ID is a global address, so an ID-keyed read is an identity read: it takes an
    // explicit filter but is never defaulted to this one. Otherwise unset scopes to this
    // application, as on every other observability query.
    // Falsy, not just undefined: a Conductor request carries an omitted list as JSON null.
    const idKeyed = (input.workflowIDs?.length ?? 0) > 0;
    whereClauses.push(
      idKeyed
        ? this.#appNameFilter('application_name', input.applicationName, params)
        : this.#observabilityFilter('application_name', input.applicationName, params),
    );
    paramCounter = params.length + 1;

    if (input.workflow_id_prefix) {
      if (Array.isArray(input.workflow_id_prefix)) {
        const likeClauses = input.workflow_id_prefix.map((_, i) => `workflow_uuid LIKE $${paramCounter + i}`);
        whereClauses.push(`(${likeClauses.join(' OR ')})`);
        params.push(...input.workflow_id_prefix.map((p) => `${p}%`));
        paramCounter += input.workflow_id_prefix.length;
      } else {
        whereClauses.push(`workflow_uuid LIKE $${paramCounter}`);
        params.push(`${input.workflow_id_prefix}%`);
        paramCounter++;
      }
    }
    if (input.workflowIDs) {
      const placeholders = input.workflowIDs.map((_, i) => `$${paramCounter + i}`).join(', ');
      whereClauses.push(`workflow_uuid IN (${placeholders})`);
      params.push(...input.workflowIDs);
      paramCounter += input.workflowIDs.length;
    }

    addFilter('authenticated_user', input.authenticatedUser);
    addFilter('forked_from', input.forkedFrom);
    addFilter('parent_workflow_id', input.parentWorkflowID);

    if (input.wasForkedFrom !== undefined) {
      whereClauses.push(`was_forked_from = $${paramCounter}`);
      params.push(input.wasForkedFrom);
      paramCounter++;
    }

    if (input.hasParent !== undefined) {
      if (input.hasParent) {
        whereClauses.push(`parent_workflow_id IS NOT NULL`);
      } else {
        whereClauses.push(`parent_workflow_id IS NULL`);
      }
    }

    // Match workflows whose attributes JSONB contains all the given key-value pairs.
    // The `@>` containment operator is served by the GIN index on the attributes column.
    if (input.attributes && Object.keys(input.attributes).length > 0) {
      whereClauses.push(`attributes @> $${paramCounter}::jsonb`);
      params.push(JSON.stringify(input.attributes));
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
    if (input.completedAfter) {
      whereClauses.push(`completed_at >= $${paramCounter}`);
      params.push(new Date(input.completedAfter).getTime());
      paramCounter++;
    }
    if (input.completedBefore) {
      whereClauses.push(`completed_at <= $${paramCounter}`);
      params.push(new Date(input.completedBefore).getTime());
      paramCounter++;
    }
    // dequeuedAfter/Before filter on started_at_epoch_ms: that column is
    // populated on dequeue and surfaced as WorkflowStatus.dequeuedAt.
    if (input.dequeuedAfter) {
      whereClauses.push(`started_at_epoch_ms >= $${paramCounter}`);
      params.push(new Date(input.dequeuedAfter).getTime());
      paramCounter++;
    }
    if (input.dequeuedBefore) {
      whereClauses.push(`started_at_epoch_ms <= $${paramCounter}`);
      params.push(new Date(input.dequeuedBefore).getTime());
      paramCounter++;
    }

    addFilter('status', input.status);
    addFilter('application_version', input.applicationVersion);
    addFilter('executor_id', input.executorId);

    const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const orderClause = `ORDER BY created_at ${input.sortDesc ? 'DESC' : 'ASC'}`;
    const limitClause = input.limit ? `LIMIT ${input.limit}` : '';
    const offsetClause = input.offset ? `OFFSET ${input.offset}` : '';

    const query = `
      SELECT ${selectColumns.join(', ')}
      FROM "${schemaName}".workflow_status
      ${whereClause}
      ${orderClause}
      ${limitClause}
      ${offsetClause}
    `;

    const result = await this.pool.query<workflow_status>(query, params);
    return result.rows.map(mapWorkflowStatus);
  }

  async getWorkflowAggregates(input: GetWorkflowAggregatesInput): Promise<WorkflowAggregateRow[]> {
    if (input.timeBucketSizeMs !== undefined && input.timeBucketSizeMs <= 0) {
      throw new Error('time_bucket_size_ms must be > 0');
    }

    const groupByFlags: [string, boolean, string][] = [
      ['status', input.groupByStatus ?? false, 'status'],
      ['name', input.groupByName ?? false, 'name'],
      ['queue_name', input.groupByQueueName ?? false, 'queue_name'],
      ['executor_id', input.groupByExecutorId ?? false, 'executor_id'],
      ['application_version', input.groupByApplicationVersion ?? false, 'application_version'],
      ['application_name', input.groupByApplicationName ?? false, 'application_name'],
    ];

    const groupNames: string[] = [];
    const groupColumns: string[] = [];
    const groupSelectColumns: string[] = [];
    for (const [colName, enabled, col] of groupByFlags) {
      if (enabled) {
        groupNames.push(colName);
        groupColumns.push(col);
        groupSelectColumns.push(col);
      }
    }

    if (input.timeBucketSizeMs !== undefined) {
      // Bucket on created_at — the indexed wall-clock timestamp on workflow_status.
      const bucket = input.timeBucketSizeMs;
      const bucketExpr = `(CAST(FLOOR(created_at / ${bucket}) AS BIGINT) * ${bucket})`;
      groupNames.push('time_bucket');
      groupColumns.push(bucketExpr);
      groupSelectColumns.push(`${bucketExpr} AS time_bucket`);
    }

    if (groupColumns.length === 0) {
      throw new Error('At least one group_by flag must be set to True');
    }

    // Build select columns from boolean flags. MAX ignores NULLs, so rows
    // missing started_at_epoch_ms or completed_at naturally drop out of the
    // latency maxes.
    const selectFlags: [string, boolean, string][] = [
      ['count', input.selectCount ?? false, 'COUNT(*)'],
      ['min_created_at', input.selectMinCreatedAt ?? false, 'MIN(created_at)'],
      ['max_queue_wait_ms', input.selectMaxQueueWaitMs ?? false, 'MAX(started_at_epoch_ms - created_at)'],
      ['max_total_latency_ms', input.selectMaxTotalLatencyMs ?? false, 'MAX(completed_at - created_at)'],
    ];
    const selectNames: string[] = [];
    const selectColumns: string[] = [];
    for (const [name, enabled, expr] of selectFlags) {
      if (enabled) {
        selectNames.push(name);
        selectColumns.push(`${expr} AS ${name}`);
      }
    }

    if (selectColumns.length === 0) {
      throw new Error('At least one select_ flag must be set to True');
    }

    const whereClauses: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    const addFilter = (column: string, values: string[] | undefined) => {
      if (!values || values.length === 0) return;
      const placeholders = values.map((_, i) => `$${paramIdx + i}`).join(', ');
      whereClauses.push(`${column} IN (${placeholders})`);
      params.push(...values);
      paramIdx += values.length;
    };

    addFilter('status', input.status);
    addFilter('name', input.name);
    addFilter('application_version', input.appVersion);
    addFilter('executor_id', input.executorId);
    addFilter('queue_name', input.queueName);

    if (input.workflowIdPrefix && input.workflowIdPrefix.length > 0) {
      const likeClauses = input.workflowIdPrefix.map((p) => {
        params.push(`${p}%`);
        return `workflow_uuid LIKE $${paramIdx++}`;
      });
      whereClauses.push(`(${likeClauses.join(' OR ')})`);
    }

    addFilter('workflow_uuid', input.workflowIDs);
    addFilter('authenticated_user', input.authenticatedUser);
    addFilter('forked_from', input.forkedFrom);
    addFilter('parent_workflow_id', input.parentWorkflowID);
    addFilter('schedule_name', input.scheduleName);

    // Unset scopes to this application, as on every other observability query.
    whereClauses.push(this.#observabilityFilter('application_name', input.applicationName, params));
    paramIdx = params.length + 1;

    // Only workflows that are actively enqueued.
    if (input.queuesOnly) {
      whereClauses.push(`queue_name IS NOT NULL`);
      whereClauses.push(`status IN ($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2})`);
      params.push(StatusString.ENQUEUED, StatusString.PENDING, StatusString.DELAYED);
      paramIdx += 3;
    }

    if (input.wasForkedFrom !== undefined) {
      whereClauses.push(`was_forked_from = $${paramIdx}`);
      params.push(input.wasForkedFrom);
      paramIdx++;
    }

    if (input.hasParent !== undefined) {
      whereClauses.push(input.hasParent ? `parent_workflow_id IS NOT NULL` : `parent_workflow_id IS NULL`);
    }

    // Match workflows whose attributes JSONB contains all the given key-value pairs.
    if (input.attributes && Object.keys(input.attributes).length > 0) {
      whereClauses.push(`attributes @> $${paramIdx}::jsonb`);
      params.push(JSON.stringify(input.attributes));
      paramIdx++;
    }

    if (input.startTime) {
      whereClauses.push(`created_at >= $${paramIdx}`);
      params.push(new Date(input.startTime).getTime());
      paramIdx++;
    }
    if (input.endTime) {
      whereClauses.push(`created_at <= $${paramIdx}`);
      params.push(new Date(input.endTime).getTime());
      paramIdx++;
    }
    if (input.completedAfter) {
      whereClauses.push(`completed_at >= $${paramIdx}`);
      params.push(new Date(input.completedAfter).getTime());
      paramIdx++;
    }
    if (input.completedBefore) {
      whereClauses.push(`completed_at <= $${paramIdx}`);
      params.push(new Date(input.completedBefore).getTime());
      paramIdx++;
    }
    // dequeuedAfter/Before filter on started_at_epoch_ms: that column is
    // populated on dequeue and surfaced as WorkflowStatus.dequeuedAt.
    if (input.dequeuedAfter) {
      whereClauses.push(`started_at_epoch_ms >= $${paramIdx}`);
      params.push(new Date(input.dequeuedAfter).getTime());
      paramIdx++;
    }
    if (input.dequeuedBefore) {
      whereClauses.push(`started_at_epoch_ms <= $${paramIdx}`);
      params.push(new Date(input.dequeuedBefore).getTime());
      paramIdx++;
    }

    const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const groupByClause = groupColumns.join(', ');
    const selectClause = [...groupSelectColumns, ...selectColumns].join(', ');

    const query = `
      SELECT ${selectClause}
      FROM "${this.schemaName}".workflow_status
      ${whereClause}
      GROUP BY ${groupByClause}
    `;

    const result = await this.pool.query<Record<string, unknown>>(query, params);

    const toIntOrNull = (v: unknown): number | null => (v === null || v === undefined ? null : Number(v));

    return result.rows.map((row) => {
      const group: Record<string, string | null> = {};
      for (const name of groupNames) {
        const v = row[name];
        group[name] = v === null || v === undefined ? null : String(v as string | number | bigint);
      }
      return {
        group,
        count: selectNames.includes('count') ? toIntOrNull(row.count) : null,
        minCreatedAt: selectNames.includes('min_created_at') ? toIntOrNull(row.min_created_at) : null,
        maxQueueWaitMs: selectNames.includes('max_queue_wait_ms') ? toIntOrNull(row.max_queue_wait_ms) : null,
        maxTotalLatencyMs: selectNames.includes('max_total_latency_ms') ? toIntOrNull(row.max_total_latency_ms) : null,
      };
    });
  }

  async getStepAggregates(input: GetStepAggregatesInput): Promise<StepAggregateRow[]> {
    if (input.timeBucketSizeMs !== undefined && input.timeBucketSizeMs <= 0) {
      throw new Error('time_bucket_size_ms must be > 0');
    }

    // operation_outputs has no explicit status column; derive it from whether `error` is populated.
    // Child-workflow mapping rows have NULL error, so they appear as SUCCESS — callers filter by function_name.
    const statusExpr = `CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END`;

    const groupByFlags: [string, boolean, string][] = [
      ['function_name', input.groupByFunctionName ?? false, 'function_name'],
      ['status', input.groupByStatus ?? false, statusExpr],
    ];

    const groupNames: string[] = [];
    const groupColumns: string[] = [];
    const groupSelectColumns: string[] = [];
    for (const [colName, enabled, expr] of groupByFlags) {
      if (enabled) {
        groupNames.push(colName);
        groupColumns.push(expr);
        groupSelectColumns.push(`${expr} AS ${colName}`);
      }
    }

    if (input.timeBucketSizeMs !== undefined) {
      // Bucket on completed_at_epoch_ms — it's the indexed timestamp on
      // this table.
      const bucket = input.timeBucketSizeMs;
      const bucketExpr = `(CAST(FLOOR(completed_at_epoch_ms / ${bucket}) AS BIGINT) * ${bucket})`;
      groupNames.push('time_bucket');
      groupColumns.push(bucketExpr);
      groupSelectColumns.push(`${bucketExpr} AS time_bucket`);
    }

    if (groupColumns.length === 0) {
      throw new Error('At least one group_by flag must be set to True');
    }

    // Build select columns from boolean flags. Child-workflow mapping rows record start and
    // complete at nearly the same instant, so they contribute ~0; DBOS.getResult and DBOS.sleep
    // rows span their whole wait, so those dominate the duration max.
    const selectFlags: [string, boolean, string][] = [
      ['count', input.selectCount ?? false, 'COUNT(*)'],
      ['max_duration_ms', input.selectMaxDurationMs ?? false, 'MAX(completed_at_epoch_ms - started_at_epoch_ms)'],
    ];
    const selectNames: string[] = [];
    const selectColumns: string[] = [];
    for (const [name, enabled, expr] of selectFlags) {
      if (enabled) {
        selectNames.push(name);
        selectColumns.push(`${expr} AS ${name}`);
      }
    }

    if (selectColumns.length === 0) {
      throw new Error('At least one select_ flag must be set to True');
    }

    const whereClauses: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (input.status && input.status.length > 0) {
      const placeholders = input.status.map((_, i) => `$${paramIdx + i}`).join(', ');
      whereClauses.push(`(${statusExpr}) IN (${placeholders})`);
      params.push(...input.status);
      paramIdx += input.status.length;
    }
    if (input.functionName && input.functionName.length > 0) {
      const placeholders = input.functionName.map((_, i) => `$${paramIdx + i}`).join(', ');
      whereClauses.push(`function_name IN (${placeholders})`);
      params.push(...input.functionName);
      paramIdx += input.functionName.length;
    }
    if (input.workflowIdPrefix && input.workflowIdPrefix.length > 0) {
      const likeClauses = input.workflowIdPrefix.map((p) => {
        params.push(`${p}%`);
        return `workflow_uuid LIKE $${paramIdx++}`;
      });
      whereClauses.push(`(${likeClauses.join(' OR ')})`);
    }
    if (input.completedAfter) {
      whereClauses.push(`completed_at_epoch_ms >= $${paramIdx}`);
      params.push(new Date(input.completedAfter).getTime());
      paramIdx++;
    }
    if (input.completedBefore) {
      whereClauses.push(`completed_at_epoch_ms <= $${paramIdx}`);
      params.push(new Date(input.completedBefore).getTime());
      paramIdx++;
    }
    // Unset scopes to this application, as on every other observability query.
    whereClauses.push(this.#observabilityFilter('application_name', input.applicationName, params));
    paramIdx = params.length + 1;

    const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
    const groupByClause = groupColumns.join(', ');
    const selectClause = [...groupSelectColumns, ...selectColumns].join(', ');

    const query = `
      SELECT ${selectClause}
      FROM "${this.schemaName}".operation_outputs
      ${whereClause}
      GROUP BY ${groupByClause}
    `;

    const result = await this.pool.query<Record<string, unknown>>(query, params);

    const toIntOrNull = (v: unknown): number | null => (v === null || v === undefined ? null : Number(v));

    return result.rows.map((row) => {
      const group: Record<string, string | null> = {};
      for (const name of groupNames) {
        const v = row[name];
        group[name] = v === null || v === undefined ? null : String(v as string | number | bigint);
      }
      return {
        group,
        count: selectNames.includes('count') ? toIntOrNull(row.count) : null,
        maxDurationMs: selectNames.includes('max_duration_ms') ? toIntOrNull(row.max_duration_ms) : null,
      };
    });
  }

  /** Rows garbage collection may delete: terminal, older than the cutoff, and ours. */
  #gcFilter(cutoffEpochTimestampMs: number, params: unknown[]): string {
    params.push(cutoffEpochTimestampMs);
    const cutoffClause = `created_at < $${params.length}`;
    const statuses = [StatusString.PENDING, StatusString.ENQUEUED, StatusString.DELAYED].map((status) => {
      params.push(status);
      return `$${params.length}`;
    });
    // Unclaimed rows included: excluding them would leak pre-upgrade rows forever.
    const scope = this.#appNameFilter('application_name', this.appName, params);
    return `${cutoffClause} AND status NOT IN (${statuses.join(', ')}) AND ${scope}`;
  }

  /**
   * Delete one batch, returning the watermark to resume from, or undefined once the last one ran.
   * The delete is its own transaction; it re-checks the filter, so it needs no snapshot shared
   * with the select that bounds it.
   */
  async #garbageCollectBatch(
    cutoffEpochTimestampMs: number,
    batchSize: number,
    watermark: number,
  ): Promise<number | undefined> {
    // Borrowed rather than pool.query'd: that releases with the error, which discards the
    // connection on a deadlock, so the retry wrapping this would churn the pool per batch.
    const client = await this.#connect();
    try {
      // The batchSize-th oldest eligible row above the watermark bounds this range
      const stepParams: unknown[] = [];
      const stepScope = this.#gcFilter(cutoffEpochTimestampMs, stepParams);
      stepParams.push(watermark);
      const stepResult = await client.query<{ created_at: string }>(
        `SELECT created_at
           FROM "${this.schemaName}".workflow_status
          WHERE ${stepScope} AND created_at > $${stepParams.length}
          ORDER BY created_at
          LIMIT 1 OFFSET ${batchSize - 1}`,
        stepParams,
      );
      // created_at is a bigint, so node-postgres hands it back as a string.
      const step = stepResult.rows.length > 0 ? Number(stepResult.rows[0].created_at) : undefined;

      const deleteParams: unknown[] = [];
      let deleteScope = this.#gcFilter(cutoffEpochTimestampMs, deleteParams);
      if (step !== undefined) {
        // Inclusive upper bound: created_at ties may push a batch over batchSize, but never split across two.
        deleteParams.push(watermark, step);
        deleteScope = `${deleteScope} AND created_at > $${deleteParams.length - 1} AND created_at <= $${deleteParams.length}`;
      }
      // The final batch drops the watermark, so rows that appeared below it are still deleted.
      await client.query(`DELETE FROM "${this.schemaName}".workflow_status WHERE ${deleteScope}`, deleteParams);

      return step;
    } finally {
      // No error argument: a genuinely dead connection is still evicted by the pool's own check.
      client.release();
    }
  }

  // Conductor sends cleared retention thresholds as JSON null, so both params must be treated as nullish
  async garbageCollect(
    cutoffEpochTimestampMs?: number | null,
    rowsThreshold?: number | null,
    options: { batchSize?: number | null } = {},
  ): Promise<void> {
    const batchSize = options.batchSize === null ? undefined : (options.batchSize ?? DEFAULT_GC_BATCH_SIZE);
    // A NaN survives a bare `< 1` test and would only fail once it reached SQL, leaving GC half-applied.
    if (batchSize !== undefined && (!Number.isInteger(batchSize) || batchSize < 1)) {
      throw new DBOSError(`batchSize must be a positive integer, got ${batchSize}`);
    }

    if (rowsThreshold !== undefined && rowsThreshold !== null) {
      // Get the created_at timestamp of the rows_threshold newest row
      const params: unknown[] = [rowsThreshold - 1];
      const scope = this.#appNameFilter('application_name', this.appName, params);
      const result = await this.pool.query<{ created_at: number }>(
        `SELECT created_at
         FROM "${this.schemaName}".workflow_status
         WHERE ${scope}
         ORDER BY created_at DESC
         LIMIT 1 OFFSET $1`,
        params,
      );

      if (result.rows.length > 0) {
        const rowsBasedCutoff = result.rows[0].created_at;
        // Use the more restrictive cutoff (higher timestamp = more recent = more deletion)
        if (
          cutoffEpochTimestampMs === undefined ||
          cutoffEpochTimestampMs === null ||
          rowsBasedCutoff > cutoffEpochTimestampMs
        ) {
          cutoffEpochTimestampMs = rowsBasedCutoff;
        }
      }
    }

    if (cutoffEpochTimestampMs === undefined || cutoffEpochTimestampMs === null) {
      return;
    }

    // Narrowed to a constant so the closures below keep it.
    const cutoff = cutoffEpochTimestampMs;

    if (batchSize === undefined) {
      await retryOnSerializationError(async () => {
        const deleteParams: unknown[] = [];
        const deleteScope = this.#gcFilter(cutoff, deleteParams);
        const client = await this.#connect();
        try {
          await client.query(`DELETE FROM "${this.schemaName}".workflow_status WHERE ${deleteScope}`, deleteParams);
        } finally {
          client.release();
        }
      });
      return;
    }

    // Advance a created_at watermark, one committed transaction per batch, so a long
    // history neither deletes in one transaction nor rescans what it already deleted.
    let watermark = 0;
    for (;;) {
      const next = await retryOnSerializationError(() => this.#garbageCollectBatch(cutoff, batchSize, watermark));
      // Fewer than a full batch remained, so that delete took the rest.
      if (next === undefined) return;
      watermark = next;
    }
  }

  /**
   * IDs of this application's in-flight workflows created at or before the cutoff.
   * Claiming-scoped, so an upgrade still times out its own unclaimed workflows.
   */
  @dbRetry()
  async listTimedOutWorkflowIds(cutoffEpochTimestampMs: number): Promise<string[]> {
    const params: unknown[] = [
      cutoffEpochTimestampMs,
      StatusString.PENDING,
      StatusString.ENQUEUED,
      StatusString.DELAYED,
    ];
    const scope = this.#appNameFilter('application_name', this.appName, params);
    const { rows } = await this.pool.query<{ workflow_uuid: string }>(
      `SELECT workflow_uuid
       FROM "${this.schemaName}".workflow_status
       WHERE created_at <= $1
         AND status IN ($2, $3, $4)
         AND ${scope}`,
      params,
    );
    return rows.map((row) => row.workflow_uuid);
  }

  @dbRetry()
  async getMetrics(startTime: string, endTime: string, applicationName?: string[]): Promise<MetricData[]> {
    const startEpochMs = new Date(startTime).getTime();
    const endEpochMs = new Date(endTime).getTime();

    const metrics: MetricData[] = [];

    // Query workflow metrics
    const workflowParams: unknown[] = [startEpochMs, endEpochMs];
    const workflowScope = this.#observabilityFilter('application_name', applicationName, workflowParams);
    const workflowResult = await this.pool.query<{ name: string; count: string }>(
      `SELECT name, COUNT(workflow_uuid) as count
       FROM "${this.schemaName}".workflow_status
       WHERE created_at >= $1 AND created_at < $2 AND ${workflowScope}
       GROUP BY name`,
      workflowParams,
    );

    for (const row of workflowResult.rows) {
      metrics.push({
        metricType: 'workflow_count',
        metricName: row.name,
        value: Number(row.count),
      });
    }

    // Query step metrics
    const stepParams: unknown[] = [startEpochMs, endEpochMs];
    const stepScope = this.#observabilityFilter('application_name', applicationName, stepParams);
    const stepResult = await this.pool.query<{ function_name: string; count: string }>(
      `SELECT function_name, COUNT(*) as count
       FROM "${this.schemaName}".operation_outputs
       WHERE completed_at_epoch_ms >= $1 AND completed_at_epoch_ms < $2 AND ${stepScope}
       GROUP BY function_name`,
      stepParams,
    );

    for (const row of stepResult.rows) {
      metrics.push({
        metricType: 'step_count',
        metricName: row.function_name,
        value: Number(row.count),
      });
    }

    return metrics;
  }

  // ==================== Scheduling ====================

  async createSchedule(schedule: WorkflowScheduleInternal, client?: PoolClient): Promise<void> {
    const q = client ?? this.pool;
    const owner = await this.#resolveRowOwner(
      q,
      'workflow_schedules',
      'schedule_name',
      schedule.scheduleName,
      schedule.applicationName,
      'Schedule',
    );
    try {
      await q.query(
        `INSERT INTO "${this.schemaName}".workflow_schedules
         (schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name, application_name)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
        [
          schedule.scheduleId,
          schedule.scheduleName,
          schedule.workflowName,
          schedule.workflowClassName,
          schedule.schedule,
          schedule.status,
          schedule.context,
          schedule.lastFiredAt,
          schedule.automaticBackfill,
          schedule.cronTimezone,
          schedule.queueName,
          owner ?? null,
        ],
      );
    } catch (e) {
      if (e instanceof DatabaseError && e.code === '23505') {
        throw new Error(`Schedule '${schedule.scheduleName}' already exists`);
      }
      throw e;
    }
  }

  /**
   * List only schedules owned by these applications, plus unclaimed ones.
   * By default, only list this application's schedules.
   */
  async listSchedules(
    filters?: {
      status?: string | string[];
      workflowName?: string | string[];
      scheduleNamePrefix?: string | string[];
      applicationName?: string | string[];
    },
    client?: PoolClient,
  ): Promise<WorkflowScheduleInternal[]> {
    const q = client ?? this.pool;
    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    if (filters?.status) {
      const vals = Array.isArray(filters.status) ? filters.status : [filters.status];
      const placeholders = vals.map((v) => {
        params.push(v);
        return `$${paramIdx++}`;
      });
      conditions.push(`status IN (${placeholders.join(', ')})`);
    }
    if (filters?.workflowName) {
      const vals = Array.isArray(filters.workflowName) ? filters.workflowName : [filters.workflowName];
      const placeholders = vals.map((v) => {
        params.push(v);
        return `$${paramIdx++}`;
      });
      conditions.push(`workflow_name IN (${placeholders.join(', ')})`);
    }
    if (filters?.scheduleNamePrefix) {
      const prefixes = Array.isArray(filters.scheduleNamePrefix)
        ? filters.scheduleNamePrefix
        : [filters.scheduleNamePrefix];
      const likeClauses = prefixes.map((p) => {
        params.push(`${p}%`);
        return `schedule_name LIKE $${paramIdx++}`;
      });
      conditions.push(`(${likeClauses.join(' OR ')})`);
    }
    // Unset scopes to this application, as on every other observability query.
    conditions.push(this.#observabilityFilter('application_name', filters?.applicationName, params));
    paramIdx = params.length + 1;

    const where = conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : '';
    const result = await q.query(
      `SELECT ${SCHEDULE_COLUMNS}
       FROM "${this.schemaName}".workflow_schedules${where}
       ORDER BY schedule_name`,
      params,
    );

    return result.rows.map((row: workflow_schedules) => mapWorkflowSchedule(row));
  }

  async getSchedule(name: string, client?: PoolClient): Promise<WorkflowScheduleInternal | null> {
    const q = client ?? this.pool;
    const result = await q.query(
      `SELECT ${SCHEDULE_COLUMNS}
       FROM "${this.schemaName}".workflow_schedules
       WHERE schedule_name = $1`,
      [name],
    );
    if (result.rows.length === 0) return null;
    return mapWorkflowSchedule(result.rows[0] as workflow_schedules);
  }

  async deleteSchedule(name: string, client?: PoolClient): Promise<void> {
    const q = client ?? this.pool;
    await q.query(`DELETE FROM "${this.schemaName}".workflow_schedules WHERE schedule_name = $1`, [name]);
  }

  async setScheduleStatus(name: string, status: string, client?: PoolClient): Promise<void> {
    const q = client ?? this.pool;
    await q.query(`UPDATE "${this.schemaName}".workflow_schedules SET status = $1 WHERE schedule_name = $2`, [
      status,
      name,
    ]);
  }

  async updateSchedule(name: string, updates: WorkflowScheduleUpdate, client?: PoolClient): Promise<void> {
    const q = client ?? this.pool;

    // Only update the definition fields the caller provided, leaving runtime state (schedule_id, status, last_fired_at) untouched.
    const columns: [keyof WorkflowScheduleUpdate, string][] = [
      ['schedule', 'schedule'],
      ['context', 'context'],
      ['automaticBackfill', 'automatic_backfill'],
      ['cronTimezone', 'cron_timezone'],
      ['queueName', 'queue_name'],
    ];
    const setClauses: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;
    for (const [key, column] of columns) {
      if (key in updates) {
        setClauses.push(`${column} = $${paramIdx++}`);
        params.push(updates[key] ?? null);
      }
    }

    if (setClauses.length === 0) {
      // Nothing to change, but still surface a missing schedule as an error.
      const existing = await q.query(`SELECT 1 FROM "${this.schemaName}".workflow_schedules WHERE schedule_name = $1`, [
        name,
      ]);
      if (existing.rows.length === 0) {
        throw new DBOSError(`Schedule '${name}' not found`);
      }
      return;
    }

    params.push(name);
    const result = await q.query(
      `UPDATE "${this.schemaName}".workflow_schedules SET ${setClauses.join(', ')} WHERE schedule_name = $${paramIdx}`,
      params,
    );
    if (result.rowCount === 0) {
      throw new DBOSError(`Schedule '${name}' not found`);
    }
  }

  async updateLastFiredAt(name: string, lastFiredAt: string): Promise<void> {
    await this.pool.query(
      `UPDATE "${this.schemaName}".workflow_schedules SET last_fired_at = $1 WHERE schedule_name = $2`,
      [lastFiredAt, name],
    );
  }

  async applySchedules(schedules: WorkflowScheduleInternal[]): Promise<void> {
    const client = await this.#connect();
    try {
      await client.query('BEGIN');
      for (const sched of schedules) {
        const owner = await this.#resolveRowOwner(
          client,
          'workflow_schedules',
          'schedule_name',
          sched.scheduleName,
          sched.applicationName,
          'Schedule',
        );
        // Upsert on schedule_name; on conflict, preserve schedule_id and runtime state (status, last_fired_at) and update only the declared definition fields, so an unchanged re-apply is a no-op.
        await client.query(
          `INSERT INTO "${this.schemaName}".workflow_schedules
           (schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name, application_name)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           ON CONFLICT (schedule_name) DO UPDATE SET
             workflow_name = EXCLUDED.workflow_name,
             workflow_class_name = EXCLUDED.workflow_class_name,
             schedule = EXCLUDED.schedule,
             context = EXCLUDED.context,
             automatic_backfill = EXCLUDED.automatic_backfill,
             cron_timezone = EXCLUDED.cron_timezone,
             queue_name = EXCLUDED.queue_name,
             -- Claim only an unclaimed row, so a registration landing between the check above and this write keeps the name it took.
             application_name = COALESCE("${this.schemaName}".workflow_schedules.application_name, EXCLUDED.application_name)`,
          [
            sched.scheduleId,
            sched.scheduleName,
            sched.workflowName,
            sched.workflowClassName,
            sched.schedule,
            sched.status,
            sched.context,
            sched.lastFiredAt,
            sched.automaticBackfill,
            sched.cronTimezone,
            sched.queueName,
            owner ?? null,
          ],
        );
        // Read back, since the guard above is silent about why it declined to claim.
        await this.#resolveRowOwner(
          client,
          'workflow_schedules',
          'schedule_name',
          sched.scheduleName,
          sched.applicationName,
          'Schedule',
        );
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  // ==================== Application Versions ====================
  /**
   * Register this version, claiming the row if nobody owns it yet so a pinned version
   * does not stay unclaimed. A peer's name is a collision, which is why this throws.
   */
  async createApplicationVersion(versionName: string, applicationName?: string): Promise<void> {
    const owner = applicationName ?? this.appName;
    const client = await this.#connect();
    try {
      await client.query('BEGIN');
      // Claim a pre-upgrade row in place, so the version is not recreated or retimed.
      const claimed = await client.query(
        `UPDATE "${this.schemaName}".application_versions
         SET application_name = $1
         WHERE version_name = $2 AND application_name IS NULL`,
        [owner ?? null, versionName],
      );
      if ((claimed.rowCount ?? 0) === 0) {
        // Targetless DO NOTHING: names no arbiter, so it survives version_name's uniqueness being dropped while still absorbing a concurrent registrar.
        await client.query(
          `INSERT INTO "${this.schemaName}".application_versions (version_id, version_name, application_name)
           VALUES ($1, $2, $3)
           ON CONFLICT DO NOTHING`,
          [randomUUID(), versionName, owner ?? null],
        );
      }
      // Read back, since the writes above are silent about why they declined to claim.
      await this.#resolveRowOwner(
        client,
        'application_versions',
        'version_name',
        versionName,
        owner,
        'Application version',
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  /**
   * Promote a version to latest. Promoting a peer's is a collision, not a retiming;
   * promotion claims an unclaimed row, which would otherwise be every peer's latest.
   */
  async updateApplicationVersionTimestamp(
    versionName: string,
    newTimestamp: number,
    applicationName?: string,
  ): Promise<void> {
    const owner = applicationName ?? this.appName;
    const client = await this.#connect();
    try {
      await client.query('BEGIN');
      const resolved = await this.#resolveRowOwner(
        client,
        'application_versions',
        'version_name',
        versionName,
        owner,
        'Application version',
      );
      // Scoped to the row this writer resolved to: once version_name is no longer globally unique, a bare name match would retime every peer's version.
      const scope =
        resolved === undefined ? 'application_name IS NULL' : '(application_name = $3 OR application_name IS NULL)';
      const params: unknown[] = [newTimestamp, versionName];
      if (resolved !== undefined) params.push(resolved);
      await client.query(
        `UPDATE "${this.schemaName}".application_versions
         SET version_timestamp = $1, application_name = ${resolved === undefined ? 'application_name' : '$3'}
         WHERE version_name = $2 AND ${scope}`,
        params,
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async listApplicationVersions(): Promise<VersionInfo[]> {
    const params: unknown[] = [];
    const scope = this.#appNameFilter('application_name', this.appName, params);
    const { rows } = await this.pool.query<application_versions>(
      `SELECT version_id, version_name, version_timestamp, created_at, application_name
       FROM "${this.schemaName}".application_versions
       WHERE ${scope}
       ORDER BY version_timestamp DESC`,
      params,
    );
    return rows.map(mapVersionInfo);
  }

  /**
   * The latest version registered by an application. Defaults to this handle's, so a
   * caller acting for another one — firing its schedule — must name it.
   */
  async getLatestApplicationVersion(applicationName?: string): Promise<VersionInfo> {
    const owner = applicationName ?? this.appName;
    const params: unknown[] = [];
    const scope = this.#appNameFilter('application_name', owner, params);
    const { rows } = await this.pool.query<application_versions>(
      `SELECT version_id, version_name, version_timestamp, created_at, application_name
       FROM "${this.schemaName}".application_versions
       WHERE ${scope}
       ORDER BY version_timestamp DESC
       LIMIT 1`,
      params,
    );
    if (rows.length === 0) {
      throw new DBOSInitializationError('No application versions found');
    }
    return mapVersionInfo(rows[0]);
  }

  // ==================== Queues ====================

  async getQueue(name: string): Promise<QueueRecord | null> {
    const { rows } = await this.pool.query<queues>(
      `SELECT ${QUEUE_COLUMNS}
         FROM "${this.schemaName}".queues
        WHERE name = $1`,
      [name],
    );
    return rows.length === 0 ? null : queueRecordFromRow(rows[0]);
  }

  /**
   * List only queues owned by these applications, plus unclaimed ones.
   * By default, only list this application's queues.
   */
  async listQueues(applicationName?: string | string[]): Promise<QueueRecord[]> {
    const params: unknown[] = [];
    const scope = this.#observabilityFilter('application_name', applicationName, params);
    const { rows } = await this.pool.query<queues>(
      `SELECT ${QUEUE_COLUMNS}
         FROM "${this.schemaName}".queues
        WHERE ${scope}`,
      params,
    );
    return rows.map(queueRecordFromRow);
  }

  async deleteQueue(name: string): Promise<void> {
    await this.pool.query(`DELETE FROM "${this.schemaName}".queues WHERE name = $1`, [name]);
  }

  async updateQueue(name: string, fields: QueueRecordUpdate): Promise<void> {
    const setClauses: string[] = [];
    const params: unknown[] = [];
    let idx = 1;
    for (const [key, value] of Object.entries(fields) as [keyof QueueRecordUpdate, unknown][]) {
      const column = QUEUE_COLUMN_BY_FIELD[key];
      setClauses.push(`"${column}" = $${idx++}`);
      params.push(value);
    }
    if (setClauses.length === 0) return;
    setClauses.push(`"updated_at" = $${idx++}`);
    params.push(Date.now());
    params.push(name);
    await this.pool.query(
      `UPDATE "${this.schemaName}".queues SET ${setClauses.join(', ')} WHERE name = $${idx}`,
      params,
    );
  }

  /** Returns true iff this call inserted a new row (i.e. the queue did not
   * previously exist). False if the row already existed, regardless of
   * whether it was updated. */
  async upsertQueue(record: QueueRecord, updateExisting: boolean): Promise<boolean> {
    const now = Date.now();
    const onConflict = updateExisting
      ? `ON CONFLICT (name) DO UPDATE SET
          concurrency = EXCLUDED.concurrency,
          worker_concurrency = EXCLUDED.worker_concurrency,
          rate_limit_max = EXCLUDED.rate_limit_max,
          rate_limit_period_sec = EXCLUDED.rate_limit_period_sec,
          priority_enabled = EXCLUDED.priority_enabled,
          partition_queue = EXCLUDED.partition_queue,
          partition_concurrency = EXCLUDED.partition_concurrency,
          partition_worker_concurrency = EXCLUDED.partition_worker_concurrency,
          partition_rate_limit_max = EXCLUDED.partition_rate_limit_max,
          partition_rate_limit_period_sec = EXCLUDED.partition_rate_limit_period_sec,
          polling_interval_sec = EXCLUDED.polling_interval_sec,
          updated_at = EXCLUDED.updated_at,
          -- Claim only an unclaimed row, so a registration landing between the check above and this write keeps the name it just took.
          application_name = COALESCE("${this.schemaName}".queues.application_name, EXCLUDED.application_name)`
      : `ON CONFLICT (name) DO NOTHING`;
    const owner = record.applicationName ?? this.appName;
    const client = await this.#connect();
    try {
      await client.query('BEGIN');
      const existed = await client.query<{ name: string }>(
        `SELECT name FROM "${this.schemaName}".queues WHERE name = $1`,
        [record.name],
      );
      // A name collision is a conflict in every mode: the name is the queue's address.
      const resolvedOwner = await this.#resolveRowOwner(client, 'queues', 'name', record.name, owner, 'Queue');
      await client.query(
        `INSERT INTO "${this.schemaName}".queues
          (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
           priority_enabled, partition_queue, partition_concurrency, partition_worker_concurrency,
           partition_rate_limit_max, partition_rate_limit_period_sec,
           polling_interval_sec, updated_at, application_name)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
         ${onConflict}`,
        [
          record.name,
          record.concurrency,
          record.workerConcurrency,
          record.rateLimitMax,
          record.rateLimitPeriodSec,
          record.priorityEnabled,
          record.partitionQueue,
          record.partitionConcurrency,
          record.partitionWorkerConcurrency,
          record.partitionRateLimitMax,
          record.partitionRateLimitPeriodSec,
          record.pollingIntervalSec,
          now,
          resolvedOwner ?? null,
        ],
      );
      // Read back, since the guard above is silent about why it declined to claim.
      await this.#resolveRowOwner(client, 'queues', 'name', record.name, owner, 'Queue');
      await client.query('COMMIT');
      return existed.rowCount === 0;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  // ==================== Application Rename ====================

  /**
   * Rows a rename moves: an application's own, unclaimed ones, or both. Unlike
   * the claiming scope, unclaimed rows are not implied; they move only when asked.
   */
  #renameSource(oldName: string | undefined, adoptUnclaimedRows: boolean, params: unknown[]): string {
    const clauses: string[] = [];
    if (oldName !== undefined) {
      params.push(oldName);
      clauses.push(`application_name = $${params.length}`);
    }
    if (adoptUnclaimedRows) {
      clauses.push('application_name IS NULL');
    }
    // Callers validate that at least one source is named.
    return `(${clauses.join(' OR ')})`;
  }

  /**
   * Re-own a table's rows in half-open key ranges, so a long history neither moves in
   * one transaction nor rescans what it already moved; a re-run resumes.
   */
  async #renameRowsInBatches(
    table: string,
    keyColumn: string,
    oldName: string | undefined,
    newName: string,
    batchSize: number | undefined,
    adoptUnclaimedRows: boolean,
  ): Promise<number> {
    if (batchSize === undefined) {
      const params: unknown[] = [newName];
      const predicate = this.#renameSource(oldName, adoptUnclaimedRows, params);
      const res = await this.pool.query(
        `UPDATE "${this.schemaName}".${table} SET application_name = $1 WHERE ${predicate}`,
        params,
      );
      return res.rowCount ?? 0;
    }

    let total = 0;
    // Ranges, not LIMIT: a LIMIT repages every row already moved, and an IN list of keys plans as a whole-table hash join.
    let watermark: string | undefined = undefined;
    for (;;) {
      const boundParams: unknown[] = [];
      const predicate = this.#renameSource(oldName, adoptUnclaimedRows, boundParams);
      let scope = predicate;
      if (watermark !== undefined) {
        boundParams.push(watermark);
        scope = `${predicate} AND ${keyColumn} > $${boundParams.length}`;
      }
      // The batchSize-th matching key bounds this range; distinct, so a key's rows are never split across batches.
      const upperResult = await this.pool.query<Record<string, string>>(
        `SELECT DISTINCT ${keyColumn} FROM "${this.schemaName}".${table}
         WHERE ${scope} ORDER BY ${keyColumn} LIMIT 1 OFFSET ${batchSize - 1}`,
        boundParams,
      );
      const upper: string | undefined = upperResult.rows[0]?.[keyColumn];

      const updateParams: unknown[] = [newName];
      const updatePredicate = this.#renameSource(oldName, adoptUnclaimedRows, updateParams);
      let batch = updatePredicate;
      if (upper !== undefined) {
        if (watermark !== undefined) {
          updateParams.push(watermark);
          batch = `${batch} AND ${keyColumn} > $${updateParams.length}`;
        }
        updateParams.push(upper);
        batch = `${batch} AND ${keyColumn} <= $${updateParams.length}`;
      }
      // The final batch drops the watermark, so rows that appeared below it still move.
      const res = await this.pool.query(
        `UPDATE "${this.schemaName}".${table} SET application_name = $1 WHERE ${batch}`,
        updateParams,
      );
      total += res.rowCount ?? 0;

      // Fewer than a full batch remained, so that update took the rest.
      if (upper === undefined) return total;
      watermark = upper;
    }
  }

  /**
   * Give `newName` ownership of rows `oldName` holds, of unclaimed rows, or of both.
   * The renamed application must be stopped, or its dequeues race this.
   */
  async renameApplication(
    oldName: string | undefined,
    newName: string,
    options: { batchSize?: number | null; adoptUnclaimedRows?: boolean } = {},
  ): Promise<ApplicationRowCounts> {
    const adoptUnclaimedRows = options.adoptUnclaimedRows ?? false;
    const batchSize = options.batchSize === null ? undefined : (options.batchSize ?? DEFAULT_RENAME_BATCH_SIZE);

    if (oldName !== undefined && oldName === '') {
      throw new DBOSError("The application's previous name cannot be empty.");
    }
    if (oldName === undefined && !adoptUnclaimedRows) {
      throw new DBOSError('Nothing to re-own: name the application to rename, adopt unclaimed rows, or both.');
    }
    if (oldName === newName) {
      throw new DBOSError(`Application '${newName}' already holds that name; nothing to rename.`);
    }
    // A NaN survives a bare `< 1` test and would only fail once it reached SQL, leaving the rename half-applied.
    if (batchSize !== undefined && (!Number.isInteger(batchSize) || batchSize < 1)) {
      throw new DBOSError(`batchSize must be a positive integer, got ${batchSize}`);
    }

    // Never a merge: queue, schedule, and version names are globally unique whatever their owner, so this cannot collide.
    const client = await this.#connect();
    let queues: number, schedules: number, versions: number, inFlight: number;
    try {
      await client.query('BEGIN');
      const move = async (table: string, statuses?: string[]): Promise<number> => {
        const params: unknown[] = [newName];
        let where = this.#renameSource(oldName, adoptUnclaimedRows, params);
        if (statuses !== undefined) {
          params.push(statuses);
          where = `${where} AND status = ANY($${params.length})`;
        }
        const res = await client.query(
          `UPDATE "${this.schemaName}".${table} SET application_name = $1 WHERE ${where}`,
          params,
        );
        return res.rowCount ?? 0;
      };

      // A half-owned application dequeues work whose version row it can no longer see, so these move together.
      queues = await move('queues');
      schedules = await move('workflow_schedules');
      versions = await move('application_versions');
      inFlight = await move('workflow_status', [StatusString.PENDING, StatusString.ENQUEUED, StatusString.DELAYED]);
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    // Only terminal rows are left to match, and they scope observability and GC alone, so they may lag behind the commit above.
    const terminal = await this.#renameRowsInBatches(
      'workflow_status',
      'workflow_uuid',
      oldName,
      newName,
      batchSize,
      adoptUnclaimedRows,
    );
    const steps = await this.#renameRowsInBatches(
      'operation_outputs',
      'workflow_uuid',
      oldName,
      newName,
      batchSize,
      adoptUnclaimedRows,
    );

    return { queues, schedules, versions, workflows: inFlight + terminal, steps };
  }

  // ==================== Internal ====================
  private async insertWorkflowStatus(
    client: ClientBase,
    initStatus: WorkflowStatusInternal,
    ownerXid: string | null,
  ): Promise<InsertWorkflowResult> {
    try {
      const { rows } = await client.query<InsertWorkflowResult>(
        `INSERT INTO "${this.schemaName}".workflow_status (
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
          recovery_attempts,
          workflow_timeout_ms,
          workflow_deadline_epoch_ms,
          inputs,
          deduplication_id,
          priority,
          queue_partition_key,
          forked_from,
          parent_workflow_id,
          serialization,
          owner_xid,
          delay_until_epoch_ms,
          attributes,
          schedule_name,
          debounce_deadline_epoch_ms,
          is_debounced,
          application_name
        ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)
        ON CONFLICT (workflow_uuid)
          DO UPDATE SET
            updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint,
            executor_id = CASE
              WHEN EXCLUDED.status != '${StatusString.ENQUEUED}' AND EXCLUDED.status != '${StatusString.DELAYED}'
              THEN EXCLUDED.executor_id
              ELSE workflow_status.executor_id
            END
          RETURNING status, name, class_name, config_name, queue_name, workflow_deadline_epoch_ms, executor_id, owner_xid, serialization`,
        [
          initStatus.workflowUUID,
          initStatus.status,
          initStatus.workflowName,
          // For cross-language compatibility, these variables MUST be NULL in the database when not set
          initStatus.workflowClassName === '' ? null : initStatus.workflowClassName,
          initStatus.workflowConfigName === '' ? null : initStatus.workflowConfigName,
          initStatus.queueName ?? null,
          initStatus.authenticatedUser,
          initStatus.assumedRole,
          JSON.stringify(initStatus.authenticatedRoles),
          JSON.stringify(initStatus.request),
          initStatus.executorId,
          initStatus.applicationVersion ?? null,
          initStatus.applicationID,
          initStatus.status === StatusString.ENQUEUED || initStatus.status === StatusString.DELAYED ? 0 : 1,
          initStatus.timeoutMS ?? null,
          initStatus.deadlineEpochMS ?? null,
          initStatus.input ?? null,
          initStatus.deduplicationID ?? null,
          initStatus.priority,
          initStatus.queuePartitionKey ?? null,
          initStatus.forkedFrom ?? null,
          initStatus.parentWorkflowID ?? null,
          initStatus.serialization,
          ownerXid,
          initStatus.delayUntilEpochMS ?? null,
          initStatus.attributes ? JSON.stringify(initStatus.attributes) : null,
          initStatus.scheduleName ?? null,
          initStatus.debounceDeadlineEpochMS ?? null,
          initStatus.isDebounced ?? false,
          // Absent from the conflict update: a re-enqueue must not re-own a claimed row.
          initStatus.applicationName ?? null,
        ],
      );
      if (rows.length === 0) {
        throw new Error(`Attempt to insert workflow ${initStatus.workflowUUID} failed`);
      }
      const ret = rows[0];
      ret.class_name = ret.class_name ?? '';
      ret.config_name = ret.config_name ?? '';
      initStatus.serialization = ret.serialization;
      return ret;
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

  private async getWorkflowStatusValue(client: PoolClient | Pool, workflowID: string): Promise<string | undefined> {
    const { rows } = await client.query<{ status: string }>(
      `SELECT status FROM "${this.schemaName}".workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    return rows.length === 0 ? undefined : rows[0].status;
  }

  private async updateWorkflowStatus(
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
        executorId?: string;
        resetNameTo?: string;
        setCompletedAt?: boolean;
        clearCompletedAt?: boolean;
      };
      where?: {
        status?: (typeof StatusString)[keyof typeof StatusString];
        notStatus?: (typeof StatusString)[keyof typeof StatusString];
      };
      throwOnFailure?: boolean;
    } = {},
  ): Promise<number> {
    // Use SQL now() so updated_at and completed_at (when set together) are
    // computed in the same statement against the same clock.
    const nowMsExpr = `(EXTRACT(EPOCH FROM now()) * 1000)::bigint`;
    let setClause = `SET status=$2, updated_at=${nowMsExpr}`;
    let whereClause = `WHERE workflow_uuid=$1`;
    const args: (string | number | undefined)[] = [workflowID, status];

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

    if (update.executorId !== undefined) {
      const param = args.push(update.executorId ?? undefined);
      setClause += `, executor_id=$${param}`;
    }

    if (update.resetNameTo !== undefined) {
      const param = args.push(update.resetNameTo ?? undefined);
      setClause += `, name=$${param}`;
    }

    if (update.setCompletedAt) {
      setClause += `, completed_at=${nowMsExpr}`;
    } else if (update.clearCompletedAt) {
      setClause += `, completed_at = NULL`;
    }

    const where = options.where ?? {};
    if (where.status) {
      const param = args.push(where.status);
      whereClause += ` AND status=$${param}`;
    }
    if (where.notStatus) {
      const param = args.push(where.notStatus);
      whereClause += ` AND status!=$${param}`;
    }

    const result = await client.query<workflow_status>(
      `UPDATE "${this.schemaName}".workflow_status ${setClause} ${whereClause}`,
      args,
    );

    const throwOnFailure = options.throwOnFailure ?? true;
    if (throwOnFailure && result.rowCount !== 1) {
      throw new DBOSWorkflowConflictError(`Attempt to record transition of nonexistent workflow ${workflowID}`);
    }
    return result.rowCount ?? 0;
  }

  private async recordOperationResultInternal(
    client: PoolClient,
    workflowID: string,
    functionID: number,
    functionName: string,
    checkConflict: boolean,
    startTimeEpochMs: number,
    endTimeEpochMs: number,
    options: {
      childWorkflowID?: string | null;
      output?: string | null;
      error?: string | null;
      serialization?: string | null;
    } = {},
  ): Promise<void> {
    try {
      const out = await client.query<operation_outputs>(
        `INSERT INTO ${this.schemaName}.operation_outputs
         (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization, application_name)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (workflow_uuid, function_id) DO UPDATE
         SET completed_at_epoch_ms = operation_outputs.completed_at_epoch_ms
         RETURNING completed_at_epoch_ms;`,
        [
          workflowID,
          functionID,
          options.output ?? null,
          options.error ?? null,
          functionName,
          options.childWorkflowID ?? null,
          startTimeEpochMs,
          endTimeEpochMs,
          options.serialization ?? null,
          // Mirrors the parent: only the running application records its steps.
          this.appName ?? null,
        ],
      );
      if (
        checkConflict &&
        (out?.rowCount ?? 0) > 0 &&
        Number(out?.rows?.[0]?.completed_at_epoch_ms) !== endTimeEpochMs
      ) {
        DBOSExecutor.globalInstance?.logger.warn(
          `Step output for ${workflowID}(${functionID}):${functionName} already recorded`,
        );
        throw new DBOSWorkflowConflictError(workflowID);
      }
      if (Number(out?.rows?.[0]?.completed_at_epoch_ms) === endTimeEpochMs) {
        // Winning the checkpoint proves this executor is advancing the workflow:
        // claim the executor_id marker (skips the write when already ours).
        await client.query(
          `UPDATE "${this.schemaName}".workflow_status
             SET executor_id = $1
           WHERE workflow_uuid = $2 AND executor_id IS DISTINCT FROM $1`,
          [globalParams.executorID, workflowID],
        );
      }
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

  async #getOperationResultAndThrowIfCancelled(
    client: PoolClient,
    workflowID: string,
    functionID: number,
  ): Promise<SystemDatabaseStoredResult | undefined> {
    await this.#checkIfCanceled(client, workflowID);

    const { rows } = await client.query<operation_outputs>(
      `SELECT output, error, child_workflow_id, function_name, serialization
       FROM "${this.schemaName}".operation_outputs
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
        // Return serialization so recv/getEvent replay deserializes with the stored format, not the default.
        serialization: rows[0].serialization,
      };
    }
  }

  async #runAndRecordResult(
    client: PoolClient,
    functionName: string,
    workflowID: string,
    functionID: number,
    func: () => Promise<string | null | undefined>,
  ): Promise<string | null | undefined> {
    const startTime = Date.now();
    const result = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
    if (result !== undefined) {
      if (result.functionName !== functionName) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, functionName, result.functionName!);
      }
      return result.output;
    }
    const output = await func();
    await this.recordOperationResultInternal(
      client,
      workflowID,
      functionID,
      functionName,
      true,
      startTime,
      Date.now(),
      {
        output,
      },
    );
    return output;
  }

  async #checkIfCanceled(client: PoolClient | Pool, workflowID: string): Promise<void> {
    const statusValue = await this.getWorkflowStatusValue(client, workflowID);
    if (statusValue === StatusString.CANCELLED) {
      throw new DBOSWorkflowCancelledError(workflowID);
    }
  }

  // Durably records (or, on recovery, reads back) the wakeup deadline for a sleep or
  // timeout so it survives recovery. Returns the absolute end time in epoch ms; the
  // caller is responsible for actually waiting until then. Throws if the workflow has
  // been cancelled.
  // For an actual sleep, completed_at is the wake deadline so the step's duration reflects the
  // sleep; a timeout marker records zero duration since its deadline may never be reached.
  async #durableSleep(
    workflowID: string,
    functionID: number,
    durationMS: number,
    recordCompletionAtDeadline: boolean = false,
  ): Promise<number> {
    const startTimeMs = Date.now();
    // Round once so the deadline stays integral: completed_at_epoch_ms is BIGINT and rejects fractional values.
    const endTimeMs = startTimeMs + Math.ceil(durationMS);

    const client = await this.#connect();
    try {
      const res = await this.#getOperationResultAndThrowIfCancelled(client, workflowID, functionID);
      if (res) {
        if (res.functionName !== DBOS_FUNCNAME_SLEEP) {
          throw new DBOSUnexpectedStepError(workflowID, functionID, DBOS_FUNCNAME_SLEEP, res.functionName!);
        }
        return JSON.parse(res.output!) as number;
      }
      await this.recordOperationResultInternal(
        client,
        workflowID,
        functionID,
        DBOS_FUNCNAME_SLEEP,
        false,
        startTimeMs,
        recordCompletionAtDeadline ? endTimeMs : startTimeMs,
        {
          output: DBOSPortableJSON.stringify(endTimeMs),
          serialization: DBOSPortableJSON.name(),
        },
      );
      return endTimeMs;
    } finally {
      client.release();
    }
  }

  /* BACKGROUND PROCESSES */
  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  reconnectTimeout: NodeJS.Timeout | null = null;
  #notificationsStopped: boolean = false;

  // Disown the client and release it once; a late socket error must not re-enter this path and release again.
  #retireNotificationsClient(client: PoolClient) {
    if (this.notificationsClient === client) {
      this.notificationsClient = null;
    }
    client.removeAllListeners();
    // Cover the release() call itself, which tears the connection down and can surface a socket error.
    client.on('error', () => {});
    try {
      client.release(true);
    } catch (e) {
      this.logger.warn(`Error releasing notifications client: ${String(e)}`);
    }
    // release() re-attached pg's idle listener, which would forward this dead client's error on to the
    // pool. A caller's pool may have no 'error' listener at all, so this client's death has to stay ours.
    client.removeAllListeners('error');
    client.on('error', (e: Error) => this.logger.warn(`Error on retired notifications client: ${e}`));
  }

  // Shutdown can begin during any await in the setup below; releasing the client instead of carrying on
  // keeps pool.end() from waiting on a connection that will never be published.
  #abandonIfStopped(client: PoolClient): boolean {
    if (!this.#notificationsStopped) {
      return false;
    }
    this.#retireNotificationsClient(client);
    return true;
  }

  async #listenForNotifications() {
    const connect = async () => {
      const reconnect = () => {
        if (this.reconnectTimeout || this.#notificationsStopped) {
          return;
        }
        this.reconnectTimeout = setTimeout(async () => {
          this.reconnectTimeout = null;
          await connect();
        }, 1000);
      };

      let acquired: PoolClient | null = null;
      try {
        const client = await this.#connect();
        acquired = client;
        if (this.#abandonIfStopped(client)) return;

        // Catch errors during setup
        const setup: { error: Error | null } = { error: null };
        const onSetupError = (err: Error) => {
          setup.error = err;
        };
        client.on('error', onSetupError);

        await client.query(`LISTEN ${DBOS_NOTIFICATIONS_CHANNEL};`);
        await client.query(`LISTEN ${DBOS_WORKFLOW_EVENTS_CHANNEL};`);
        await client.query(`LISTEN ${DBOS_STREAMS_CHANNEL};`);

        // The self-test's NOTIFY needs a second client, which can queue forever on an ending pool.
        if (this.#abandonIfStopped(client)) return;

        // Self-test: verify LISTEN actually works by sending a NOTIFY and checking it arrives.
        // If a transaction-mode pooler (e.g. PgBouncer pool_mode=transaction) is in the path,
        // LISTEN succeeds but the subscription is silently lost when the backend is released.
        let selfTestReceived = false;
        const onSelfTest = (msg: Notification) => {
          if (msg.channel === 'dbos_notifications_channel' && msg.payload === 'dbos_listen_selftest') {
            selfTestReceived = true;
          }
        };
        client.on('notification', onSelfTest);
        await this.pool.query("NOTIFY dbos_notifications_channel, 'dbos_listen_selftest'");
        for (let i = 0; i < 30 && !selfTestReceived && !setup.error && !this.#notificationsStopped; i++) {
          await new Promise((r) => setTimeout(r, 100));
        }
        client.removeListener('notification', onSelfTest);

        // Both checked before the warning below, so an abandoned self-test is not reported as a pooler problem.
        if (this.#abandonIfStopped(client)) return;
        if (setup.error) {
          throw setup.error;
        }

        if (!selfTestReceived) {
          this.logger.warn(
            'LISTEN/NOTIFY self-test failed: notification was not received within 3 seconds. ' +
              'This typically means the connection is going through a transaction-mode pooler ' +
              '(e.g. PgBouncer with pool_mode=transaction), which silently breaks LISTEN/NOTIFY. ' +
              'Workflow notifications will fall back to polling, which may increase latency.',
          );
        }

        const handler = (msg: Notification) => {
          if (!this.shouldUseDBNotifications) return;
          if (msg.channel === DBOS_NOTIFICATIONS_CHANNEL && msg.payload) {
            this.notificationsMap.callCallbacks(msg.payload);
          } else if (msg.channel === DBOS_WORKFLOW_EVENTS_CHANNEL && msg.payload) {
            this.workflowEventsMap.callCallbacks(msg.payload);
          } else if (msg.channel === DBOS_STREAMS_CHANNEL && msg.payload) {
            this.streamsMap.callCallbacks(msg.payload);
          }
        };

        client.removeListener('error', onSetupError);
        client.on('notification', handler);
        client.on('error', (err: Error) => {
          this.logger.warn(`Error in notifications client: ${err}`);
          this.#retireNotificationsClient(client);
          reconnect();
        });
        this.notificationsClient = client;
      } catch (error) {
        this.logger.warn(`Error in notifications listener: ${String(error)}`);
        if (acquired) {
          this.#retireNotificationsClient(acquired);
        }
        reconnect();
      }
    };

    await connect();
  }
}
