import { DBOSExecutor } from './dbos-executor';
import { DBOS } from './dbos';
import {
  DEBUG_TRIGGER_WORKFLOW_QUEUE_START,
  DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES,
  debugTriggerPoint,
} from './debugpoint';
import type { QueueRecord, SystemDatabase } from './system_database';
import type { GlobalLogger } from './telemetry/logs';
import { globalParams, INTERNAL_QUEUE_NAME } from './utils';

/**
 * Log a single queue's name and its set parameters. Unset parameters are
 * omitted, matching `Queue: <name> (concurrency=…, worker_concurrency=…,
 * limit=N/Ts, priority, partitioned)`.
 */
export function logQueue(logger: GlobalLogger, q: WorkflowQueue): void {
  const opts: string[] = [];
  if (q.concurrency !== undefined) {
    // On a partitioned queue the queue-wide scope is worth naming explicitly.
    opts.push(`${hasPartitionLimits(q) ? 'global_concurrency' : 'concurrency'}=${q.concurrency}`);
  }
  if (q.workerConcurrency !== undefined) opts.push(`worker_concurrency=${q.workerConcurrency}`);
  if (q.rateLimit !== undefined) opts.push(`limit=${q.rateLimit.limitPerPeriod}/${q.rateLimit.periodSec}s`);
  if (q.partitionConcurrency !== undefined) opts.push(`partition_concurrency=${q.partitionConcurrency}`);
  if (q.partitionWorkerConcurrency !== undefined) {
    opts.push(`partition_worker_concurrency=${q.partitionWorkerConcurrency}`);
  }
  if (q.partitionRateLimit !== undefined) {
    opts.push(`partition_limit=${q.partitionRateLimit.limitPerPeriod}/${q.partitionRateLimit.periodSec}s`);
  }
  if (q.priorityEnabled) opts.push('priority');
  if (q.partitionQueue) opts.push('partitioned');
  const optsStr = opts.length > 0 ? ` (${opts.join(', ')})` : '';
  logger.info(`Queue: ${q.name}${optsStr}`);
}

/**
 * Limit the maximum number of functions started from a `WorkflowQueue`
 *   per given time period.
 * If the limit is 5 and the period is 10, no more than 5 functions can be
 *   started per 10 seconds.
 */
export interface QueueRateLimit {
  /** Number of queue dispateches per `periodSec` */
  limitPerPeriod: number;
  /** Period of time during which `limitPerPeriod` queued workflows may be dispatched */
  periodSec: number;
}

/**
 * Limit the number of concurrent workflows running for a queue.
 *
 * Queue-wide limits bound the queue as a whole. Setting any `partition` limit
 * additionally partitions the queue, so every enqueue must supply a partition
 * key, and that limit is then enforced separately within each partition.
 */
export interface QueueParameters {
  /** If defined, this limits the number of running workflows for a single DBOS process */
  workerConcurrency?: number;
  /** If defined, this limits the number of running workflows globally in the app */
  globalConcurrency?: number;
  /** If set, this limits the rate at which queued workflows are started */
  rateLimit?: QueueRateLimit;
  /** If defined, this limits the number of running workflows globally within each partition */
  partitionConcurrency?: number;
  /** If defined, this limits the number of running workflows on a single DBOS process within each partition */
  partitionWorkerConcurrency?: number;
  /** If set, this limits the rate at which queued workflows are started within each partition */
  partitionRateLimit?: QueueRateLimit;
  /** Base (minimum) polling interval in ms for this queue's dispatch loop (default 1000) */
  minPollingIntervalMs?: number;
  /** @deprecated Use `globalConcurrency`. */
  concurrency?: number;
  /** @deprecated Priority is always enabled. */
  priorityEnabled?: boolean;
  /** @deprecated Use the partition limits, any of which partitions the queue. */
  partitionQueue?: boolean;
}

/**
 * Behavior of `DBOS.registerQueue` / `DBOSClient.registerQueue` when a queue
 * with the same name already has a row in the `queues` table.
 *
 * - `update_if_latest_version`: overwrite the existing row only when the
 *   running application version is the latest registered version. Older
 *   versions in a rolling deploy will not overwrite a newer config.
 * - `always_update`: always overwrite the existing row.
 * - `never_update`: leave the existing row unchanged. The returned queue
 *   reflects the persisted config, not the supplied parameters.
 */
export type QueueConflictResolution = 'update_if_latest_version' | 'always_update' | 'never_update';

export interface RegisterQueueOptions extends QueueParameters {
  /** How to behave when a queue with the same name already exists. */
  onConflict?: QueueConflictResolution;
}

/** A queue's limits, each resolved to the scope it is enforced at. */
export interface ResolvedQueueLimits {
  globalConcurrency?: number;
  workerConcurrency?: number;
  rateLimit?: QueueRateLimit;
  partitionConcurrency?: number;
  partitionWorkerConcurrency?: number;
  partitionRateLimit?: QueueRateLimit;
}

/** The per-partition limits, any of which partitions a queue. */
type PartitionLimits = Pick<
  QueueParameters,
  'partitionConcurrency' | 'partitionWorkerConcurrency' | 'partitionRateLimit'
>;

/** True when any per-partition limit is set, which is what partitions a queue. */
function hasPartitionLimits(limits: PartitionLimits): boolean {
  return (
    limits.partitionConcurrency !== undefined ||
    limits.partitionWorkerConcurrency !== undefined ||
    limits.partitionRateLimit !== undefined
  );
}

/**
 * True for the deprecated `partitionQueue` mode, under which `concurrency`,
 * `workerConcurrency`, and `rateLimit` all apply per partition.
 */
function isLegacyPartitioned(q: WorkflowQueue): boolean {
  return q.partitionQueue && !hasPartitionLimits(q);
}

/** Resolve every limit on a queue to the scope it is actually enforced at. */
export function resolveQueueLimits(q: WorkflowQueue): ResolvedQueueLimits {
  if (isLegacyPartitioned(q)) {
    return {
      partitionConcurrency: q.concurrency,
      partitionWorkerConcurrency: q.workerConcurrency,
      partitionRateLimit: q.rateLimit,
    };
  }
  return {
    globalConcurrency: q.concurrency,
    workerConcurrency: q.workerConcurrency,
    rateLimit: q.rateLimit,
    partitionConcurrency: q.partitionConcurrency,
    partitionWorkerConcurrency: q.partitionWorkerConcurrency,
    partitionRateLimit: q.partitionRateLimit,
  };
}

/**
 * Room left under this worker's queue-wide concurrency limit, given how many of
 * its workflows are already running or claimed.
 */
function workerBudget(limits: ResolvedQueueLimits, running: number): number {
  if (limits.partitionWorkerConcurrency !== undefined && limits.partitionWorkerConcurrency <= 0) {
    // Zero per partition pauses this worker; the batched sweep enforces no per-partition worker limit of its own.
    return 0;
  }
  if (limits.workerConcurrency === undefined) {
    // A non-zero per-partition worker limit is enforced per partition instead.
    return Infinity;
  }
  return Math.max(0, limits.workerConcurrency - running);
}

/** 40001 serialization_failure or 55P03 lock_not_available: a peer is claiming the same rows. */
function isContentionError(e: unknown): boolean {
  const code = (e as NodeJS.ErrnoException).code;
  return code === '40001' || code === '55P03';
}

/** Fisher-Yates copy, so a sweep visits partitions in a different order each poll. */
function shuffled<T>(items: T[]): T[] {
  const result = [...items];
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

/**
 * Per-instance association of a client-bound queue to its `SystemDatabase`.
 * Stored off-class because any class member — including TS `private` — gives
 * the class a nominal brand, so the type-only members below all live as
 * module-level helpers to keep `WorkflowQueue` structurally compatible across
 * separate compiled copies of this package.
 */
const clientSystemDatabases = new WeakMap<WorkflowQueue, SystemDatabase>();

function requireDatabaseBacked(q: WorkflowQueue): void {
  if (!q.databaseBacked) {
    throw new Error(
      `Cannot configure queue ${q.name}: dynamic configuration is only supported for queues registered via DBOS.registerQueue.`,
    );
  }
}

function sysDBFor(q: WorkflowQueue): SystemDatabase {
  const clientDb = clientSystemDatabases.get(q);
  if (clientDb) return clientDb;
  const exec = DBOSExecutor.globalInstance;
  if (!exec) {
    throw new Error(`Cannot access system database for queue ${q.name}: DBOS has not been launched.`);
  }
  return exec.systemDatabase;
}

/** Reject a write that would silently re-scope the other limits on a legacy queue. */
function requireNotLegacyPartitioned(q: WorkflowQueue, field: string): void {
  if (isLegacyPartitioned(q)) {
    throw new Error(
      `Cannot set ${field} on queue ${q.name}: it is registered with the deprecated partitionQueue option, ` +
        `under which concurrency, workerConcurrency, and rateLimit apply per partition. ` +
        `Re-register the queue with the partition limits instead.`,
    );
  }
}

/** Validate a new queue-wide concurrency against the queue's other cached limits. */
function checkConcurrencyBounds(q: WorkflowQueue, value: number | undefined): void {
  if (value === undefined) return;
  if (q.workerConcurrency !== undefined && q.workerConcurrency > value) {
    throw new Error('workerConcurrency must be less than or equal to concurrency');
  }
  if (q.partitionConcurrency !== undefined && q.partitionConcurrency > value) {
    throw new Error('partitionConcurrency must be less than or equal to globalConcurrency');
  }
  if (q.partitionWorkerConcurrency !== undefined && q.partitionWorkerConcurrency > value) {
    throw new Error('partitionWorkerConcurrency must be less than or equal to concurrency');
  }
}

/** Whether the queue is still partitioned once these limits take these values. */
function partitionedAfter(q: WorkflowQueue, overrides: PartitionLimits): boolean {
  return hasPartitionLimits({
    partitionConcurrency: q.partitionConcurrency,
    partitionWorkerConcurrency: q.partitionWorkerConcurrency,
    partitionRateLimit: q.partitionRateLimit,
    ...overrides,
  });
}

function rateLimitFromRecord(max: number | null, periodSec: number | null): QueueRateLimit | undefined {
  return max !== null && periodSec !== null ? { limitPerPeriod: max, periodSec } : undefined;
}

/** Copy a persisted row's configuration onto a queue instance. */
function applyRecord(q: WorkflowQueue, record: QueueRecord): void {
  q.concurrency = record.concurrency ?? undefined;
  q.workerConcurrency = record.workerConcurrency ?? undefined;
  q.rateLimit = rateLimitFromRecord(record.rateLimitMax, record.rateLimitPeriodSec);
  q.priorityEnabled = record.priorityEnabled;
  q.partitionConcurrency = record.partitionConcurrency ?? undefined;
  q.partitionWorkerConcurrency = record.partitionWorkerConcurrency ?? undefined;
  q.partitionRateLimit = rateLimitFromRecord(record.partitionRateLimitMax, record.partitionRateLimitPeriodSec);
  // Partitioning is inferred from the limits, so a row whose flag disagrees with them heals on read.
  q.partitionQueue = record.partitionQueue || hasPartitionLimits(q);
  q.minPollingIntervalMs = record.pollingIntervalSec * 1000;
  q.applicationName = record.applicationName;
}

/**
 * Re-read the queue's row from the database and update the cached fields on
 * `q` in place. No-op for in-memory queues. Throws if the row has been
 * deleted.
 */
async function refreshFromDb(q: WorkflowQueue): Promise<void> {
  if (!q.databaseBacked) return;
  const record = await sysDBFor(q).getQueue(q.name);
  if (record === null) {
    throw new Error(`Queue '${q.name}' was not found in the database.`);
  }
  applyRecord(q, record);
}

/**
 * Settings structure for a named workflow queue.
 * Workflow queues limit the rate and concurrency at which DBOS executes workflows.
 * Queue policies apply to workflows started by `DBOS.startWorkflow`,
 *   `DBOS.withWorkflowQueue`, etc.
 */
export class WorkflowQueue {
  readonly name: string;
  /**
   * Last-known cached values. May be stale for database-backed queues if
   * another process has modified the row. Use getters instead.
   */
  concurrency?: number;
  rateLimit?: QueueRateLimit;
  workerConcurrency?: number;
  priorityEnabled: boolean = false;
  partitionQueue: boolean = false;
  partitionConcurrency?: number;
  partitionWorkerConcurrency?: number;
  partitionRateLimit?: QueueRateLimit;
  minPollingIntervalMs?: number;
  /** Owner from the queues table; undefined for in-memory and unclaimed queues. */
  applicationName?: string;

  /**
   * When true, this queue's configuration is persisted in the `queues` system
   * table and may be mutated at runtime via the `setX` methods. When false,
   * the queue's configuration is fixed at construction and lives only in
   * process memory.
   */
  readonly databaseBacked: boolean = false;

  /**
   * True when configuration reads/writes target a `DBOSClient`-supplied
   * SystemDatabase rather than the global executor's. The actual handle is
   * kept off this class's public type — see the module-level WeakMap below —
   * so that `WorkflowQueue` does not transitively depend on `SystemDatabase`.
   */
  readonly clientBound: boolean = false;

  constructor(name: string);

  /**
   *
   * @param name - Name to give the `WorkflowQueue`, accepted by `DBOS.startWorkflow`
   * @param queueParameters - Policy for limiting workflow initiation rate and execution concurrency
   */
  constructor(name: string, queueParameters: QueueParameters);

  constructor(name: string, arg2?: QueueParameters | number, rateLimit?: QueueRateLimit) {
    this.name = name;

    if (DBOS.isInitialized()) {
      DBOS.logger.warn(
        `In-memory workflow queue '${name}' was created after DBOS initialization and will not be picked up by the queue dispatcher. ` +
          `Use DBOS.registerQueue to register a database-backed queue at runtime.`,
      );
    }

    let params: QueueParameters;
    if (typeof arg2 === 'object' && arg2 !== null) {
      params = arg2;
    } else {
      params = { concurrency: arg2, rateLimit };
    }
    WorkflowQueue.validateQueueParams(params);

    this.concurrency = params.globalConcurrency ?? params.concurrency;
    this.rateLimit = params.rateLimit;
    this.workerConcurrency = params.workerConcurrency;
    this.priorityEnabled = params.priorityEnabled ?? false;
    this.partitionConcurrency = params.partitionConcurrency;
    this.partitionWorkerConcurrency = params.partitionWorkerConcurrency;
    this.partitionRateLimit = params.partitionRateLimit;
    // Partitioning is inferred from any per-partition limit; the deprecated flag tracks it.
    this.partitionQueue = (params.partitionQueue ?? false) || hasPartitionLimits(params);
    this.minPollingIntervalMs = params.minPollingIntervalMs;

    if (wfQueueRunner.wfQueuesByName.has(name)) {
      throw Error(`Workflow Queue '${name}' defined multiple times`);
    }
    wfQueueRunner.wfQueuesByName.set(name, this);
  }

  /** Throws if any combination of queue parameters is invalid. */
  static validateQueueParams(params: QueueParameters): void {
    const {
      concurrency,
      globalConcurrency,
      workerConcurrency,
      rateLimit,
      partitionConcurrency,
      partitionWorkerConcurrency,
      partitionRateLimit,
      partitionQueue,
      minPollingIntervalMs,
    } = params;
    if (concurrency !== undefined && globalConcurrency !== undefined) {
      throw new Error('concurrency is deprecated in favor of globalConcurrency; set only one of them');
    }
    if (partitionQueue && hasPartitionLimits(params)) {
      throw new Error('partitionQueue is deprecated in favor of the partition limits; set only one of them');
    }
    if (partitionQueue && globalConcurrency !== undefined) {
      throw new Error(
        'partitionQueue applies every limit per partition, so it cannot be combined with globalConcurrency; use partitionConcurrency instead',
      );
    }
    if (partitionConcurrency !== undefined && partitionConcurrency < 1) {
      throw new Error('partitionConcurrency must be at least 1');
    }
    if (partitionWorkerConcurrency !== undefined && partitionWorkerConcurrency < 1) {
      throw new Error('partitionWorkerConcurrency must be at least 1');
    }
    if (
      partitionRateLimit !== undefined &&
      (partitionRateLimit.limitPerPeriod === undefined || partitionRateLimit.periodSec === undefined)
    ) {
      throw new Error('partitionRateLimit must specify both limitPerPeriod and periodSec');
    }
    if (
      partitionWorkerConcurrency !== undefined &&
      partitionConcurrency !== undefined &&
      partitionWorkerConcurrency > partitionConcurrency
    ) {
      throw new Error('partitionConcurrency must be greater than or equal to partitionWorkerConcurrency');
    }
    if (
      partitionWorkerConcurrency !== undefined &&
      workerConcurrency !== undefined &&
      partitionWorkerConcurrency > workerConcurrency
    ) {
      throw new Error('workerConcurrency must be greater than or equal to partitionWorkerConcurrency');
    }
    // Under the deprecated partitionQueue mode concurrency is itself a per-partition limit, so these compare like with like.
    const queueConcurrency = globalConcurrency ?? concurrency;
    if (workerConcurrency !== undefined && queueConcurrency !== undefined && workerConcurrency > queueConcurrency) {
      throw new Error('concurrency must be greater than or equal to workerConcurrency');
    }
    if (
      partitionConcurrency !== undefined &&
      queueConcurrency !== undefined &&
      partitionConcurrency > queueConcurrency
    ) {
      throw new Error('globalConcurrency must be greater than or equal to partitionConcurrency');
    }
    if (
      partitionWorkerConcurrency !== undefined &&
      queueConcurrency !== undefined &&
      partitionWorkerConcurrency > queueConcurrency
    ) {
      throw new Error('concurrency must be greater than or equal to partitionWorkerConcurrency');
    }
    if (minPollingIntervalMs !== undefined && minPollingIntervalMs <= 0) {
      throw new Error('minPollingIntervalMs must be positive');
    }
    if (rateLimit !== undefined && (rateLimit.limitPerPeriod === undefined || rateLimit.periodSec === undefined)) {
      throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
    }
  }

  /** Build a persistable record from user-supplied registration parameters. */
  static recordFromParams(name: string, params: QueueParameters): QueueRecord {
    return {
      name,
      concurrency: params.globalConcurrency ?? params.concurrency ?? null,
      workerConcurrency: params.workerConcurrency ?? null,
      rateLimitMax: params.rateLimit ? params.rateLimit.limitPerPeriod : null,
      rateLimitPeriodSec: params.rateLimit ? params.rateLimit.periodSec : null,
      priorityEnabled: params.priorityEnabled ?? false,
      // Any per-partition limit implies partitioning, whichever mode was used.
      partitionQueue: (params.partitionQueue ?? false) || hasPartitionLimits(params),
      partitionConcurrency: params.partitionConcurrency ?? null,
      partitionWorkerConcurrency: params.partitionWorkerConcurrency ?? null,
      partitionRateLimitMax: params.partitionRateLimit ? params.partitionRateLimit.limitPerPeriod : null,
      partitionRateLimitPeriodSec: params.partitionRateLimit ? params.partitionRateLimit.periodSec : null,
      pollingIntervalSec: (params.minPollingIntervalMs ?? 1000) / 1000,
    };
  }

  /**
   * Construct a database-backed queue from a persisted record. Bypasses the
   * legacy constructor so the instance is not added to the global registry —
   * the queues table is the source of truth.
   * @internal
   */
  static _fromRecord(record: QueueRecord, clientSystemDatabase?: SystemDatabase): WorkflowQueue {
    // Allocate without invoking the constructor (which would auto-register
    // in `wfQueuesByName`) and strip `readonly` so we can set the fields here.
    const q = Object.create(WorkflowQueue.prototype) as { -readonly [K in keyof WorkflowQueue]: WorkflowQueue[K] };
    q.name = record.name;
    q.databaseBacked = true;
    q.clientBound = clientSystemDatabase !== undefined;
    applyRecord(q as WorkflowQueue, record);
    if (clientSystemDatabase !== undefined) {
      clientSystemDatabases.set(q as WorkflowQueue, clientSystemDatabase);
    }
    return q as WorkflowQueue;
  }

  /** @deprecated Use `setGlobalConcurrency`. */
  async setConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    // Refresh so the cross-field checks see the limits currently stored in the database.
    await refreshFromDb(this);
    checkConcurrencyBounds(this, value);
    await sysDBFor(this).updateQueue(this.name, { concurrency: value ?? null });
    this.concurrency = value;
  }

  async setGlobalConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    await refreshFromDb(this);
    requireNotLegacyPartitioned(this, 'globalConcurrency');
    checkConcurrencyBounds(this, value);
    await sysDBFor(this).updateQueue(this.name, { concurrency: value ?? null });
    this.concurrency = value;
  }

  async setWorkerConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    await refreshFromDb(this);
    if (value !== undefined) {
      if (this.concurrency !== undefined && value > this.concurrency) {
        throw new Error('workerConcurrency must be less than or equal to concurrency');
      }
      if (this.partitionWorkerConcurrency !== undefined && this.partitionWorkerConcurrency > value) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to workerConcurrency');
      }
    }
    await sysDBFor(this).updateQueue(this.name, { workerConcurrency: value ?? null });
    this.workerConcurrency = value;
  }

  async setRateLimit(value: QueueRateLimit | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && (value.limitPerPeriod === undefined || value.periodSec === undefined)) {
      throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
    }
    await sysDBFor(this).updateQueue(this.name, {
      rateLimitMax: value ? value.limitPerPeriod : null,
      rateLimitPeriodSec: value ? value.periodSec : null,
    });
    this.rateLimit = value;
  }

  async setPartitionConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && value < 1) {
      throw new Error('partitionConcurrency must be at least 1');
    }
    await refreshFromDb(this);
    requireNotLegacyPartitioned(this, 'partitionConcurrency');
    if (value !== undefined) {
      if (this.concurrency !== undefined && value > this.concurrency) {
        throw new Error('partitionConcurrency must be less than or equal to globalConcurrency');
      }
      if (this.partitionWorkerConcurrency !== undefined && this.partitionWorkerConcurrency > value) {
        throw new Error('partitionConcurrency must be greater than or equal to partitionWorkerConcurrency');
      }
    }
    // Partitioning is inferred from the limits, so the deprecated flag follows them.
    const partitioned = partitionedAfter(this, { partitionConcurrency: value });
    await sysDBFor(this).updateQueue(this.name, {
      partitionConcurrency: value ?? null,
      partitionQueue: partitioned,
    });
    this.partitionConcurrency = value;
    this.partitionQueue = partitioned;
  }

  async setPartitionWorkerConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && value < 1) {
      throw new Error('partitionWorkerConcurrency must be at least 1');
    }
    await refreshFromDb(this);
    requireNotLegacyPartitioned(this, 'partitionWorkerConcurrency');
    if (value !== undefined) {
      if (this.partitionConcurrency !== undefined && value > this.partitionConcurrency) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to partitionConcurrency');
      }
      if (this.workerConcurrency !== undefined && value > this.workerConcurrency) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to workerConcurrency');
      }
      if (this.concurrency !== undefined && value > this.concurrency) {
        throw new Error('partitionWorkerConcurrency must be less than or equal to concurrency');
      }
    }
    const partitioned = partitionedAfter(this, { partitionWorkerConcurrency: value });
    await sysDBFor(this).updateQueue(this.name, {
      partitionWorkerConcurrency: value ?? null,
      partitionQueue: partitioned,
    });
    this.partitionWorkerConcurrency = value;
    this.partitionQueue = partitioned;
  }

  async setPartitionRateLimit(value: QueueRateLimit | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && (value.limitPerPeriod === undefined || value.periodSec === undefined)) {
      throw new Error('partitionRateLimit must specify both limitPerPeriod and periodSec');
    }
    await refreshFromDb(this);
    requireNotLegacyPartitioned(this, 'partitionRateLimit');
    const partitioned = partitionedAfter(this, { partitionRateLimit: value });
    await sysDBFor(this).updateQueue(this.name, {
      partitionRateLimitMax: value ? value.limitPerPeriod : null,
      partitionRateLimitPeriodSec: value ? value.periodSec : null,
      partitionQueue: partitioned,
    });
    this.partitionRateLimit = value;
    this.partitionQueue = partitioned;
  }

  /** @deprecated Priority is always enabled. */
  async setPriorityEnabled(value: boolean): Promise<void> {
    requireDatabaseBacked(this);
    await sysDBFor(this).updateQueue(this.name, { priorityEnabled: value });
    this.priorityEnabled = value;
  }

  /** @deprecated Use the partition limit setters. */
  async setPartitionQueue(value: boolean): Promise<void> {
    requireDatabaseBacked(this);
    // Refresh so the check below sees the partition limits currently stored in the database.
    await refreshFromDb(this);
    if (hasPartitionLimits(this)) {
      throw new Error(
        `Cannot set partitionQueue on queue ${this.name}: it is partitioned by its partition limits. Clear those instead.`,
      );
    }
    await sysDBFor(this).updateQueue(this.name, { partitionQueue: value });
    this.partitionQueue = value;
  }

  async setMinPollingIntervalMs(value: number): Promise<void> {
    requireDatabaseBacked(this);
    if (value <= 0) {
      throw new Error('minPollingIntervalMs must be positive');
    }
    await sysDBFor(this).updateQueue(this.name, { pollingIntervalSec: value / 1000 });
    this.minPollingIntervalMs = value;
  }

  /** @deprecated Use `getGlobalConcurrency`. */
  async getConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.concurrency;
  }

  async getGlobalConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return resolveQueueLimits(this).globalConcurrency;
  }

  async getWorkerConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return resolveQueueLimits(this).workerConcurrency;
  }

  async getRateLimit(): Promise<QueueRateLimit | undefined> {
    await refreshFromDb(this);
    return resolveQueueLimits(this).rateLimit;
  }

  async getPartitionConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return resolveQueueLimits(this).partitionConcurrency;
  }

  async getPartitionWorkerConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return resolveQueueLimits(this).partitionWorkerConcurrency;
  }

  async getPartitionRateLimit(): Promise<QueueRateLimit | undefined> {
    await refreshFromDb(this);
    return resolveQueueLimits(this).partitionRateLimit;
  }

  /** @deprecated Priority is always enabled. */
  async getPriorityEnabled(): Promise<boolean> {
    await refreshFromDb(this);
    return this.priorityEnabled;
  }

  /** @deprecated Use the partition limit getters. */
  async getPartitionQueue(): Promise<boolean> {
    await refreshFromDb(this);
    return this.partitionQueue;
  }

  async getMinPollingIntervalMs(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.minPollingIntervalMs;
  }
}

/** Per-queue runtime scheduling state tracked by the shared dispatcher. */
interface QueueRuntimeState {
  /** Latest config snapshot; replaced in place when a DB-backed row is refreshed. */
  queue: WorkflowQueue;
  /** Current polling interval in ms after contention backoff / scaleback. */
  currentPollingMs: number;
  /** Epoch ms at which this queue should next be polled. */
  nextPollAt: number;
}

class WFQueueRunner {
  readonly wfQueuesByName: Map<string, WorkflowQueue> = new Map();

  /**
   * Queues fed by this process's own pollers (e.g. a Kafka consumer). Always dispatched,
   * regardless of any listenQueues filter, so this process executes what it enqueues.
   */
  readonly pollerQueueNames: Set<string> = new Set();

  private isRunning: boolean = false;
  private abortController?: AbortController;
  private listenQueueNames: Set<string> | null = null;
  /** Per-queue scheduling state, keyed by queue name. */
  private readonly states: Map<string, QueueRuntimeState> = new Map();
  /** Names already warned about colliding with an in-memory queue (warn-once). */
  private readonly conflictWarned: Set<string> = new Set();

  private static readonly defaultMinPollingIntervalMs: number = 1000;
  private static readonly defaultMaxPollingIntervalMs: number = 120000;
  private static readonly reconcileIntervalMs: number = 1000;
  private static readonly transitionIntervalMs: number = 1000;
  private readonly backoffFactor: number = 2.0;
  private readonly scalebackFactor: number = 0.9;
  private readonly jitterMin: number = 0.95;
  private readonly jitterMax: number = 1.05;

  stop() {
    if (!this.isRunning) return;
    this.isRunning = false;
    this.abortController?.abort();
  }

  clearRegistrations() {
    this.wfQueuesByName.clear();
    this.pollerQueueNames.clear();
  }

  async dispatchLoop(
    exec: DBOSExecutor,
    listenQueuesArg: (WorkflowQueue | string)[] | null,
    maxConcurrentQueueDispatches: number = 3,
  ): Promise<void> {
    this.isRunning = true;
    this.states.clear();
    this.conflictWarned.clear();
    this.listenQueueNames = listenQueuesArg
      ? new Set(listenQueuesArg.map((entry) => (typeof entry === 'string' ? entry : entry.name)))
      : null;
    this.abortController = new AbortController();

    const startNow = Date.now();

    // The internal queue is process-private and bypasses the listenQueues filter.
    const internal = this.wfQueuesByName.get(INTERNAL_QUEUE_NAME);
    if (internal) this.ensureState(internal, startNow);

    // Unmatched string entries are deferred to refreshDbQueues as DB-backed queues.
    for (const q of this.resolveInMemoryQueues(listenQueuesArg)) {
      this.ensureState(q, startNow);
    }

    // Add pre-launch DB-backed queues now so an immediate enqueue can't race the first reconcile.
    await this.refreshDbQueues(exec, startNow);

    // Log everything we're now dispatching for, before the loop starts.
    this.logRunningQueues(exec);

    // One loop drives global maintenance; queue polls run in a bounded set of independent lanes.
    await this.schedulerLoop(exec, startNow, maxConcurrentQueueDispatches);
  }

  /** Resolve the listenQueues argument to the set of in-memory queues to dispatch for. */
  private resolveInMemoryQueues(listenQueuesArg: (WorkflowQueue | string)[] | null): WorkflowQueue[] {
    if (listenQueuesArg === null) {
      return Array.from(this.wfQueuesByName.values()).filter((q) => q.name !== INTERNAL_QUEUE_NAME);
    }
    const result: WorkflowQueue[] = [];
    for (const entry of listenQueuesArg) {
      if (typeof entry === 'string') {
        const q = this.wfQueuesByName.get(entry);
        if (q) result.push(q);
      } else {
        result.push(entry);
      }
    }
    // Poller-fed queues are always dispatched: this process enqueues onto them, so under a
    // listenQueues filter their workflows would otherwise sit ENQUEUED forever.
    for (const name of this.pollerQueueNames) {
      const q = this.wfQueuesByName.get(name);
      if (q && !result.some((r) => r.name === name)) result.push(q);
    }
    return result;
  }

  /** Begin tracking a queue if it isn't already, scheduling its first poll one interval out. */
  private ensureState(queue: WorkflowQueue, now: number): void {
    if (this.states.has(queue.name)) return;
    const interval = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
    this.states.set(queue.name, { queue, currentPollingMs: interval, nextPollAt: now + interval });
  }

  /** Reconcile DB-backed queues against the queues table in one query: refresh, add, or drop them. */
  private async refreshDbQueues(exec: DBOSExecutor, now: number): Promise<void> {
    let records: QueueRecord[];
    try {
      records = await exec.systemDatabase.listQueues(exec.systemDatabase.appName);
    } catch (e) {
      exec.logger.warn(`Error listing database-backed queues: ${(e as Error).message}`);
      return;
    }

    const present = new Set<string>();
    for (const record of records) {
      if (record.name === INTERNAL_QUEUE_NAME) continue;
      if (this.wfQueuesByName.has(record.name)) {
        if (!this.conflictWarned.has(record.name)) {
          this.conflictWarned.add(record.name);
          exec.logger.warn(
            `Database-backed queue '${record.name}' has the same name as an in-memory queue. ` +
              `The in-memory queue's configuration is being used; the database-backed queue is ignored. ` +
              `Rename one of them to resolve the conflict.`,
          );
        }
        continue;
      }
      if (
        this.listenQueueNames !== null &&
        !this.listenQueueNames.has(record.name) &&
        !this.pollerQueueNames.has(record.name)
      ) {
        continue;
      }
      present.add(record.name);
      const existing = this.states.get(record.name);
      if (existing) {
        // Refresh config in place, preserving this queue's polling/backoff state.
        existing.queue = WorkflowQueue._fromRecord(record);
      } else {
        this.ensureState(WorkflowQueue._fromRecord(record), now);
      }
    }

    // A database-backed queue whose row is gone stops being dispatched.
    for (const [name, state] of this.states) {
      if (!state.queue.databaseBacked) continue;
      if (!present.has(name)) {
        exec.logger.info(`Queue '${name}' has been deleted from the database; no longer dispatching it.`);
        this.states.delete(name);
      }
    }
  }

  /** Log every queue this process will dispatch for, once at startup after discovery. */
  private logRunningQueues(exec: DBOSExecutor): void {
    const names = Array.from(this.states.keys()).filter((n) => n !== INTERNAL_QUEUE_NAME);
    exec.logger.info(`Listening to ${names.length} queues:`);
    for (const name of names) {
      logQueue(exec.logger, this.states.get(name)!.queue);
    }
  }

  /** Reconcile queues and schedule due polls across a bounded number of independent lanes. */
  private async schedulerLoop(
    exec: DBOSExecutor,
    startNow: number,
    maxConcurrentQueueDispatches: number,
  ): Promise<void> {
    const signal = this.abortController!.signal;
    const inFlightPolls = new Map<string, Promise<void>>();
    let wakePending = false;
    let wakeScheduler: (() => void) | undefined;
    const wake = () => {
      if (wakeScheduler) {
        wakeScheduler();
      } else {
        wakePending = true;
      }
    };

    const waitForWakeOrTimeout = async (ms: number): Promise<void> => {
      if (signal.aborted) return;
      if (wakePending) {
        wakePending = false;
        return;
      }
      await new Promise<void>((resolve) => {
        const finish = () => {
          clearTimeout(timer);
          signal.removeEventListener('abort', onAbort);
          if (wakeScheduler === finish) wakeScheduler = undefined;
          resolve();
        };
        const onAbort = () => finish();
        wakeScheduler = finish;
        signal.addEventListener('abort', onAbort, { once: true });
        const timer = setTimeout(finish, ms);
      });
    };

    // Discovery already ran during setup; defer the next reconcile a full interval.
    let lastReconcileAt = startNow;
    // Global op: run on a fixed cadence, not once per wake (destaggered wakeups would push it to ~N/sec).
    let lastTransitionAt = 0;

    while (this.isRunning) {
      const now = Date.now();

      // Reconcile DB-backed queues with a single query, independent of queue count.
      if (now - lastReconcileAt >= WFQueueRunner.reconcileIntervalMs) {
        await this.refreshDbQueues(exec, now);
        lastReconcileAt = now;
      }

      // Transition delayed workflows at most once per interval — it is global, so one call covers every queue.
      if (now - lastTransitionAt >= WFQueueRunner.transitionIntervalMs) {
        try {
          await exec.systemDatabase.transitionDelayedWorkflows();
        } catch (e) {
          exec.logger.warn(`Error transitioning delayed workflows: ${(e as Error).message}`);
        }
        lastTransitionAt = now;
      }

      this.scheduleDueQueues(exec, now, maxConcurrentQueueDispatches, inFlightPolls, wake);

      if (!this.isRunning) break;

      // Sleep until global maintenance or an idle queue's next poll.
      let nextWakeAt = Math.min(
        lastReconcileAt + WFQueueRunner.reconcileIntervalMs,
        lastTransitionAt + WFQueueRunner.transitionIntervalMs,
      );
      // Skip queue times while all lanes are busy: a completing poll wakes us, so folding a due-but-unlaned queue in would spin at 0ms.
      if (inFlightPolls.size < maxConcurrentQueueDispatches) {
        for (const state of this.states.values()) {
          if (!inFlightPolls.has(state.queue.name) && state.nextPollAt < nextWakeAt) nextWakeAt = state.nextPollAt;
        }
      }
      const sleepMs = Math.max(0, nextWakeAt - Date.now());
      await waitForWakeOrTimeout(sleepMs);
    }

    await Promise.allSettled(Array.from(inFlightPolls.values()));
  }

  /** Start due queue polls up to the lane limit, in nextPollAt order so the longest-overdue queue goes first. */
  private scheduleDueQueues(
    exec: DBOSExecutor,
    now: number,
    maxConcurrentQueueDispatches: number,
    inFlightPolls: Map<string, Promise<void>>,
    wake: () => void,
  ): void {
    if (!this.isRunning) return;
    // Earliest nextPollAt first: a queue passed over while the lanes were full keeps its older
    // nextPollAt, so it outranks freshly-scheduled queues on the next pass and cannot be starved.
    const due = Array.from(this.states.values())
      .filter((state) => now >= state.nextPollAt && !inFlightPolls.has(state.queue.name))
      .sort((a, b) => a.nextPollAt - b.nextPollAt);
    for (const state of due) {
      if (inFlightPolls.size >= maxConcurrentQueueDispatches) break;
      inFlightPolls.set(state.queue.name, this.runQueuePoll(exec, state, inFlightPolls, wake));
    }
  }

  /** Run one queue's poll while reserving that queue's lane until its backoff state is updated. */
  private async runQueuePoll(
    exec: DBOSExecutor,
    state: QueueRuntimeState,
    inFlightPolls: Map<string, Promise<void>>,
    wake: () => void,
  ): Promise<void> {
    const queueName = state.queue.name;
    // pollQueue swallows DB errors, so a rejection here is abnormal: back off instead of scaling back toward the minimum interval.
    let contentionDetected = true;
    try {
      contentionDetected = await this.pollQueue(exec, state.queue);
    } catch (e) {
      exec.logger.warn(`Unexpected error polling queue ${queueName}: ${(e as Error).message}`);
    } finally {
      this.adjustInterval(exec, state, contentionDetected);
      inFlightPolls.delete(queueName);
      wake();
    }
  }

  /** Poll one queue once, starting ready workflows; returns true if DB contention was detected. */
  private async pollQueue(exec: DBOSExecutor, queue: WorkflowQueue): Promise<boolean> {
    let contentionDetected = false;
    // Helper function that starts dequeued workflows
    const dispatch = async (wfids: string[]) => {
      if (wfids.length > 0) {
        await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_QUEUE_START);
      }
      await exec.dispatchDequeuedWorkflows(wfids);
    };
    const limits = resolveQueueLimits(queue);
    const sysdb = exec.systemDatabase;
    // Dequeue workflows for this queue, either in one batched sweep across partitions or one partition at a time.
    try {
      if (!queue.partitionQueue) {
        const wfids = await sysdb.findAndMarkStartableWorkflows(
          queue,
          exec.executorID,
          globalParams.appVersion,
          undefined,
          sysdb.countRunningWorkflowsForQueue(queue.name),
        );
        await dispatch(wfids);
      } else if (
        limits.partitionConcurrency === 1 &&
        limits.globalConcurrency === undefined &&
        limits.rateLimit === undefined &&
        limits.partitionRateLimit === undefined
      ) {
        // Batched path: one transaction claims every partition's head (see findAndMarkStartablePartitionedWorkflows).
        const maxTasks = workerBudget(limits, sysdb.countRunningWorkflowsForQueue(queue.name));
        if (maxTasks > 0) {
          const wfids = await sysdb.findAndMarkStartablePartitionedWorkflows(
            queue,
            exec.executorID,
            globalParams.appVersion,
            maxTasks,
          );
          await dispatch(wfids);
        }
      } else {
        // Every other partitioned config sweeps one partition at a time, in random order to prevent starvation.
        const partitionKeys = shuffled(await sysdb.getQueuePartitions(queue.name));
        // Snapshot once: dispatch is asynchronous, so re-reading would count this sweep's own claims twice.
        const running = sysdb.countRunningWorkflowsForQueue(queue.name);
        let claimed = 0;
        for (const partitionKey of partitionKeys) {
          if (workerBudget(limits, running + claimed) <= 0) break;
          let partitionWfids: string[];
          try {
            partitionWfids = await sysdb.findAndMarkStartableWorkflows(
              queue,
              exec.executorID,
              globalParams.appVersion,
              partitionKey,
              running + claimed,
              sysdb.countRunningWorkflowsForPartition(queue.name, partitionKey),
            );
          } catch (e) {
            // Lock held or claim raced by another worker: skip just this partition, no queue-wide backoff.
            if (isContentionError(e)) continue;
            throw e;
          }
          claimed += partitionWfids.length;
          await dispatch(partitionWfids);
          await debugTriggerPoint(DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES);
        }
      }
    } catch (e) {
      const err = e as Error;
      // Handle serialization errors and lock contention with backoff
      if (isContentionError(err)) {
        contentionDetected = true;
        exec.logger.warn(`Contention detected in queue ${queue.name}.`);
      } else {
        exec.logger.warn(`Error getting startable workflows for queue ${queue.name}: ${err.message}`);
      }
    }
    return contentionDetected;
  }

  /** After a poll, grow the interval on contention or shrink it toward the minimum, then schedule the next poll with jitter. */
  private adjustInterval(exec: DBOSExecutor, state: QueueRuntimeState, contentionDetected: boolean): void {
    const minPollingMs = state.queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
    const maxPollingMs = WFQueueRunner.defaultMaxPollingIntervalMs;
    if (contentionDetected) {
      state.currentPollingMs = Math.min(maxPollingMs, state.currentPollingMs * this.backoffFactor);
      exec.logger.warn(
        `Increasing polling interval for queue ${state.queue.name} to ${(state.currentPollingMs / 1000).toFixed(2)}s due to contention.`,
      );
    } else {
      state.currentPollingMs = Math.max(minPollingMs, state.currentPollingMs * this.scalebackFactor);
    }
    // Clamp into the current [min, max] range in case config changed under us.
    state.currentPollingMs = Math.max(minPollingMs, Math.min(state.currentPollingMs, maxPollingMs));
    const jitter = this.jitterMin + Math.random() * (this.jitterMax - this.jitterMin);
    state.nextPollAt = Date.now() + state.currentPollingMs * jitter;
  }
}

export const wfQueueRunner = new WFQueueRunner();
