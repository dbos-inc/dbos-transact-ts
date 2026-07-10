import { DBOSExecutor } from './dbos-executor';
import { DBOS } from './dbos';
import {
  DEBUG_TRIGGER_WORKFLOW_QUEUE_START,
  DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES,
  debugTriggerPoint,
} from './debugpoint';
import type { QueueRecord, SystemDatabase } from './system_database';
import type { GlobalLogger } from './telemetry/logs';
import { globalParams, interruptibleSleep, INTERNAL_QUEUE_NAME } from './utils';

export const DEFAULT_MAX_DEQUEUES_PER_POLL = 100;

/**
 * Log a single queue's name and its set parameters. Unset parameters are
 * omitted, matching `Queue: <name> (concurrency=…, worker_concurrency=…,
 * limit=N/Ts, priority, partitioned)`.
 */
export function logQueue(logger: GlobalLogger, q: WorkflowQueue): void {
  const opts: string[] = [];
  if (q.concurrency !== undefined) opts.push(`concurrency=${q.concurrency}`);
  if (q.workerConcurrency !== undefined) opts.push(`worker_concurrency=${q.workerConcurrency}`);
  if (q.rateLimit !== undefined) opts.push(`limit=${q.rateLimit.limitPerPeriod}/${q.rateLimit.periodSec}s`);
  if (q.priorityEnabled) opts.push('priority');
  if (q.partitionQueue) opts.push('partitioned');
  if (q.maxDequeuesPerPoll !== undefined) opts.push(`max_dequeues_per_poll=${q.maxDequeuesPerPoll}`);
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
 * This limit may be per worker or global
 */
export interface QueueParameters {
  /** If defined, this limits the number of running workflows for a single DBOS process */
  workerConcurrency?: number;
  /** If defined, this limits the number of running workflows globally in the app */
  concurrency?: number;
  /** If set, this limits the rate at which queued workflows are started */
  rateLimit?: QueueRateLimit;
  /** If set, this queue supports priority */
  priorityEnabled?: boolean;
  /** If set, this queue supports partitioning */
  partitionQueue?: boolean;
  /** Base (minimum) polling interval in ms for this queue's dispatch loop (default 1000) */
  minPollingIntervalMs?: number;
  /** Maximum workflows this worker claims in one dequeue transaction (default 100) */
  maxDequeuesPerPoll?: number;
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
  q.concurrency = record.concurrency ?? undefined;
  q.workerConcurrency = record.workerConcurrency ?? undefined;
  q.rateLimit =
    record.rateLimitMax !== null && record.rateLimitPeriodSec !== null
      ? { limitPerPeriod: record.rateLimitMax, periodSec: record.rateLimitPeriodSec }
      : undefined;
  q.priorityEnabled = record.priorityEnabled;
  q.partitionQueue = record.partitionQueue;
  q.minPollingIntervalMs = record.pollingIntervalSec * 1000;
  q.maxDequeuesPerPoll = record.maxDequeuesPerPoll ?? undefined;
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
  minPollingIntervalMs?: number;
  maxDequeuesPerPoll?: number;

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

    this.concurrency = params.concurrency;
    this.rateLimit = params.rateLimit;
    this.workerConcurrency = params.workerConcurrency;
    this.priorityEnabled = params.priorityEnabled ?? false;
    this.partitionQueue = params.partitionQueue ?? false;
    this.minPollingIntervalMs = params.minPollingIntervalMs;
    this.maxDequeuesPerPoll = params.maxDequeuesPerPoll;

    if (wfQueueRunner.wfQueuesByName.has(name)) {
      throw Error(`Workflow Queue '${name}' defined multiple times`);
    }
    wfQueueRunner.wfQueuesByName.set(name, this);
  }

  /** Throws if any combination of queue parameters is invalid. */
  static validateQueueParams(params: QueueParameters): void {
    const { concurrency, workerConcurrency, rateLimit, minPollingIntervalMs, maxDequeuesPerPoll } = params;
    if (workerConcurrency !== undefined && concurrency !== undefined && workerConcurrency > concurrency) {
      throw new Error('concurrency must be greater than or equal to workerConcurrency');
    }
    if (minPollingIntervalMs !== undefined && minPollingIntervalMs <= 0) {
      throw new Error('minPollingIntervalMs must be positive');
    }
    if (maxDequeuesPerPoll !== undefined && (!Number.isInteger(maxDequeuesPerPoll) || maxDequeuesPerPoll <= 0)) {
      throw new Error('maxDequeuesPerPoll must be a positive integer');
    }
    if (rateLimit !== undefined && (rateLimit.limitPerPeriod === undefined || rateLimit.periodSec === undefined)) {
      throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
    }
  }

  /** Build a persistable record from user-supplied registration parameters. */
  static recordFromParams(name: string, params: QueueParameters): QueueRecord {
    return {
      name,
      concurrency: params.concurrency ?? null,
      workerConcurrency: params.workerConcurrency ?? null,
      rateLimitMax: params.rateLimit ? params.rateLimit.limitPerPeriod : null,
      rateLimitPeriodSec: params.rateLimit ? params.rateLimit.periodSec : null,
      priorityEnabled: params.priorityEnabled ?? false,
      partitionQueue: params.partitionQueue ?? false,
      pollingIntervalSec: (params.minPollingIntervalMs ?? 1000) / 1000,
      maxDequeuesPerPoll: params.maxDequeuesPerPoll ?? null,
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
    q.concurrency = record.concurrency ?? undefined;
    q.workerConcurrency = record.workerConcurrency ?? undefined;
    q.rateLimit =
      record.rateLimitMax !== null && record.rateLimitPeriodSec !== null
        ? { limitPerPeriod: record.rateLimitMax, periodSec: record.rateLimitPeriodSec }
        : undefined;
    q.priorityEnabled = record.priorityEnabled;
    q.partitionQueue = record.partitionQueue;
    q.minPollingIntervalMs = record.pollingIntervalSec * 1000;
    q.maxDequeuesPerPoll = record.maxDequeuesPerPoll ?? undefined;
    q.databaseBacked = true;
    q.clientBound = clientSystemDatabase !== undefined;
    if (clientSystemDatabase !== undefined) {
      clientSystemDatabases.set(q as WorkflowQueue, clientSystemDatabase);
    }
    return q as WorkflowQueue;
  }

  async setConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && this.workerConcurrency !== undefined && this.workerConcurrency > value) {
      throw new Error('workerConcurrency must be less than or equal to concurrency');
    }
    await sysDBFor(this).updateQueue(this.name, { concurrency: value ?? null });
    this.concurrency = value;
  }

  async setWorkerConcurrency(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && this.concurrency !== undefined && value > this.concurrency) {
      throw new Error('workerConcurrency must be less than or equal to concurrency');
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

  async setPriorityEnabled(value: boolean): Promise<void> {
    requireDatabaseBacked(this);
    await sysDBFor(this).updateQueue(this.name, { priorityEnabled: value });
    this.priorityEnabled = value;
  }

  async setPartitionQueue(value: boolean): Promise<void> {
    requireDatabaseBacked(this);
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

  async setMaxDequeuesPerPoll(value: number | undefined): Promise<void> {
    requireDatabaseBacked(this);
    if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
      throw new Error('maxDequeuesPerPoll must be a positive integer');
    }
    await sysDBFor(this).updateQueue(this.name, { maxDequeuesPerPoll: value ?? null });
    this.maxDequeuesPerPoll = value;
  }

  async getConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.concurrency;
  }

  async getWorkerConcurrency(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.workerConcurrency;
  }

  async getRateLimit(): Promise<QueueRateLimit | undefined> {
    await refreshFromDb(this);
    return this.rateLimit;
  }

  async getPriorityEnabled(): Promise<boolean> {
    await refreshFromDb(this);
    return this.priorityEnabled;
  }

  async getPartitionQueue(): Promise<boolean> {
    await refreshFromDb(this);
    return this.partitionQueue;
  }

  async getMinPollingIntervalMs(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.minPollingIntervalMs;
  }

  async getMaxDequeuesPerPoll(): Promise<number | undefined> {
    await refreshFromDb(this);
    return this.maxDequeuesPerPoll;
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
  }

  async dispatchLoop(exec: DBOSExecutor, listenQueuesArg: (WorkflowQueue | string)[] | null): Promise<void> {
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

    // One loop drives every queue, so idle cost is O(1) not O(#queues); returns when stop() aborts.
    await this.schedulerLoop(exec, startNow);
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
    return result;
  }

  /** Begin tracking a queue if it isn't already, scheduling its first poll one interval out. */
  private ensureState(queue: WorkflowQueue, now: number): void {
    if (this.states.has(queue.name)) return;
    const interval = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
    this.states.set(queue.name, {
      queue,
      currentPollingMs: interval,
      nextPollAt: now + interval,
    });
  }

  /** Reconcile DB-backed queues against the queues table in one query: refresh, add, or drop them. */
  private async refreshDbQueues(exec: DBOSExecutor, now: number): Promise<void> {
    let records: QueueRecord[];
    try {
      records = await exec.systemDatabase.listQueues();
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
      if (this.listenQueueNames !== null && !this.listenQueueNames.has(record.name)) continue;
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

  /** The single dispatcher loop: reconcile queues, transition delayed workflows once, poll due queues sequentially. */
  private async schedulerLoop(exec: DBOSExecutor, startNow: number): Promise<void> {
    const signal = this.abortController!.signal;
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

      // Collect the queues due to poll this tick and dispatch them sequentially.
      const due: QueueRuntimeState[] = [];
      for (const state of this.states.values()) {
        if (now >= state.nextPollAt) due.push(state);
      }
      for (const state of due) {
        if (!this.isRunning) break;
        const contentionDetected = await this.pollQueue(exec, state.queue);
        this.adjustInterval(exec, state, contentionDetected);
      }

      if (!this.isRunning) break;

      // Sleep until the earliest of the next scheduled poll, reconcile, or transition tick.
      let wake = Math.min(
        lastReconcileAt + WFQueueRunner.reconcileIntervalMs,
        lastTransitionAt + WFQueueRunner.transitionIntervalMs,
      );
      for (const state of this.states.values()) {
        if (state.nextPollAt < wake) wake = state.nextPollAt;
      }
      const sleepMs = Math.max(0, wake - Date.now());
      await interruptibleSleep(sleepMs, signal);
    }
  }

  /** Poll one queue once, starting ready workflows. */
  private async pollQueue(exec: DBOSExecutor, queue: WorkflowQueue): Promise<boolean> {
    let contentionDetected = false;
    // Helper function that starts dequeued workflows
    const dispatch = async (wfids: string[]) => {
      if (wfids.length > 0) {
        await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_QUEUE_START);
      }
      for (const wfid of wfids) {
        try {
          await exec.executeWorkflowId(wfid, { isQueueDispatch: true });
        } catch (e) {
          exec.logger.warn(`Could not execute workflow with id ${wfid}: ${(e as Error).message}`);
        }
      }
    };
    // Dequeue workflows for this queue. If the queue is partitioned, successively dequeue and start
    // workflows from each active partition.
    try {
      if (queue.partitionQueue) {
        const partitionKeys = await exec.systemDatabase.getQueuePartitions(queue.name);
        for (const partitionKey of partitionKeys) {
          const partitionWfids = await exec.systemDatabase.findAndMarkStartableWorkflows(
            queue,
            exec.executorID,
            globalParams.appVersion,
            partitionKey,
          );
          await dispatch(partitionWfids);
          await debugTriggerPoint(DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES);
        }
      } else {
        const wfids = await exec.systemDatabase.findAndMarkStartableWorkflows(
          queue,
          exec.executorID,
          globalParams.appVersion,
          undefined,
        );
        await dispatch(wfids);
      }
    } catch (e) {
      const err = e as Error;
      // Handle serialization errors and lock contention with backoff
      if ('code' in err && (err.code === '40001' || err.code === '55P03')) {
        // 40001: serialization_failure, 55P03: lock_not_available
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
