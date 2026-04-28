import { DBOSExecutor } from './dbos-executor';
import { DBOS } from './dbos';
import { DEBUG_TRIGGER_WORKFLOW_QUEUE_START, debugTriggerPoint } from './debugpoint';
import type { QueueRecord, SystemDatabase } from './system_database';
import { globalParams, INTERNAL_QUEUE_NAME } from './utils';

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
        `Workflow queue '${name}' is being created after DBOS initialization and will not be considered for dequeue.`,
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

    if (wfQueueRunner.wfQueuesByName.has(name)) {
      throw Error(`Workflow Queue '${name}' defined multiple times`);
    }
    wfQueueRunner.wfQueuesByName.set(name, this);
  }

  /** Throws if any combination of queue parameters is invalid. */
  static validateQueueParams(params: QueueParameters): void {
    const { concurrency, workerConcurrency, rateLimit, minPollingIntervalMs } = params;
    if (workerConcurrency !== undefined && concurrency !== undefined && workerConcurrency > concurrency) {
      throw new Error('concurrency must be greater than or equal to workerConcurrency');
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
      concurrency: params.concurrency ?? null,
      workerConcurrency: params.workerConcurrency ?? null,
      rateLimitMax: params.rateLimit ? params.rateLimit.limitPerPeriod : null,
      rateLimitPeriodSec: params.rateLimit ? params.rateLimit.periodSec : null,
      priorityEnabled: params.priorityEnabled ?? false,
      partitionQueue: params.partitionQueue ?? false,
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
    q.concurrency = record.concurrency ?? undefined;
    q.workerConcurrency = record.workerConcurrency ?? undefined;
    q.rateLimit =
      record.rateLimitMax !== null && record.rateLimitPeriodSec !== null
        ? { limitPerPeriod: record.rateLimitMax, periodSec: record.rateLimitPeriodSec }
        : undefined;
    q.priorityEnabled = record.priorityEnabled;
    q.partitionQueue = record.partitionQueue;
    q.minPollingIntervalMs = record.pollingIntervalSec * 1000;
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
}

class WFQueueRunner {
  readonly wfQueuesByName: Map<string, WorkflowQueue> = new Map();

  private isRunning: boolean = false;
  private stopResolve?: () => void;
  private stopPromise?: Promise<void>;
  private exec?: DBOSExecutor;
  private listenQueueNames: Set<string> | null = null;
  private readonly activeLoops: Set<Promise<void>> = new Set();
  private readonly runningQueueNames: Set<string> = new Set();

  private static readonly defaultMinPollingIntervalMs: number = 1000;
  private static readonly defaultMaxPollingIntervalMs: number = 120000;
  private static readonly supervisorIntervalMs: number = 1000;
  private readonly backoffFactor: number = 2.0;
  private readonly scalebackFactor: number = 0.9;
  private readonly jitterMin: number = 0.95;
  private readonly jitterMax: number = 1.05;

  stop() {
    if (!this.isRunning) return;
    this.isRunning = false;
    this.stopResolve?.();
  }

  clearRegistrations() {
    this.wfQueuesByName.clear();
  }

  private launchQueueLoop(queue: WorkflowQueue) {
    if (this.runningQueueNames.has(queue.name)) return;
    this.runningQueueNames.add(queue.name);
    const loop = this.runQueue(this.exec!, queue);
    this.activeLoops.add(loop);
    void loop.finally(() => {
      this.activeLoops.delete(loop);
      this.runningQueueNames.delete(queue.name);
    });
  }

  async dispatchLoop(exec: DBOSExecutor, listenQueuesArg: (WorkflowQueue | string)[] | null): Promise<void> {
    this.isRunning = true;
    this.exec = exec;
    this.listenQueueNames = listenQueuesArg
      ? new Set(listenQueuesArg.map((entry) => (typeof entry === 'string' ? entry : entry.name)))
      : null;
    this.stopPromise = new Promise<void>((resolve) => {
      this.stopResolve = resolve;
    });

    // Always run the internal queue worker — it is process-private and not
    // subject to the user's listenQueues filter.
    const internal = this.wfQueuesByName.get(INTERNAL_QUEUE_NAME);
    if (internal) this.launchQueueLoop(internal);

    // Resolve the user-supplied listen list against the in-memory registry.
    // String entries that don't match an in-memory queue are deferred — a
    // database-backed queue with that name will be picked up by the
    // supervisor below.
    let inMemoryToLaunch: WorkflowQueue[];
    if (listenQueuesArg !== null) {
      inMemoryToLaunch = [];
      for (const entry of listenQueuesArg) {
        if (typeof entry === 'string') {
          const q = this.wfQueuesByName.get(entry);
          if (q) inMemoryToLaunch.push(q);
        } else {
          inMemoryToLaunch.push(entry);
        }
      }
    } else {
      inMemoryToLaunch = Array.from(this.wfQueuesByName.values()).filter((q) => q.name !== INTERNAL_QUEUE_NAME);
    }

    for (const q of inMemoryToLaunch) {
      this.launchQueueLoop(q);
    }

    // Discover any database-backed queues registered before launch and start
    // their workers synchronously so an immediate enqueue on a DB-backed
    // queue does not race the first supervisor cycle.
    await this.discoverAndLaunchDbQueues(exec);

    // Periodic supervisor: picks up queues registered after launch.
    const supervisor = this.superviseLoop(exec);
    this.activeLoops.add(supervisor);
    void supervisor.finally(() => this.activeLoops.delete(supervisor));

    // Wait until stop() is called, then wait for all loops to drain
    await this.stopPromise;
    await Promise.all(this.activeLoops);
  }

  /**
   * List the queues table and launch a worker for any database-backed queue
   * that isn't already running. Skips names that collide with an in-memory
   * queue (with a warn-once log) and respects the `listenQueues` filter.
   */
  private async discoverAndLaunchDbQueues(exec: DBOSExecutor): Promise<void> {
    let records: QueueRecord[];
    try {
      records = await exec.systemDatabase.listQueues();
    } catch (e) {
      exec.logger.warn(`Error listing database-backed queues: ${(e as Error).message}`);
      return;
    }

    for (const record of records) {
      if (record.name === INTERNAL_QUEUE_NAME) continue;
      if (this.wfQueuesByName.has(record.name)) {
        exec.logger.warn(
          `Database-backed queue '${record.name}' has the same name as an in-memory queue. ` +
            `The in-memory queue's configuration is being used; the database-backed queue is ignored. ` +
            `Rename one of them to resolve the conflict.`,
        );
        continue;
      }
      if (this.runningQueueNames.has(record.name)) continue;
      if (this.listenQueueNames !== null && !this.listenQueueNames.has(record.name)) continue;
      this.launchQueueLoop(WorkflowQueue._fromRecord(record));
    }
  }

  private async superviseLoop(exec: DBOSExecutor): Promise<void> {
    while (this.isRunning) {
      let timer: NodeJS.Timeout;
      const timeoutPromise = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, WFQueueRunner.supervisorIntervalMs);
      });
      await Promise.race([timeoutPromise, this.stopPromise!]);
      clearTimeout(timer!);
      if (!this.isRunning) return;

      await this.discoverAndLaunchDbQueues(exec);
    }
  }

  private async runQueue(exec: DBOSExecutor, initialQueue: WorkflowQueue): Promise<void> {
    let queue = initialQueue;
    const maxPollingMs = WFQueueRunner.defaultMaxPollingIntervalMs;
    let currentPollingMs = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;

    while (this.isRunning) {
      // For database-backed queues, refresh config from the row each
      // iteration so changes to concurrency, polling interval, etc. take
      // effect without a restart. If the row is gone, this worker exits.
      if (queue.databaseBacked) {
        try {
          const record = await exec.systemDatabase.getQueue(queue.name);
          if (record === null) {
            exec.logger.info(`Queue '${queue.name}' has been deleted from the database; stopping its worker.`);
            return;
          }
          queue = WorkflowQueue._fromRecord(record);
        } catch (e) {
          exec.logger.warn(`Error reloading queue '${queue.name}' from the database: ${(e as Error).message}`);
        }
      }

      // Recompute min/max each iteration so a setMinPollingIntervalMs takes
      // effect on the very next tick. Clamp the running value into the new
      // range to avoid sleeping longer than the configured maximum after a
      // shrink, or shorter than the minimum after a grow.
      const minPollingMs = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
      currentPollingMs = Math.max(minPollingMs, Math.min(currentPollingMs, maxPollingMs));

      // Sleep with jitter, racing against the stop signal
      const jitter = this.jitterMin + Math.random() * (this.jitterMax - this.jitterMin);
      const sleepMs = currentPollingMs * jitter;
      let timer: NodeJS.Timeout;
      const timeoutPromise = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, sleepMs);
      });

      await Promise.race([timeoutPromise, this.stopPromise!]);
      clearTimeout(timer!);

      if (!this.isRunning) break;

      let contentionDetected = false;
      try {
        // Transition delayed workflows that are ready to execute
        try {
          await exec.systemDatabase.transitionDelayedWorkflows();
        } catch (e) {
          exec.logger.warn(`Error transitioning delayed workflows: ${(e as Error).message}`);
        }

        // Dequeue workflows for this queue
        let wfids: string[] = [];
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
              wfids.push(...partitionWfids);
            }
          } else {
            wfids = await exec.systemDatabase.findAndMarkStartableWorkflows(
              queue,
              exec.executorID,
              globalParams.appVersion,
              undefined,
            );
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
          wfids = [];
        }

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
      } catch (e) {
        exec.logger.warn(`Error in queue ${queue.name} dispatch loop: ${(e as Error).message}`);
      }

      // Adjust polling interval based on contention
      if (contentionDetected) {
        currentPollingMs = Math.min(maxPollingMs, currentPollingMs * this.backoffFactor);
        exec.logger.warn(
          `Increasing polling interval for queue ${queue.name} to ${(currentPollingMs / 1000).toFixed(2)}s due to contention.`,
        );
      } else {
        currentPollingMs = Math.max(minPollingMs, currentPollingMs * this.scalebackFactor);
      }
    }
  }

  logRegisteredEndpoints(exec: DBOSExecutor) {
    const logger = exec.logger;
    logger.info('Workflow queues:');
    for (const [qn, q] of this.wfQueuesByName) {
      const conc =
        q.concurrency !== undefined ? `global concurrency limit: ${q.concurrency}` : 'No concurrency limit set';
      logger.info(`    ${qn}: ${conc}`);
      const workerconc =
        q.workerConcurrency !== undefined
          ? `worker concurrency limit: ${q.workerConcurrency}`
          : 'No worker concurrency limit set';
      logger.info(`    ${qn}: ${workerconc}`);
    }
  }
}

export const wfQueueRunner = new WFQueueRunner();
