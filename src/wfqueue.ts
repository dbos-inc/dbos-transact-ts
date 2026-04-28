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
   * If set, configuration reads/writes target this SystemDatabase instead of
   * the global executor's. This lets a queue handle operate on the system
   * database directly, without requiring a launched DBOS runtime in the same
   * process.
   */
  readonly clientSystemDatabase?: SystemDatabase;

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
    q.clientSystemDatabase = clientSystemDatabase;
    return q as WorkflowQueue;
  }

  private requireDatabaseBacked(): void {
    if (!this.databaseBacked) {
      throw new Error(
        `Cannot configure queue ${this.name}: dynamic configuration is only supported for queues registered via DBOS.registerQueue.`,
      );
    }
  }

  private sysDB(): SystemDatabase {
    if (this.clientSystemDatabase) return this.clientSystemDatabase;
    const exec = DBOSExecutor.globalInstance;
    if (!exec) {
      throw new Error(`Cannot access system database for queue ${this.name}: DBOS has not been launched.`);
    }
    return exec.systemDatabase;
  }

  async setConcurrency(value: number | undefined): Promise<void> {
    this.requireDatabaseBacked();
    if (value !== undefined && this.workerConcurrency !== undefined && this.workerConcurrency > value) {
      throw new Error('workerConcurrency must be less than or equal to concurrency');
    }
    await this.sysDB().updateQueue(this.name, { concurrency: value ?? null });
    this.concurrency = value;
  }

  async setWorkerConcurrency(value: number | undefined): Promise<void> {
    this.requireDatabaseBacked();
    if (value !== undefined && this.concurrency !== undefined && value > this.concurrency) {
      throw new Error('workerConcurrency must be less than or equal to concurrency');
    }
    await this.sysDB().updateQueue(this.name, { workerConcurrency: value ?? null });
    this.workerConcurrency = value;
  }

  async setRateLimit(value: QueueRateLimit | undefined): Promise<void> {
    this.requireDatabaseBacked();
    if (value !== undefined && (value.limitPerPeriod === undefined || value.periodSec === undefined)) {
      throw new Error('rateLimit must specify both limitPerPeriod and periodSec');
    }
    await this.sysDB().updateQueue(this.name, {
      rateLimitMax: value ? value.limitPerPeriod : null,
      rateLimitPeriodSec: value ? value.periodSec : null,
    });
    this.rateLimit = value;
  }

  async setPriorityEnabled(value: boolean): Promise<void> {
    this.requireDatabaseBacked();
    await this.sysDB().updateQueue(this.name, { priorityEnabled: value });
    this.priorityEnabled = value;
  }

  async setPartitionQueue(value: boolean): Promise<void> {
    this.requireDatabaseBacked();
    await this.sysDB().updateQueue(this.name, { partitionQueue: value });
    this.partitionQueue = value;
  }

  async setMinPollingIntervalMs(value: number): Promise<void> {
    this.requireDatabaseBacked();
    if (value <= 0) {
      throw new Error('minPollingIntervalMs must be positive');
    }
    await this.sysDB().updateQueue(this.name, { pollingIntervalSec: value / 1000 });
    this.minPollingIntervalMs = value;
  }

  /**
   * Re-read the queue's row from the database and update the cached fields
   * in place. No-op for in-memory queues. Throws if the row has been deleted.
   */
  private async refreshFromDb(): Promise<void> {
    if (!this.databaseBacked) return;
    const record = await this.sysDB().getQueue(this.name);
    if (record === null) {
      throw new Error(`Queue '${this.name}' was not found in the database.`);
    }
    this.concurrency = record.concurrency ?? undefined;
    this.workerConcurrency = record.workerConcurrency ?? undefined;
    this.rateLimit =
      record.rateLimitMax !== null && record.rateLimitPeriodSec !== null
        ? { limitPerPeriod: record.rateLimitMax, periodSec: record.rateLimitPeriodSec }
        : undefined;
    this.priorityEnabled = record.priorityEnabled;
    this.partitionQueue = record.partitionQueue;
    this.minPollingIntervalMs = record.pollingIntervalSec * 1000;
  }

  async getConcurrency(): Promise<number | undefined> {
    await this.refreshFromDb();
    return this.concurrency;
  }

  async getWorkerConcurrency(): Promise<number | undefined> {
    await this.refreshFromDb();
    return this.workerConcurrency;
  }

  async getRateLimit(): Promise<QueueRateLimit | undefined> {
    await this.refreshFromDb();
    return this.rateLimit;
  }

  async getPriorityEnabled(): Promise<boolean> {
    await this.refreshFromDb();
    return this.priorityEnabled;
  }

  async getPartitionQueue(): Promise<boolean> {
    await this.refreshFromDb();
    return this.partitionQueue;
  }

  async getMinPollingIntervalMs(): Promise<number | undefined> {
    await this.refreshFromDb();
    return this.minPollingIntervalMs;
  }
}

class WFQueueRunner {
  readonly wfQueuesByName: Map<string, WorkflowQueue> = new Map();

  private isRunning: boolean = false;
  private stopResolve?: () => void;
  private stopPromise?: Promise<void>;
  private exec?: DBOSExecutor;
  private listenQueuesArg: WorkflowQueue[] | null = null;
  private readonly activeLoops: Set<Promise<void>> = new Set();

  private static readonly defaultMinPollingIntervalMs: number = 1000;
  private static readonly defaultMaxPollingIntervalMs: number = 120000;
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
    const loop = this.runQueue(this.exec!, queue);
    this.activeLoops.add(loop);
    void loop.finally(() => this.activeLoops.delete(loop));
  }

  async dispatchLoop(exec: DBOSExecutor, listenQueuesArg: WorkflowQueue[] | null): Promise<void> {
    this.isRunning = true;
    this.exec = exec;
    this.listenQueuesArg = listenQueuesArg;
    this.stopPromise = new Promise<void>((resolve) => {
      this.stopResolve = resolve;
    });

    let listenQueues: WorkflowQueue[];
    if (listenQueuesArg !== null) {
      listenQueues = [...listenQueuesArg, this.wfQueuesByName.get(INTERNAL_QUEUE_NAME)!];
    } else {
      listenQueues = Array.from(this.wfQueuesByName.values());
    }

    // Start one loop per queue
    for (const q of listenQueues) {
      this.launchQueueLoop(q);
    }

    // Wait until stop() is called, then wait for all loops to drain
    await this.stopPromise;
    await Promise.all(this.activeLoops);
  }

  private async runQueue(exec: DBOSExecutor, queue: WorkflowQueue): Promise<void> {
    const minPollingMs = queue.minPollingIntervalMs ?? WFQueueRunner.defaultMinPollingIntervalMs;
    const maxPollingMs = WFQueueRunner.defaultMaxPollingIntervalMs;
    let currentPollingMs = minPollingMs;

    while (this.isRunning) {
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
