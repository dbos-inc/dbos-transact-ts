import { DBOSExecutor } from './dbos-executor';
import { DBOS } from './dbos';
import { DEBUG_TRIGGER_WORKFLOW_QUEUE_START, debugTriggerPoint } from './debugpoint';
import { globalParams, INTERNAL_QUEUE_NAME } from './utils';

/**
 * Limit the maximum number of functions started from a `WorkflowQueue`
 *   per given time period.
 * If the limit is 5 and the period is 10, no more than 5 functions can be
 *   started per 10 seconds.
 */
interface QueueRateLimit {
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
 * Settings structure for a named workflow queue.
 * Workflow queues limit the rate and concurrency at which DBOS executes workflows.
 * Queue policies apply to workflows started by `DBOS.startWorkflow`,
 *   `DBOS.withWorkflowQueue`, etc.
 */
export class WorkflowQueue {
  readonly name: string;
  readonly concurrency?: number;
  readonly rateLimit?: QueueRateLimit;
  readonly workerConcurrency?: number;
  readonly priorityEnabled: boolean = false;
  readonly partitionQueue: boolean = false;
  readonly minPollingIntervalMs?: number;

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

    if (typeof arg2 === 'object' && arg2 !== null) {
      // Handle the case where the second argument is QueueParameters
      this.concurrency = arg2.concurrency;
      this.rateLimit = arg2.rateLimit;
      this.workerConcurrency = arg2.workerConcurrency;
      this.priorityEnabled = arg2.priorityEnabled ?? false;
      this.partitionQueue = arg2.partitionQueue ?? false;
      this.minPollingIntervalMs = arg2.minPollingIntervalMs;
    } else {
      // Handle the case where the second argument is a number
      this.concurrency = arg2;
      this.rateLimit = rateLimit;
    }

    if (wfQueueRunner.wfQueuesByName.has(name)) {
      throw Error(`Workflow Queue '${name}' defined multiple times`);
    }
    wfQueueRunner.wfQueuesByName.set(name, this);
    wfQueueRunner.onQueueRegistered(this);
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

  /** Called when a new queue is registered while the runner is already active. */
  onQueueRegistered(queue: WorkflowQueue) {
    if (!this.isRunning || !this.exec) return;
    // If explicitly listening to specific queues, don't auto-start for dynamically added ones
    if (this.listenQueuesArg !== null) return;
    this.launchQueueLoop(queue);
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
