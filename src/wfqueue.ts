import { DBOSExecutor } from './dbos-executor';
import { DBOS } from './dbos';
import { DEBUG_TRIGGER_WORKFLOW_QUEUE_START, debugTriggerPoint } from './debugpoint';
import { globalParams } from './utils';

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

  constructor(name: string);

  /** @deprecated @see QueueParameters */
  constructor(name: string, concurrency?: number, rateLimit?: QueueRateLimit);

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
    } else {
      // Handle the case where the second argument is a number
      this.concurrency = arg2;
      this.rateLimit = rateLimit;
    }

    if (wfQueueRunner.wfQueuesByName.has(name)) {
      DBOS.logger.warn(`Workflow Queue '${name}' defined multiple times`);
    }
    wfQueueRunner.wfQueuesByName.set(name, this);
  }
}

class WFQueueRunner {
  readonly wfQueuesByName: Map<string, WorkflowQueue> = new Map();

  private isRunning: boolean = false;
  private interruptResolve?: () => void;
  private pollingIntervalMs: number = 1000;
  private readonly minPollingIntervalMs: number = 1000;
  private readonly maxPollingIntervalMs: number = 120000;

  stop() {
    if (!this.isRunning) return;
    this.isRunning = false;
    if (this.interruptResolve) {
      this.interruptResolve();
    }
  }

  async dispatchLoop(exec: DBOSExecutor): Promise<void> {
    this.isRunning = true;
    while (this.isRunning) {
      // Wait for either the timeout or an interruption
      let timer: NodeJS.Timeout;
      const timeoutPromise = new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          resolve();
        }, this.pollingIntervalMs);
      });

      await Promise.race([timeoutPromise, new Promise<void>((_, reject) => (this.interruptResolve = reject))]).catch(
        () => {
          exec.logger.debug('Workflow queue loop interrupted!');
        },
      ); // Interrupt sleep throws
      clearTimeout(timer!);

      if (!this.isRunning) {
        break;
      }

      // Check queues
      for (const [_qn, q] of this.wfQueuesByName) {
        let wfids: string[];
        try {
          wfids = await exec.systemDatabase.findAndMarkStartableWorkflows(q, exec.executorID, globalParams.appVersion);
        } catch (e) {
          const err = e as Error;
          // Handle serialization errors and lock contention with backoff
          if ('code' in err && (err.code === '40001' || err.code === '55P03')) {
            // 40001: serialization_failure, 55P03: lock_not_available
            // Increase the polling interval on contention
            this.pollingIntervalMs = Math.min(this.maxPollingIntervalMs, this.pollingIntervalMs * 2.0);
            exec.logger.warn(
              `Contention detected in queue thread for ${q.name}. Increasing polling interval to ${(this.pollingIntervalMs / 1000).toFixed(2)}s.`,
            );
          } else {
            exec.logger.warn(`Error getting startable workflows: ${err.message}`);
          }
          wfids = [];
        }

        if (wfids.length > 0) {
          await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_QUEUE_START);
        }

        for (const wfid of wfids) {
          try {
            const _wfh = await exec.executeWorkflowUUID(wfid);
          } catch (e) {
            exec.logger.warn(`Could not execute workflow with id ${wfid}: ${(e as Error).message}`);
          }
        }
      }

      // Gradually decrease the polling interval when there's no contention
      this.pollingIntervalMs = Math.max(this.minPollingIntervalMs, this.pollingIntervalMs * 0.9);
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
