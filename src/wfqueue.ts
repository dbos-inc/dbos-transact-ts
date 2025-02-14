import { DBOSExecutor } from './dbos-executor';
import { DEBUG_TRIGGER_WORKFLOW_QUEUE_START, debugTriggerPoint } from './debugpoint';
import { DBOSInitializationError } from './error';

/**
 Limit the maximum number of functions from this queue
   that can be started in a given period.
 If the limit is 5 and the period is 10, no more than 5 functions can be
   started per 10 seconds.
*/
interface QueueRateLimit {
  limitPerPeriod: number;
  periodSec: number;
}

export interface QueueParameters {
  workerConcurrency?: number;
  concurrency?: number;
  rateLimit?: QueueRateLimit;
}

export class WorkflowQueue {
  readonly name: string;
  readonly concurrency?: number;
  readonly rateLimit?: QueueRateLimit;
  readonly workerConcurrency?: number;

  constructor(name: string, queueParameters: QueueParameters);
  constructor(name: string, concurrency?: number, rateLimit?: QueueRateLimit);

  constructor(name: string, arg2?: QueueParameters | number, rateLimit?: QueueRateLimit) {
    this.name = name;

    if (typeof arg2 === 'object' && arg2 !== null) {
      // Handle the case where the second argument is QueueParameters
      this.concurrency = arg2.concurrency;
      this.rateLimit = arg2.rateLimit;
      this.workerConcurrency = arg2.workerConcurrency;
    } else {
      // Handle the case where the second argument is a number
      this.concurrency = arg2;
      this.rateLimit = rateLimit;
    }

    if (wfQueueRunner.wfQueuesByName.has(name)) {
      throw new DBOSInitializationError(`Workflow Queue '${name}' defined multiple times`);
    }
    wfQueueRunner.wfQueuesByName.set(name, this);
  }
}

class WFQueueRunner {
  readonly wfQueuesByName: Map<string, WorkflowQueue> = new Map();

  private isRunning: boolean = false;
  private interruptResolve?: () => void;

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
        }, 1000);
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
          wfids = await exec.systemDatabase.findAndMarkStartableWorkflows(q, exec.executorID);
        } catch (e) {
          const err = e as Error;
          exec.logger.warn(`Error getting startable workflows: ${err.message}`);
          // On the premise that this was a transaction conflict error, just try again later.
          wfids = [];
        }

        if (wfids.length > 0) {
          await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_QUEUE_START);
        }

        for (const wfid of wfids) {
          try {
            const _wfh = await exec.executeWorkflowUUID(wfid);
          } catch (e) {
            exec.logger.warn(`Could not execute workflow with id ${wfid}`);
            exec.logger.warn(e);
          }
        }
      }
    }
  }

  logRegisteredEndpoints(exec: DBOSExecutor) {
    const logger = exec.logger;
    logger.info('Workflow queues:');
    for (const [qn, q] of this.wfQueuesByName) {
      const conc = q.concurrency !== undefined ? `${q.concurrency}` : 'No concurrency limit set';
      logger.info(`    ${qn}: ${conc}`);
    }
  }
}

export const wfQueueRunner = new WFQueueRunner();
