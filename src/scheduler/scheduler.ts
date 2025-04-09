import { DBOS, DBOSEventReceiverState, WorkflowContext } from '..';
import { DBOSExecutor } from '../dbos-executor';
import { MethodRegistrationBase, registerAndWrapFunctionTakingContext } from '../decorators';
import { TimeMatcher } from './crontab';
import { Workflow } from '../workflow';

////
// Configuration
////

/**
 * Choices for scheduler mode for `@DBOS.scheduled` workflows
 */
export enum SchedulerMode {
  /**
   * Using `ExactlyOncePerInterval` causes the scheduler to add "make-up work" for any
   *  schedule slots that occurred when the app was not running
   */
  ExactlyOncePerInterval = 'ExactlyOncePerInterval',
  /**
   * Using `ExactlyOncePerIntervalWhenActive` causes the scheduler to run the workflow once
   *  per interval when the application is active.  If the app is not running at a time
   *  otherwise indicated by the schedule, no workflow will be run.
   */
  ExactlyOncePerIntervalWhenActive = 'ExactlyOncePerIntervalWhenActive',
}

/**
 * Configuration for a `@DBOS.scheduled` workflow
 */
export class SchedulerConfig {
  /** Schedule, in 5- or 6-spot crontab format; defaults to scheduling every minute */
  crontab: string = '* * * * *';
  /**
   * Indicates whether or not to retroactively start workflows that were scheduled during
   *  times when the app was not running.  @see `SchedulerMode`.
   */
  mode?: SchedulerMode = SchedulerMode.ExactlyOncePerIntervalWhenActive;
  /** If set, workflows will be enqueued on the named queue, rather than being started immediately */
  queueName?: string;
}

////
// Method Decorator
////

// Scheduled Time. Actual Time.
export type ScheduledArgs = [Date, Date];

export interface SchedulerRegistrationBase extends MethodRegistrationBase {
  schedulerConfig?: SchedulerConfig;
}

/** @deprecated Remove decorated method's `WorkflowContext` argument and use `@DBOS.scheduled` */
export function Scheduled(schedulerConfig: SchedulerConfig) {
  function scheddec<This, Ctx extends WorkflowContext, Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: ScheduledArgs) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapFunctionTakingContext(target, propertyKey, inDescriptor);
    const schedRegistration = registration as unknown as SchedulerRegistrationBase;
    schedRegistration.schedulerConfig = schedulerConfig;

    return descriptor;
  }
  return scheddec;
}

///////////////////////////
// Scheduler Management
///////////////////////////

export class DBOSScheduler {
  constructor(readonly dbosExec: DBOSExecutor) {
    dbosExec.scheduler = this;
  }

  schedLoops: DetachableLoop[] = [];
  schedTasks: Promise<void>[] = [];

  initScheduler() {
    for (const registeredOperation of this.dbosExec.registeredOperations) {
      const ro = registeredOperation as SchedulerRegistrationBase;
      if (ro.schedulerConfig) {
        const loop = new DetachableLoop(
          this.dbosExec,
          ro.schedulerConfig.crontab ?? '* * * * *',
          ro.schedulerConfig.mode ?? SchedulerMode.ExactlyOncePerInterval,
          ro,
          ro.schedulerConfig?.queueName,
        );
        this.schedLoops.push(loop);
        this.schedTasks.push(loop.startLoop());
      }
    }
  }

  async destroyScheduler() {
    for (const l of this.schedLoops) {
      l.setStopLoopFlag();
    }
    this.schedLoops = [];
    try {
      await Promise.allSettled(this.schedTasks);
    } catch (e) {
      //  What gets caught here is the loop stopping, which is what we wanted.
    }
    this.schedTasks = [];
  }

  logRegisteredSchedulerEndpoints() {
    DBOS.logger.info('Scheduled endpoints:');
    this.dbosExec.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as SchedulerRegistrationBase;
      if (ro.schedulerConfig) {
        DBOS.logger.info(
          `    ${ro.name} @ ${ro.schedulerConfig.crontab}; ${ro.schedulerConfig.mode ?? SchedulerMode.ExactlyOncePerInterval}`,
        );
      }
    });
  }
}

const SCHEDULER_EVENT_SERVICE_NAME = 'dbos.scheduler';

class DetachableLoop {
  private isRunning: boolean = false;
  private interruptResolve?: () => void;
  private lastExec: Date;
  private timeMatcher: TimeMatcher;
  private scheduledMethodName: string;

  constructor(
    readonly dbosExec: DBOSExecutor,
    readonly crontab: string,
    readonly schedMode: SchedulerMode,
    readonly scheduledMethod: SchedulerRegistrationBase,
    readonly queueName?: string,
  ) {
    this.lastExec = new Date();
    this.lastExec.setMilliseconds(0);
    this.timeMatcher = new TimeMatcher(crontab);
    this.scheduledMethodName = `${scheduledMethod.className}.${scheduledMethod.name}`;
  }

  async startLoop(): Promise<void> {
    // See if the exec time is available in durable storage...
    if (this.schedMode === SchedulerMode.ExactlyOncePerInterval) {
      const lastState = await DBOS.getEventDispatchState(
        SCHEDULER_EVENT_SERVICE_NAME,
        this.scheduledMethodName,
        'lastState',
      );
      const lasttm = lastState?.value;
      if (lasttm) {
        this.lastExec = new Date(parseFloat(lasttm));
      }
    }

    this.isRunning = true;
    while (this.isRunning) {
      const nextExecTime = this.timeMatcher.nextWakeupTime(this.lastExec);
      const sleepTime = nextExecTime.getTime() - new Date().getTime();

      if (sleepTime > 0) {
        // Wait for either the timeout or an interruption
        let timer: NodeJS.Timeout;
        const timeoutPromise = new Promise<void>((resolve) => {
          timer = setTimeout(() => {
            resolve();
          }, sleepTime);
        });
        await Promise.race([timeoutPromise, new Promise<void>((_, reject) => (this.interruptResolve = reject))]).catch(
          () => {
            DBOS.logger.debug('Scheduler loop interrupted!');
          },
        ); // Interrupt sleep throws
        clearTimeout(timer!);
      }

      if (!this.isRunning) {
        break;
      }

      // Check crontab
      // If this "wake up" time is not on the schedule, we shouldn't execute.
      //  (While ATOW this wake-up time is a scheduled run time, it is not
      //   contractually obligated to be so.  If this is obligated to be a
      //   scheduled execution time, then we could make this an assertion
      //   instead of a check.)
      if (!this.timeMatcher.match(nextExecTime)) {
        this.lastExec = nextExecTime;
        continue;
      }

      // Init workflow
      const workflowUUID = `sched-${this.scheduledMethodName}-${nextExecTime.toISOString()}`;
      DBOS.logger.debug(`Executing scheduled workflow ${workflowUUID}`);
      const wfParams = {
        workflowUUID: workflowUUID,
        configuredInstance: null,
        queueName: this.queueName,
      };
      // All operations annotated with Scheduled decorators must take in these four
      const args: ScheduledArgs = [nextExecTime, new Date()];

      // We currently only support scheduled workflows
      if (this.scheduledMethod.workflowConfig) {
        // Execute the workflow
        await this.dbosExec.workflow(
          this.scheduledMethod.registeredFunction as Workflow<ScheduledArgs, unknown>,
          wfParams,
          ...args,
        );
      } else {
        DBOS.logger.error(`Function ${this.scheduledMethodName} is @scheduled but not a workflow`);
      }

      // Record the time of the wf kicked off
      const ers: DBOSEventReceiverState = {
        service: SCHEDULER_EVENT_SERVICE_NAME,
        workflowFnName: this.scheduledMethodName,
        key: 'lastState',
        value: `${nextExecTime.getTime()}`,
        updateTime: nextExecTime.getTime(),
      };
      const updRec = await DBOS.upsertEventDispatchState(ers);
      const dbTime = parseFloat(updRec.value!);
      if (dbTime && dbTime > nextExecTime.getTime()) nextExecTime.setTime(dbTime);
      this.lastExec = nextExecTime;
    }
  }

  setStopLoopFlag() {
    if (!this.isRunning) return;
    this.isRunning = false;
    if (this.interruptResolve) {
      this.interruptResolve(); // Trigger the interruption
    }
  }
}
