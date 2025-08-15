import { DBOS, DBOSExternalState } from '..';
import { DBOSLifecycleCallback, FunctionName, MethodRegistrationBase } from '../decorators';
import { INTERNAL_QUEUE_NAME } from '../utils';
import { TimeMatcher } from './crontab';

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
export interface SchedulerConfig {
  /** Schedule, in 5- or 6-spot crontab format */
  crontab: string;
  /**
   * Indicates whether or not to retroactively start workflows that were scheduled during
   *  times when the app was not running.  @see `SchedulerMode`.
   */
  mode?: SchedulerMode;
  /** If set, workflows will be enqueued on the named queue, rather than being started immediately */
  queueName?: string;
}

////
// Method Decorator
////

// Scheduled Time. Actual Time.
export type ScheduledArgs = [Date, Date];
type ScheduledHandler<Return> = (...args: ScheduledArgs) => Promise<Return>;

///////////////////////////
// Scheduler Management
///////////////////////////

const SCHEDULER_EVENT_SERVICE_NAME = 'dbos.scheduler';

export class ScheduledReceiver implements DBOSLifecycleCallback {
  readonly #controller = new AbortController();
  readonly #disposables = new Array<Promise<void>>();

  constructor() {
    DBOS.registerLifecycleCallback(this);
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  async initialize(): Promise<void> {
    for (const regOp of DBOS.getAssociatedInfo(SCHEDULER_EVENT_SERVICE_NAME)) {
      if (regOp.methodReg.registeredFunction === undefined) {
        DBOS.logger.warn(
          `Scheduled workflow ${regOp.methodReg.className}.${regOp.methodReg.name} is missing registered function; skipping`,
        );
        continue;
      }

      const { crontab, mode, queueName } = regOp.methodConfig as Partial<SchedulerConfig>;
      if (!crontab) {
        DBOS.logger.warn(
          `Scheduled workflow ${regOp.methodReg.className}.${regOp.methodReg.name} is missing crontab; skipping`,
        );
        continue;
      }

      const timeMatcher = new TimeMatcher(crontab);
      const promise = ScheduledReceiver.#schedulerLoop(
        regOp.methodReg,
        timeMatcher,
        mode ?? SchedulerMode.ExactlyOncePerIntervalWhenActive,
        queueName,
        this.#controller.signal,
      );
      this.#disposables.push(promise);
    }
  }

  async destroy(): Promise<void> {
    this.#controller.abort();
    const promises = this.#disposables.splice(0);
    await Promise.allSettled(promises);
  }

  logRegisteredEndpoints(): void {
    DBOS.logger.info('Scheduled endpoints:');
    for (const regOp of DBOS.getAssociatedInfo(SCHEDULER_EVENT_SERVICE_NAME)) {
      const name = `${regOp.methodReg.className}.${regOp.methodReg.name}`;
      const { crontab, mode } = regOp.methodConfig as Partial<SchedulerConfig>;
      if (crontab) {
        DBOS.logger.info(`    ${name} @ ${crontab}; ${mode ?? SchedulerMode.ExactlyOncePerIntervalWhenActive}`);
      } else {
        DBOS.logger.info(`    ${name} is missing crontab; skipping`);
      }
    }
  }

  static async #schedulerLoop(
    methodReg: MethodRegistrationBase,
    timeMatcher: TimeMatcher,
    mode: SchedulerMode,
    queueName: string | undefined,
    signal: AbortSignal,
  ) {
    const name = `${methodReg.className}.${methodReg.name}`;

    let lastExec = new Date().setMilliseconds(0);
    if (mode === SchedulerMode.ExactlyOncePerInterval) {
      const lastState = await DBOS.getEventDispatchState(SCHEDULER_EVENT_SERVICE_NAME, name, 'lastState');
      if (lastState?.value) {
        lastExec = parseFloat(lastState.value);
      }
    }

    while (!signal.aborted) {
      const nextExec = timeMatcher.nextWakeupTime(lastExec).getTime();
      const sleepTime = nextExec - Date.now();

      if (sleepTime > 0) {
        await new Promise<void>((resolve, reject) => {
          // eslint-disable-next-line prefer-const
          let timeoutID: NodeJS.Timeout;

          const onAbort = () => {
            clearTimeout(timeoutID);
            reject(new Error('Abort signal received'));
          };

          signal.addEventListener('abort', onAbort, { once: true });

          if (signal.aborted) {
            signal.removeEventListener('abort', onAbort);
            reject(new Error('Abort signal received'));
          }

          timeoutID = setTimeout(() => {
            signal.removeEventListener('abort', onAbort);
            resolve();
          }, sleepTime);
        });
      }

      if (signal.aborted) {
        break;
      }

      if (!timeMatcher.match(nextExec)) {
        lastExec = nextExec;
        continue;
      }

      const date = new Date(nextExec);
      if (methodReg.workflowConfig && methodReg.registeredFunction) {
        const workflowID = `sched-${name}-${date.toISOString()}`;
        const wfParams = { workflowID, queueName: queueName ?? INTERNAL_QUEUE_NAME };
        DBOS.logger.debug(`Executing scheduled workflow ${workflowID}`);
        await DBOS.startWorkflow(methodReg.registeredFunction as ScheduledHandler<unknown>, wfParams)(date, new Date());
      } else {
        DBOS.logger.error(`${name} is @scheduled but not a workflow`);
      }

      lastExec = await ScheduledReceiver.#setLastExecTime(name, nextExec);
    }
  }

  static async #setLastExecTime(name: string, time: number) {
    // Record the time of the wf kicked off
    try {
      const state: DBOSExternalState = {
        service: SCHEDULER_EVENT_SERVICE_NAME,
        workflowFnName: name,
        key: 'lastState',
        value: `${time}`,
        updateTime: time,
      };
      const newState = await DBOS.upsertEventDispatchState(state);
      const dbTime = parseFloat(newState.value!);
      if (dbTime && dbTime > time) {
        return dbTime;
      }
    } catch (e) {
      // This write is not strictly essential and the scheduler is often the "canary in the coal mine"
      //  We will simply continue after giving full details.
      const err = e as Error;
      DBOS.logger.warn(`Scheculer caught an error writing to system DB: ${err.message}`);
      DBOS.logger.error(e);
    }
    return time;
  }

  // registerScheduled is static so it can be called before an instance is created during DBOS.launch.
  // This means we can't use the instance as the external info key for associateFunctionWithInfo below
  // or in getAssociatedInfo above...which means we can only have one scheduled receiver instance.
  // However, since this is an internal receiver, it's safe to assume there is ever only one instnace.

  static registerScheduled<This, Return>(
    func: (this: This, ...args: ScheduledArgs) => Promise<Return>,
    config: SchedulerConfig & FunctionName,
  ) {
    const { regInfo } = DBOS.associateFunctionWithInfo(SCHEDULER_EVENT_SERVICE_NAME, func, {
      ctorOrProto: config.ctorOrProto,
      className: config.className,
      name: config.name ?? func.name,
    });

    const schedRegInfo = regInfo as SchedulerConfig;
    schedRegInfo.crontab = config.crontab;
    schedRegInfo.mode = config.mode;
    schedRegInfo.queueName = config.queueName;
  }
}
