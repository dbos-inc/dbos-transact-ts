import { DBOS, DBOSEventReceiverState } from '..';
import { DBOSLifecycleCallback, MethodRegistrationBase } from '../decorators';
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

interface SchedulerMethodConfig {
  crontab?: string;
  mode?: SchedulerMode;
  queueName?: string;
}

////
// Method Decorator
////

// Scheduled Time. Actual Time.
export type ScheduledArgs = [Date, Date];
type ScheduledMessageHandler<Return> = (...args: ScheduledArgs) => Promise<Return>;

export interface SchedulerRegistrationBase extends MethodRegistrationBase {
  schedulerConfig?: SchedulerConfig;
}

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
      const func = regOp.methodReg.registeredFunction as ScheduledMessageHandler<unknown> | undefined;
      if (func === undefined) {
        continue; // TODO: Log?
      }
      const { name, crontab, mode, queueName } = ScheduledReceiver.#getConfig(regOp);
      const timeMatcher = new TimeMatcher(crontab);

      const promise = ScheduledReceiver.#schedulerLoop(
        func,
        name,
        this.#controller.signal,
        timeMatcher,
        mode,
        queueName,
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
      const { name, crontab, mode } = ScheduledReceiver.#getConfig(regOp);
      DBOS.logger.info(`    ${name} @ ${crontab}; ${mode}`);
    }
  }

  static async #schedulerLoop(
    func: ScheduledMessageHandler<unknown>,
    name: string,
    signal: AbortSignal,
    timeMatcher: TimeMatcher,
    mode: SchedulerMode,
    queueName?: string,
  ) {
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
            // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
            reject();
          };

          signal.addEventListener('abort', onAbort, { once: true });

          if (signal.aborted) {
            signal.removeEventListener('abort', onAbort);
            // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
            return reject();
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
      const workflowID = `sched-${name}-${date.toISOString()}`;
      const wfParams = { workflowID, queueName };
      DBOS.logger.debug(`Executing scheduled workflow ${workflowID}`);
      await DBOS.startWorkflow(func, wfParams)(date, new Date());

      lastExec = await ScheduledReceiver.#setLastExecTime(name, nextExec);
    }
  }

  static #getConfig(regOp: { methodConfig?: unknown; methodReg: MethodRegistrationBase }) {
    const name = `${regOp.methodReg.className}.${regOp.methodReg.name}`;
    const methodConfig = regOp.methodConfig as SchedulerMethodConfig;
    const crontab = methodConfig.crontab ?? '* * * * *';
    const mode = methodConfig.mode ?? SchedulerMode.ExactlyOncePerIntervalWhenActive;
    const queueName = methodConfig.queueName;
    return { name, crontab, mode, queueName };
  }

  static async #setLastExecTime(name: string, time: number) {
    // Record the time of the wf kicked off
    const state: DBOSEventReceiverState = {
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
    return time;
  }

  // registerScheduled is static so it can be called before an instance is created during DBOS.launch
  // This also means we can't use the instance as the external info key for associateFunctionWithInfo
  // below or getAssociatedInfo above
  static registerScheduled<This, Return>(
    func: (this: This, ...args: ScheduledArgs) => Promise<Return>,
    options: {
      ctorOrProto?: object;
      className?: string;
      name?: string;
      queueName?: string;
      crontab?: string;
      mode?: SchedulerMode;
    } = {},
  ) {
    const { regInfo } = DBOS.associateFunctionWithInfo(SCHEDULER_EVENT_SERVICE_NAME, func, {
      ctorOrProto: options.ctorOrProto,
      className: options.className,
      name: options.name ?? func.name,
    });

    const schedRegInfo = regInfo as SchedulerMethodConfig;
    schedRegInfo.crontab = options.crontab;
    schedRegInfo.mode = options.mode;
    schedRegInfo.queueName = options.queueName;
  }
}
