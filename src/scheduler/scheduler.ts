import { WorkflowScheduleInternal } from '../system_database';
import { DBOSSerializer } from '../serialization';
import { randomUUID } from 'crypto';
import { DBOS } from '..';
import { DBOSLifecycleCallback, getFunctionRegistrationByName } from '../decorators';
import { INTERNAL_QUEUE_NAME } from '../utils';
import { TimeMatcher } from './crontab';
import { DBOSExecutor } from '../dbos-executor';

export type ScheduledWorkflowFn = (scheduledDate: Date, context: unknown) => Promise<void>;

export interface WorkflowSchedule {
  scheduleId: string;
  scheduleName: string;
  workflowName: string;
  workflowClassName: string;
  schedule: string;
  status: string; // "ACTIVE" | "PAUSED"
  context: unknown; // deserialized
}

export function toWorkflowSchedule(internal: WorkflowScheduleInternal, serializer: DBOSSerializer): WorkflowSchedule {
  let context: unknown;
  try {
    context = serializer.parse(internal.context);
  } catch {
    context = null;
  }

  return {
    scheduleId: internal.scheduleId,
    scheduleName: internal.scheduleName,
    workflowName: internal.workflowName,
    workflowClassName: internal.workflowClassName,
    schedule: internal.schedule,
    status: internal.status,
    context,
  };
}

export function createScheduleId(): string {
  return randomUUID();
}

interface ScheduleLoopEntry {
  controller: AbortController;
  promise: Promise<void>;
  scheduleId: string;
}

export class DynamicSchedulerLoop implements DBOSLifecycleCallback {
  readonly #mainController = new AbortController();
  #pollingPromise: Promise<void> | undefined;
  readonly #scheduleLoops = new Map<string, ScheduleLoopEntry>();
  readonly #pollingIntervalMs: number;

  constructor() {
    this.#pollingIntervalMs = 2000;
    DBOS.registerLifecycleCallback(this);
  }

  async initialize(): Promise<void> {
    this.#pollingPromise = this.#pollingLoop(this.#mainController.signal);
    await Promise.resolve();
  }

  async destroy(): Promise<void> {
    this.#mainController.abort();
    // Abort all per-schedule loops
    for (const entry of this.#scheduleLoops.values()) {
      entry.controller.abort();
    }
    const allPromises: Promise<void>[] = [];
    if (this.#pollingPromise) {
      allPromises.push(this.#pollingPromise);
    }
    for (const entry of this.#scheduleLoops.values()) {
      allPromises.push(entry.promise);
    }
    await Promise.allSettled(allPromises);
    this.#scheduleLoops.clear();
  }

  async #pollingLoop(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      let schedules: WorkflowScheduleInternal[];
      try {
        const executor = DBOSExecutor.globalInstance!;
        schedules = await executor.systemDatabase.listSchedules();
      } catch (e) {
        DBOS.logger.warn(`Dynamic scheduler: error listing schedules: ${(e as Error).message}`);
        await DynamicSchedulerLoop.#cancellableSleep(this.#pollingIntervalMs, signal);
        continue;
      }

      // Build set of current schedule names
      const currentNames = new Set(schedules.map((s) => s.scheduleName));

      // Stop loops for deleted schedules
      for (const [name, entry] of this.#scheduleLoops) {
        if (!currentNames.has(name)) {
          entry.controller.abort();
          this.#scheduleLoops.delete(name);
        }
      }

      // Process each schedule
      for (const sched of schedules) {
        const existing = this.#scheduleLoops.get(sched.scheduleName);

        if (sched.status === 'PAUSED' && existing) {
          // Paused but has a running loop — stop it
          existing.controller.abort();
          this.#scheduleLoops.delete(sched.scheduleName);
        } else if (sched.status === 'ACTIVE') {
          // If schedule was replaced (different scheduleId), restart the loop
          if (existing && existing.scheduleId !== sched.scheduleId) {
            existing.controller.abort();
            this.#scheduleLoops.delete(sched.scheduleName);
          }

          if (!this.#scheduleLoops.has(sched.scheduleName)) {
            // Active and no running loop — start one
            const controller = new AbortController();
            const executor = DBOSExecutor.globalInstance!;
            const promise = DynamicSchedulerLoop.#scheduleLoop(
              sched.scheduleName,
              sched.workflowName,
              sched.workflowClassName,
              sched.schedule,
              sched.context,
              executor.serializer,
              controller.signal,
            );
            this.#scheduleLoops.set(sched.scheduleName, { controller, promise, scheduleId: sched.scheduleId });
          }
        }
      }

      await DynamicSchedulerLoop.#cancellableSleep(this.#pollingIntervalMs, signal);
    }
  }

  static async #scheduleLoop(
    scheduleName: string,
    workflowName: string,
    workflowClassName: string,
    cronExpression: string,
    serializedContext: string,
    serializer: DBOSSerializer,
    signal: AbortSignal,
  ): Promise<void> {
    // Look up the registered workflow function
    const methReg = getFunctionRegistrationByName(workflowClassName, workflowName);
    if (!methReg || !methReg.registeredFunction) {
      DBOS.logger.warn(
        `Dynamic scheduler: workflow ${workflowClassName}.${workflowName} for schedule "${scheduleName}" is not registered; skipping`,
      );
      return;
    }

    const timeMatcher = new TimeMatcher(cronExpression);

    let context: unknown;
    try {
      context = serializer.parse(serializedContext);
    } catch {
      context = null;
    }

    let lastExec = new Date().setMilliseconds(0);

    while (!signal.aborted) {
      const nextExec = timeMatcher.nextWakeupTime(lastExec).getTime();
      let sleepTime = nextExec - Date.now();

      // Apply jitter to prevent thundering herd
      if (sleepTime > 0) {
        const maxJitter = Math.min(sleepTime / 10, 10000);
        sleepTime += Math.random() * maxJitter;
      }

      if (sleepTime > 0) {
        await DynamicSchedulerLoop.#cancellableSleep(sleepTime, signal);
      }

      if (signal.aborted) {
        break;
      }

      const date = new Date(nextExec);
      const workflowID = `sched-${scheduleName}-${date.toISOString()}`;

      // Idempotency check
      const existing = await DBOS.getWorkflowStatus(workflowID);
      if (existing) {
        lastExec = nextExec;
        continue;
      }

      try {
        const wfParams = { workflowID, queueName: INTERNAL_QUEUE_NAME };
        await DBOS.startWorkflow(methReg.registeredFunction as ScheduledWorkflowFn, wfParams)(date, context);
      } catch (e) {
        DBOS.logger.warn(
          `Dynamic scheduler: error firing workflow for schedule "${scheduleName}": ${(e as Error).message}`,
        );
      }

      lastExec = nextExec;
    }
  }

  static async #cancellableSleep(ms: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted) return;
    await new Promise<void>((resolve) => {
      // eslint-disable-next-line prefer-const
      let timeoutID: NodeJS.Timeout;

      const onAbort = () => {
        clearTimeout(timeoutID);
        resolve();
      };

      signal.addEventListener('abort', onAbort, { once: true });

      if (signal.aborted) {
        signal.removeEventListener('abort', onAbort);
        resolve();
        return;
      }

      timeoutID = setTimeout(() => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      }, ms);
    });
  }
}
