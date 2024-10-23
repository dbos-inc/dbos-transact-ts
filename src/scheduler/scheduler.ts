import { DBOSEventReceiverState, WorkflowContext } from "..";
import { DBOSExecutor } from "../dbos-executor";
import { MethodRegistrationBase, registerAndWrapFunction } from "../decorators";
import { TimeMatcher } from "./crontab";
import { Workflow } from "../workflow";

////
// Configuration
////

export enum SchedulerMode {
    ExactlyOncePerInterval = 'ExactlyOncePerInterval',
    ExactlyOncePerIntervalWhenActive = 'ExactlyOncePerIntervalWhenActive',
}

export class SchedulerConfig {
    crontab: string = '* * * * *'; // Every minute
    mode ?: SchedulerMode = SchedulerMode.ExactlyOncePerIntervalWhenActive;
    queueName ?: string;
}

////
// Method Decorator
////

// Scheduled Time. Actual Time.
export type ScheduledArgs = [Date, Date]

export interface SchedulerRegistrationBase extends MethodRegistrationBase
{
    schedulerConfig?: SchedulerConfig;
}

export function Scheduled(schedulerConfig: SchedulerConfig) {
    function scheddec<This, Ctx extends WorkflowContext, Return>(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: ScheduledArgs) => Promise<Return>>
    ) {
        const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
        const schedRegistration = registration as unknown as SchedulerRegistrationBase;
        schedRegistration.schedulerConfig = schedulerConfig;

        return descriptor;
    }
    return scheddec;
}

///////////////////////////
// Scheduler Management
///////////////////////////

export class DBOSScheduler{
    constructor(readonly dbosExec: DBOSExecutor) {
        dbosExec.scheduler = this;
    }

    schedLoops: DetachableLoop[] = [];
    schedTasks: Promise<void> [] = [];

    initScheduler() {
        for (const registeredOperation of this.dbosExec.registeredOperations) {
            const ro = registeredOperation as SchedulerRegistrationBase;
            if (ro.schedulerConfig) {
                const loop = new DetachableLoop(
                    this.dbosExec,
                    ro.schedulerConfig.crontab ?? '* * * * *',
                    ro.schedulerConfig.mode ?? SchedulerMode.ExactlyOncePerInterval,
                    ro,
                    ro.schedulerConfig?.queueName
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
        }
        catch (e) {
           //  What gets caught here is the loop stopping, which is what we wanted.
        }
        this.schedTasks = [];
    }

    logRegisteredSchedulerEndpoints() {
        const logger = this.dbosExec.logger;
        logger.info("Scheduled endpoints:");
        this.dbosExec.registeredOperations.forEach((registeredOperation) => {
            const ro = registeredOperation as SchedulerRegistrationBase;
            if (ro.schedulerConfig) {
                logger.info(`    ${ro.name} @ ${ro.schedulerConfig.crontab}; ${ro.schedulerConfig.mode ?? SchedulerMode.ExactlyOncePerInterval}`);
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

    constructor(readonly dbosExec: DBOSExecutor, readonly crontab: string, readonly schedMode:SchedulerMode,
                readonly scheduledMethod: SchedulerRegistrationBase, readonly queueName?: string)
    {
        this.lastExec = new Date();
        this.lastExec.setMilliseconds(0);
        this.timeMatcher = new TimeMatcher(crontab);
        this.scheduledMethodName = `${scheduledMethod.className}.${scheduledMethod.name}`;
    }

    async startLoop(): Promise<void> {
        // See if the exec time is available in durable storage...
        if (this.schedMode === SchedulerMode.ExactlyOncePerInterval)
        {
            const lastState = await this.dbosExec.systemDatabase.getEventDispatchState(SCHEDULER_EVENT_SERVICE_NAME, this.scheduledMethodName, 'lastState');
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
                await Promise.race([
                    timeoutPromise,
                    new Promise<void>((_, reject) => this.interruptResolve = reject)
                ])
                .catch(() => {this.dbosExec.logger.debug("Scheduler loop interrupted!")}); // Interrupt sleep throws
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
            this.dbosExec.logger.debug(`Executing scheduled workflow ${workflowUUID}`);
            const wfParams = { workflowUUID: workflowUUID, configuredInstance: null, queueName: this.queueName };
            // All operations annotated with Scheduled decorators must take in these four
            const args: ScheduledArgs = [nextExecTime, new Date()];

            // We currently only support scheduled workflows
            if (this.scheduledMethod.workflowConfig) {
                // Execute the workflow
                await this.dbosExec.workflow(this.scheduledMethod.registeredFunction as Workflow<ScheduledArgs, unknown>, wfParams, ...args);
            }
            else {
                this.dbosExec.logger.error(`Function ${this.scheduledMethodName} is @scheduled but not a workflow`);
            }

            // Record the time of the wf kicked off
            const ers: DBOSEventReceiverState = {
                service: SCHEDULER_EVENT_SERVICE_NAME,
                workflowFnName: this.scheduledMethodName,
                key: 'lastState',
                value: `${nextExecTime.getTime()}`,
                updateTime: nextExecTime.getTime(),
            }
            const updRec = await this.dbosExec.systemDatabase.upsertEventDispatchState(ers);
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
