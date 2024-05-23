import { WorkflowContext } from "..";
import { DBOSExecutor } from "../dbos-executor";
import { MethodRegistration, registerAndWrapFunction } from "../decorators";
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
    mode ?: SchedulerMode = SchedulerMode.ExactlyOncePerInterval;
}

////
// Method Decorator
////

// Scheduled Time. Actual Time.
export type ScheduledArgs = [Date, Date]

export interface SchedulerRegistrationConfig {
    schedulerConfig?: SchedulerConfig;
}

export class SchedulerRegistration<This, Args extends unknown[], Return> extends MethodRegistration<This, Args, Return>
    implements SchedulerRegistrationConfig
{
    schedulerConfig?: SchedulerConfig;
    constructor(origFunc: (this: This, ...args: Args) => Promise<Return>) {
        super(origFunc);
    }
}

export function Scheduled(schedulerConfig: SchedulerConfig) {
    function scheddec<This, Ctx extends WorkflowContext, Return>(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: ScheduledArgs) => Promise<Return>>
    ) {
        const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
        const schedRegistration = registration as unknown as SchedulerRegistration<This, ScheduledArgs, Return>;
        schedRegistration.schedulerConfig = schedulerConfig;

        return descriptor;
    }
    return scheddec;
}

///////////////////////////
// Scheduler Management
///////////////////////////

export class DBOSScheduler{
    constructor(readonly dbosExec: DBOSExecutor) {}

    schedLoops: DetachableLoop[] = [];
    schedTasks: Promise<void> [] = [];

    initScheduler() {
        for (const registeredOperation of this.dbosExec.registeredOperations) {
            const ro = registeredOperation as SchedulerRegistration<unknown, unknown[], unknown>;
            if (ro.schedulerConfig) {
                const loop = new DetachableLoop(
                    this.dbosExec,
                    ro.schedulerConfig.crontab ?? '* * * * *',
                    ro.schedulerConfig.mode ?? SchedulerMode.ExactlyOncePerInterval,
                    ro
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
            const ro = registeredOperation as SchedulerRegistration<unknown, unknown[], unknown>;
            if (ro.schedulerConfig) {
                logger.info(`    ${ro.name} @ ${ro.schedulerConfig.crontab}; ${ro.schedulerConfig.mode ?? SchedulerMode.ExactlyOncePerInterval}`);
            }
        });
    }
}

class DetachableLoop {
    private isRunning: boolean = false;
    private interruptResolve?: () => void;
    private lastExec: Date;
    private timeMatcher: TimeMatcher;

    constructor(readonly dbosExec: DBOSExecutor, readonly crontab: string, readonly schedMode:SchedulerMode,
                readonly scheduledMethod: SchedulerRegistration<unknown, unknown[], unknown>)
    {
        this.lastExec = new Date();
        this.lastExec.setMilliseconds(0);
        this.timeMatcher = new TimeMatcher(crontab);
    }

    async startLoop(): Promise<void> {
        // See if the exec time is available in durable storage...
        if (this.schedMode !== SchedulerMode.ExactlyOncePerInterval)
        {
            const lasttm = await this.dbosExec.systemDatabase.getLastScheduledTime(this.scheduledMethod.name);
            if (lasttm) {
                this.lastExec = new Date(lasttm);
            }
        }

        this.isRunning = true;
        while (this.isRunning) {
            const nextExecTime = this.timeMatcher.nextWakeupTime(this.lastExec);
            const sleepTime = nextExecTime.getTime() - new Date().getTime();

            if (sleepTime > 0) {
                // Wait for either the timeout or an interruption
                await Promise.race([
                    this.sleepms(sleepTime),
                    new Promise<void>((_, reject) => this.interruptResolve = reject)
                ])
                .catch(); // Interrupt sleep throws
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
            const workflowUUID = `sched-${this.scheduledMethod.name}-${nextExecTime.toISOString()}`;
            this.dbosExec.logger.debug(`Executing scheduled workflow ${workflowUUID}`);
            const wfParams = { workflowUUID: workflowUUID };
            // All operations annotated with Scheduled decorators must take in these four
            const args: ScheduledArgs = [nextExecTime, new Date()];

            // We currently only support scheduled workflows
            if (this.scheduledMethod.workflowConfig) {
                // Execute the workflow
                await this.dbosExec.workflow(this.scheduledMethod.registeredFunction as Workflow<ScheduledArgs, unknown>, wfParams, ...args);
            }
            else {
                this.dbosExec.logger.error(`Function ${this.scheduledMethod.name} is @scheduled but not a workflow`);
            }

            // Record the time of the wf kicked off
            const dbTime = await this.dbosExec.systemDatabase.setLastScheduledTime(this.scheduledMethod.name, nextExecTime.getTime());
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

    private sleepms(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
