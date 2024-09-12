////
// Configuration
////

import { WorkflowContext } from "..";
import { DBOSExecutor } from "../dbos-executor";
import { MethodRegistrationBase, registerAndWrapFunction } from "../decorators";

export enum TriggerOperation {
    RecordInserted = 'RecordInserted',
    RecordDeleted = 'RecordDeleted',
    RecordUpdated = 'RecordUpdated',
}

export class DBTriggerConfig {
    // Database table to trigger
    tableName: string = "";
    // Database table schema (optional)
    schemaName?: string = undefined;

    // These identify the record, for elevation to function parameters
    recordIDColumns?: string[] = undefined;
}

export class DBTriggerWorkflowConfig extends DBTriggerConfig {
    // This identify the record sequence number, for checkpointing the sys db
    sequenceNumColumn?: string = undefined;
    // In case sequence numbers aren't perfectly in order, how far off could they be?
    sequenceNumJitter?: number = undefined;

    // This identifies the record timestamp, for checkpointing the sysdb
    timestampColumn?: string = undefined;
    // In case sequence numbers aren't perfectly in order, how far off could they be?
    timestampSkewMS?: number = undefined;
}

interface DBTriggerRegistration extends MethodRegistrationBase
{
    triggerConfig?: DBTriggerConfig;
}

///////////////////////////
// DB Trigger Management
///////////////////////////

function quoteIdentifier(identifier: string): string {
    // Escape double quotes within the identifier by doubling them
    return `"${identifier.replace(/"/g, '""')}"`;
}

export class DBOSDBTrigger {
    executor: DBOSExecutor;

    constructor(executor: DBOSExecutor) {
        this.executor = executor;
    }

    async destroy() {}

    async initialize() {
        const regops = this.executor.getRegistrationsFor(this);
        for (const registeredOperation of regops) {
            const mo = registeredOperation.methodConfig as DBTriggerRegistration;
            if (mo.triggerConfig) {
                const cname = registeredOperation.methodReg.className;
                const mname = registeredOperation.methodReg.name;
                const tname = mo.triggerConfig.schemaName
                    ? `${quoteIdentifier(mo.triggerConfig.schemaName)}.${quoteIdentifier(mo.triggerConfig.tableName)}`
                    : quoteIdentifier(mo.triggerConfig.tableName);

                const tfname = `tf_${cname}_${mname}`;
                const trigname = `dbt_${cname}_${mname}`;
                const nname = `table_update_${cname}_${mname}`;

                await this.executor.runDDL(`
                    CREATE OR REPLACE FUNCTION ${tfname}() RETURNS trigger AS $$
                    BEGIN
                    IF TG_OP = 'INSERT' THEN
                        PERFORM pg_notify('${nname}', 'insert');
                    ELSIF TG_OP = 'UPDATE' THEN
                        PERFORM pg_notify('${nname}', 'update');
                    ELSIF TG_OP = 'DELETE' THEN
                        PERFORM pg_notify('${nname}', 'delete');
                    END IF;
                    RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;

                    CREATE TRIGGER ${trigname}
                    AFTER INSERT OR UPDATE OR DELETE ON ${tname}
                    FOR EACH ROW EXECUTE FUNCTION ${tfname}();
                `);
            }
        }

        return Promise.resolve();
    }

    logRegisteredEndpoints() {
        if (!this.executor) return;
        const logger = this.executor.logger;
        logger.info("Database trigger endpoints registered:");
        const regops = this.executor.getRegistrationsFor(this);
        regops.forEach((registeredOperation) => {
            const mo = registeredOperation.methodConfig as DBTriggerRegistration;
            if (mo.triggerConfig) {
                const cname = registeredOperation.methodReg.className;
                const mname = registeredOperation.methodReg.name;
                const tname = mo.triggerConfig.schemaName
                    ? `${mo.triggerConfig.schemaName}.${mo.triggerConfig.tableName}`
                    : mo.triggerConfig.tableName;
                logger.info(`    ${tname} -> ${cname}.${mname}`);
            }
        });
    }

/*
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
            const ro = registeredOperation as SchedulerRegistrationBase;
            if (ro.schedulerConfig) {
                logger.info(`    ${ro.name} @ ${ro.schedulerConfig.crontab}; ${ro.schedulerConfig.mode ?? SchedulerMode.ExactlyOncePerInterval}`);
            }
        });
    }
*/
}

/*
class DetachableLoop {
    private isRunning: boolean = false;
    private interruptResolve?: () => void;
    private lastExec: Date;
    private timeMatcher: TimeMatcher;
    private scheduledMethodName: string;

    constructor(readonly dbosExec: DBOSExecutor, readonly crontab: string, readonly schedMode:SchedulerMode,
                readonly scheduledMethod: SchedulerRegistrationBase)
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
            const lasttm = await this.dbosExec.systemDatabase.getLastScheduledTime(this.scheduledMethodName);
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
            const wfParams = { workflowUUID: workflowUUID, configuredInstance: null };
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
            const dbTime = await this.dbosExec.systemDatabase.setLastScheduledTime(this.scheduledMethodName, nextExecTime.getTime());
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
*/

export function DBTrigger(triggerConfig: DBTriggerConfig) {
    function trigdec<This, Return, Key extends unknown[] >(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, operation: TriggerOperation, key: Key, record: unknown) => Promise<Return>>
    ) {
        const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
        const triggerRegistration = registration as unknown as DBTriggerRegistration;
        triggerRegistration.triggerConfig = triggerConfig;

        return descriptor;
    }
    return trigdec;
}

export function DBTriggerWorkflow(wfTriggerConfig: DBTriggerWorkflowConfig) {
    function trigdec<This, Ctx extends WorkflowContext, Return, Key extends unknown[]>(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, operation: TriggerOperation, key: Key, record: unknown) => Promise<Return>>
    ) {
        const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
        const triggerRegistration = registration as unknown as DBTriggerRegistration;
        triggerRegistration.triggerConfig = wfTriggerConfig;

        return descriptor;
    }
    return trigdec;
}
