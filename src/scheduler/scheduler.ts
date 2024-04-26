import { WorkflowContext } from "..";
import { DBOSExecutor } from "../dbos-executor";
import { MethodRegistration, registerAndWrapFunction } from "../decorators";
import { TimeMatcher } from "./crontab";

////
// Configuration
////

export enum SchedulerConcurrencyMode {
    ExactlyOncePerInterval = 'ExactlyOncePerInterval',
}

export class SchedulerConfig {
    crontab ?: string = '* * * * *'; // Every minute
    mode ?: SchedulerConcurrencyMode = SchedulerConcurrencyMode.ExactlyOncePerInterval;
}

////
// Method Decorator
////

// Scheduled Time. Actual Time, number running globally, number running locally
type ScheduledArgs = [Date, Date, number, number]

export class SchedulerRegistration<This, Args extends unknown[], Return> extends MethodRegistration<This, Args, Return> {
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
                const loop = new DetachableLoop(this.dbosExec, ro.schedulerConfig.crontab ?? '* * * * *', ro);
                this.schedLoops.push(loop);
                this.schedTasks.push(loop.startLoop());
                /*
                if (!ro.txnConfig && !ro.workflowConfig) {
                    throw new DBOSError(`Error registering method ${defaults.name}.${ro.name}: A Kafka decorator can only be assigned to a transaction or workflow!`)
                }
                if (!defaults.kafkaConfig) {
                    throw new DBOSError(`Error registering method ${defaults.name}.${ro.name}: Kafka configuration not found. Does class ${defaults.name} have an @Kafka decorator?`)
                }
                const kafka = new KafkaJS(defaults.kafkaConfig);
                const consumerConfig = ro.consumerConfig ?? { groupId: `dbos-kafka-group-${ro.kafkaTopic}`}
                const consumer = kafka.consumer(consumerConfig);
                await consumer.connect()
                await consumer.subscribe({topic: ro.kafkaTopic, fromBeginning: true})
                await consumer.run({
                    eachMessage: async ({ topic, partition, message }) => {
                    // This combination uniquely identifies a message for a given Kafka cluster
                    const workflowUUID = `kafka-unique-id-${topic}-${partition}-${message.offset}`
                    const wfParams = { workflowUUID: workflowUUID };
                    // All operations annotated with Kafka decorators must take in these three arguments
                    const args: KafkaArgs = [topic, partition, message]
                    // We can only guarantee exactly-once-per-message execution of transactions and workflows.
                    if (ro.txnConfig) {
                        // Execute the transaction
                        await this.dbosExec.transaction(ro.registeredFunction as Transaction<unknown[], unknown>, wfParams, ...args);
                    } else if (ro.workflowConfig) {
                        // Safely start the workflow
                        await this.dbosExec.workflow(ro.registeredFunction as Workflow<unknown[], unknown>, wfParams, ...args);
                    }
                    },
                })
                this.consumers.push(consumer);
                */
            }
        }
    }
  
    async destroyScheduler() {
        for (const l of this.schedLoops) {
            await l.stopLoop();
        }
        this.schedLoops = [];
        await Promise.all(this.schedTasks);
        this.schedTasks = [];
    }
  
    logRegisteredSchedulerEndpoints() {
        const logger = this.dbosExec.logger;
        logger.info("Scheduled endpoints:");
        this.dbosExec.registeredOperations.forEach((registeredOperation) => {
            const ro = registeredOperation as SchedulerRegistration<unknown, unknown[], unknown>;
            if (ro.schedulerConfig) {
                logger.info(`    ${ro.name} @ ${ro.schedulerConfig.crontab ?? '* * * * *'}; ${ro.schedulerConfig.mode ?? 'Exactly Once Per Interval'}`);
            }
        });
    }
}

class DetachableLoop {
    private isRunning: boolean = false;
    private resolveCompletion?: (value: void | PromiseLike<void>) => void;
    private interruptResolve?: () => void;
    private lastExec: Date;
    private timeMatcher: TimeMatcher;

    constructor(readonly dbosExec: DBOSExecutor, readonly crontab: string, readonly mtd: SchedulerRegistration<unknown, unknown[], unknown>) {
        this.lastExec = new Date();
        // TODO: Get the exec time out of durable storage
        this.lastExec.setMilliseconds(0);
        this.timeMatcher = new TimeMatcher(crontab);
    }

    async startLoop(): Promise<void> {
        this.isRunning = true;
        while (this.isRunning) {
            const nextExecTime = this.timeMatcher.nextWakeupTime(this.lastExec);
            const sleepTime = nextExecTime.getTime() - new Date().getTime();
            console.log(`Loop iteration with sleep time: ${sleepTime}ms`);

            if (sleepTime > 0) {
                // Wait for either the timeout or an interruption
                await Promise.race([
                    this.sleep(sleepTime),
                    new Promise<void>((_, reject) => this.interruptResolve = reject)
                ])
                .catch(); // Interrupt sleep throws
            }

            if (!this.isRunning) {
                break;
            }

            // TODO: Check crontab
            this.lastExec = nextExecTime;
            if (!this.timeMatcher.match(this.lastExec)) {
                continue;
            }

            // TODO: Init workflow
            console.log ("Time to run task!");

            // TODO: Record the time
        }

        if (this.resolveCompletion) {
            this.resolveCompletion();
        }
    }

    stopLoop(): Promise<void> {
        if (!this.isRunning) return Promise.resolve();
        this.isRunning = false;
        if (this.interruptResolve) {
            this.interruptResolve(); // Trigger the interruption
        }
        return new Promise((resolve) => {
            this.resolveCompletion = resolve;
        });
    }

    private sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
