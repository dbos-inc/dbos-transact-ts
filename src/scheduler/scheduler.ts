import { WorkflowContext } from "..";
import { MethodRegistration, registerAndWrapFunction } from "../decorators";

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
