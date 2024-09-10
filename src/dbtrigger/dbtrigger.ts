////
// Configuration
////

import { WorkflowContext } from "..";
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

export class DBWorkflowTriggerConfig extends DBTriggerConfig {
    // This identify the record sequence number, for checkpointing the sys db
    sequenceNumColumn?: string = undefined;
    // In case sequence numbers aren't perfectly in order, how far off could they be?
    sequenceNumJitter?: number = undefined;

    // This identifies the record timestamp, for checkpointing the sysdb
    timestampColumn?: string = undefined;
    // In case sequence numbers aren't perfectly in order, how far off could they be?
    timestampSkewMS?: number = undefined;
}

export interface DBTriggerRegistration extends MethodRegistrationBase
{
    triggerConfig?: DBTriggerConfig;
}

export function DBTrigger(triggerConfig: DBTriggerConfig) {
    function scheddec<This, Return>(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, operation: TriggerOperation, key: unknown[], record: unknown) => Promise<Return>>
    ) {
        const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
        const triggerRegistration = registration as unknown as DBTriggerRegistration;
        triggerRegistration.triggerConfig = triggerConfig;

        return descriptor;
    }
    return scheddec;
}

export function DBWorkflowTrigger(wfTriggerConfig: DBWorkflowTriggerConfig) {
    function scheddec<This, Ctx extends WorkflowContext, Return>(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, operation: TriggerOperation, key: unknown[], record: unknown) => Promise<Return>>
    ) {
        const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
        const triggerRegistration = registration as unknown as DBTriggerRegistration;
        triggerRegistration.triggerConfig = wfTriggerConfig;

        return descriptor;
    }
    return scheddec;
}
