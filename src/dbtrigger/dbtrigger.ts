////
// Configuration
////

import { WorkflowContext } from "..";
import { DBOSExecutor } from "../dbos-executor";
import { MethodRegistration, MethodRegistrationBase, registerAndWrapFunction } from "../decorators";
import { Notification, PoolClient } from "pg";
import { Workflow } from "../workflow";

export enum TriggerOperation {
    RecordInserted = 'insert',
    RecordDeleted = 'delete',
    RecordUpdated = 'update',
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
    triggerIsWorkflow?: boolean;
}

///////////////////////////
// DB Trigger Management
///////////////////////////

function quoteIdentifier(identifier: string): string {
    // Escape double quotes within the identifier by doubling them
    return `"${identifier.replace(/"/g, '""')}"`;
}

interface TriggerPayload {
    operation: TriggerOperation,
    record: {[key: string]: unknown},
}

export type TriggerFunction<Key extends unknown[]> = (op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;
export type TriggerFunctionWF<Key extends unknown[]> = (ctx: WorkflowContext, op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;

export class DBOSDBTrigger {
    executor: DBOSExecutor;
    listeners: {client: PoolClient, nn: string}[] = [];

    constructor(executor: DBOSExecutor) {
        this.executor = executor;
    }

    async initialize() {
        for (const registeredOperation of this.executor.registeredOperations) {
            const mo = registeredOperation as DBTriggerRegistration;
            if (mo.triggerConfig) {
                const mr = registeredOperation as MethodRegistration<unknown, unknown[], unknown>;
                const tfunc = mr.origFunction as TriggerFunction<unknown[]>;
                //const tfunc = mo.registeredFunction as TriggerFunction<unknown[]>;
                const cname = mo.className;
                const mname = mo.name;
                const tname = mo.triggerConfig.schemaName
                    ? `${quoteIdentifier(mo.triggerConfig.schemaName)}.${quoteIdentifier(mo.triggerConfig.tableName)}`
                    : quoteIdentifier(mo.triggerConfig.tableName);

                const tfname = `tf_${cname}_${mname}`;
                const trigname = `dbt_${cname}_${mname}`;
                const nname = 'tableupdate'; //`table_update_${cname}_${mname}`;

                await this.executor.runDDL(`
                    CREATE OR REPLACE FUNCTION ${tfname}() RETURNS trigger AS $$
                    DECLARE
                        payload json;
                    BEGIN
                    IF TG_OP = 'INSERT' THEN
                        payload = json_build_object(
                            'operation', 'insert',
                            'record', row_to_json(NEW)
                        );
                    ELSIF TG_OP = 'UPDATE' THEN
                        payload = json_build_object(
                            'operation', 'update',
                            'record', row_to_json(NEW)
                        );
                    ELSIF TG_OP = 'DELETE' THEN
                        payload = json_build_object(
                            'operation', 'delete',
                            'record', row_to_json(OLD)
                        );
                    END IF;

                    PERFORM pg_notify('${nname}', payload::text);
                    RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;

                    CREATE TRIGGER ${trigname}
                    AFTER INSERT OR UPDATE OR DELETE ON ${tname}
                    FOR EACH ROW EXECUTE FUNCTION ${tfname}();
                `);

                const notificationsClient = await this.executor.procedurePool.connect();
                await notificationsClient.query(`LISTEN ${nname};`);
                const handler = async (msg: Notification) => {
                    if (msg.channel === nname) {
                        const payload = JSON.parse(msg.payload!) as TriggerPayload;
                        const key: unknown[] = [];
                        const keystr: string[] = [];
                        for (const kn of mo.triggerConfig?.recordIDColumns ?? []) {
                            const cv = Object.hasOwn(payload.record, kn) ? payload.record[kn] : undefined;
                            key.push(cv);
                            keystr.push(`${cv?.toString()}`);
                        }
                        try {
                            if (mo.triggerIsWorkflow) {
                                const wfParams = {
                                    workflowUUID: `dbt_${cname}_${mname}_${payload.operation}_${keystr.join('|')}`,
                                    configuredInstance: null
                                };
                                await this.executor.workflow(mo.registeredFunction as Workflow<unknown[], void>, wfParams, payload.operation, key, payload.record);
                            }
                            else {
                                await tfunc.call(undefined, payload.operation, key, payload.record);
                            }
                        }
                        catch(e) {
                            this.executor.logger.warn(`Caught an exception in trigger handling for ${tfunc.name}`);
                            this.executor.logger.warn(e);
                        }
                    }
                };
                notificationsClient.on("notification", handler);
                this.listeners.push({client: notificationsClient, nn: nname});
                this.executor.logger.info(`DB Triggers now listening for '${nname}'`);
            }
        }
    }

    async destroy() {
        for (const l of this.listeners) {
            try {
                await l.client.query(`UNLISTEN ${l.nn};`);
            }
            catch(e) {
                this.executor.logger.warn(e);
            }
            l.client.release();
        }
        this.listeners = [];
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
}

export function DBTrigger(triggerConfig: DBTriggerConfig) {
    function trigdec<This, Return, Key extends unknown[] >(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, operation: TriggerOperation, key: Key, record: unknown) => Promise<Return>>
    ) {
        const { descriptor, registration } = registerAndWrapFunction(target, propertyKey, inDescriptor);
        const triggerRegistration = registration as unknown as DBTriggerRegistration;
        triggerRegistration.triggerConfig = triggerConfig;
        triggerRegistration.triggerIsWorkflow = false;

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
        triggerRegistration.triggerIsWorkflow = true;

        return descriptor;
    }
    return trigdec;
}
