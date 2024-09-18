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
    RecordUpserted = 'upsert', // Workflow recovery cannot tell you about delete, only update/insert and can't distinguish them
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

function quoteConstant(cval: string): string {
    // Escape double quotes within the identifier by doubling them
    return `${cval.replace(/'/g, "''")}`;
}

interface TriggerPayload {
    operation: TriggerOperation,
    tname: string,
    record: {[key: string]: unknown},
}

export type TriggerFunction<Key extends unknown[]> = (op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;
export type TriggerFunctionWF<Key extends unknown[]> = (ctx: WorkflowContext, op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;

export class DBOSDBTrigger {
    executor: DBOSExecutor;
    listeners: {client: PoolClient, nn: string}[] = [];
    tableToReg: Map<string, DBTriggerRegistration[]> = new Map();
    shutdown: boolean = false;
    catchupLoops: Promise<void>[] = [];

    constructor(executor: DBOSExecutor) {
        this.executor = executor;
    }

    async initialize() {
        const hasTrigger: Set<string> = new Set();
        let hasAnyTrigger: boolean = false;
        const nname = 'dbos_table_update';

        const catchups: {query: string, params: unknown[]}[] = [];

        for (const registeredOperation of this.executor.registeredOperations) {
            const mo = registeredOperation as DBTriggerRegistration;
            if (mo.triggerConfig) {
                const cname = mo.className;
                const mname = mo.name;
                const tname = mo.triggerConfig.schemaName
                    ? `${quoteIdentifier(mo.triggerConfig.schemaName)}.${quoteIdentifier(mo.triggerConfig.tableName)}`
                    : quoteIdentifier(mo.triggerConfig.tableName);

                const tfname = `tf_${cname}_${mname}`;
                const tstr = mo.triggerConfig.schemaName
                    ? `${mo.triggerConfig.schemaName}.${mo.triggerConfig.tableName}`
                : mo.triggerConfig.tableName;
                const trigname = `dbt_${cname}_${mname}`;

                if (!this.tableToReg.has(tstr)) {
                    this.tableToReg.set(tstr, []);
                }
                this.tableToReg.get(tstr)!.push(mo);

                if (!hasTrigger.has(tname)) {
                    await this.executor.runDDL(`
                        CREATE OR REPLACE FUNCTION ${tfname}() RETURNS trigger AS $$
                        DECLARE
                            payload json;
                        BEGIN
                        IF TG_OP = 'INSERT' THEN
                            payload = json_build_object(
                                'tname', '${quoteConstant(tstr)}',
                                'operation', 'insert',
                                'record', row_to_json(NEW)
                            );
                        ELSIF TG_OP = 'UPDATE' THEN
                            payload = json_build_object(
                                'tname', '${quoteConstant(tstr)}',
                                'operation', 'update',
                                'record', row_to_json(NEW)
                            );
                        ELSIF TG_OP = 'DELETE' THEN
                            payload = json_build_object(
                                'tname', '${quoteConstant(tstr)}',
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
                    hasTrigger.add(tname);
                    hasAnyTrigger = true;
                }

                if (mo.triggerIsWorkflow) {
                    const cname = mo.className;
                    const mname = mo.name;
                    const fullname = `${cname}.${mname}`;
                    // Initiate catchup work
                    const tc = mo.triggerConfig as DBTriggerWorkflowConfig;
                    let recseqnum: number | null = null;
                    let rectmstmp: number | null = null;
                    if (tc.sequenceNumColumn || tc.timestampColumn) {
                        const lasts = await this.executor.systemDatabase.getLastDBTriggerTimeSeq(fullname);
                        recseqnum = lasts.last_run_seq;
                        rectmstmp = lasts.last_run_time;
                        if (recseqnum && tc.sequenceNumJitter) {
                            recseqnum-=tc.sequenceNumJitter;
                        }
                        if (rectmstmp && tc.timestampSkewMS) {
                            rectmstmp -= tc.timestampSkewMS;
                        }
                    }

                    // Query string
                    let sncpred = '';
                    const params = [];
                    if (tc.sequenceNumColumn && recseqnum !== null) {
                        params.push(recseqnum);
                        sncpred = ` ${quoteIdentifier(tc.sequenceNumColumn)} > $${params.length} AND `
                    }
                    let tscpred = '';
                    if (tc.timestampColumn && rectmstmp !== null) {
                        params.push(new Date(rectmstmp));
                        tscpred = ` ${quoteIdentifier(tc.timestampColumn)} > $${params.length} AND `;
                    }

                    const catchup_query = `
                        SELECT json_build_object(
                            'tname', '${quoteConstant(tstr)}',
                            'operation', 'upsert',
                            'record', row_to_json(t)
                        )::text as payload
                        FROM (
                            SELECT *
                            FROM ${tname} 
                            WHERE ${sncpred} ${tscpred} 1=1
                        ) t
                    `;

                    catchups.push({query: catchup_query, params});
                }
            }
        }

        if (hasAnyTrigger) {
            const notificationsClient = await this.executor.procedurePool.connect();
            await notificationsClient.query(`LISTEN ${nname};`);

            const payloadFunc = async(payload: TriggerPayload) => {
                for (const mo of this.tableToReg.get(payload.tname) ?? []) {
                    if (!mo.triggerConfig) continue;
                    const key: unknown[] = [];
                    const keystr: string[] = [];
                    for (const kn of mo.triggerConfig?.recordIDColumns ?? []) {
                        const cv = Object.hasOwn(payload.record, kn) ? payload.record[kn] : undefined;
                        key.push(cv);
                        keystr.push(`${cv?.toString()}`);
                    }
                    try {
                        const cname = mo.className;
                        const mname = mo.name;
                        const fullname = `${cname}.${mname}`;
                        if (mo.triggerIsWorkflow) {
                            // Record the time of the wf kicked off (if given)
                            const tc = mo.triggerConfig as DBTriggerWorkflowConfig;
                            let recseqnum: number | null = null;
                            let rectmstmp: number | null = null;
                            if (tc.sequenceNumColumn) {
                                if (!Object.hasOwn(payload.record, tc.sequenceNumColumn)) {
                                    this.executor.logger.warn(`DB Trigger on '${fullname}' specifies sequence number column '${tc.sequenceNumColumn}, but is not in database record.'`);
                                    continue;
                                }
                                const sn = payload.record[tc.sequenceNumColumn];
                                if (typeof(sn) === 'number') {
                                    recseqnum = sn;
                                }
                                else if (typeof(sn) === 'string') {
                                    recseqnum = parseInt(sn)
                                }
                                else {
                                    this.executor.logger.warn(`DB Trigger on '${fullname}' specifies sequence number column '${tc.sequenceNumColumn}, but received "${JSON.stringify(sn)}" instead of number'`);
                                    continue;
                                }
                            }
                            if (tc.timestampColumn) {
                                if (!Object.hasOwn(payload.record, tc.timestampColumn)) {
                                    this.executor.logger.warn(`DB Trigger on '${fullname}' specifies timestamp column '${tc.timestampColumn}, but is not in database record.'`);
                                    continue;
                                }
                                const ts = payload.record[tc.timestampColumn];
                                if (ts instanceof Date) {
                                    rectmstmp = ts.getTime();
                                }
                                else if (typeof(ts) === 'number') {
                                    rectmstmp = ts;
                                }
                                else if (typeof(ts) === 'string') {
                                    rectmstmp = new Date(ts).getTime();
                                }
                                else {
                                    this.executor.logger.warn(`DB Trigger on '${fullname}' specifies timestamp column '${tc.timestampColumn}, but received "${JSON.stringify(ts)}" instead of date/number'`);
                                    continue;
                                }
                            }

                            const wfParams = {
                                workflowUUID: `dbt_${cname}_${mname}_${keystr.join('|')}`,
                                configuredInstance: null
                            };
                            if (payload.operation === TriggerOperation.RecordDeleted) {
                                this.executor.logger.warn(`DB Trigger ${fullname} on '${payload.tname}' witnessed a record deletion.   Record deletion workflow triggers are not supported.`);
                                continue;
                            }
                            if (payload.operation === TriggerOperation.RecordUpdated && recseqnum === null && rectmstmp === null) {
                                this.executor.logger.warn(`DB Trigger ${fullname} on '${payload.tname}' witnessed a record update, but no sequence number / timestamp is defined.   Record update workflow triggers will not work in this case.`);
                                continue;
                            }
                            if (rectmstmp !== null) wfParams.workflowUUID += `_${rectmstmp}`;
                            if (recseqnum !== null) wfParams.workflowUUID += `_${recseqnum}`;
                            payload.operation = TriggerOperation.RecordUpserted;
                            await this.executor.workflow(mo.registeredFunction as Workflow<unknown[], void>, wfParams, payload.operation, key, payload.record);

                            await this.executor.systemDatabase.setLastDBTriggerTimeSeq(fullname, rectmstmp, recseqnum);
                        }
                        else {
                            // Use original func, this may not be wrapped
                            const mr = mo as MethodRegistration<unknown, unknown[], unknown>;
                            const tfunc = mr.origFunction as TriggerFunction<unknown[]>;
                            await tfunc.call(undefined, payload.operation, key, payload.record);
                        }
                    }
                    catch(e) {
                        this.executor.logger.warn(`Caught an exception in trigger handling for "${mo.className}.${mo.name}"`);
                        this.executor.logger.warn(e);
                    }
                }
            }

            const handler = async (msg: Notification) => {
                if (msg.channel !== nname) return;
                const payload = JSON.parse(msg.payload!) as TriggerPayload;
                await payloadFunc(payload);
            };
            notificationsClient.on("notification", handler);
            this.listeners.push({client: notificationsClient, nn: nname});
            this.executor.logger.info(`DB Triggers now listening for '${nname}'`);

            for (const q of catchups) {
                const catchupFunc =  async () => {
                    try {
                        const catchupClient = await this.executor.procedurePool.connect();
                        const rows = await catchupClient.query<{payload: string}>(q.query, q.params);
                        catchupClient.release();
                        for (const r of rows.rows) {
                            const payload = JSON.parse(r.payload) as TriggerPayload;
                            await payloadFunc(payload);
                        }
                    }
                    catch(e) {
                        console.log(e);
                        this.executor.logger.error(e);
                    }
                };
                this.catchupLoops.push(
                    catchupFunc()
                );
            }
        }
    }

    async destroy() {
        this.shutdown = true;
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
        for (const p of this.catchupLoops) {
            try {
                await p;
            }
            catch (e) {
                // Error in destroy, NBD
            }
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
