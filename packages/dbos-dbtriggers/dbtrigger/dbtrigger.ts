import {
    DBOSEventReceiver,
    DBOSEventReceiverRegistration,
    DBOSExecutorContext,
    DBNotification,
    DBNotificationListener,
    WorkflowContext,
    WorkflowFunction,
    associateMethodWithEventReceiver,
} from "@dbos-inc/dbos-sdk";

////
// Configuration
////

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

    // Should DB trigger / notification be used?
    useDBNotifications?: boolean = false;

    // Should DB trigger be auto-installed?
    installDBTrigger?: boolean = false;

    // If not using triggers, frequency of polling, ms
    dbPollingInterval?: number = 5000;
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

    // Use a workflow queue if set
    queueName?: string = undefined;
}

interface DBTriggerRegistration
{
    triggerConfig?: DBTriggerConfig;
    triggerIsWorkflow?: boolean;
}

///
// SQL Gen
///

function quoteIdentifier(identifier: string): string {
    // Escape double quotes within the identifier by doubling them
    return `"${identifier.replace(/"/g, '""')}"`;
}

function quoteConstant(cval: string): string {
    // Escape double quotes within the identifier by doubling them
    return `${cval.replace(/'/g, "''")}`;
}

function createTriggerSQL(
    triggerFuncName: string, // Name of function for trigger to call
    triggerName: string, // Trigger name
    tableName: string, // As known to DB
    tableNameString: string, // Passed as a value to notifier
    notifierName: string, // Notifier name
)
{
    return `
        CREATE OR REPLACE FUNCTION ${triggerFuncName}() RETURNS trigger AS $$
        DECLARE
            payload json;
        BEGIN
        IF TG_OP = 'INSERT' THEN
            payload = json_build_object(
                'tname', '${quoteConstant(tableNameString)}',
                'operation', 'insert',
                'record', row_to_json(NEW)
            );
        ELSIF TG_OP = 'UPDATE' THEN
            payload = json_build_object(
                'tname', '${quoteConstant(tableNameString)}',
                'operation', 'update',
                'record', row_to_json(NEW)
            );
        ELSIF TG_OP = 'DELETE' THEN
            payload = json_build_object(
                'tname', '${quoteConstant(tableNameString)}',
                'operation', 'delete',
                'record', row_to_json(OLD)
            );
        END IF;

        PERFORM pg_notify('${notifierName}', payload::text);
        RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE TRIGGER ${triggerName}
        AFTER INSERT OR UPDATE OR DELETE ON ${tableName}
        FOR EACH ROW EXECUTE FUNCTION ${triggerFuncName}();
    `;
}

function createCatchupSql(
    tc: DBTriggerWorkflowConfig,
    tableName: string,
    tableNameString: string,
    startSeqNum?: bigint | null,
    startTimeStamp?: number | null)
{
    // Query string
    let sncpred = '';
    let oby = '';
    const params = [];
    if (tc.sequenceNumColumn && startSeqNum) {
        params.push(startSeqNum);
        sncpred = ` ${quoteIdentifier(tc.sequenceNumColumn)} > $${params.length} AND `
        oby = `ORDER BY ${quoteIdentifier(tc.sequenceNumColumn)}`;
    }
    let tscpred = '';
    if (tc.timestampColumn && startTimeStamp) {
        params.push(new Date(startTimeStamp));
        tscpred = ` ${quoteIdentifier(tc.timestampColumn)} > $${params.length} AND `;
        oby = `ORDER BY ${quoteIdentifier(tc.timestampColumn)}`;
    }

    const query = `
        SELECT json_build_object(
            'tname', '${quoteConstant(tableNameString)}',
            'operation', 'upsert',
            'record', row_to_json(t)
        )::text as payload
        FROM (
            SELECT *
            FROM ${tableName} 
            WHERE ${sncpred} ${tscpred} 1=1
            ${oby}
        ) t
    `;

    return {query, params};
}

///////////////////////////
// DB Trigger Management
///////////////////////////

interface TriggerPayload {
    operation: TriggerOperation,
    tname: string,
    record: {[key: string]: unknown},
}

export type TriggerFunction<Key extends unknown[]> = (op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;
export type TriggerFunctionWF<Key extends unknown[]> = (ctx: WorkflowContext, op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;

class TriggerPayloadQueue
{
    notifyPayloads: TriggerPayload[] = [];
    catchupPayloads: TriggerPayload[] = [];
    catchupFinished: boolean = false;
    shutdown: boolean = false;
    waiting: ((value: TriggerPayload | null) => void)[] = [];

    enqueueCatchup(tp: TriggerPayload) {
        const resolve = this.waiting.shift();
        if (resolve) {
            resolve(tp);
        }
        else {
            this.catchupPayloads.push(tp);
        }
    }

    enqueueNotify(tp: TriggerPayload) {
        if (!this.catchupFinished) {
            this.notifyPayloads.push(tp);
            return;
        }

        const resolve = this.waiting.shift();
        if (resolve) {
            resolve(tp);
        }
        else {
            this.notifyPayloads.push(tp);
        }
    }

    async dequeue() : Promise<TriggerPayload | null> {
        if (this.shutdown) return null;
        if (this.catchupPayloads.length > 0) {
            return this.catchupPayloads.shift()!;
        }
        else if (this.catchupFinished && this.notifyPayloads.length > 0) {
            return this.notifyPayloads.shift()!;
        }
        else {
            return new Promise<TriggerPayload | null>((resolve) => {
                this.waiting.push(resolve);
            });
        }
    }

    finishCatchup() {
        this.catchupFinished = true;
        while(true) {
            if (!this.waiting[0] || !this.notifyPayloads[0]) break;
            this.waiting.shift()!(this.notifyPayloads.shift()!);
        }
    }

    stop() {
        this.shutdown = true;
        while(true) {
            const resolve = this.waiting.shift();
            if (!resolve) break;
            resolve(null);
        }
    }

    restart() {
        this.shutdown = false;
    }
}

export class DBOSDBTrigger implements DBOSEventReceiver {
    executor?: DBOSExecutorContext;
    listeners: DBNotificationListener[] = [];
    tableToReg: Map<string, DBOSEventReceiverRegistration[]> = new Map();
    shutdown: boolean = false;
    payloadQ: TriggerPayloadQueue = new TriggerPayloadQueue();
    dispatchLoops: Promise<void>[] = [];
    pollLoops: Promise<void>[] = [];

    constructor() {        
    }

    async initialize(executor: DBOSExecutorContext) {
        this.executor = executor;
        this.shutdown = false;
        this.payloadQ.restart();

        const hasTrigger: Set<string> = new Set();
        let hasAnyTrigger: boolean = false;
        const nname = 'dbos_table_update';

        const catchups: {query: string, params: unknown[]}[] = [];

        const regops = this.executor.getRegistrationsFor(this);
        for (const registeredOperation of regops) {
            const mo = registeredOperation.methodConfig as DBTriggerRegistration;

            if (mo.triggerConfig) {
                const mr = registeredOperation.methodReg;
                const cname = mr.className;
                const mname = mr.name;
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
                this.tableToReg.get(tstr)!.push(registeredOperation);

                if (!hasTrigger.has(tname)) {
                    const trigSQL = createTriggerSQL(tfname, trigname, tname, tstr, nname);
                    if (mo.triggerConfig.installDBTrigger) {
                        await this.executor.queryUserDB(trigSQL);
                    }
                    else {
                        this.executor.logger.info(` DBOS DB Trigger: For DB notifications, install the following SQL: \n${trigSQL}`);
                    }
                    hasTrigger.add(tname);
                    hasAnyTrigger = true;
                }

                if (mo.triggerIsWorkflow) {
                    const fullname = `${cname}.${mname}`;
                    // Initiate catchup work
                    const tc = mo.triggerConfig as DBTriggerWorkflowConfig;
                    let recseqnum: bigint | null = null;
                    let rectmstmp: number | null = null;
                    if (tc.sequenceNumColumn || tc.timestampColumn) {
                        const lasts = await this.executor.getEventDispatchState('trigger', fullname, 'last');
                        recseqnum = (lasts?.updateSeq) ? BigInt(lasts.updateSeq) : null;
                        rectmstmp = lasts?.updateTime ?? null;
                        if (recseqnum && tc.sequenceNumJitter) {
                            recseqnum -= BigInt(tc.sequenceNumJitter);
                        }
                        if (rectmstmp && tc.timestampSkewMS) {
                            rectmstmp -= tc.timestampSkewMS;
                        }
                    }

                    // Catchup query
                    catchups.push(createCatchupSql(tc, tname, tstr, recseqnum, rectmstmp));
                }
            }
        }

        if (hasAnyTrigger) {
            const handler = (msg: DBNotification) => {
                if (msg.channel !== nname) return;
                const payload = JSON.parse(msg.payload!) as TriggerPayload;
                this.payloadQ.enqueueNotify(payload);
            };

            this.listeners.push(await this.executor.userDBListen([nname], handler));
            this.executor.logger.info(`DB Triggers now listening for '${nname}'`);

            for (const q of catchups) {
                const catchupFunc =  async () => {
                    try {
                        const rows = await this.executor!.queryUserDB(q.query, q.params) as {payload: string}[];
                        for (const r of rows) {
                            const payload = JSON.parse(r.payload) as TriggerPayload;
                            this.payloadQ.enqueueCatchup(payload);
                        }
                    }
                    catch(e) {
                        console.log(e);
                        this.executor?.logger.error(e);
                    }
                };
  
                await catchupFunc();
            }

            this.payloadQ.finishCatchup();

            const payloadFunc = async(payload: TriggerPayload) => {
                for (const regOp of this.tableToReg.get(payload.tname) ?? []) {
                    const mr = regOp.methodReg;
                    const mo = regOp.methodConfig as DBTriggerRegistration;
                    if (!mo.triggerConfig) continue;
                    const key: unknown[] = [];
                    const keystr: string[] = [];
                    for (const kn of mo.triggerConfig?.recordIDColumns ?? []) {
                        const cv = Object.hasOwn(payload.record, kn) ? payload.record[kn] : undefined;
                        key.push(cv);
                        keystr.push(`${cv?.toString()}`);
                    }
                    try {
                        const cname = mr.className;
                        const mname = mr.name;
                        const fullname = `${cname}.${mname}`;
                        if (mo.triggerIsWorkflow) {
                            // Record the time of the wf kicked off (if given)
                            const tc = mo.triggerConfig as DBTriggerWorkflowConfig;
                            let recseqnum: bigint | null = null;
                            let rectmstmp: number | null = null;
                            if (tc.sequenceNumColumn) {
                                if (!Object.hasOwn(payload.record, tc.sequenceNumColumn)) {
                                    this.executor?.logger.warn(`DB Trigger on '${fullname}' specifies sequence number column '${tc.sequenceNumColumn}, but is not in database record.'`);
                                    continue;
                                }
                                const sn = payload.record[tc.sequenceNumColumn];
                                if (typeof(sn) === 'number') {
                                    recseqnum = BigInt(sn);
                                }
                                else if (typeof(sn) === 'string') {
                                    recseqnum = BigInt(sn);
                                }
                                else if (typeof(sn) === 'bigint') {
                                    recseqnum = sn;
                                }
                                else {
                                    this.executor?.logger.warn(`DB Trigger on '${fullname}' specifies sequence number column '${tc.sequenceNumColumn}, but received "${JSON.stringify(sn)}" instead of number'`);
                                    continue;
                                }
                            }
                            if (tc.timestampColumn) {
                                if (!Object.hasOwn(payload.record, tc.timestampColumn)) {
                                    this.executor?.logger.warn(`DB Trigger on '${fullname}' specifies timestamp column '${tc.timestampColumn}, but is not in database record.'`);
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
                                    this.executor?.logger.warn(`DB Trigger on '${fullname}' specifies timestamp column '${tc.timestampColumn}, but received "${JSON.stringify(ts)}" instead of date/number'`);
                                    continue;
                                }
                            }

                            const wfParams = {
                                workflowUUID: `dbt_${cname}_${mname}_${keystr.join('|')}`,
                                configuredInstance: null,
                                queueName: tc.queueName,
                            };
                            if (payload.operation === TriggerOperation.RecordDeleted) {
                                this.executor?.logger.warn(`DB Trigger ${fullname} on '${payload.tname}' witnessed a record deletion.   Record deletion workflow triggers are not supported.`);
                                continue;
                            }
                            if (payload.operation === TriggerOperation.RecordUpdated && recseqnum === null && rectmstmp === null) {
                                this.executor?.logger.warn(`DB Trigger ${fullname} on '${payload.tname}' witnessed a record update, but no sequence number / timestamp is defined.   Record update workflow triggers will not work in this case.`);
                                continue;
                            }
                            if (rectmstmp !== null) wfParams.workflowUUID += `_${rectmstmp}`;
                            if (recseqnum !== null) wfParams.workflowUUID += `_${recseqnum}`;
                            payload.operation = TriggerOperation.RecordUpserted;
                            await this.executor?.workflow(regOp.methodReg.registeredFunction as WorkflowFunction<unknown[], void>, wfParams, payload.operation, key, payload.record);

                            await this.executor?.upsertEventDispatchState({
                                service: 'trigger',
                                workflowFnName: fullname,
                                key: 'last',
                                value: '',
                                updateSeq: recseqnum ? recseqnum : undefined,
                                updateTime: rectmstmp ? rectmstmp : undefined,
                            });
                        }
                        else {
                            // Use original func, this may not be wrapped
                            const tfunc = mr.origFunction as TriggerFunction<unknown[]>;
                            await tfunc.call(undefined, payload.operation, key, payload.record);
                        }
                    }
                    catch(e) {
                        this.executor?.logger.warn(`Caught an exception in trigger handling for "${mr.className}.${mr.name}"`);
                        this.executor?.logger.warn(e);
                    }
                }
            }

            const processingFunc = async () => {
                while (true) {
                    const p = await this.payloadQ.dequeue();
                    if (p === null) break;
                    await payloadFunc(p);
                }
            }

            this.dispatchLoops.push(processingFunc())
        }
    }

    async destroy() {
        this.shutdown = true;
        this.payloadQ.stop();
        for (const l of this.listeners) {
            try {
                await l.close();
            }
            catch(e) {
                this.executor?.logger.warn(e);
            }
        }
        this.listeners = [];
        for (const p of this.dispatchLoops) {
            try {
                await p;
            }
            catch (e) {
                // Error in destroy, NBD
            }
        }
        this.dispatchLoops = [];
        for (const p of this.pollLoops) {
            try {
                await p;
            }
            catch (e) {
                // Error in destroy, NBD
            }
        }
        this.pollLoops = [];
        this.tableToReg = new Map();
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

let dbTrig: DBOSDBTrigger | undefined = undefined;

export function DBTrigger(triggerConfig: DBTriggerConfig) {
    function trigdec<This, Return, Key extends unknown[] >(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, operation: TriggerOperation, key: Key, record: unknown) => Promise<Return>>
    ) {
        if (!dbTrig) dbTrig = new DBOSDBTrigger();
        const {descriptor, receiverInfo} = associateMethodWithEventReceiver(dbTrig, target, propertyKey, inDescriptor);

        const triggerRegistration = receiverInfo as unknown as DBTriggerRegistration;
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
        if (!dbTrig) dbTrig = new DBOSDBTrigger();
        const {descriptor, receiverInfo} = associateMethodWithEventReceiver(dbTrig, target, propertyKey, inDescriptor);

        const triggerRegistration = receiverInfo as unknown as DBTriggerRegistration;
        triggerRegistration.triggerConfig = wfTriggerConfig;
        triggerRegistration.triggerIsWorkflow = true;

        return descriptor;
    }
    return trigdec;
}
