import { DBOS, DBOSLifecycleCallback, ExternalRegistration } from '@dbos-inc/dbos-sdk';

import { ClientBase, Notification } from 'pg';

export type DBNotification = Notification;
export type DBNotificationCallback = (n: DBNotification) => void;
export interface DBNotificationListener {
  close(): Promise<void>;
}

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
  tableName: string = '';
  // Database table schema (optional)
  schemaName?: string = undefined;

  // These identify the record, for elevation to function parameters
  recordIDColumns?: string[] = undefined;

  // Should DB trigger / notification be used?
  useDBNotifications?: boolean = false;

  // Should DB trigger be auto-installed?
  installDBTrigger?: boolean = false;

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

  // If not using triggers, frequency of polling, ms
  dbPollingInterval?: number = 5000;
}

export interface DBConfig {
  connect: () => Promise<ClientBase>;
  disconnect: (c: ClientBase) => Promise<void>;
  query: <R>(sql: string, params?: unknown[]) => Promise<R[]>;
}

export async function dbListen(
  cfg: DBConfig,
  channels: string[],
  callback: DBNotificationCallback,
): Promise<DBNotificationListener> {
  const notificationsClient = await cfg.connect();
  for (const nname of channels) {
    await notificationsClient.query(`LISTEN ${nname};`);
  }

  notificationsClient.on('notification', callback);

  return {
    close: async () => {
      for (const nname of channels) {
        try {
          await notificationsClient.query(`UNLISTEN ${nname};`);
        } catch (e) {
          DBOS.logger.warn(e);
        }
        await cfg.disconnect(notificationsClient);
      }
    },
  };
}

interface DBTriggerRegistration {
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
) {
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
  tc: DBTriggerConfig,
  tableName: string,
  tableNameString: string,
  startSeqNum?: bigint | null,
  startTimeStamp?: number | null,
) {
  // Query string
  let sncpred = '';
  let oby = '';
  const params = [];
  if (tc.sequenceNumColumn && startSeqNum) {
    params.push(startSeqNum);
    sncpred = ` ${quoteIdentifier(tc.sequenceNumColumn)} > $${params.length} AND `;
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

  return { query, params };
}

///////////////////////////
// DB Trigger Management
///////////////////////////
interface TriggerPayload {
  operation: TriggerOperation;
  tname: string;
  record: { [key: string]: unknown };
}

export type TriggerFunction<Key extends unknown[]> = (op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;
export type TriggerFunctionWF<Key extends unknown[]> = (op: TriggerOperation, key: Key, rec: unknown) => Promise<void>;

class TriggerPayloadQueue {
  notifyPayloads: TriggerPayload[] = [];
  catchupPayloads: TriggerPayload[] = [];
  catchupFinished: boolean = false;
  shutdown: boolean = false;
  waiting: ((value: TriggerPayload | null) => void)[] = [];

  enqueueCatchup(tp: TriggerPayload) {
    const resolve = this.waiting.shift();
    if (resolve) {
      resolve(tp);
    } else {
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
    } else {
      this.notifyPayloads.push(tp);
    }
  }

  async dequeue(): Promise<TriggerPayload | null> {
    if (this.shutdown) return null;
    if (this.catchupPayloads.length > 0) {
      return this.catchupPayloads.shift()!;
    } else if (this.catchupFinished && this.notifyPayloads.length > 0) {
      return this.notifyPayloads.shift()!;
    } else {
      return new Promise<TriggerPayload | null>((resolve) => {
        this.waiting.push(resolve);
      });
    }
  }

  finishCatchup() {
    this.catchupFinished = true;
    while (true) {
      if (!this.waiting[0] || !this.notifyPayloads[0]) break;
      this.waiting.shift()!(this.notifyPayloads.shift()!);
    }
  }

  stop() {
    this.shutdown = true;
    while (true) {
      const resolve = this.waiting.shift();
      if (!resolve) break;
      resolve(null);
    }
  }

  restart() {
    this.shutdown = false;
  }
}

export class DBTrigger implements DBOSLifecycleCallback {
  listeners: DBNotificationListener[] = [];
  tableToReg: Map<string, ExternalRegistration[]> = new Map();
  shutdown: boolean = false;
  payloadQ: TriggerPayloadQueue = new TriggerPayloadQueue();
  dispatchLoops: Promise<void>[] = [];
  pollers: DBTPollingLoop[] = [];
  pollLoops: Promise<void>[] = [];

  constructor(readonly db: DBConfig) {
    DBOS.registerLifecycleCallback(this);
  }

  async createPoll(tc: DBTriggerConfig, fullname: string, tname: string, tstr: string) {
    // Initiate catchup work
    let recseqnum: bigint | null = null;
    let rectmstmp: number | null = null;
    if (tc.sequenceNumColumn || tc.timestampColumn) {
      const lasts = await DBOS.getEventDispatchState('trigger', fullname, 'last');
      recseqnum = lasts?.updateSeq ? BigInt(lasts.updateSeq) : null;
      rectmstmp = lasts?.updateTime ?? null;
      if (recseqnum && tc.sequenceNumJitter) {
        recseqnum -= BigInt(tc.sequenceNumJitter);
      }
      if (rectmstmp && tc.timestampSkewMS) {
        rectmstmp -= tc.timestampSkewMS;
      }
    }

    // Catchup query
    return createCatchupSql(tc, tname, tstr, recseqnum, rectmstmp);
  }

  async initialize() {
    this.shutdown = false;
    this.payloadQ.restart();
    this.tableToReg.clear();

    const hasTrigger: Set<string> = new Set();
    let hasAnyTrigger: boolean = false;
    let hasAnyPoller: boolean = false;
    const nname = 'dbos_table_update';

    const catchups: { query: string; params: unknown[] }[] = [];

    const regops = DBOS.getAssociatedInfo(this);
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
        const fullname = `${cname}.${mname}`;

        if (!this.tableToReg.has(tstr)) {
          this.tableToReg.set(tstr, []);
        }
        this.tableToReg.get(tstr)!.push(registeredOperation);

        let registeredThis = false;
        if (mo.triggerConfig.useDBNotifications || mo.triggerConfig.installDBTrigger) {
          if (!hasTrigger.has(tname)) {
            const trigSQL = createTriggerSQL(tfname, trigname, tname, tstr, nname);
            if (mo.triggerConfig.installDBTrigger) {
              await this.db.query(trigSQL);
            } else {
              DBOS.logger.info(` DBOS DB Trigger: For DB notifications, install the following SQL: \n${trigSQL}`);
            }
            hasTrigger.add(tname);
            hasAnyTrigger = true;
          }
          registeredThis = true;
        }

        if (mo.triggerIsWorkflow) {
          // Initiate catchup work
          const tc = mo.triggerConfig;
          const catchup = await this.createPoll(tc, fullname, tname, tstr);

          // Catchup query
          catchups.push(catchup);

          // Launch poller if needed
          if (!(mo.triggerConfig.useDBNotifications || mo.triggerConfig.installDBTrigger) && tc.dbPollingInterval) {
            const poller = new DBTPollingLoop(this, tc, registeredOperation, tname, tstr);
            this.pollers.push(poller);
            this.pollLoops.push(poller.startLoop());
            hasAnyPoller = true;
            registeredThis = true;
          }
        }

        if (!registeredThis) {
          DBOS.logger.warn(
            `The DB trigger configuration for ${fullname} does not specify to use DB notifications, nor does it provide a polling interval, and will therefore never run.`,
          );
        }
      }
    }

    if (hasAnyTrigger || hasAnyPoller) {
      if (hasAnyTrigger) {
        const handler = (msg: DBNotification) => {
          if (msg.channel !== nname) return;
          const payload = JSON.parse(msg.payload!) as TriggerPayload;
          this.payloadQ.enqueueNotify(payload);
        };

        this.listeners.push(await dbListen(this.db, [nname], handler));
        DBOS.logger.info(`DB Triggers now listening for '${nname}'`);

        for (const q of catchups) {
          const catchupFunc = async () => {
            try {
              const rows = await this.db.query<{ payload: string }>(q.query, q.params);
              for (const r of rows) {
                const payload = JSON.parse(r.payload) as TriggerPayload;
                this.payloadQ.enqueueCatchup(payload);
              }
            } catch (e) {
              DBOS.logger.error(e);
            }
          };

          await catchupFunc();
        }

        this.payloadQ.finishCatchup();
      }

      const payloadFunc = async (payload: TriggerPayload) => {
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
              const tc = mo.triggerConfig;
              let recseqnum: bigint | null = null;
              let rectmstmp: number | null = null;
              if (tc.sequenceNumColumn) {
                if (!Object.hasOwn(payload.record, tc.sequenceNumColumn)) {
                  DBOS.logger.warn(
                    `DB Trigger on '${fullname}' specifies sequence number column '${tc.sequenceNumColumn}, but is not in database record.'`,
                  );
                  continue;
                }
                const sn = payload.record[tc.sequenceNumColumn];
                if (typeof sn === 'number') {
                  recseqnum = BigInt(sn);
                } else if (typeof sn === 'string') {
                  recseqnum = BigInt(sn);
                } else if (typeof sn === 'bigint') {
                  recseqnum = sn;
                } else {
                  DBOS.logger.warn(
                    `DB Trigger on '${fullname}' specifies sequence number column '${tc.sequenceNumColumn}, but received "${JSON.stringify(sn)}" instead of number'`,
                  );
                  continue;
                }
                keystr.push(`${recseqnum.toString()}`);
              }
              if (tc.timestampColumn) {
                if (!Object.hasOwn(payload.record, tc.timestampColumn)) {
                  DBOS.logger.warn(
                    `DB Trigger on '${fullname}' specifies timestamp column '${tc.timestampColumn}, but is not in database record.'`,
                  );
                  continue;
                }
                const ts = payload.record[tc.timestampColumn];
                if (ts instanceof Date) {
                  rectmstmp = ts.getTime();
                } else if (typeof ts === 'number') {
                  rectmstmp = ts;
                } else if (typeof ts === 'string') {
                  rectmstmp = new Date(ts).getTime();
                } else {
                  DBOS.logger.warn(
                    `DB Trigger on '${fullname}' specifies timestamp column '${tc.timestampColumn}, but received "${JSON.stringify(ts)}" instead of date/number'`,
                  );
                  continue;
                }
                keystr.push(`${rectmstmp}`);
              }

              const wfParams = {
                workflowUUID: `dbt_${cname}_${mname}_${keystr.join('|')}`,
                configuredInstance: null,
                queueName: tc.queueName,
              };
              if (payload.operation === TriggerOperation.RecordDeleted) {
                DBOS.logger.warn(
                  `DB Trigger ${fullname} on '${payload.tname}' witnessed a record deletion.   Record deletion workflow triggers are not supported.`,
                );
                continue;
              }
              if (payload.operation === TriggerOperation.RecordUpdated && recseqnum === null && rectmstmp === null) {
                DBOS.logger.warn(
                  `DB Trigger ${fullname} on '${payload.tname}' witnessed a record update, but no sequence number / timestamp is defined.   Record update workflow triggers will not work in this case.`,
                );
                continue;
              }
              if (rectmstmp !== null) wfParams.workflowUUID += `_${rectmstmp}`;
              if (recseqnum !== null) wfParams.workflowUUID += `_${recseqnum}`;
              payload.operation = TriggerOperation.RecordUpserted;
              DBOS.logger.debug(`Executing ${fullname} on ID ${wfParams.workflowUUID} queue ${wfParams.queueName}`);
              await DBOS.startWorkflow(regOp.methodReg.registeredFunction as TriggerFunctionWF<unknown[]>, wfParams)(
                payload.operation,
                key,
                payload.record,
              );

              await DBOS.upsertEventDispatchState({
                service: 'trigger',
                workflowFnName: fullname,
                key: 'last',
                value: '',
                updateSeq: recseqnum ? recseqnum : undefined,
                updateTime: rectmstmp ? rectmstmp : undefined,
              });
            } else {
              // Use original func, this may not be wrapped
              (await regOp.methodReg.invoke(undefined, [payload.operation, key, payload.record])) as TriggerFunction<
                unknown[]
              >;
            }
          } catch (e) {
            DBOS.logger.warn(`Caught an exception in trigger handling for "${mr.className}.${mr.name}"`);
            DBOS.logger.warn(e);
          }
        }
      };

      const processingFunc = async () => {
        while (true) {
          const p = await this.payloadQ.dequeue();
          if (p === null) break;
          await payloadFunc(p);
        }
      };

      this.dispatchLoops.push(processingFunc());
    }
  }

  async destroy() {
    this.shutdown = true;
    this.payloadQ.stop();
    for (const l of this.listeners) {
      try {
        await l.close();
      } catch (e) {
        DBOS.logger.warn(e);
      }
    }
    this.listeners = [];
    for (const p of this.dispatchLoops) {
      try {
        await p;
      } catch (e) {
        // Error in destroy, NBD
      }
    }
    this.dispatchLoops = [];
    for (const p of this.pollers) {
      p.setStopLoopFlag();
    }
    this.pollers = [];
    for (const p of this.pollLoops) {
      try {
        await p;
      } catch (e) {
        // Error in destroy, NBD
      }
    }
    this.pollLoops = [];
    this.tableToReg = new Map();
  }

  logRegisteredEndpoints() {
    DBOS.logger.info('Database trigger endpoints registered:');
    const eps = DBOS.getAssociatedInfo(this);

    for (const e of eps) {
      const { methodConfig, methodReg } = e;
      const mo = methodConfig as DBTriggerRegistration;
      if (mo.triggerConfig) {
        const cname = methodReg.className;
        const mname = methodReg.name;
        const tname = mo.triggerConfig.schemaName
          ? `${mo.triggerConfig.schemaName}.${mo.triggerConfig.tableName}`
          : mo.triggerConfig.tableName;
        DBOS.logger.info(`    ${tname} -> ${cname}.${mname}`);
      }
    }
  }

  trigger(triggerConfig: DBTriggerConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const dbt = this;
    function trigdec<This, Return, Key extends unknown[]>(
      target: object,
      propertyKey: string,
      descriptor: TypedPropertyDescriptor<
        (this: This, operation: TriggerOperation, key: Key, record: unknown) => Promise<Return>
      >,
    ) {
      const { regInfo } = DBOS.associateFunctionWithInfo(dbt, descriptor.value!, {
        ctorOrProto: target,
        name: propertyKey,
      });

      const triggerRegistration = regInfo as DBTriggerRegistration;

      triggerRegistration.triggerConfig = triggerConfig;
      triggerRegistration.triggerIsWorkflow = false;

      return descriptor;
    }
    return trigdec;
  }

  triggerWorkflow(wfTriggerConfig: DBTriggerConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const dbt = this;
    function trigdec<This, Return, Key extends unknown[]>(
      target: object,
      propertyKey: string,
      descriptor: TypedPropertyDescriptor<
        (this: This, operation: TriggerOperation, key: Key, record: unknown) => Promise<Return>
      >,
    ) {
      const { regInfo } = DBOS.associateFunctionWithInfo(dbt, descriptor.value!, {
        ctorOrProto: target,
        name: propertyKey,
      });

      const triggerRegistration = regInfo as DBTriggerRegistration;
      triggerRegistration.triggerConfig = wfTriggerConfig;
      triggerRegistration.triggerIsWorkflow = true;

      return descriptor;
    }
    return trigdec;
  }
}

class DBTPollingLoop {
  private isRunning: boolean = false;
  private interruptResolve?: () => void;
  private trigMethodName: string;

  constructor(
    readonly trigER: DBTrigger,
    readonly trigReg: DBTriggerConfig,
    readonly reg: ExternalRegistration,
    readonly tname: string,
    readonly tstr: string,
  ) {
    this.trigMethodName = `${reg.methodReg.className}.${reg.methodReg.name}`;
  }

  async startLoop(): Promise<void> {
    // See if the exec time is available in durable storage...
    let execTime = new Date().getTime();

    this.isRunning = true;
    while (this.isRunning) {
      const nextExecTime = execTime + (this.trigReg.dbPollingInterval ?? 5000);
      const sleepTime = nextExecTime - new Date().getTime();
      execTime = nextExecTime;

      if (sleepTime > 0) {
        // Wait for either the timeout or an interruption
        let timer: NodeJS.Timeout;
        const timeoutPromise = new Promise<void>((resolve) => {
          timer = setTimeout(() => {
            resolve();
          }, sleepTime);
        });
        await Promise.race([timeoutPromise, new Promise<void>((_, reject) => (this.interruptResolve = reject))]).catch(
          () => {
            DBOS.logger.debug('Trigger polling loop interrupted!');
          },
        ); // Interrupt sleep throws
        clearTimeout(timer!);
      }

      if (!this.isRunning) {
        break;
      }

      // To catch-up poll
      const catchup = await this.trigER.createPoll(this.trigReg, this.trigMethodName, this.tname, this.tstr);
      try {
        const rows = await this.trigER.db.query<{ payload: string }>(catchup.query, catchup.params);
        for (const r of rows) {
          const payload = JSON.parse(r.payload) as TriggerPayload;
          // Post workflows back to dispatch loop; queue processor will do the updates
          this.trigER.payloadQ.enqueueCatchup(payload);
        }
      } catch (e) {
        DBOS.logger.error(e);
      }
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
