/* eslint-disable @typescript-eslint/no-explicit-any */
import { DBOSError, DBOSInitializationError, DBOSWorkflowConflictUUIDError, DBOSNotRegisteredError, DBOSDebuggerError, DBOSConfigKeyTypeError } from "./error";
import {
  InvokedHandle,
  Workflow,
  WorkflowConfig,
  WorkflowContext,
  WorkflowHandle,
  WorkflowParams,
  RetrievedHandle,
  WorkflowContextImpl,
  WorkflowStatus,
  StatusString,
  BufferedResult,
} from './workflow';

import { IsolationLevel, Transaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, Communicator } from './communicator';
import { TelemetryCollector } from './telemetry/collector';
import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import { TelemetryExporter } from './telemetry/exporters';
import { TelemetryConfig } from './telemetry';
import { Pool, PoolClient, PoolConfig, QueryResultRow } from 'pg';
import { SystemDatabase, PostgresSystemDatabase, WorkflowStatusInternal } from './system_database';
import { v4 as uuidv4 } from 'uuid';
import {
  PGNodeUserDatabase,
  PrismaUserDatabase,
  UserDatabase,
  TypeORMDatabase,
  UserDatabaseName,
  KnexUserDatabase,
} from './user_database';
import { MethodRegistrationBase, getRegisteredOperations, getOrCreateClassRegistration, MethodRegistration, getRegisteredMethodClassName, getRegisteredMethodName, getConfiguredInstance, ConfiguredInstance, getAllRegisteredClasses } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import { DBOSContextImpl, InitContext } from './context';
import { HandlerRegistrationBase } from './httpServer/handler';
import { WorkflowContextDebug } from './debugger/debug_workflow';
import { serializeError } from 'serialize-error';
import { DBOSJSON, sleepms } from './utils';
import path from 'node:path';
import { StoredProcedure, StoredProcedureConfig } from './procedure';
import { NoticeMessage } from "pg-protocol/dist/messages";
import { DBOSEventReceiver, DBOSExecutorContext } from ".";

import { get } from "lodash";

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface DBOSNull { }
export const dbosNull: DBOSNull = {};

/* Interface for DBOS configuration */
export interface DBOSConfig {
  readonly poolConfig: PoolConfig;
  readonly userDbclient?: UserDatabaseName;
  readonly telemetry?: TelemetryConfig;
  readonly system_database: string;
  readonly env?: Record<string, string>;
  readonly application?: object;
  readonly debugProxy?: string;
  readonly debugMode?: boolean;
  readonly appVersion?: string;
  readonly http?: {
    readonly cors_middleware?: boolean;
    readonly credentials?: boolean;
    readonly allowed_origins?: string[];
  };
}

interface WorkflowInfo {
  workflow: Workflow<unknown[], unknown>;
  config: WorkflowConfig;
}

interface TransactionInfo {
  transaction: Transaction<unknown[], unknown>;
  config: TransactionConfig;
}

interface CommunicatorInfo {
  communicator: Communicator<unknown[], unknown>;
  config: CommunicatorConfig;
}

interface ProcedureInfo {
  procedure: StoredProcedure<unknown>;
  config: StoredProcedureConfig;
}

interface InternalWorkflowParams extends WorkflowParams {
  readonly tempWfType?: string;
  readonly tempWfName?: string;
  readonly tempWfClass?: string;
}

export const OperationType = {
  HANDLER: "handler",
  WORKFLOW: "workflow",
  TRANSACTION: "transaction",
  COMMUNICATOR: "communicator",
  PROCEDURE: "procedure",
} as const;

const TempWorkflowType = {
  transaction: "transaction",
  procedure: "procedure",
  external: "external",
  send: "send",
} as const;

export class DBOSExecutor implements DBOSExecutorContext {
  initialized: boolean;
  // User Database
  userDatabase: UserDatabase = null as unknown as UserDatabase;
  // System Database
  readonly systemDatabase: SystemDatabase;
  readonly procedurePool: Pool;

  // Temporary workflows are created by calling transaction/send/recv directly from the executor class
  static readonly tempWorkflowName = "temp_workflow";

  readonly workflowInfoMap: Map<string, WorkflowInfo> = new Map([
    // We initialize the map with an entry for temporary workflows.
    [
      DBOSExecutor.tempWorkflowName,
      {
        workflow: async () => {
          this.logger.error("UNREACHABLE: Indirect invoke of temp workflow");
          return Promise.resolve();
        },
        config: {},
      },
    ],
  ]);
  readonly transactionInfoMap: Map<string, TransactionInfo> = new Map();
  readonly communicatorInfoMap: Map<string, CommunicatorInfo> = new Map();
  readonly procedureInfoMap: Map<string, ProcedureInfo> = new Map();
  readonly registeredOperations: Array<MethodRegistrationBase> = [];
  readonly pendingWorkflowMap: Map<string, Promise<unknown>> = new Map(); // Map from workflowUUID to workflow promise
  readonly workflowResultBuffer: Map<string, Map<number, BufferedResult>> = new Map(); // Map from workflowUUID to its remaining result buffer.

  readonly telemetryCollector: TelemetryCollector;
  readonly flushBufferIntervalMs: number = 1000;
  readonly flushBufferID: NodeJS.Timeout;
  isFlushingBuffers = false;

  static readonly defaultNotificationTimeoutSec = 60;

  readonly debugMode: boolean;
  readonly debugProxy: string | undefined;
  static systemDBSchemaName = "dbos";

  readonly logger: Logger;
  readonly tracer: Tracer;
  // eslint-disable-next-line @typescript-eslint/ban-types
  entities: Function[] = [];

  eventReceivers: DBOSEventReceiver[] = [];

  /* WORKFLOW EXECUTOR LIFE CYCLE MANAGEMENT */
  constructor(readonly config: DBOSConfig, systemDatabase?: SystemDatabase) {
    this.debugMode = config.debugMode ?? false;
    this.debugProxy = config.debugProxy;

    // Set configured environment variables
    if (config.env) {
      for (const [key, value] of Object.entries(config.env)) {
        process.env[key] = value;
      }
    }

    if (config.telemetry?.OTLPExporter) {
      const OTLPExporter = new TelemetryExporter(config.telemetry.OTLPExporter);
      this.telemetryCollector = new TelemetryCollector(OTLPExporter);
    } else {
      // We always setup a collector to drain the signals queue, even if we don't have an exporter.
      this.telemetryCollector = new TelemetryCollector();
    }
    this.logger = new Logger(this.telemetryCollector, this.config.telemetry?.logs);
    this.tracer = new Tracer(this.telemetryCollector);

    if (this.debugMode) {
      this.logger.info("Running in debug mode!");
      if (this.debugProxy) {
        try {
          const url = new URL(this.config.debugProxy!);
          this.config.poolConfig.host = url.hostname;
          this.config.poolConfig.port = parseInt(url.port, 10);
          this.logger.info(`Debugging mode proxy: ${this.config.poolConfig.host}:${this.config.poolConfig.port}`);
        } catch (err) {
          this.logger.error(err);
          throw err;
        }
      }
    }

    this.procedurePool = new Pool(this.config.poolConfig);

    if (systemDatabase) {
      this.logger.debug("Using provided system database"); // XXX print the name or something
      this.systemDatabase = systemDatabase;
    } else {
      this.logger.debug("Using Postgres system database");
      this.systemDatabase = new PostgresSystemDatabase(this.config.poolConfig, this.config.system_database, this.logger);
    }

    this.flushBufferID = setInterval(() => {
      if (!this.debugMode && !this.isFlushingBuffers) {
        this.isFlushingBuffers = true;
        void this.flushWorkflowBuffers();
      }
    }, this.flushBufferIntervalMs);
    this.logger.debug("Started workflow status buffer worker");

    this.initialized = false;
  }

  configureDbClient() {
    const userDbClient = this.config.userDbclient;
    const userDBConfig = this.config.poolConfig;
    if (userDbClient === UserDatabaseName.PRISMA) {
      // TODO: make Prisma work with debugger proxy.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const { PrismaClient } = require(path.join(process.cwd(), "node_modules", "@prisma", "client")); // Find the prisma client in the node_modules of the current project
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call
      this.userDatabase = new PrismaUserDatabase(new PrismaClient(
        {
          datasources: {
            db: {
              url: `postgresql://${userDBConfig.user}:${userDBConfig.password as string}@${userDBConfig.host}:${userDBConfig.port}/${userDBConfig.database}`,
            },
          }
        }
      ));
      this.logger.debug("Loaded Prisma user database");
    } else if (userDbClient === UserDatabaseName.TYPEORM) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const DataSourceExports = require("typeorm");
      try {
        this.userDatabase = new TypeORMDatabase(
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          new DataSourceExports.DataSource({
            type: "postgres", // perhaps should move to config file
            host: userDBConfig.host,
            port: userDBConfig.port,
            username: userDBConfig.user,
            password: userDBConfig.password,
            database: userDBConfig.database,
            entities: this.entities,
            ssl: userDBConfig.ssl,
          })
        );
      } catch (s) {
        (s as Error).message = `Error loading TypeORM user database: ${(s as Error).message}`;
        this.logger.error(s);
      }
      this.logger.debug("Loaded TypeORM user database");
    } else if (userDbClient === UserDatabaseName.KNEX) {
      const knexConfig: Knex.Config = {
        client: "postgres",
        connection: {
          host: userDBConfig.host,
          port: userDBConfig.port,
          user: userDBConfig.user,
          password: userDBConfig.password,
          database: userDBConfig.database,
          ssl: userDBConfig.ssl,
        },
      };
      this.userDatabase = new KnexUserDatabase(knex(knexConfig));
      this.logger.debug("Loaded Knex user database");
    } else {
      this.userDatabase = new PGNodeUserDatabase(userDBConfig);
      this.logger.debug("Loaded Postgres user database");
    }
  }

  #registerClass(cls: object) {
    const registeredClassOperations = getRegisteredOperations(cls);
    this.registeredOperations.push(...registeredClassOperations);
    for (const ro of registeredClassOperations) {
      if (ro.workflowConfig) {
        this.#registerWorkflow(ro);
      } else if (ro.txnConfig) {
        this.#registerTransaction(ro);
      } else if (ro.commConfig) {
        this.#registerCommunicator(ro);
      } else if (ro.procConfig) {
        this.#registerProcedure(ro);
      }
      for (const [evtRcvr, _cfg] of ro.eventReceiverInfo) {
        if (!this.eventReceivers.includes(evtRcvr)) this.eventReceivers.push(evtRcvr);
      }
    }
  }

  getRegistrationsFor(obj: DBOSEventReceiver) {
    const res: {methodConfig: unknown, classConfig: unknown, methodReg: MethodRegistrationBase}[] = [];
    for (const r of this.registeredOperations) {
      if (!r.eventReceiverInfo.has(obj)) continue;
      const methodConfig = r.eventReceiverInfo.get(obj)!;
      const classConfig = r.defaults?.eventReceiverInfo.get(obj) ?? {};
      res.push({methodReg: r, methodConfig, classConfig})
    }
    return res;
  }

  async init(classes?: object[]): Promise<void> {
    if (this.initialized) {
      this.logger.error("Workflow executor already initialized!");
      return;
    }

    if (!classes || !classes.length) {
      classes = getAllRegisteredClasses();
    }

    type AnyConstructor = new (...args: unknown[]) => object;
    try {
      for (const cls of classes) {
        const reg = getOrCreateClassRegistration(cls as AnyConstructor);
        if (reg.ormEntities.length > 0) {
          this.entities = this.entities.concat(reg.ormEntities);
          this.logger.debug(`Loaded ${reg.ormEntities.length} ORM entities`);
        }
      }

      this.configureDbClient();

      if (!this.userDatabase) {
        this.logger.error("No user database configured!");
        throw new DBOSInitializationError("No user database configured!");
      }

      for (const cls of classes) {
        this.#registerClass(cls);
      }

      // Debug mode doesn't need to initialize the DBs. Everything should appear to be read-only.
      await this.userDatabase.init(this.debugMode);
      if (!this.debugMode) {
        await this.systemDatabase.init();
        await this.recoverPendingWorkflows();
      }
    } catch (err) {
      (err as Error).message = `failed to initialize workflow executor: ${(err as Error).message}`;
      throw new DBOSInitializationError(`${(err as Error).message}`);
    }
    this.initialized = true;

    // Only execute init code if under non-debug mode
    if (!this.debugMode) {
      for (const cls of classes) {
        // Init its configurations
        const creg = getOrCreateClassRegistration(cls as AnyConstructor);
        for (const [_cfgname, cfg] of creg.configuredInstances) {
          await cfg.initialize(new InitContext(this));
        }
      }

      for (const v of this.registeredOperations) {
        const m = v as MethodRegistration<unknown, unknown[], unknown>;
        if (m.init === true) {
          this.logger.debug("Executing init method: " + m.name);
          await m.origFunction(new InitContext(this));
        }
      }
    }

    this.logger.info("Workflow executor initialized");
  }

  #logNotice(msg: NoticeMessage) {
    switch (msg.severity) {
      case "INFO":
      case "LOG":
      case "NOTICE":
        this.logger.info(msg.message);
        break;
      case "WARNING":
        this.logger.warn(msg.message);
        break;
      case "DEBUG":
        this.logger.debug(msg.message);
        break;
      case "ERROR":
      case "FATAL":
      case "PANIC":
        this.logger.error(msg.message);
        break;
      default:
        this.logger.error(`Unknown notice severity: ${msg.severity} - ${msg.message}`);
    }
  }

  async callProcedure<R extends QueryResultRow = any>(proc: StoredProcedure<unknown>, args: unknown[]): Promise<R[]> {
    const client = await this.procedurePool.connect();
    const log = (msg: NoticeMessage) => this.#logNotice(msg);

    const procClassName = this.getProcedureClassName(proc);
    const plainProcName = `${procClassName}_${proc.name}_p`;
    const procName = this.config.appVersion
      ? `v${this.config.appVersion}_${plainProcName}`
      : plainProcName;

    const sql = `CALL "${procName}"(${args.map((_v, i) => `$${i + 1}`).join()});`;
    try {
      client.on('notice', log);
      return await client.query<R>(sql, args).then(value => value.rows);
    } finally {
      client.off('notice', log);
      client.release();
    }
  }

  async destroy() {
    if (this.pendingWorkflowMap.size > 0) {
      this.logger.info("Waiting for pending workflows to finish.");
      await Promise.allSettled(this.pendingWorkflowMap.values());
    }
    clearInterval(this.flushBufferID);
    if (!this.debugMode && !this.isFlushingBuffers) {
      // Don't flush the buffers if we're already flushing them in the background.
      await this.flushWorkflowBuffers();
    }
    while (this.isFlushingBuffers) {
      this.logger.info("Waiting for result buffers to be exported.");
      await sleepms(1000);
    }
    await this.systemDatabase.destroy();
    if (this.userDatabase) {
      await this.userDatabase.destroy();
    }
    await this.procedurePool.end();
    await this.logger.destroy();
  }

  /* WORKFLOW OPERATIONS */

  #registerWorkflow(ro :MethodRegistrationBase) {
    const wf = ro.registeredFunction as Workflow<unknown[], unknown>;
    if (wf.name === DBOSExecutor.tempWorkflowName) {
      throw new DBOSError(`Unexpected use of reserved workflow name: ${wf.name}`);
    }
    const wfn = ro.className + '.' + ro.name;
    if (this.workflowInfoMap.has(wfn)) {
      throw new DBOSError(`Repeated workflow name: ${wfn}`);
    }
    const workflowInfo: WorkflowInfo = {
      workflow: wf,
      config: {...ro.workflowConfig},
    };
    this.workflowInfoMap.set(wfn, workflowInfo);
    this.logger.debug(`Registered workflow ${wfn}`);
  }

  #registerTransaction(ro: MethodRegistrationBase) {
    const txf = ro.registeredFunction as Transaction<unknown[], unknown>;
    const tfn = ro.className + '.' + ro.name;

    if (this.transactionInfoMap.has(tfn)) {
      throw new DBOSError(`Repeated Transaction name: ${tfn}`);
    }
    const txnInfo: TransactionInfo = {
      transaction: txf,
      config: {...ro.txnConfig},
    };
    this.transactionInfoMap.set(tfn, txnInfo);
    this.logger.debug(`Registered transaction ${tfn}`);
  }

  #registerCommunicator(ro: MethodRegistrationBase) {
    const comm = ro.registeredFunction as Communicator<unknown[], unknown>;
    const cfn = ro.className + '.' + ro.name;
    if (this.communicatorInfoMap.has(cfn)) {
      throw new DBOSError(`Repeated Commmunicator name: ${cfn}`);
    }
    const commInfo: CommunicatorInfo = {
      communicator: comm,
      config: {...ro.commConfig},
    };
    this.communicatorInfoMap.set(cfn, commInfo);
    this.logger.debug(`Registered communicator ${cfn}`);
  }

  #registerProcedure(ro: MethodRegistrationBase) {
    const proc = ro.registeredFunction as StoredProcedure<unknown>;
    const cfn = ro.className + '.' + ro.name;

    if (this.procedureInfoMap.has(cfn)) {
      throw new DBOSError(`Repeated Procedure name: ${cfn}`);
    }
    const procInfo: ProcedureInfo = {
      procedure: proc,
      config: {...ro.procConfig},
    };
    this.procedureInfoMap.set(cfn, procInfo);
    this.logger.debug(`Registered stored proc ${cfn}`);
  }

  getWorkflowInfo(wf: Workflow<unknown[], unknown>) {
    const wfname = (wf.name === DBOSExecutor.tempWorkflowName)
      ? wf.name
      : getRegisteredMethodClassName(wf) + '.' + wf.name;
    return this.workflowInfoMap.get(wfname);
  }
  getWorkflowInfoByStatus(wf: WorkflowStatus) {
    const wfname = wf.workflowClassName + '.' + wf.workflowName;
    let wfInfo = this.workflowInfoMap.get(wfname);
    if (!wfInfo && !wf.workflowClassName) {
      for (const [_wfn, wfr] of this.workflowInfoMap) {
        if (wf.workflowName === wfr.workflow.name) {
          if (wfInfo) {
            throw new DBOSError(`Recovered workflow function name '${wf.workflowName}' is ambiguous.  The ambiguous name was recently added; remove it and recover pending workflows before re-adding the new function.`);
          }
          else {
            wfInfo = wfr;
          }
        }
      }
    }

    return {wfInfo, configuredInst: getConfiguredInstance(wf.workflowClassName, wf.workflowConfigName)};
  }

  getTransactionInfo(tf: Transaction<unknown[], unknown>) {
    const tfname = getRegisteredMethodClassName(tf) + '.' + tf.name;
    return this.transactionInfoMap.get(tfname);
  }
  getTransactionInfoByNames(className: string, functionName: string, cfgName: string) {
    const tfname = className + '.' + functionName;
    let txnInfo: TransactionInfo | undefined = this.transactionInfoMap.get(tfname);

    if (!txnInfo && !className) {
      for (const [_wfn, tfr] of this.transactionInfoMap) {
        if (functionName === tfr.transaction.name) {
          if (txnInfo) {
            throw new DBOSError(`Recovered transaction function name '${functionName}' is ambiguous.  The ambiguous name was recently added; remove it and recover pending workflows before re-adding the new function.`);
          }
          else {
            txnInfo = tfr;
          }
        }
      }
    }

    return {txnInfo, clsInst: getConfiguredInstance(className, cfgName)};
  }

  getCommunicatorInfo(cf: Communicator<unknown[], unknown>) {
    const cfname = getRegisteredMethodClassName(cf) + '.' + cf.name;
    return this.communicatorInfoMap.get(cfname);
  }
  getCommunicatorInfoByNames(className: string, functionName: string, cfgName: string) {
    const cfname = className + '.' + functionName;
    let commInfo: CommunicatorInfo | undefined = this.communicatorInfoMap.get(cfname);

    if (!commInfo && !className) {
      for (const [_wfn, cfr] of this.communicatorInfoMap) {
        if (functionName === cfr.communicator.name) {
          if (commInfo) {
            throw new DBOSError(`Recovered communicator function name '${functionName}' is ambiguous.  The ambiguous name was recently added; remove it and recover pending workflows before re-adding the new function.`);
          }
          else {
            commInfo = cfr;
          }
        }
      }
    }

    return {commInfo, clsInst: getConfiguredInstance(className, cfgName)};
  }

  getProcedureClassName(pf: StoredProcedure<unknown>) {
    return getRegisteredMethodClassName(pf);
  }

  getProcedureInfo(pf: StoredProcedure<unknown>) {
    const pfName = getRegisteredMethodClassName(pf) + '.' + pf.name;
    return this.procedureInfoMap.get(pfName);
  }
  // TODO: getProcedureInfoByNames??


  async workflow<T extends unknown[], R>(wf: Workflow<T, R>, params: InternalWorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    if (this.debugMode) {
      return this.debugWorkflow(wf, params, undefined, undefined, ...args);
    }
    return this.internalWorkflow(wf, params, undefined, undefined, ...args);
  }

  // If callerUUID and functionID are set, it means the workflow is invoked from within a workflow.
  async internalWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, params: InternalWorkflowParams, callerUUID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>> {
    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();
    const presetUUID: boolean = params.workflowUUID ? true : false;

    const wInfo = this.getWorkflowInfo(wf as Workflow<unknown[], unknown>);
    if (wInfo === undefined) {
      throw new DBOSNotRegisteredError(wf.name);
    }
    const wConfig = wInfo.config;

    const wCtxt: WorkflowContextImpl = new WorkflowContextImpl(this, params.parentCtx, workflowUUID, wConfig, wf.name, presetUUID, params.tempWfType, params.tempWfName);

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowUUID,
      status: StatusString.PENDING,
      name: wf.name,
      className: wCtxt.isTempWorkflow ? "" : getRegisteredMethodClassName(wf),
      configName: params.configuredInstance?.name || "",
      authenticatedUser: wCtxt.authenticatedUser,
      output: undefined,
      error: "",
      assumedRole: wCtxt.assumedRole,
      authenticatedRoles: wCtxt.authenticatedRoles,
      request: wCtxt.request,
      executorID: wCtxt.executorID,
      applicationVersion: wCtxt.applicationVersion,
      applicationID: wCtxt.applicationID,
      createdAt: Date.now(), // Remember the start time of this workflow
      maxRetries: wCtxt.maxRecoveryAttempts,
      recovery: params.recovery === true,
    };

    if (wCtxt.isTempWorkflow) {
      internalStatus.name = `${DBOSExecutor.tempWorkflowName}-${wCtxt.tempWfOperationType}-${wCtxt.tempWfOperationName}`;
      internalStatus.className = params.tempWfClass ?? "";
    }

    // Synchronously set the workflow's status to PENDING and record workflow inputs (for non single-transaction workflows).
    // We have to do it for all types of workflows because operation_outputs table has a foreign key constraint on workflow status table.
    if (wCtxt.tempWfOperationType !== TempWorkflowType.transaction
      && wCtxt.tempWfOperationType !== TempWorkflowType.procedure
    ) {
      args = await this.systemDatabase.initWorkflowStatus(internalStatus, args);
    }

    const runWorkflow = async () => {
      let result: R;

      // Execute the workflow.
      try {
        result = await wf.call(params.configuredInstance, wCtxt, ...args);
        internalStatus.output = result;
        internalStatus.status = StatusString.SUCCESS;
        this.systemDatabase.bufferWorkflowOutput(workflowUUID, internalStatus);
        wCtxt.span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        if (err instanceof DBOSWorkflowConflictUUIDError) {
          // Retrieve the handle and wait for the result.
          const retrievedHandle = this.retrieveWorkflow<R>(workflowUUID);
          result = await retrievedHandle.getResult();
          wCtxt.span.setAttribute("cached", true);
          wCtxt.span.setStatus({ code: SpanStatusCode.OK });
        } else {
          // Record the error.
          const e: Error = err as Error;
          this.logger.error(e);
          if (wCtxt.isTempWorkflow) {
            internalStatus.name = `${DBOSExecutor.tempWorkflowName}-${wCtxt.tempWfOperationType}-${wCtxt.tempWfOperationName}`;
          }
          internalStatus.error = DBOSJSON.stringify(serializeError(e));
          internalStatus.status = StatusString.ERROR;
          await this.systemDatabase.recordWorkflowError(workflowUUID, internalStatus);
          // TODO: Log errors, but not in the tests when they're expected.
          wCtxt.span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
          throw err;
        }
      } finally {
        this.tracer.endSpan(wCtxt.span);
        if (wCtxt.tempWfOperationType === TempWorkflowType.transaction
          || wCtxt.tempWfOperationType === TempWorkflowType.procedure
        ) {
          // For single-transaction workflows, asynchronously record inputs.
          // We must buffer inputs after workflow status is buffered/flushed because workflow_inputs table has a foreign key reference to the workflow_status table.
          this.systemDatabase.bufferWorkflowInputs(workflowUUID, args);
        }
      }
      // Asynchronously flush the result buffer.
      if (wCtxt.resultBuffer.size > 0) {
        this.workflowResultBuffer.set(wCtxt.workflowUUID, wCtxt.resultBuffer);
      }
      return result;
    };
    const workflowPromise: Promise<R> = runWorkflow();

    // Need to await for the workflow and capture errors.
    const awaitWorkflowPromise = workflowPromise
      .catch((error) => {
        this.logger.debug("Captured error in awaitWorkflowPromise: " + error);
      })
      .finally(() => {
        // Remove itself from pending workflow map.
        this.pendingWorkflowMap.delete(workflowUUID);
      });
    this.pendingWorkflowMap.set(workflowUUID, awaitWorkflowPromise);

    // Return the normal handle that doesn't capture errors.
    return new InvokedHandle(this.systemDatabase, workflowPromise, workflowUUID, wf.name, callerUUID, callerFunctionID);
  }

  /**
   * DEBUG MODE workflow execution, skipping all the recording
   */
  async debugWorkflow<T extends unknown[], R>(wf: Workflow<T, R>, params: WorkflowParams, callerUUID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>> {
    // In debug mode, we must have a specific workflow UUID.
    if (!params.workflowUUID) {
      throw new DBOSDebuggerError("Workflow UUID not found!");
    }
    const workflowUUID = params.workflowUUID;
    const wInfo = this.getWorkflowInfo(wf as Workflow<unknown[], unknown>);
    if (wInfo === undefined) {
      throw new DBOSDebuggerError("Workflow unregistered! " + wf.name);
    }
    const wConfig = wInfo.config;

    const wCtxt = new WorkflowContextDebug(this, params.parentCtx, workflowUUID, wConfig, wf.name);

    // A workflow must have run before.
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
    const recordedInputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
    if (!wfStatus || !recordedInputs) {
      throw new DBOSDebuggerError("Workflow status or inputs not found! UUID: " + workflowUUID);
    }

    // Make sure we use the same input.
    if (DBOSJSON.stringify(args) !== DBOSJSON.stringify(recordedInputs)) {
      throw new DBOSDebuggerError(`Detect different input for the workflow UUID ${workflowUUID}!\n Received: ${DBOSJSON.stringify(args)}\n Original: ${DBOSJSON.stringify(recordedInputs)}`);
    }

    const workflowPromise: Promise<R> = wf.call(params.configuredInstance, wCtxt, ...args)
      .then(async (result) => {
        // Check if the result is the same.
        const recordedResult = await this.systemDatabase.getWorkflowResult<R>(workflowUUID);
        if (result === undefined && !recordedResult) {
          return result;
        }
        if (DBOSJSON.stringify(result) !== DBOSJSON.stringify(recordedResult)) {
          this.logger.error(`Detect different output for the workflow UUID ${workflowUUID}!\n Received: ${DBOSJSON.stringify(result)}\n Original: ${DBOSJSON.stringify(recordedResult)}`);
        }
        return recordedResult; // Always return the recorded result.
      });
    return new InvokedHandle(this.systemDatabase, workflowPromise, workflowUUID, wf.name, callerUUID, callerFunctionID);
  }

  async transaction<T extends unknown[], R>(txn: Transaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.transaction(txn, params.configuredInstance ?? null, ...args);
    };
    return (await this.workflow(temp_workflow, {
      ...params,
      tempWfType: TempWorkflowType.transaction,
      tempWfName: getRegisteredMethodName(txn),
      tempWfClass: getRegisteredMethodClassName(txn),
    }, ...args)).getResult();
  }

  async procedure<R>(proc: StoredProcedure<R>, params: WorkflowParams, ...args: unknown[]): Promise<R> {
    // Create a workflow and call procedure.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: unknown[]) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.procedure(proc, ...args);
    };
    return (await this.workflow(temp_workflow, 
      { ...params, 
        tempWfType: TempWorkflowType.procedure, 
        tempWfName: getRegisteredMethodName(proc),
        tempWfClass: getRegisteredMethodClassName(proc),
        }, ...args)).getResult();
  }

  async executeProcedure<R>(func: (client: PoolClient) => Promise<R>, config: TransactionConfig): Promise<R> {
    const client = await this.procedurePool.connect();
    try {
      const readOnly = config.readOnly ?? false;
      const isolationLevel = config.isolationLevel ?? IsolationLevel.Serializable;
      await client.query(`BEGIN ISOLATION LEVEL ${isolationLevel}`);
      if (readOnly) {
        await client.query(`SET TRANSACTION READ ONLY`);
      }
      const result: R = await func(client);
      await client.query(`COMMIT`);
      return result;
    } catch (err) {
      await client.query(`ROLLBACK`);
      throw err;
    } finally {
      client.release();
    }
  }

  async external<T extends unknown[], R>(commFn: Communicator<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call external.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.external(commFn, params.configuredInstance ?? null, ...args);
    };
    return (await this.workflow(temp_workflow, {
      ...params,
      tempWfType: TempWorkflowType.external,
      tempWfName: getRegisteredMethodName(commFn),
      tempWfClass: getRegisteredMethodClassName(commFn),
    }, ...args)).getResult();
  }

  async send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    // Create a workflow and call send.
    const temp_workflow = async (ctxt: WorkflowContext, destinationUUID: string, message: T, topic?: string) => {
      return await ctxt.send<T>(destinationUUID, message, topic);
    };
    const workflowUUID = idempotencyKey ? destinationUUID + idempotencyKey : undefined;
    return (await this.workflow(temp_workflow, { workflowUUID: workflowUUID, tempWfType: TempWorkflowType.send, configuredInstance: null }, destinationUUID, message, topic)).getResult();
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  async getEvent<T>(workflowUUID: string, key: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    return this.systemDatabase.getEvent(workflowUUID, key, timeoutSeconds);
  }

  /**
   * Retrieve a handle for a workflow UUID.
   */
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    return new RetrievedHandle(this.systemDatabase, workflowUUID);
  }

  /* INTERNAL HELPERS */
  #generateUUID(): string {
    return uuidv4();
  }

  /**
   * A recovery process that by default runs during executor init time.
   * It runs to completion all pending workflows that were executing when the previous executor failed.
   */
  async recoverPendingWorkflows(executorIDs: string[] = ["local"]): Promise<WorkflowHandle<unknown>[]> {
    const pendingWorkflows: string[] = [];
    for (const execID of executorIDs) {
      if (execID == "local" && process.env.DBOS__VMID) {
        this.logger.debug(`Skip local recovery because it's running in a VM: ${process.env.DBOS__VMID}`);
        continue;
      }
      this.logger.debug(`Recovering workflows of executor: ${execID}`);
      const wIDs = await this.systemDatabase.getPendingWorkflows(execID);
      pendingWorkflows.push(...wIDs);
    }

    const handlerArray: WorkflowHandle<unknown>[] = [];
    for (const workflowUUID of pendingWorkflows) {
      try {
        handlerArray.push(await this.executeWorkflowUUID(workflowUUID));
      } catch (e) {
        this.logger.warn(`Recovery of workflow ${workflowUUID} failed: ${(e as Error).message}`);
      }
    }
    return handlerArray;
  }

  async executeWorkflowUUID(workflowUUID: string, startNewWorkflow: boolean = false): Promise<WorkflowHandle<unknown>> {
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
    const inputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
    if (!inputs || !wfStatus) {
      this.logger.error(`Failed to find inputs for workflowUUID: ${workflowUUID}`);
      throw new DBOSError(`Failed to find inputs for workflow UUID: ${workflowUUID}`);
    }
    const parentCtx = this.#getRecoveryContext(workflowUUID, wfStatus);

    const {wfInfo, configuredInst} = this.getWorkflowInfoByStatus(wfStatus);

    // If starting a new workflow, assign a new UUID. Otherwise, use the workflow's original UUID.
    const workflowStartUUID = startNewWorkflow ? undefined : workflowUUID;

    if (wfInfo) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      return this.workflow(wfInfo.workflow, { workflowUUID: workflowStartUUID, parentCtx: parentCtx, configuredInstance: configuredInst, recovery: true }, ...inputs);
    }

    // Should be temporary workflows. Parse the name of the workflow.
    const wfName = wfStatus.workflowName;
    const nameArr = wfName.split("-");
    if (!nameArr[0].startsWith(DBOSExecutor.tempWorkflowName)) {
      // CB - Doesn't this happen if the user changed the function name in their code?
      throw new DBOSError(`This should never happen! Cannot find workflow info for a non-temporary workflow! UUID ${workflowUUID}, name ${wfName}`);
    }

    let temp_workflow: Workflow<unknown[], unknown>;
    let clsinst: ConfiguredInstance | null = null;
    let tempWfType: string;
    let tempWfName: string | undefined;
    let tempWfClass: string | undefined;
    if (nameArr[1] === TempWorkflowType.transaction) {
      const {txnInfo, clsInst} = this.getTransactionInfoByNames(wfStatus.workflowClassName, nameArr[2], wfStatus.workflowConfigName);
      if (!txnInfo) {
        this.logger.error(`Cannot find transaction info for UUID ${workflowUUID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      tempWfType = TempWorkflowType.transaction;
      tempWfName = getRegisteredMethodName(txnInfo.transaction);
      tempWfClass = getRegisteredMethodClassName(txnInfo.transaction);
      temp_workflow = async (ctxt: WorkflowContext, ...args: unknown[]) => {
        const ctxtImpl = ctxt as WorkflowContextImpl;
        return await ctxtImpl.transaction(txnInfo.transaction, clsInst, ...args);
      };
      clsinst = clsInst;
    } else if (nameArr[1] === TempWorkflowType.external) {
      const {commInfo, clsInst} = this.getCommunicatorInfoByNames(wfStatus.workflowClassName, nameArr[2], wfStatus.workflowConfigName);
      if (!commInfo) {
        this.logger.error(`Cannot find communicator info for UUID ${workflowUUID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      tempWfType = TempWorkflowType.external;
      tempWfName = getRegisteredMethodName(commInfo.communicator);
      tempWfClass = getRegisteredMethodClassName(commInfo.communicator);
      temp_workflow = async (ctxt: WorkflowContext, ...args: unknown[]) => {
        const ctxtImpl = ctxt as WorkflowContextImpl;
        return await ctxtImpl.external(commInfo.communicator, clsInst, ...args);
      };
      clsinst = clsInst;
    } else if (nameArr[1] === TempWorkflowType.send) {
      tempWfType = TempWorkflowType.send;
      temp_workflow = async (ctxt: WorkflowContext, ...args: unknown[]) => {
        return await ctxt.send<unknown>(args[0] as string, args[1], args[2] as string); // id, value, topic
      };
      clsinst = null;
    } else {
      this.logger.error(`Unrecognized temporary workflow! UUID ${workflowUUID}, name ${wfName}`);
      throw new DBOSNotRegisteredError(wfName);
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return this.workflow(temp_workflow, { workflowUUID: workflowStartUUID, parentCtx: parentCtx ?? undefined, configuredInstance: clsinst, recovery: true, tempWfType, tempWfClass, tempWfName}, ...inputs);
  }

  // NOTE: this creates a new span, it does not inherit the span from the original workflow
  #getRecoveryContext(workflowUUID: string, status: WorkflowStatus): DBOSContextImpl {
    const span = this.tracer.startSpan(status.workflowName, {
      operationUUID: workflowUUID,
      operationType: OperationType.WORKFLOW,
      status: status.status,
      authenticatedUser: status.authenticatedUser,
      assumedRole: status.assumedRole,
      authenticatedRoles: status.authenticatedRoles,
    });
    const oc = new DBOSContextImpl(status.workflowName, span, this.logger);
    oc.request = status.request;
    oc.authenticatedUser = status.authenticatedUser;
    oc.authenticatedRoles = status.authenticatedRoles;
    oc.assumedRole = status.assumedRole;
    oc.workflowUUID = workflowUUID;
    return oc;
  }

  /* BACKGROUND PROCESSES */
  /**
   * Periodically flush the workflow output buffer to the system database.
   */
  async flushWorkflowBuffers() {
    if (this.initialized) {
      await this.flushWorkflowResultBuffer();
      await this.systemDatabase.flushWorkflowSystemBuffers();
    }
    this.isFlushingBuffers = false;
  }

  async flushWorkflowResultBuffer(): Promise<void> {
    const localBuffer = new Map(this.workflowResultBuffer);
    this.workflowResultBuffer.clear();
    const totalSize = localBuffer.size;
    const flushBatchSize = 50;
    try {
      let finishedCnt = 0;
      while (finishedCnt < totalSize) {
        let sqlStmt = "INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, output, error, txn_id, txn_snapshot, created_at) VALUES ";
        let paramCnt = 1;
        const values: any[] = [];
        const batchUUIDs: string[] = [];
        for (const [workflowUUID, wfBuffer] of localBuffer) {
          for (const [funcID, recorded] of wfBuffer) {
            const output = recorded.output;
            const txnSnapshot = recorded.txn_snapshot;
            const createdAt = recorded.created_at!;
            if (paramCnt > 1) {
              sqlStmt += ", ";
            }
            sqlStmt += `($${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, null, $${paramCnt++}, $${paramCnt++})`;
            values.push(workflowUUID, funcID, DBOSJSON.stringify(output), DBOSJSON.stringify(null), txnSnapshot, createdAt);
          }
          batchUUIDs.push(workflowUUID);
          finishedCnt++;
          if (batchUUIDs.length >= flushBatchSize) {
            // Cap at the batch size.
            break;
          }
        }
        this.logger.debug(sqlStmt);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        await this.userDatabase.query(sqlStmt, ...values);

        // Clean up after each batch succeeds
        batchUUIDs.forEach((value) => { localBuffer.delete(value); });
      }
    } catch (error) {
      (error as Error).message = `Error flushing workflow result buffer: ${(error as Error).message}`;
      this.logger.error(error);
      // If there is a failure in flushing the buffer, return items to the global buffer for retrying later.
      for (const [workflowUUID, wfBuffer] of localBuffer) {
        if (!this.workflowResultBuffer.has(workflowUUID)) {
          this.workflowResultBuffer.set(workflowUUID, wfBuffer);
        }
      }
    }
  }

  logRegisteredHTTPUrls() {
    this.logger.info("HTTP endpoints supported:");
    this.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as HandlerRegistrationBase;
      if (ro.apiURL) {
        this.logger.info("    " + ro.apiType.padEnd(6) + "  :  " + ro.apiURL);
        const roles = ro.getRequiredRoles();
        if (roles.length > 0) {
          this.logger.info("        Required Roles: " + DBOSJSON.stringify(roles));
        }
      }
    });
  }

  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const value = get(this.config.application, key, defaultValue);
    // If the key is found and the default value is provided, check whether the value is of the same type.
    if (value && defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }
    return value;
  }
}
