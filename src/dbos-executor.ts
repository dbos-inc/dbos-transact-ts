/* eslint-disable @typescript-eslint/no-explicit-any */
import { Span } from "@opentelemetry/sdk-trace-base";
import { DBOSError, DBOSInitializationError, DBOSWorkflowConflictUUIDError, DBOSNotRegisteredError, DBOSDebuggerError, DBOSConfigKeyTypeError, DBOSFailedSqlTransactionError } from "./error";
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
  ContextFreeFunction,
  GetWorkflowQueueInput,
  GetWorkflowQueueOutput,
} from './workflow';

import { IsolationLevel, Transaction, TransactionConfig, TransactionContextImpl } from './transaction';
import { StepConfig, StepContextImpl, StepFunction } from './step';
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
  DrizzleUserDatabase,
  UserDatabaseClient,
} from './user_database';
import { MethodRegistrationBase, getRegisteredOperations, getOrCreateClassRegistration, MethodRegistration, getRegisteredMethodClassName, getRegisteredMethodName, getConfiguredInstance, ConfiguredInstance, getAllRegisteredClasses } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import { DBOSContextImpl, InitContext, runWithWorkflowContext, runWithTransactionContext, runWithStepContext } from './context';
import { HandlerRegistrationBase } from './httpServer/handler';
import { WorkflowContextDebug } from './debugger/debug_workflow';
import { serializeError } from 'serialize-error';
import { DBOSJSON, sleepms } from './utils';
import path from 'node:path';
import { StoredProcedure, StoredProcedureConfig } from './procedure';
import { NoticeMessage } from "pg-protocol/dist/messages";
import { DBOSEventReceiver, DBOSExecutorContext, GetWorkflowsInput, GetWorkflowsOutput} from ".";

import { get } from "lodash";
import { wfQueueRunner, WorkflowQueue } from "./wfqueue";
import { debugTriggerPoint, DEBUG_TRIGGER_WORKFLOW_ENQUEUE } from "./debugpoint";
import { DBOSScheduler } from './scheduler/scheduler';
import { DBOSEventReceiverState, DBOSEventReceiverQuery, DBNotificationCallback, DBNotificationListener } from "./eventreceiver";


// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface DBOSNull { }
export const dbosNull: DBOSNull = {};

/* Interface for DBOS configuration */
export interface DBOSConfig {
  poolConfig: PoolConfig;
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

interface WorkflowRegInfo {
  workflow: Workflow<unknown[], unknown>;
  config: WorkflowConfig;
  registration?: MethodRegistrationBase; // Always set except for temp WF...
}

interface TransactionRegInfo {
  transaction: Transaction<unknown[], unknown>;
  config: TransactionConfig;
  registration: MethodRegistrationBase;
}

interface StepRegInfo {
  step: StepFunction<unknown[], unknown>;
  config: StepConfig;
  registration: MethodRegistrationBase;
}

interface ProcedureRegInfo {
  procedure: StoredProcedure<unknown>;
  config: StoredProcedureConfig;
  registration: MethodRegistrationBase;
}

export interface InternalWorkflowParams extends WorkflowParams {
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

  readonly workflowInfoMap: Map<string, WorkflowRegInfo> = new Map([
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
  readonly transactionInfoMap: Map<string, TransactionRegInfo> = new Map();
  readonly stepInfoMap: Map<string, StepRegInfo> = new Map();
  readonly procedureInfoMap: Map<string, ProcedureRegInfo> = new Map();
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
  typeormEntities: Function[] = [];
  drizzleEntities: { [key: string]: object } = {};

  eventReceivers: DBOSEventReceiver[] = [];

  scheduler?: DBOSScheduler = undefined;
  wfqEnded?: Promise<void> = undefined;

  readonly executorID: string = process.env.DBOS__VMID || "local";

  static globalInstance: DBOSExecutor | undefined = undefined;

  /* WORKFLOW EXECUTOR LIFE CYCLE MANAGEMENT */
  constructor(readonly config: DBOSConfig, systemDatabase?: SystemDatabase) {
    this.debugMode = config.debugMode ?? false;
    this.debugProxy = config.debugProxy;

    // Set configured environment variables
    if (config.env) {
      for (const [key, value] of Object.entries(config.env)) {
        if (typeof value === "string") {
          process.env[key] = value;
        } else {
          console.warn(`Invalid value type for environment variable ${key}: ${typeof value}`);
        }
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
    DBOSExecutor.globalInstance = this;
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
            entities: this.typeormEntities,
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
    } else if (userDbClient === UserDatabaseName.DRIZZLE) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const DrizzleExports = require("drizzle-orm/node-postgres");
      const drizzlePool = new Pool(userDBConfig);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const drizzle = DrizzleExports.drizzle(drizzlePool, { schema: this.drizzleEntities });
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      this.userDatabase = new DrizzleUserDatabase(drizzlePool, drizzle);
      this.logger.debug("Loaded Drizzle user database");
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
        this.#registerStep(ro);
      } else if (ro.procConfig) {
        this.#registerProcedure(ro);
      }
      for (const [evtRcvr, _cfg] of ro.eventReceiverInfo) {
        if (!this.eventReceivers.includes(evtRcvr)) this.eventReceivers.push(evtRcvr);
      }
    }
  }

  getRegistrationsFor(obj: DBOSEventReceiver) {
    const res: { methodConfig: unknown, classConfig: unknown, methodReg: MethodRegistrationBase }[] = [];
    for (const r of this.registeredOperations) {
      if (!r.eventReceiverInfo.has(obj)) continue;
      const methodConfig = r.eventReceiverInfo.get(obj)!;
      const classConfig = r.defaults?.eventReceiverInfo.get(obj) ?? {};
      res.push({ methodReg: r, methodConfig, classConfig })
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
      let length; // Track the length of the array (or number of keys of the object)
      for (const cls of classes) {
        const reg = getOrCreateClassRegistration(cls as AnyConstructor);
        /**
         * With TSORM, we take an array of entities (Function[]) and add them to this.entities:
         */
        if (Array.isArray(reg.ormEntities)) {
          this.typeormEntities = (this.typeormEntities).concat(reg.ormEntities as any[]);
          length = reg.ormEntities.length;
        } else {
          /**
           * With Drizzle, we need to take an object of entities, since the object keys are used to access the entities from ctx.client.query:
           */
          this.drizzleEntities = { ...this.drizzleEntities, ...reg.ormEntities };
          length = Object.keys(reg.ormEntities).length;
        }
        this.logger.debug(`Loaded ${length} ORM entities`);
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
      }
    } catch (err) {
      if (err instanceof AggregateError) {
        let combinedMessage = 'Failed to initialize workflow executor: ';
        for (const error of err.errors) {
          combinedMessage += `${(error as Error).message}; `;
        }
        throw new DBOSInitializationError(combinedMessage);
      } else if (err instanceof Error) {
        const errorMessage = `Failed to initialize workflow executor: ${err.message}`;
        throw new DBOSInitializationError(errorMessage);
      } else {
        const errorMessage = `Failed to initialize workflow executor: ${String(err)}`;
        throw new DBOSInitializationError(errorMessage);
      }
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

      await this.recoverPendingWorkflows();
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

    if (DBOSExecutor.globalInstance === this) {
      DBOSExecutor.globalInstance = undefined;
    }
  }

  /* WORKFLOW OPERATIONS */

  #registerWorkflow(ro: MethodRegistrationBase) {
    const wf = ro.registeredFunction as Workflow<unknown[], unknown>;
    if (wf.name === DBOSExecutor.tempWorkflowName) {
      throw new DBOSError(`Unexpected use of reserved workflow name: ${wf.name}`);
    }
    const wfn = ro.className + '.' + ro.name;
    if (this.workflowInfoMap.has(wfn)) {
      throw new DBOSError(`Repeated workflow name: ${wfn}`);
    }
    const workflowInfo: WorkflowRegInfo = {
      workflow: wf,
      config: { ...ro.workflowConfig },
      registration: ro,
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
    const txnInfo: TransactionRegInfo = {
      transaction: txf,
      config: { ...ro.txnConfig },
      registration: ro,
    };
    this.transactionInfoMap.set(tfn, txnInfo);
    this.logger.debug(`Registered transaction ${tfn}`);
  }

  #registerStep(ro: MethodRegistrationBase) {
    const comm = ro.registeredFunction as StepFunction<unknown[], unknown>;
    const cfn = ro.className + '.' + ro.name;
    if (this.stepInfoMap.has(cfn)) {
      throw new DBOSError(`Repeated Commmunicator name: ${cfn}`);
    }
    const stepInfo: StepRegInfo = {
      step: comm,
      config: { ...ro.commConfig },
      registration: ro,
    };
    this.stepInfoMap.set(cfn, stepInfo);
    this.logger.debug(`Registered step ${cfn}`);
  }

  #registerProcedure(ro: MethodRegistrationBase) {
    const proc = ro.registeredFunction as StoredProcedure<unknown>;
    const cfn = ro.className + '.' + ro.name;

    if (this.procedureInfoMap.has(cfn)) {
      throw new DBOSError(`Repeated Procedure name: ${cfn}`);
    }
    const procInfo: ProcedureRegInfo = {
      procedure: proc,
      config: { ...ro.procConfig },
      registration: ro,
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
    const wfInfo = this.workflowInfoMap.get(wfname);

    // wfInfo may be undefined here, if this is a temp workflow

    return { wfInfo, configuredInst: getConfiguredInstance(wf.workflowClassName, wf.workflowConfigName) };
  }

  getTransactionInfo(tf: Transaction<unknown[], unknown>) {
    const tfname = getRegisteredMethodClassName(tf) + '.' + tf.name;
    return this.transactionInfoMap.get(tfname);
  }
  getTransactionInfoByNames(className: string, functionName: string, cfgName: string) {
    const tfname = className + '.' + functionName;
    const txnInfo: TransactionRegInfo | undefined = this.transactionInfoMap.get(tfname);

    if (!txnInfo) {
      throw new DBOSNotRegisteredError(`Transaction function name '${tfname}' is not registered.`);
    }

    return { txnInfo, clsInst: getConfiguredInstance(className, cfgName) };
  }

  getStepInfo(cf: StepFunction<unknown[], unknown>) {
    const cfname = getRegisteredMethodClassName(cf) + '.' + cf.name;
    return this.stepInfoMap.get(cfname);
  }
  getStepInfoByNames(className: string, functionName: string, cfgName: string) {
    const cfname = className + '.' + functionName;
    const stepInfo: StepRegInfo | undefined = this.stepInfoMap.get(cfname);

    if (!stepInfo) {
      throw new DBOSNotRegisteredError(`Step function name '${cfname}' is not registered.`);
    }

    return {commInfo: stepInfo, clsInst: getConfiguredInstance(className, cfgName)};
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

    const passContext = wInfo.registration?.passContext ?? true;
    const wCtxt: WorkflowContextImpl = new WorkflowContextImpl(this, params.parentCtx, workflowUUID, wConfig, wf.name, presetUUID, params.tempWfType, params.tempWfName);

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowUUID,
      status: (params.queueName !== undefined) ? StatusString.ENQUEUED : StatusString.PENDING,
      name: wf.name,
      className: wCtxt.isTempWorkflow ? "" : getRegisteredMethodClassName(wf),
      configName: params.configuredInstance?.name || "",
      queueName: params.queueName,
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
    if ((wCtxt.tempWfOperationType !== TempWorkflowType.transaction
        && wCtxt.tempWfOperationType !== TempWorkflowType.procedure)
      || params.queueName !== undefined
    ) {
      // TODO: Make this transactional (and with the queue step below)
      args = await this.systemDatabase.initWorkflowStatus(internalStatus, args);
      await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_ENQUEUE);
    }

    const runWorkflow = async () => {
      let result: R;

      // Execute the workflow.
      try {
        let cresult: R | undefined;
        await runWithWorkflowContext(wCtxt, async () => {
          if (passContext) {
            cresult = await wf.call(params.configuredInstance, wCtxt, ...args);
          }
          else {
            cresult = await (wf as unknown as ContextFreeFunction<T, R>).call(params.configuredInstance, ...args);
          }
        });
        result = cresult!

        internalStatus.output = result;
        internalStatus.status = StatusString.SUCCESS;
        if (internalStatus.queueName) {
          // Now... the workflow isn't certainly done.
          //  But waiting this long is for concurrency control anyway,
          //   so it is probably done enough.
          await this.systemDatabase.dequeueWorkflow(workflowUUID, this.#getQueueByName(internalStatus.queueName));
        }
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
          const e = err as Error & { dbos_already_logged?: boolean };
          this.logger.error(e);
          e.dbos_already_logged = true
          if (wCtxt.isTempWorkflow) {
            internalStatus.name = `${DBOSExecutor.tempWorkflowName}-${wCtxt.tempWfOperationType}-${wCtxt.tempWfOperationName}`;
          }
          internalStatus.error = DBOSJSON.stringify(serializeError(e));
          internalStatus.status = StatusString.ERROR;
          if (internalStatus.queueName) {
            await this.systemDatabase.dequeueWorkflow(workflowUUID, this.#getQueueByName(internalStatus.queueName));
          }
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

    if (params.queueName === undefined || params.executeWorkflow) {
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
    else {
      await this.systemDatabase.enqueueWorkflow(workflowUUID, this.#getQueueByName(params.queueName));
      return new RetrievedHandle(this.systemDatabase, workflowUUID, callerUUID, callerFunctionID);
    }
  }

  #getQueueByName(name: string): WorkflowQueue {
    const q = wfQueueRunner.wfQueuesByName.get(name);
    if (!q) throw new DBOSNotRegisteredError(`Workflow queue '${name}' does is not defined.`);
    return q;
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

    const workflowPromise: Promise<R> = runWithWorkflowContext(wCtxt, async () => {
      return await wf.call(params.configuredInstance, wCtxt, ...args)
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

  async callTransactionFunction<T extends unknown[], R>(
    txn: Transaction<T, R>, clsinst: ConfiguredInstance | null, wfCtx: WorkflowContextImpl, ...args: T
  ): Promise<R> {
    const txnInfo = this.getTransactionInfo(txn as Transaction<unknown[], unknown>);
    if (txnInfo === undefined) {
      throw new DBOSNotRegisteredError(txn.name);
    }
    const readOnly = txnInfo.config.readOnly ?? false;
    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.
    const funcId = wfCtx.functionIDGetIncrement();
    const span: Span = this.tracer.startSpan(
      txn.name,
      {
        operationUUID: wfCtx.workflowUUID,
        operationType: OperationType.TRANSACTION,
        authenticatedUser: wfCtx.authenticatedUser,
        assumedRole: wfCtx.assumedRole,
        authenticatedRoles: wfCtx.authenticatedRoles,
        readOnly: readOnly,
        isolationLevel: txnInfo.config.isolationLevel,
      },
      wfCtx.span,
    );

    while (true) {
      let txn_snapshot = "invalid";
      const workflowUUID = wfCtx.workflowUUID;
      const wrappedTransaction = async (client: UserDatabaseClient): Promise<R> => {
        const tCtxt = new TransactionContextImpl(
          this.userDatabase.getName(), client, wfCtx,
          span, this.logger, funcId, txn.name);

        // If the UUID is preset, it is possible this execution previously happened. Check, and return its original result if it did.
        // Note: It is possible to retrieve a generated ID from a workflow handle, run a concurrent execution, and cause trouble for yourself. We recommend against this.
        if (wfCtx.presetUUID) {
          const check: BufferedResult = await wfCtx.checkTxExecution<R>(client, funcId);
          txn_snapshot = check.txn_snapshot;
          if (check.output !== dbosNull) {
            tCtxt.span.setAttribute("cached", true);
            tCtxt.span.setStatus({ code: SpanStatusCode.OK });
            this.tracer.endSpan(tCtxt.span);
            return check.output as R;
          }
        } else {
          // Collect snapshot information for read-only transactions and non-preset UUID transactions, if not already collected above
          txn_snapshot = await wfCtx.retrieveTxSnapshot(client);
        }

        // For non-read-only transactions, flush the result buffer.
        if (!readOnly) {
          await wfCtx.flushResultBuffer(client);
        }

        // Execute the user's transaction.
        let cresult: R | undefined;
        if (txnInfo.registration.passContext) {
          await runWithTransactionContext(tCtxt, async ()=> {
            cresult = await txn.call(clsinst, tCtxt, ...args);
          });
        }
        else {
          await runWithTransactionContext(tCtxt, async ()=> {
            const tf = txn as unknown as (...args: T)=>Promise<R>;
            cresult = await tf.call(clsinst, ...args);
          });
        }
        const result = cresult!

        // Record the execution, commit, and return.
        if (readOnly) {
          // Buffer the output of read-only transactions instead of synchronously writing it.
          const readOutput: BufferedResult = {
            output: result,
            txn_snapshot: txn_snapshot,
            created_at: Date.now(),
          }
          wfCtx.resultBuffer.set(funcId, readOutput);
        } else {
          try {
            // Synchronously record the output of write transactions and obtain the transaction ID.
            const pg_txn_id = await wfCtx.recordOutputTx<R>(client, funcId, txn_snapshot, result);
            tCtxt.span.setAttribute("pg_txn_id", pg_txn_id);
            wfCtx.resultBuffer.clear();
          } catch (error) {
            if (this.userDatabase.isFailedSqlTransactionError(error)) {
              this.logger.error(`Postgres aborted the ${txn.name} @Transaction of Workflow ${workflowUUID}, but the function did not raise an exception.  Please ensure that the @Transaction method raises an exception if the database transaction is aborted.`);
              throw new DBOSFailedSqlTransactionError(workflowUUID, txn.name)
            } else {
              throw error;
            }
          }
        }

        return result;
      };

      try {
        const result = await this.userDatabase.transaction(wrappedTransaction, txnInfo.config);
        span.setStatus({ code: SpanStatusCode.OK });
        this.tracer.endSpan(span);
        return result;
      } catch (err) {
        if (this.userDatabase.isRetriableTransactionError(err)) {
          // serialization_failure in PostgreSQL
          span.addEvent("TXN SERIALIZATION FAILURE", { "retryWaitMillis": retryWaitMillis }, performance.now());
          // Retry serialization failures.
          await sleepms(retryWaitMillis);
          retryWaitMillis *= backoffFactor;
          retryWaitMillis = retryWaitMillis < maxRetryWaitMs ? retryWaitMillis : maxRetryWaitMs;
          continue;
        }

        // Record and throw other errors.
        const e: Error = err as Error;
        await this.userDatabase.transaction(async (client: UserDatabaseClient) => {
          await wfCtx.flushResultBuffer(client);
          await wfCtx.recordErrorTx(client, funcId, txn_snapshot, e);
        }, { isolationLevel: IsolationLevel.ReadCommitted });
        wfCtx.resultBuffer.clear();
        span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
        this.tracer.endSpan(span);
        throw err;
      }
    }
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

  async external<T extends unknown[], R>(stepFn: StepFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call external.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.external(stepFn, params.configuredInstance ?? null, ...args);
    };
    return (await this.workflow(temp_workflow, {
      ...params,
      tempWfType: TempWorkflowType.external,
      tempWfName: getRegisteredMethodName(stepFn),
      tempWfClass: getRegisteredMethodClassName(stepFn),
    }, ...args)).getResult();
  }

  /**
   * Execute a step function.
   * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
   * The step may execute many times, but once it is complete, it will not re-execute.
   */
  async callStepFunction<T extends unknown[], R>(
    stepFn: StepFunction<T, R>, clsInst: ConfiguredInstance | null, wfCtx: WorkflowContextImpl, ...args: T
  ): Promise<R> {
    const commInfo = this.getStepInfo(stepFn as StepFunction<unknown[], unknown>);
    if (commInfo === undefined) {
      throw new DBOSNotRegisteredError(stepFn.name);
    }

    const funcID = wfCtx.functionIDGetIncrement();
    const maxRetryIntervalSec = 3600 // Maximum retry interval: 1 hour

    const span: Span = this.tracer.startSpan(
      stepFn.name,
      {
        operationUUID: wfCtx.workflowUUID,
        operationType: OperationType.COMMUNICATOR,
        authenticatedUser: wfCtx.authenticatedUser,
        assumedRole: wfCtx.assumedRole,
        authenticatedRoles: wfCtx.authenticatedRoles,
        retriesAllowed: commInfo.config.retriesAllowed,
        intervalSeconds: commInfo.config.intervalSeconds,
        maxAttempts: commInfo.config.maxAttempts,
        backoffRate: commInfo.config.backoffRate,
      },
      wfCtx.span,
    );

    const ctxt: StepContextImpl = new StepContextImpl(wfCtx, funcID, span, this.logger, commInfo.config, stepFn.name);

    await this.userDatabase.transaction(async (client: UserDatabaseClient) => {
      await wfCtx.flushResultBuffer(client);
    }, { isolationLevel: IsolationLevel.ReadCommitted });
    wfCtx.resultBuffer.clear();

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | DBOSNull = await this.systemDatabase.checkOperationOutput<R>(wfCtx.workflowUUID, ctxt.functionID);
    if (check !== dbosNull) {
      ctxt.span.setAttribute("cached", true);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.tracer.endSpan(ctxt.span);
      return check as R;
    }

    // Execute the step function.  If it throws an exception, retry with exponential backoff.
    // After reaching the maximum number of retries, throw an DBOSError.
    let result: R | DBOSNull = dbosNull;
    let err: Error | DBOSNull = dbosNull;
    if (ctxt.retriesAllowed) {
      let numAttempts = 0;
      let intervalSeconds: number = ctxt.intervalSeconds;
      if (intervalSeconds > maxRetryIntervalSec) {
        this.logger.warn(`Step config interval exceeds maximum allowed interval, capped to ${maxRetryIntervalSec} seconds!`)
      }
      while (result === dbosNull && numAttempts++ < ctxt.maxAttempts) {
        try {
          let cresult: R | undefined;
          if (commInfo.registration.passContext) {
            await runWithStepContext(ctxt, async () => {
              cresult = await stepFn.call(clsInst, ctxt, ...args);
            });
          }
          else {
            await runWithStepContext(ctxt, async () => {
              const sf = stepFn as unknown as (...args: T) => Promise<R>;
              cresult = await sf.call(clsInst, ...args);
            });
          }
          result = cresult!
        } catch (error) {
          const e = error as Error
          this.logger.warn(`Error in step being automatically retried. Attempt ${numAttempts} of ${ctxt.maxAttempts}. ${e.stack}`);
          span.addEvent(`Step attempt ${numAttempts + 1} failed`, { "retryIntervalSeconds": intervalSeconds, "error": (error as Error).message }, performance.now());
          if (numAttempts < ctxt.maxAttempts) {
            // Sleep for an interval, then increase the interval by backoffRate.
            // Cap at the maximum allowed retry interval.
            await sleepms(intervalSeconds * 1000);
            intervalSeconds *= ctxt.backoffRate;
            intervalSeconds = intervalSeconds < maxRetryIntervalSec ? intervalSeconds : maxRetryIntervalSec;
          }
        }
      }
    } else {
      try {
        let cresult: R | undefined;
        if (commInfo.registration.passContext) {
          await runWithStepContext(ctxt, async () => {
            cresult = await stepFn.call(clsInst, ctxt, ...args);
          });
        }
        else {
          await runWithStepContext(ctxt, async () => {
            const sf = stepFn as unknown as (...args: T) => Promise<R>;
            cresult = await sf.call(clsInst, ...args);
          });
        }
        result = cresult!
      } catch (error) {
        err = error as Error;
      }
    }

    // `result` can only be dbosNull when the step timed out
    if (result === dbosNull) {
      // Record the error, then throw it.
      err = err === dbosNull ? new DBOSError("Step reached maximum retries.", 1) : err;
      await this.systemDatabase.recordOperationError(wfCtx.workflowUUID, ctxt.functionID, err as Error);
      ctxt.span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      this.tracer.endSpan(ctxt.span);
      throw err as Error;
    } else {
      // Record the execution and return.
      await this.systemDatabase.recordOperationOutput<R>(wfCtx.workflowUUID, ctxt.functionID, result as R);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.tracer.endSpan(ctxt.span);
      return result as R;
    }
  }

  async send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    // Create a workflow and call send.
    const temp_workflow = async (ctxt: WorkflowContext, destinationUUID: string, message: T, topic?: string) => {
      return await ctxt.send<T>(destinationUUID, message, topic);
    };
    const workflowUUID = idempotencyKey ? destinationUUID + idempotencyKey : undefined;
    return (await this.workflow(temp_workflow, {
      workflowUUID: workflowUUID, tempWfType: TempWorkflowType.send, configuredInstance: null,
    }, destinationUUID, message, topic)).getResult();
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
  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R> {
    return new RetrievedHandle(this.systemDatabase, workflowID);
  }

  getWorkflowStatus(workflowID: string): Promise<WorkflowStatus | null> {
    return this.systemDatabase.getWorkflowStatus(workflowID);
  }

  getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput> {
    return this.systemDatabase.getWorkflows(input);
  }

  getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput> {
    return this.systemDatabase.getWorkflowQueue(input);
  }

  async queryUserDB(sql: string, params?: unknown[]) {
    if (params !== undefined) {
      return await this.userDatabase.query(sql, ...params);
    }
    else {
      return await this.userDatabase.query(sql);
    }
  }

  async userDBListen(channels: string[], callback: DBNotificationCallback): Promise<DBNotificationListener> {
    const notificationsClient = await this.procedurePool.connect();
    for (const nname of channels) {
      await notificationsClient.query(`LISTEN ${nname};`);
    }

    notificationsClient.on("notification", callback);

    return {
      close: async () => {
        for (const nname of channels) {
          try {
            await notificationsClient.query(`UNLISTEN ${nname};`);
          }
          catch (e) {
            this.logger.warn(e);
          }
          notificationsClient.release();
        }
      }
    }
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
      if (execID === "local" && process.env.DBOS__VMID) {
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

  async deactivateEventReceivers() {
    this.logger.info("Deactivating event receivers");
    for (const evtRcvr of this.eventReceivers || []) {
      await evtRcvr.destroy();
    }
    await this.scheduler?.destroyScheduler();
    wfQueueRunner.stop();
    await this.wfqEnded;
  }

  async executeWorkflowUUID(workflowUUID: string, startNewWorkflow: boolean = false): Promise<WorkflowHandle<unknown>> {
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
    const inputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
    if (!inputs || !wfStatus) {
      this.logger.error(`Failed to find inputs for workflowUUID: ${workflowUUID}`);
      throw new DBOSError(`Failed to find inputs for workflow UUID: ${workflowUUID}`);
    }
    const parentCtx = this.#getRecoveryContext(workflowUUID, wfStatus);

    const { wfInfo, configuredInst } = this.getWorkflowInfoByStatus(wfStatus);

    // If starting a new workflow, assign a new UUID. Otherwise, use the workflow's original UUID.
    const workflowStartUUID = startNewWorkflow ? undefined : workflowUUID;

    if (wfInfo) {
      return this.workflow(wfInfo.workflow, {
        workflowUUID: workflowStartUUID, parentCtx: parentCtx, configuredInstance: configuredInst, recovery: true,
        queueName: wfStatus.queueName, executeWorkflow: true,
      },
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      ...inputs);
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
      const { txnInfo, clsInst } = this.getTransactionInfoByNames(wfStatus.workflowClassName, nameArr[2], wfStatus.workflowConfigName);
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
      const { commInfo, clsInst } = this.getStepInfoByNames(wfStatus.workflowClassName, nameArr[2], wfStatus.workflowConfigName);
      if (!commInfo) {
        this.logger.error(`Cannot find step info for UUID ${workflowUUID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      tempWfType = TempWorkflowType.external;
      tempWfName = getRegisteredMethodName(commInfo.step);
      tempWfClass = getRegisteredMethodClassName(commInfo.step);
      temp_workflow = async (ctxt: WorkflowContext, ...args: unknown[]) => {
        const ctxtImpl = ctxt as WorkflowContextImpl;
        return await ctxtImpl.external(commInfo.step, clsInst, ...args);
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

    return this.workflow(temp_workflow, {
      workflowUUID: workflowStartUUID, parentCtx: parentCtx ?? undefined, configuredInstance: clsinst,
      recovery: true, tempWfType, tempWfClass, tempWfName,
    },
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    ...inputs);
  }

  async getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSEventReceiverState | undefined> {
    return await this.systemDatabase.getEventDispatchState(svc, wfn, key);
  }
  async queryEventDispatchState(query: DBOSEventReceiverQuery): Promise<DBOSEventReceiverState[]> {
    return await this.systemDatabase.queryEventDispatchState(query);
  }
  async upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState> {
    return await this.systemDatabase.upsertEventDispatchState(state);
  }

  #getRecoveryContext(workflowUUID: string, status: WorkflowStatus): DBOSContextImpl {
    // Note: this doesn't inherit the original parent context's span.
    const oc = new DBOSContextImpl(status.workflowName, undefined as unknown as Span, this.logger);
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
