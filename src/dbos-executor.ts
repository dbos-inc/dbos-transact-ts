/* eslint-disable @typescript-eslint/no-explicit-any */
import { Span } from '@opentelemetry/sdk-trace-base';
import {
  DBOSError,
  DBOSInitializationError,
  DBOSWorkflowConflictUUIDError,
  DBOSNotRegisteredError,
  DBOSDebuggerError,
  DBOSConfigKeyTypeError,
  DBOSFailedSqlTransactionError,
  DBOSMaxStepRetriesError,
  DBOSWorkflowCancelledError,
  DBOSUnexpectedStepError,
} from './error';
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
import {
  SystemDatabase,
  PostgresSystemDatabase,
  WorkflowStatusInternal,
  SystemDatabaseStoredResult,
} from './system_database';
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
  pgNodeIsKeyConflictError,
  createDBIfDoesNotExist,
} from './user_database';
import {
  MethodRegistrationBase,
  getRegisteredOperations,
  getOrCreateClassRegistration,
  MethodRegistration,
  getRegisteredMethodClassName,
  getRegisteredMethodName,
  getConfiguredInstance,
  ConfiguredInstance,
  getAllRegisteredClasses,
} from './decorators';
import { step_info } from '../schemas/system_db_schema';
import { SpanStatusCode } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import {
  DBOSContextImpl,
  runWithWorkflowContext,
  runWithTransactionContext,
  runWithStepContext,
  runWithStoredProcContext,
} from './context';
import { HandlerRegistrationBase } from './httpServer/handler';
import { deserializeError, ErrorObject, serializeError } from 'serialize-error';
import { globalParams, DBOSJSON, sleepms } from './utils';
import path from 'node:path';
import { StoredProcedure, StoredProcedureConfig, StoredProcedureContextImpl } from './procedure';
import { NoticeMessage } from 'pg-protocol/dist/messages';
import { DBOSEventReceiver, DBOSExecutorContext, GetWorkflowsInput, GetWorkflowsOutput, InitContext } from '.';

import { get } from 'lodash';
import { wfQueueRunner, WorkflowQueue } from './wfqueue';
import { debugTriggerPoint, DEBUG_TRIGGER_WORKFLOW_ENQUEUE } from './debugpoint';
import { DBOSScheduler } from './scheduler/scheduler';
import { DBOSEventReceiverState, DBNotificationCallback, DBNotificationListener } from './eventreceiver';
import { transaction_outputs } from '../schemas/user_db_schema';
import * as crypto from 'crypto';

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface DBOSNull {}
export const dbosNull: DBOSNull = {};

/* Interface for DBOS configuration */
export interface DBOSConfig {
  // Public fields
  name?: string;
  readonly databaseUrl?: string;
  readonly userDbclient?: UserDatabaseName;
  readonly userDbPoolSize?: number;
  readonly sysDbName?: string;
  readonly sysDbPoolSize?: number;
  readonly logLevel?: string;
  readonly otlpTracesEndpoints?: string[];
  readonly otlpLogsEndpoints?: string[];
  readonly adminPort?: number;
  readonly runAdminServer?: boolean;

  // Internal fields
  poolConfig?: PoolConfig;
  readonly telemetry?: TelemetryConfig;
  readonly system_database?: string;
  readonly env?: Record<string, string>;
  readonly application?: object;
  readonly http?: {
    readonly cors_middleware?: boolean;
    readonly credentials?: boolean;
    readonly allowed_origins?: string[];
  };
}

export type DBOSConfigInternal = Omit<DBOSConfig, 'poolConfig' | 'system_database' | 'telemetry'> & {
  poolConfig: PoolConfig;
  system_database: string;
  telemetry: TelemetryConfig;
};

export function isDeprecatedDBOSConfig(config: DBOSConfig): boolean {
  const isDeprecated =
    config.poolConfig !== undefined ||
    config.telemetry !== undefined ||
    config.system_database !== undefined ||
    config.env !== undefined ||
    config.application !== undefined ||
    config.http !== undefined;
  return isDeprecated;
}

export enum DebugMode {
  DISABLED,
  ENABLED,
  TIME_TRAVEL,
}

interface WorkflowRegInfo {
  workflow: Workflow<unknown[], unknown>;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  workflowOrigFunction: Function;
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
  procedure: StoredProcedure<unknown[], unknown>;
  config: StoredProcedureConfig;
  registration: MethodRegistrationBase;
}

export interface InternalWorkflowParams extends WorkflowParams {
  readonly tempWfType?: string;
  readonly tempWfName?: string;
  readonly tempWfClass?: string;
}

export const OperationType = {
  HANDLER: 'handler',
  WORKFLOW: 'workflow',
  TRANSACTION: 'transaction',
  COMMUNICATOR: 'communicator',
  PROCEDURE: 'procedure',
} as const;

export const TempWorkflowType = {
  transaction: 'transaction',
  procedure: 'procedure',
  step: 'step',
  send: 'send',
} as const;

type QueryFunction = <T>(sql: string, args: unknown[]) => Promise<T[]>;

export interface DBOSExecutorOptions {
  systemDatabase?: SystemDatabase;
  debugMode?: DebugMode;
}

export class DBOSExecutor implements DBOSExecutorContext {
  initialized: boolean;
  // User Database
  userDatabase: UserDatabase = null as unknown as UserDatabase;
  // System Database
  readonly systemDatabase: SystemDatabase;
  readonly procedurePool: Pool;

  // Temporary workflows are created by calling transaction/send/recv directly from the executor class
  static readonly tempWorkflowName = 'temp_workflow';

  readonly workflowInfoMap: Map<string, WorkflowRegInfo> = new Map([
    // We initialize the map with an entry for temporary workflows.
    [
      DBOSExecutor.tempWorkflowName,
      {
        workflow: async () => {
          this.logger.error('UNREACHABLE: Indirect invoke of temp workflow');
          return Promise.resolve();
        },
        workflowOrigFunction: async () => {
          this.logger.error('UNREACHABLE: Indirect invoke of temp workflow');
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
  readonly workflowCancellationMap: Map<string, boolean> = new Map(); // Map from workflowUUID to its cancellation status.

  readonly telemetryCollector: TelemetryCollector;

  static readonly defaultNotificationTimeoutSec = 60;

  readonly debugMode: DebugMode;
  get isDebugging() {
    switch (this.debugMode) {
      case DebugMode.DISABLED:
        return false;
      case DebugMode.ENABLED:
      case DebugMode.TIME_TRAVEL:
        return true;
      default: {
        const _never: never = this.debugMode;
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        throw new Error(`Unexpected DBOS debug mode: ${this.debugMode}`);
      }
    }
  }

  static systemDBSchemaName = 'dbos';

  readonly logger: Logger;
  readonly tracer: Tracer;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  typeormEntities: Function[] = [];
  drizzleEntities: { [key: string]: object } = {};

  eventReceivers: DBOSEventReceiver[] = [];

  scheduler?: DBOSScheduler = undefined;
  wfqEnded?: Promise<void> = undefined;

  readonly executorID: string = globalParams.executorID;

  static globalInstance: DBOSExecutor | undefined = undefined;

  /* WORKFLOW EXECUTOR LIFE CYCLE MANAGEMENT */
  constructor(
    readonly config: DBOSConfigInternal,
    { systemDatabase, debugMode }: DBOSExecutorOptions = {},
  ) {
    this.debugMode = debugMode ?? DebugMode.DISABLED;

    // Set configured environment variables
    if (config.env) {
      for (const [key, value] of Object.entries(config.env)) {
        if (typeof value === 'string') {
          process.env[key] = value;
        } else {
          console.warn(`Invalid value type for environment variable ${key}: ${typeof value}`);
        }
      }
    }

    if (config.telemetry.OTLPExporter) {
      const OTLPExporter = new TelemetryExporter(config.telemetry.OTLPExporter);
      this.telemetryCollector = new TelemetryCollector(OTLPExporter);
    } else {
      // We always setup a collector to drain the signals queue, even if we don't have an exporter.
      this.telemetryCollector = new TelemetryCollector();
    }
    this.logger = new Logger(this.telemetryCollector, this.config.telemetry.logs);
    this.tracer = new Tracer(this.telemetryCollector);

    if (this.isDebugging) {
      this.logger.info('Running in debug mode!');
    }

    this.procedurePool = new Pool(this.config.poolConfig);

    if (systemDatabase) {
      this.logger.debug('Using provided system database'); // XXX print the name or something
      this.systemDatabase = systemDatabase;
    } else {
      this.logger.debug('Using Postgres system database');
      this.systemDatabase = new PostgresSystemDatabase(
        this.config.poolConfig,
        this.config.system_database,
        this.logger,
        this.config.sysDbPoolSize,
      );
    }

    this.initialized = false;
    DBOSExecutor.globalInstance = this;
  }

  configureDbClient() {
    const userDbClient = this.config.userDbclient;
    const userDBConfig = this.config.poolConfig;
    if (userDbClient === UserDatabaseName.PRISMA) {
      // TODO: make Prisma work with debugger proxy.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const { PrismaClient } = require(path.join(process.cwd(), 'node_modules', '@prisma', 'client')); // Find the prisma client in the node_modules of the current project
      this.userDatabase = new PrismaUserDatabase(
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call
        new PrismaClient({
          datasources: {
            db: {
              url: userDBConfig.connectionString,
            },
          },
        }),
      );
      this.logger.debug('Loaded Prisma user database');
    } else if (userDbClient === UserDatabaseName.TYPEORM) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const DataSourceExports = require('typeorm');
      try {
        this.userDatabase = new TypeORMDatabase(
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          new DataSourceExports.DataSource({
            type: 'postgres',
            url: userDBConfig.connectionString,
            connectTimeoutMS: userDBConfig.connectionTimeoutMillis,
            entities: this.typeormEntities,
            poolSize: userDBConfig.max,
          }),
        );
      } catch (s) {
        (s as Error).message = `Error loading TypeORM user database: ${(s as Error).message}`;
        this.logger.error(s);
      }
      this.logger.debug('Loaded TypeORM user database');
    } else if (userDbClient === UserDatabaseName.KNEX) {
      const knexConfig: Knex.Config = {
        client: 'postgres',
        connection: {
          connectionString: userDBConfig.connectionString,
          connectionTimeoutMillis: userDBConfig.connectionTimeoutMillis,
        },
        pool: {
          min: 0,
          max: userDBConfig.max,
        },
      };
      this.userDatabase = new KnexUserDatabase(knex(knexConfig));
      this.logger.debug('Loaded Knex user database');
    } else if (userDbClient === UserDatabaseName.DRIZZLE) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const DrizzleExports = require('drizzle-orm/node-postgres');
      const drizzlePool = new Pool(userDBConfig);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const drizzle = DrizzleExports.drizzle(drizzlePool, { schema: this.drizzleEntities });
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      this.userDatabase = new DrizzleUserDatabase(drizzlePool, drizzle);
      this.logger.debug('Loaded Drizzle user database');
    } else {
      this.userDatabase = new PGNodeUserDatabase(userDBConfig);
      this.logger.debug('Loaded Postgres user database');
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
      } else if (ro.stepConfig) {
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
    const res: { methodConfig: unknown; classConfig: unknown; methodReg: MethodRegistrationBase }[] = [];
    for (const r of this.registeredOperations) {
      if (!r.eventReceiverInfo.has(obj)) continue;
      const methodConfig = r.eventReceiverInfo.get(obj)!;
      const classConfig = r.defaults?.eventReceiverInfo.get(obj) ?? {};
      res.push({ methodReg: r, methodConfig, classConfig });
    }
    return res;
  }

  async init(classes?: object[]): Promise<void> {
    if (this.initialized) {
      this.logger.error('Workflow executor already initialized!');
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
          this.typeormEntities = this.typeormEntities.concat(reg.ormEntities as any[]);
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

      if (!this.isDebugging) {
        await createDBIfDoesNotExist(this.config.poolConfig, this.logger);
      }
      this.configureDbClient();

      if (!this.userDatabase) {
        this.logger.error('No user database configured!');
        throw new DBOSInitializationError('No user database configured!');
      }

      for (const cls of classes) {
        this.#registerClass(cls);
      }

      // Debug mode doesn't need to initialize the DBs. Everything should appear to be read-only.
      await this.userDatabase.init(this.isDebugging);
      if (!this.isDebugging) {
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
    if (!this.isDebugging) {
      for (const cls of classes) {
        // Init its configurations
        const creg = getOrCreateClassRegistration(cls as AnyConstructor);
        for (const [_cfgname, cfg] of creg.configuredInstances) {
          await cfg.initialize(new InitContext());
        }
      }

      for (const v of this.registeredOperations) {
        const m = v as MethodRegistration<unknown, unknown[], unknown>;
        if (m.init === true) {
          this.logger.debug('Executing init method: ' + m.name);
          await m.origFunction(new InitContext());
        }
      }

      // Compute the application version if not provided
      if (globalParams.appVersion === '') {
        globalParams.appVersion = this.computeAppVersion();
        globalParams.wasComputed = true;
      }
      this.logger.info(`Application version: ${globalParams.appVersion}`);

      await this.recoverPendingWorkflows([this.executorID]);
    }

    this.logger.info('DBOS launched!');
  }

  #logNotice(msg: NoticeMessage) {
    switch (msg.severity) {
      case 'INFO':
      case 'LOG':
      case 'NOTICE':
        this.logger.info(msg.message);
        break;
      case 'WARNING':
        this.logger.warn(msg.message);
        break;
      case 'DEBUG':
        this.logger.debug(msg.message);
        break;
      case 'ERROR':
      case 'FATAL':
      case 'PANIC':
        this.logger.error(msg.message);
        break;
      default:
        this.logger.error(`Unknown notice severity: ${msg.severity} - ${msg.message}`);
    }
  }

  async destroy() {
    try {
      if (this.pendingWorkflowMap.size > 0) {
        this.logger.info('Waiting for pending workflows to finish.');
        await Promise.allSettled(this.pendingWorkflowMap.values());
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
    } catch (err) {
      const e = err as Error;
      this.logger.error(e);
      throw err;
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
      workflowOrigFunction: ro.origFunction,
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
      config: { ...ro.stepConfig },
      registration: ro,
    };
    this.stepInfoMap.set(cfn, stepInfo);
    this.logger.debug(`Registered step ${cfn}`);
  }

  #registerProcedure(ro: MethodRegistrationBase) {
    const proc = ro.registeredFunction as StoredProcedure<unknown[], unknown>;
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
    const wfname =
      wf.name === DBOSExecutor.tempWorkflowName ? wf.name : getRegisteredMethodClassName(wf) + '.' + wf.name;
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
      throw new DBOSNotRegisteredError(tfname, `Transaction function name '${tfname}' is not registered.`);
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
      throw new DBOSNotRegisteredError(cfname, `Step function name '${cfname}' is not registered.`);
    }

    return { commInfo: stepInfo, clsInst: getConfiguredInstance(className, cfgName) };
  }

  getProcedureClassName<T extends unknown[], R>(pf: StoredProcedure<T, R>) {
    return getRegisteredMethodClassName(pf);
  }

  getProcedureInfo<T extends unknown[], R>(pf: StoredProcedure<T, R>) {
    const pfName = getRegisteredMethodClassName(pf) + '.' + pf.name;
    return this.procedureInfoMap.get(pfName);
  }
  // TODO: getProcedureInfoByNames??

  static reviveResultOrError<R = unknown>(r: SystemDatabaseStoredResult, success?: boolean) {
    if (success === true || !r.err) {
      return DBOSJSON.parse(r.res ?? null) as R;
    } else {
      throw deserializeError(DBOSJSON.parse(r.err));
    }
  }

  async workflow<T extends unknown[], R>(
    wf: Workflow<T, R>,
    params: InternalWorkflowParams,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    return this.internalWorkflow(wf, params, undefined, undefined, ...args);
  }

  // If callerUUID and functionID are set, it means the workflow is invoked from within a workflow.
  async internalWorkflow<T extends unknown[], R>(
    wf: Workflow<T, R>,
    params: InternalWorkflowParams,
    callerID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    const workflowID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();
    const presetID: boolean = params.workflowUUID ? true : false;

    const wInfo = this.getWorkflowInfo(wf as Workflow<unknown[], unknown>);
    if (wInfo === undefined) {
      throw new DBOSNotRegisteredError(wf.name);
    }
    const wConfig = wInfo.config;

    const passContext = wInfo.registration?.passContext ?? true;
    const wCtxt: WorkflowContextImpl = new WorkflowContextImpl(
      this,
      params.parentCtx,
      workflowID,
      wConfig,
      wf.name,
      presetID,
      params.tempWfType,
      params.tempWfName,
    );

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowID,
      status: params.queueName !== undefined ? StatusString.ENQUEUED : StatusString.PENDING,
      workflowName: wf.name,
      workflowClassName: wCtxt.isTempWorkflow ? '' : getRegisteredMethodClassName(wf),
      workflowConfigName: params.configuredInstance?.name || '',
      queueName: params.queueName,
      authenticatedUser: wCtxt.authenticatedUser,
      output: null,
      error: null,
      assumedRole: wCtxt.assumedRole,
      authenticatedRoles: wCtxt.authenticatedRoles,
      request: wCtxt.request,
      executorId: wCtxt.executorID,
      applicationVersion: globalParams.appVersion,
      applicationID: wCtxt.applicationID,
      createdAt: Date.now(), // Remember the start time of this workflow
      maxRetries: wCtxt.maxRecoveryAttempts,
    };

    if (wCtxt.isTempWorkflow) {
      internalStatus.workflowName = `${DBOSExecutor.tempWorkflowName}-${wCtxt.tempWfOperationType}-${wCtxt.tempWfOperationName}`;
      internalStatus.workflowClassName = params.tempWfClass ?? '';
    }

    let status: string | undefined = undefined;

    // Synchronously set the workflow's status to PENDING and record workflow inputs.
    // We have to do it for all types of workflows because operation_outputs table has a foreign key constraint on workflow status table.
    if (this.isDebugging) {
      const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowID);
      const wfInputs = await this.systemDatabase.getWorkflowInputs<T>(workflowID);
      if (!wfStatus || !wfInputs) {
        throw new DBOSDebuggerError(`Failed to find inputs for workflow UUID ${workflowID}`);
      }

      // Make sure we use the same input.
      if (DBOSJSON.stringify(args) !== DBOSJSON.stringify(wfInputs)) {
        throw new DBOSDebuggerError(
          `Detected different inputs for workflow UUID ${workflowID}.\n Received: ${DBOSJSON.stringify(args)}\n Original: ${DBOSJSON.stringify(wfInputs)}`,
        );
      }
      status = wfStatus.status;
    } else {
      // TODO: Make this transactional (and with the queue step below)
      if (callerFunctionID !== undefined && callerID !== undefined) {
        const cr = await this.systemDatabase.getOperationResult(callerID, callerFunctionID);
        if (cr.res !== undefined) {
          return new RetrievedHandle(this.systemDatabase, cr.res.child!, callerID, callerFunctionID);
        }
      }
      const ires = await this.systemDatabase.initWorkflowStatus(internalStatus, args);

      if (callerFunctionID !== undefined && callerID !== undefined) {
        await this.systemDatabase.recordOperationResult(
          callerID,
          callerFunctionID,
          {
            childWfId: workflowID,
            functionName: internalStatus.workflowName,
          },
          true,
        );
      }

      args = ires.args;
      status = ires.status;
      await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_ENQUEUE);
    }

    const runWorkflow = async () => {
      let result: R;

      // Execute the workflow.
      try {
        const callResult = await runWithWorkflowContext(wCtxt, async () => {
          const callPromise = passContext
            ? wf.call(params.configuredInstance, wCtxt, ...args)
            : (wf as unknown as ContextFreeFunction<T, R>).call(params.configuredInstance, ...args);
          return await callPromise;
        });

        if (this.isDebugging) {
          const recordedResult = DBOSExecutor.reviveResultOrError<Awaited<R>>(
            (await this.systemDatabase.awaitWorkflowResult(workflowID))!,
          );
          if (!resultsMatch(recordedResult, callResult)) {
            this.logger.error(
              `Detect different output for the workflow UUID ${workflowID}!\n Received: ${DBOSJSON.stringify(callResult)}\n Original: ${DBOSJSON.stringify(recordedResult)}`,
            );
          }
          result = recordedResult;
        } else {
          result = callResult!;
        }

        function resultsMatch(recordedResult: Awaited<R>, callResult: Awaited<R>): boolean {
          if (recordedResult === null) {
            return callResult === undefined || callResult === null;
          }
          return DBOSJSON.stringify(recordedResult) === DBOSJSON.stringify(callResult);
        }

        internalStatus.output = DBOSJSON.stringify(result);
        internalStatus.status = StatusString.SUCCESS;
        if (internalStatus.queueName && !this.isDebugging) {
          await this.systemDatabase.dequeueWorkflow(workflowID, this.#getQueueByName(internalStatus.queueName));
        }
        if (!this.isDebugging) {
          await this.systemDatabase.recordWorkflowOutput(workflowID, internalStatus);
        }
        wCtxt.span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        if (err instanceof DBOSWorkflowConflictUUIDError) {
          // Retrieve the handle and wait for the result.
          const retrievedHandle = this.retrieveWorkflow<R>(workflowID);
          result = await retrievedHandle.getResult();
          wCtxt.span.setAttribute('cached', true);
          wCtxt.span.setStatus({ code: SpanStatusCode.OK });
        } else if (err instanceof DBOSWorkflowCancelledError) {
          internalStatus.error = err.message;
          internalStatus.status = StatusString.CANCELLED;

          if (!this.isDebugging) {
            await this.systemDatabase.setWorkflowStatus(workflowID, StatusString.CANCELLED, false);
          }
          wCtxt.span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
          this.logger.info(`Cancelled workflow ${workflowID}`);

          throw err;
        } else {
          // Record the error.
          const e = err as Error & { dbos_already_logged?: boolean };
          this.logger.error(e);
          e.dbos_already_logged = true;
          if (wCtxt.isTempWorkflow) {
            internalStatus.workflowName = `${DBOSExecutor.tempWorkflowName}-${wCtxt.tempWfOperationType}-${wCtxt.tempWfOperationName}`;
          }
          internalStatus.error = DBOSJSON.stringify(serializeError(e));
          internalStatus.status = StatusString.ERROR;
          if (internalStatus.queueName && !this.isDebugging) {
            await this.systemDatabase.dequeueWorkflow(workflowID, this.#getQueueByName(internalStatus.queueName));
          }
          if (!this.isDebugging) {
            await this.systemDatabase.recordWorkflowError(workflowID, internalStatus);
          }
          // TODO: Log errors, but not in the tests when they're expected.
          wCtxt.span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
          throw err;
        }
      } finally {
        this.tracer.endSpan(wCtxt.span);
      }
      return result;
    };

    if (
      this.isDebugging ||
      (status !== 'SUCCESS' && status !== 'ERROR' && (params.queueName === undefined || params.executeWorkflow))
    ) {
      const workflowPromise: Promise<R> = runWorkflow();

      // Need to await for the workflow and capture errors.
      const awaitWorkflowPromise = workflowPromise
        .catch((error) => {
          this.logger.debug('Captured error in awaitWorkflowPromise: ' + error);
        })
        .finally(() => {
          // Remove itself from pending workflow map.
          this.pendingWorkflowMap.delete(workflowID);
        });
      this.pendingWorkflowMap.set(workflowID, awaitWorkflowPromise);

      // Return the normal handle that doesn't capture errors.
      return new InvokedHandle(this.systemDatabase, workflowPromise, workflowID, wf.name, callerID, callerFunctionID);
    } else {
      if (params.queueName && status === 'ENQUEUED' && !this.isDebugging) {
        await this.systemDatabase.enqueueWorkflow(workflowID, this.#getQueueByName(params.queueName).name);
      }
      return new RetrievedHandle(this.systemDatabase, workflowID, callerID, callerFunctionID);
    }
  }

  #getQueueByName(name: string): WorkflowQueue {
    const q = wfQueueRunner.wfQueuesByName.get(name);
    if (!q) throw new DBOSNotRegisteredError(name, `Workflow queue '${name}' is not defined.`);
    return q;
  }

  /**
   * Retrieve the transaction snapshot information of the current transaction
   */
  static async #retrieveSnapshot(query: QueryFunction): Promise<string> {
    const rows = await query<{ txn_snapshot: string }>('SELECT pg_current_snapshot()::text as txn_snapshot;', []);
    return rows[0].txn_snapshot;
  }

  /**
   * Check if an operation has already executed in a workflow.
   * If it previously executed successfully, return its output.
   * If it previously executed and threw an error, return that error.
   * Otherwise, return DBOSNull.
   * Also return the transaction snapshot and id information of the original or current transaction.
   */
  async #checkExecution<R>(
    query: QueryFunction,
    workflowUUID: string,
    funcID: number,
    funcName: string,
  ): Promise<{ result: Error | R | DBOSNull; txn_snapshot: string; txn_id?: string }> {
    type TxOutputRow = Pick<transaction_outputs, 'output' | 'error' | 'txn_snapshot' | 'txn_id' | 'function_name'> & {
      recorded: boolean;
    };
    const rows = await query<TxOutputRow>(
      `(SELECT output, error, txn_snapshot, txn_id, function_name, true as recorded
          FROM dbos.transaction_outputs
          WHERE workflow_uuid=$1 AND function_id=$2
        UNION ALL
          SELECT null as output, null as error, pg_current_snapshot()::text as txn_snapshot,
                 null as txn_id, '' as function_name, false as recorded
       ) ORDER BY recorded`,
      [workflowUUID, funcID],
    );

    if (rows.length === 0 || rows.length > 2) {
      const returnedRows = JSON.stringify(rows);
      this.logger.error('Unexpected! This should never happen. Returned rows: ' + returnedRows);
      throw new DBOSError('This should never happen. Returned rows: ' + returnedRows);
    }

    if (rows.length === 2) {
      if (rows[1].function_name !== funcName) {
        throw new DBOSUnexpectedStepError(workflowUUID, funcID, funcName, rows[0].function_name);
      }

      const { txn_snapshot, txn_id } = rows[1];
      const error = DBOSJSON.parse(rows[1].error) as ErrorObject | null;
      if (error) {
        return { result: deserializeError(error), txn_snapshot, txn_id: txn_id ?? undefined };
      } else {
        return { result: DBOSJSON.parse(rows[1].output) as R, txn_snapshot, txn_id: txn_id ?? undefined };
      }
    } else {
      const { txn_snapshot } = rows[0];
      return { result: dbosNull, txn_snapshot, txn_id: undefined };
    }
  }

  /**
   * Write a operation's output to the database.
   */
  async #recordOutput<R>(
    query: QueryFunction,
    workflowUUID: string,
    funcID: number,
    txnSnapshot: string,
    output: R,
    isKeyConflict: (error: unknown) => boolean,
    function_name: string,
  ): Promise<string> {
    if (this.isDebugging) {
      throw new DBOSDebuggerError('Cannot record output in debug mode.');
    }
    try {
      const serialOutput = DBOSJSON.stringify(output);
      const rows = await query<transaction_outputs>(
        'INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, output, txn_id, txn_snapshot, created_at, function_name) VALUES ($1, $2, $3, (select pg_current_xact_id_if_assigned()::text), $4, $5, $6) RETURNING txn_id;',
        [workflowUUID, funcID, serialOutput, txnSnapshot, Date.now(), function_name],
      );
      return rows[0].txn_id!;
    } catch (error) {
      if (isKeyConflict(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
      } else {
        throw error;
      }
    }
  }

  /**
   * Record an error in an operation to the database.
   */
  async #recordError(
    query: QueryFunction,
    workflowUUID: string,
    funcID: number,
    txnSnapshot: string,
    err: Error,
    isKeyConflict: (error: unknown) => boolean,
    function_name: string,
  ): Promise<void> {
    if (this.isDebugging) {
      throw new DBOSDebuggerError('Cannot record error in debug mode.');
    }
    try {
      const serialErr = DBOSJSON.stringify(serializeError(err));
      await query<transaction_outputs>(
        'INSERT INTO dbos.transaction_outputs (workflow_uuid, function_id, error, txn_id, txn_snapshot, created_at, function_name) VALUES ($1, $2, $3, null, $4, $5, $6) RETURNING txn_id;',
        [workflowUUID, funcID, serialErr, txnSnapshot, Date.now(), function_name],
      );
    } catch (error) {
      if (isKeyConflict(error)) {
        // Serialization and primary key conflict (Postgres).
        throw new DBOSWorkflowConflictUUIDError(workflowUUID);
      } else {
        throw error;
      }
    }
  }

  async getTransactions(workflowUUID: string): Promise<step_info[]> {
    const rows = await this.userDatabase.query<step_info, [string]>(
      `SELECT function_id, function_name, output, error FROM ${DBOSExecutor.systemDBSchemaName}.transaction_outputs WHERE workflow_uuid=$1`,
      workflowUUID,
    );

    for (const row of rows) {
      row.output = row.output !== null ? DBOSJSON.parse(row.output as string) : null;
      row.error = row.error !== null ? deserializeError(DBOSJSON.parse(row.error as unknown as string)) : null;
    }

    return rows;
  }

  async transaction<T extends unknown[], R>(txn: Transaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    return await (await this.startTransactionTempWF(txn, params, undefined, undefined, ...args)).getResult();
  }

  async startTransactionTempWF<T extends unknown[], R>(
    txn: Transaction<T, R>,
    params: InternalWorkflowParams,
    callerUUID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    // Create a workflow and call transaction.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await this.callTransactionFunction(txn, params.configuredInstance ?? null, ctxtImpl, ...args);
    };
    return await this.internalWorkflow(
      temp_workflow,
      {
        ...params,
        tempWfType: TempWorkflowType.transaction,
        tempWfName: getRegisteredMethodName(txn),
        tempWfClass: getRegisteredMethodClassName(txn),
      },
      callerUUID,
      callerFunctionID,
      ...args,
    );
  }

  async callTransactionFunction<T extends unknown[], R>(
    txn: Transaction<T, R>,
    clsinst: ConfiguredInstance | null,
    wfCtx: WorkflowContextImpl,
    ...args: T
  ): Promise<R> {
    const txnInfo = this.getTransactionInfo(txn as Transaction<unknown[], unknown>);
    if (txnInfo === undefined) {
      throw new DBOSNotRegisteredError(txn.name);
    }

    if (this.workflowCancellationMap.get(wfCtx.workflowUUID) === true) {
      throw new DBOSWorkflowCancelledError(wfCtx.workflowUUID);
    }

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
        isolationLevel: txnInfo.config.isolationLevel,
      },
      wfCtx.span,
    );

    while (true) {
      if (this.workflowCancellationMap.get(wfCtx.workflowUUID) === true) {
        throw new DBOSWorkflowCancelledError(wfCtx.workflowUUID);
      }

      let txn_snapshot = 'invalid';
      const workflowUUID = wfCtx.workflowUUID;
      const wrappedTransaction = async (client: UserDatabaseClient): Promise<R> => {
        const tCtxt = new TransactionContextImpl(
          this.userDatabase.getName(),
          client,
          wfCtx,
          span,
          this.logger,
          funcId,
          txn.name,
        );

        // If the UUID is preset, it is possible this execution previously happened. Check, and return its original result if it did.
        // Note: It is possible to retrieve a generated ID from a workflow handle, run a concurrent execution, and cause trouble for yourself. We recommend against this.
        let prevResult: R | Error | DBOSNull = dbosNull;
        const queryFunc = <T>(sql: string, args: unknown[]) =>
          this.userDatabase.queryWithClient<T>(client, sql, ...args);
        if (wfCtx.presetUUID) {
          const executionResult = await this.#checkExecution<R>(queryFunc, workflowUUID, funcId, txn.name);
          prevResult = executionResult.result;
          txn_snapshot = executionResult.txn_snapshot;
          if (prevResult !== dbosNull) {
            tCtxt.span.setAttribute('cached', true);

            if (this.debugMode === DebugMode.TIME_TRAVEL) {
              // for time travel debugging, navigate the proxy to the time of this transaction's snapshot
              await queryFunc(`--proxy:${executionResult.txn_id ?? ''}:${txn_snapshot}`, []);
            } else {
              // otherwise, return/throw the previous result
              if (prevResult instanceof Error) {
                throw prevResult;
              } else {
                return prevResult as R;
              }
            }
          }
        } else {
          // Collect snapshot information for read-only transactions and non-preset UUID transactions, if not already collected above
          txn_snapshot = await DBOSExecutor.#retrieveSnapshot(queryFunc);
        }

        if (this.isDebugging && prevResult === dbosNull) {
          throw new DBOSDebuggerError(
            `Failed to find the recorded output for the transaction: workflow UUID ${workflowUUID}, step number ${funcId}`,
          );
        }

        // Execute the user's transaction.
        const result = await (async function () {
          try {
            return await runWithTransactionContext(tCtxt, async () => {
              if (txnInfo.registration.passContext) {
                return await txn.call(clsinst, tCtxt, ...args);
              } else {
                const tf = txn as unknown as (...args: T) => Promise<R>;
                return await tf.call(clsinst, ...args);
              }
            });
          } catch (e) {
            return e instanceof Error ? e : new Error(`${e as any}`);
          }
        })();

        if (this.isDebugging) {
          if (prevResult instanceof Error) {
            throw prevResult;
          }

          const prevResultJson = DBOSJSON.stringify(prevResult);
          const resultJson = DBOSJSON.stringify(result);
          if (prevResultJson !== resultJson) {
            this.logger.error(
              `Detected different transaction output than the original one!\n Result: ${resultJson}\n Original: ${DBOSJSON.stringify(prevResultJson)}`,
            );
          }
          return prevResult as R;
        }

        if (result instanceof Error) {
          throw result;
        }

        // Record the execution, commit, and return.

        try {
          // Synchronously record the output of write transactions and obtain the transaction ID.
          const pg_txn_id = await this.#recordOutput(
            queryFunc,
            wfCtx.workflowUUID,
            funcId,
            txn_snapshot,
            result,
            (error) => this.userDatabase.isKeyConflictError(error),
            txn.name,
          );
          tCtxt.span.setAttribute('pg_txn_id', pg_txn_id);
        } catch (error) {
          if (this.userDatabase.isFailedSqlTransactionError(error)) {
            this.logger.error(
              `Postgres aborted the ${txn.name} @DBOS.transaction of Workflow ${workflowUUID}, but the function did not raise an exception.  Please ensure that the @DBOS.transaction method raises an exception if the database transaction is aborted.`,
            );
            throw new DBOSFailedSqlTransactionError(workflowUUID, txn.name);
          } else {
            throw error;
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
        const e: Error = err as Error;
        if (!this.debugMode && !(e instanceof DBOSUnexpectedStepError)) {
          if (this.userDatabase.isRetriableTransactionError(err)) {
            // serialization_failure in PostgreSQL
            span.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMillis }, performance.now());
            // Retry serialization failures.
            await sleepms(retryWaitMillis);
            retryWaitMillis *= backoffFactor;
            retryWaitMillis = retryWaitMillis < maxRetryWaitMs ? retryWaitMillis : maxRetryWaitMs;
            continue;
          }

          // Record and throw other errors.
          const e: Error = err as Error;
          await this.userDatabase.transaction(
            async (client: UserDatabaseClient) => {
              const func = <T>(sql: string, args: unknown[]) =>
                this.userDatabase.queryWithClient<T>(client, sql, ...args);
              await this.#recordError(
                func,
                wfCtx.workflowUUID,
                funcId,
                txn_snapshot,
                e,
                (error) => this.userDatabase.isKeyConflictError(error),
                txn.name,
              );
            },
            { isolationLevel: IsolationLevel.ReadCommitted },
          );
        }
        span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
        this.tracer.endSpan(span);
        throw err;
      }
    }
  }

  async procedure<T extends unknown[], R>(proc: StoredProcedure<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call procedure.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return this.callProcedureFunction(proc, ctxtImpl, ...args);
    };
    return await (
      await this.workflow(
        temp_workflow,
        {
          ...params,
          tempWfType: TempWorkflowType.procedure,
          tempWfName: getRegisteredMethodName(proc),
          tempWfClass: getRegisteredMethodClassName(proc),
        },
        ...args,
      )
    ).getResult();
  }

  async callProcedureFunction<T extends unknown[], R>(
    proc: StoredProcedure<T, R>,
    wfCtx: WorkflowContextImpl,
    ...args: T
  ): Promise<R> {
    const procInfo = this.getProcedureInfo(proc);
    if (procInfo === undefined) {
      throw new DBOSNotRegisteredError(proc.name);
    }

    if (this.workflowCancellationMap.get(wfCtx.workflowUUID) === true) {
      throw new DBOSWorkflowCancelledError(wfCtx.workflowUUID);
    }

    const executeLocally = this.isDebugging || (procInfo.config.executeLocally ?? false);
    const funcId = wfCtx.functionIDGetIncrement();
    const span: Span = this.tracer.startSpan(
      proc.name,
      {
        operationUUID: wfCtx.workflowUUID,
        operationType: OperationType.PROCEDURE,
        authenticatedUser: wfCtx.authenticatedUser,
        assumedRole: wfCtx.assumedRole,
        authenticatedRoles: wfCtx.authenticatedRoles,
        isolationLevel: procInfo.config.isolationLevel,
        executeLocally,
      },
      wfCtx.span,
    );

    try {
      const result = executeLocally
        ? await this.#callProcedureFunctionLocal(proc, args, wfCtx, span, procInfo, funcId)
        : await this.#callProcedureFunctionRemote(proc, args, wfCtx, span, procInfo.config, funcId);
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (e) {
      const { message } = e as { message: string };
      span.setStatus({ code: SpanStatusCode.ERROR, message });
      throw e;
    } finally {
      this.tracer.endSpan(span);
    }
  }

  async #callProcedureFunctionLocal<T extends unknown[], R>(
    proc: StoredProcedure<T, R>,
    args: T,
    wfCtx: WorkflowContextImpl,
    span: Span,
    procInfo: ProcedureRegInfo,
    funcId: number,
  ): Promise<R> {
    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.

    while (true) {
      if (this.workflowCancellationMap.get(wfCtx.workflowUUID) === true) {
        throw new DBOSWorkflowCancelledError(wfCtx.workflowUUID);
      }

      let txn_snapshot = 'invalid';
      const wrappedProcedure = async (client: PoolClient): Promise<R> => {
        const ctxt = new StoredProcedureContextImpl(client, wfCtx, span, this.logger, funcId, proc.name);

        let prevResult: R | Error | DBOSNull = dbosNull;
        const queryFunc = <T>(sql: string, args: unknown[]) =>
          this.procedurePool.query(sql, args).then((v) => v.rows as T[]);
        if (wfCtx.presetUUID) {
          const executionResult = await this.#checkExecution<R>(
            queryFunc,
            wfCtx.workflowUUID,
            funcId,
            wfCtx.operationName,
          );
          prevResult = executionResult.result;
          txn_snapshot = executionResult.txn_snapshot;
          if (prevResult !== dbosNull) {
            ctxt.span.setAttribute('cached', true);

            if (this.debugMode === DebugMode.TIME_TRAVEL) {
              // for time travel debugging, navigate the proxy to the time of this transaction's snapshot
              await queryFunc(`--proxy:${executionResult.txn_id ?? ''}:${txn_snapshot}`, []);
            } else {
              // otherwise, return/throw the previous result
              if (prevResult instanceof Error) {
                throw prevResult;
              } else {
                return prevResult as R;
              }
            }
          }
        } else {
          // Collect snapshot information for read-only transactions and non-preset UUID transactions, if not already collected above
          txn_snapshot = await DBOSExecutor.#retrieveSnapshot(queryFunc);
        }

        if (this.isDebugging && prevResult === dbosNull) {
          throw new DBOSDebuggerError(
            `Failed to find the recorded output for the procedure: workflow UUID ${wfCtx.workflowUUID}, step number ${funcId}`,
          );
        }

        // Execute the user's transaction.
        const result = await (async function () {
          try {
            return await runWithStoredProcContext(ctxt, async () => {
              if (procInfo.registration.passContext) {
                return await proc(ctxt, ...args);
              } else {
                const pf = proc as unknown as (...args: T) => Promise<R>;
                return await pf(...args);
              }
            });
          } catch (e) {
            return e instanceof Error ? e : new Error(`${e as any}`);
          }
        })();

        if (this.isDebugging) {
          if (prevResult instanceof Error) {
            throw prevResult;
          }
          const prevResultJson = DBOSJSON.stringify(prevResult);
          const resultJson = DBOSJSON.stringify(result);
          if (prevResultJson !== resultJson) {
            this.logger.error(
              `Detected different transaction output than the original one!\n Result: ${resultJson}\n Original: ${DBOSJSON.stringify(prevResultJson)}`,
            );
          }
          return prevResult as R;
        }

        if (result instanceof Error) {
          throw result;
        }

        // Synchronously record the output of write transactions and obtain the transaction ID.
        const func = <T>(sql: string, args: unknown[]) => client.query(sql, args).then((v) => v.rows as T[]);
        const pg_txn_id = await this.#recordOutput(
          func,
          wfCtx.workflowUUID,
          funcId,
          txn_snapshot,
          result,
          pgNodeIsKeyConflictError,
          wfCtx.operationName,
        );

        // const pg_txn_id = await wfCtx.recordOutputProc<R>(client, funcId, txn_snapshot, result);
        ctxt.span.setAttribute('pg_txn_id', pg_txn_id);

        return result;
      };

      try {
        const result = await this.invokeStoredProcFunction(wrappedProcedure, {
          isolationLevel: procInfo.config.isolationLevel,
        });
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err) {
        if (!this.isDebugging) {
          if (this.userDatabase.isRetriableTransactionError(err)) {
            // serialization_failure in PostgreSQL
            span.addEvent('TXN SERIALIZATION FAILURE', { retryWaitMillis: retryWaitMillis }, performance.now());
            // Retry serialization failures.
            await sleepms(retryWaitMillis);
            retryWaitMillis *= backoffFactor;
            retryWaitMillis = retryWaitMillis < maxRetryWaitMs ? retryWaitMillis : maxRetryWaitMs;
            continue;
          }

          // Record and throw other errors.
          const e: Error = err as Error;
          await this.invokeStoredProcFunction(
            async (client: PoolClient) => {
              const func = <T>(sql: string, args: unknown[]) => client.query(sql, args).then((v) => v.rows as T[]);
              await this.#recordError(
                func,
                wfCtx.workflowUUID,
                funcId,
                txn_snapshot,
                e,
                pgNodeIsKeyConflictError,
                wfCtx.operationName,
              );
            },
            { isolationLevel: IsolationLevel.ReadCommitted },
          );

          await this.userDatabase.transaction(
            async (client: UserDatabaseClient) => {
              const func = <T>(sql: string, args: unknown[]) =>
                this.userDatabase.queryWithClient<T>(client, sql, ...args);
              await this.#recordError(
                func,
                wfCtx.workflowUUID,
                funcId,
                txn_snapshot,
                e,
                (error) => this.userDatabase.isKeyConflictError(error),
                wfCtx.operationName,
              );
            },
            { isolationLevel: IsolationLevel.ReadCommitted },
          );
        }
        throw err;
      }
    }
  }

  async #callProcedureFunctionRemote<T extends unknown[], R>(
    proc: StoredProcedure<T, R>,
    args: T,
    wfCtx: WorkflowContextImpl,
    span: Span,
    config: StoredProcedureConfig,
    funcId: number,
  ): Promise<R> {
    if (this.isDebugging) {
      throw new DBOSDebuggerError("Can't invoke stored procedure in debug mode.");
    }

    if (this.workflowCancellationMap.get(wfCtx.workflowUUID) === true) {
      throw new DBOSWorkflowCancelledError(wfCtx.workflowUUID);
    }

    const $jsonCtx = {
      request: wfCtx.request,
      authenticatedUser: wfCtx.authenticatedUser,
      authenticatedRoles: wfCtx.authenticatedRoles,
      assumedRole: wfCtx.assumedRole,
    };

    // TODO (Qian/Harry): remove this unshift when we remove the resultBuffer argument
    // Note, node-pg converts JS arrays to postgres array literals, so must call JSON.strigify on
    // args and bufferedResults before being passed to #invokeStoredProc
    const $args = [wfCtx.workflowUUID, funcId, wfCtx.presetUUID, $jsonCtx, null, JSON.stringify(args)] as unknown[];

    const readonly = config.readOnly ?? false;
    if (!readonly) {
      $args.unshift(null);
    }

    type ReturnValue = {
      return_value: { output?: R; error?: unknown; txn_id?: string; txn_snapshot?: string; created_at?: number };
    };
    const [{ return_value }] = await this.#invokeStoredProc<ReturnValue>(
      proc as StoredProcedure<unknown[], unknown>,
      $args,
    );

    const { error, output, txn_id } = return_value;

    // if the stored proc returns an error, deserialize and throw it.
    // stored proc saves the error in tx_output before returning
    if (error) {
      throw deserializeError(error);
    }

    if (txn_id) {
      span.setAttribute('pg_txn_id', txn_id);
    }
    span.setStatus({ code: SpanStatusCode.OK });
    return output!;
  }

  async #invokeStoredProc<R extends QueryResultRow = any>(
    proc: StoredProcedure<unknown[], unknown>,
    args: unknown[],
  ): Promise<R[]> {
    const client = await this.procedurePool.connect();
    const log = (msg: NoticeMessage) => this.#logNotice(msg);

    const procClassName = this.getProcedureClassName(proc);
    const plainProcName = `${procClassName}_${proc.name}_p`;
    const procName = globalParams.wasComputed ? plainProcName : `v${globalParams.appVersion}_${plainProcName}`;

    const sql = `CALL "${procName}"(${args.map((_v, i) => `$${i + 1}`).join()});`;
    try {
      client.on('notice', log);
      return await client.query<R>(sql, args).then((value) => value.rows);
    } finally {
      client.off('notice', log);
      client.release();
    }
  }

  async invokeStoredProcFunction<R>(func: (client: PoolClient) => Promise<R>, config: TransactionConfig): Promise<R> {
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
    return await (await this.startStepTempWF(stepFn, params, undefined, undefined, ...args)).getResult();
  }

  async startStepTempWF<T extends unknown[], R>(
    stepFn: StepFunction<T, R>,
    params: InternalWorkflowParams,
    callerUUID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    // Create a workflow and call external.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await this.callStepFunction(stepFn, params.configuredInstance ?? null, ctxtImpl, ...args);
    };

    return await this.internalWorkflow(
      temp_workflow,
      {
        ...params,
        tempWfType: TempWorkflowType.step,
        tempWfName: getRegisteredMethodName(stepFn),
        tempWfClass: getRegisteredMethodClassName(stepFn),
      },
      callerUUID,
      callerFunctionID,
      ...args,
    );
  }

  /**
   * Execute a step function.
   * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
   * The step may execute many times, but once it is complete, it will not re-execute.
   */
  async callStepFunction<T extends unknown[], R>(
    stepFn: StepFunction<T, R>,
    clsInst: ConfiguredInstance | null,
    wfCtx: WorkflowContextImpl,
    ...args: T
  ): Promise<R> {
    const commInfo = this.getStepInfo(stepFn as StepFunction<unknown[], unknown>);
    if (commInfo === undefined) {
      throw new DBOSNotRegisteredError(stepFn.name);
    }

    if (this.workflowCancellationMap.get(wfCtx.workflowUUID) === true) {
      throw new DBOSWorkflowCancelledError(wfCtx.workflowUUID);
    }

    const funcID = wfCtx.functionIDGetIncrement();
    const maxRetryIntervalSec = 3600; // Maximum retry interval: 1 hour

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

    // Check if this execution previously happened, returning its original result if it did.
    const checkr = await this.systemDatabase.getOperationResult(wfCtx.workflowUUID, ctxt.functionID);
    if (checkr.res !== undefined) {
      if (checkr.res.functionName !== ctxt.operationName) {
        throw new DBOSUnexpectedStepError(
          ctxt.workflowUUID,
          ctxt.functionID,
          ctxt.operationName,
          checkr.res.functionName ?? '?',
        );
      }
      const check = DBOSExecutor.reviveResultOrError<R>(checkr.res);
      ctxt.span.setAttribute('cached', true);
      ctxt.span.setStatus({ code: SpanStatusCode.OK });
      this.tracer.endSpan(ctxt.span);
      return check;
    }

    if (this.isDebugging) {
      throw new DBOSDebuggerError(
        `Failed to find the recorded output for the step: workflow UUID: ${wfCtx.workflowUUID}, step number: ${funcID}`,
      );
    }

    // Execute the step function.  If it throws an exception, retry with exponential backoff.
    // After reaching the maximum number of retries, throw an DBOSError.
    let result: R | DBOSNull = dbosNull;
    let err: Error | DBOSNull = dbosNull;
    const errors: Error[] = [];
    if (ctxt.retriesAllowed) {
      let numAttempts = 0;
      let intervalSeconds: number = ctxt.intervalSeconds;
      if (intervalSeconds > maxRetryIntervalSec) {
        this.logger.warn(
          `Step config interval exceeds maximum allowed interval, capped to ${maxRetryIntervalSec} seconds!`,
        );
      }
      while (result === dbosNull && numAttempts++ < ctxt.maxAttempts) {
        try {
          if (this.workflowCancellationMap.get(wfCtx.workflowUUID) === true) {
            throw new DBOSWorkflowCancelledError(wfCtx.workflowUUID);
          }

          let cresult: R | undefined;
          if (commInfo.registration.passContext) {
            await runWithStepContext(ctxt, numAttempts, async () => {
              cresult = await stepFn.call(clsInst, ctxt, ...args);
            });
          } else {
            await runWithStepContext(ctxt, numAttempts, async () => {
              const sf = stepFn as unknown as (...args: T) => Promise<R>;
              cresult = await sf.call(clsInst, ...args);
            });
          }
          result = cresult!;
        } catch (error) {
          const e = error as Error;
          errors.push(e);
          this.logger.warn(
            `Error in step being automatically retried. Attempt ${numAttempts} of ${ctxt.maxAttempts}. ${e.stack}`,
          );
          span.addEvent(
            `Step attempt ${numAttempts + 1} failed`,
            { retryIntervalSeconds: intervalSeconds, error: (error as Error).message },
            performance.now(),
          );
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
          await runWithStepContext(ctxt, undefined, async () => {
            cresult = await stepFn.call(clsInst, ctxt, ...args);
          });
        } else {
          await runWithStepContext(ctxt, undefined, async () => {
            const sf = stepFn as unknown as (...args: T) => Promise<R>;
            cresult = await sf.call(clsInst, ...args);
          });
        }
        result = cresult!;
      } catch (error) {
        err = error as Error;
      }
    }

    // `result` can only be dbosNull when the step timed out
    if (result === dbosNull) {
      // Record the error, then throw it.
      err = err === dbosNull ? new DBOSMaxStepRetriesError(stepFn.name, ctxt.maxAttempts, errors) : err;
      await this.systemDatabase.recordOperationResult(
        wfCtx.workflowUUID,
        ctxt.functionID,
        {
          serialError: DBOSJSON.stringify(serializeError(err)),
          functionName: ctxt.operationName,
        },
        true,
      );
      ctxt.span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      this.tracer.endSpan(ctxt.span);
      throw err as Error;
    } else {
      // Record the execution and return.
      await this.systemDatabase.recordOperationResult(
        wfCtx.workflowUUID,
        ctxt.functionID,
        {
          serialOutput: DBOSJSON.stringify(result),
          functionName: ctxt.operationName,
        },
        true,
      );
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
    return (
      await this.workflow(
        temp_workflow,
        {
          workflowUUID: workflowUUID,
          tempWfType: TempWorkflowType.send,
          configuredInstance: null,
        },
        destinationUUID,
        message,
        topic,
      )
    ).getResult();
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  async getEvent<T>(
    workflowUUID: string,
    key: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
  ): Promise<T | null> {
    return DBOSJSON.parse(await this.systemDatabase.getEvent(workflowUUID, key, timeoutSeconds)) as T;
  }

  /**
   * Retrieve a handle for a workflow UUID.
   */
  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R> {
    return new RetrievedHandle(this.systemDatabase, workflowID);
  }

  async runAsStep<T>(
    callback: () => Promise<T>,
    functionName: string,
    workflowID?: string,
    functionID?: number,
  ): Promise<T> {
    if (workflowID !== undefined && functionID !== undefined) {
      const res = await this.systemDatabase.getOperationResult(workflowID, functionID);
      if (res.res !== undefined) {
        if (res.res.functionName !== functionName) {
          throw new DBOSUnexpectedStepError(workflowID, functionID, functionName, res.res.functionName!);
        }
        return DBOSExecutor.reviveResultOrError<T>(res.res);
      }
    }
    try {
      const output: T = await callback();
      if (workflowID !== undefined && functionID !== undefined) {
        await this.systemDatabase.recordOperationResult(
          workflowID,
          functionID,
          { serialOutput: DBOSJSON.stringify(output), functionName },
          true,
        );
      }
      return output;
    } catch (e) {
      if (workflowID !== undefined && functionID !== undefined) {
        await this.systemDatabase.recordOperationResult(
          workflowID,
          functionID,
          { serialError: DBOSJSON.stringify(serializeError(e)), functionName },
          false,
        );
      }

      throw e;
    }
  }

  getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null> {
    return this.systemDatabase.getWorkflowStatus(workflowID, callerID, callerFN);
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
    } else {
      return await this.userDatabase.query(sql);
    }
  }

  async userDBListen(channels: string[], callback: DBNotificationCallback): Promise<DBNotificationListener> {
    const notificationsClient = await this.procedurePool.connect();
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
            this.logger.warn(e);
          }
          notificationsClient.release();
        }
      },
    };
  }

  /* INTERNAL HELPERS */
  #generateUUID(): string {
    return uuidv4();
  }

  /**
   * A recovery process that by default runs during executor init time.
   * It runs to completion all pending workflows that were executing when the previous executor failed.
   */
  async recoverPendingWorkflows(executorIDs: string[] = ['local']): Promise<WorkflowHandle<unknown>[]> {
    if (this.isDebugging) {
      throw new DBOSDebuggerError('Cannot recover pending workflows in debug mode.');
    }

    const handlerArray: WorkflowHandle<unknown>[] = [];
    for (const execID of executorIDs) {
      this.logger.debug(`Recovering workflows assigned to executor: ${execID}`);
      const pendingWorkflows = await this.systemDatabase.getPendingWorkflows(execID, globalParams.appVersion);
      if (pendingWorkflows.length > 0) {
        this.logger.info(
          `Recovering ${pendingWorkflows.length} workflows from application version ${globalParams.appVersion}`,
        );
      } else {
        this.logger.info(`No workflows to recover from application version ${globalParams.appVersion}`);
      }
      for (const pendingWorkflow of pendingWorkflows) {
        this.logger.debug(
          `Recovering workflow: ${pendingWorkflow.workflowUUID}. Queue name: ${pendingWorkflow.queueName}`,
        );
        try {
          // If the workflow is member of a queue, re-enqueue it.
          if (pendingWorkflow.queueName) {
            const cleared = await this.systemDatabase.clearQueueAssignment(pendingWorkflow.workflowUUID);
            if (cleared) {
              handlerArray.push(this.retrieveWorkflow(pendingWorkflow.workflowUUID));
            } else {
              handlerArray.push(await this.executeWorkflowUUID(pendingWorkflow.workflowUUID));
            }
          } else {
            handlerArray.push(await this.executeWorkflowUUID(pendingWorkflow.workflowUUID));
          }
        } catch (e) {
          this.logger.warn(`Recovery of workflow ${pendingWorkflow.workflowUUID} failed: ${(e as Error).message}`);
        }
      }
    }
    return handlerArray;
  }

  async initEventReceivers() {
    this.scheduler = new DBOSScheduler(this);

    this.scheduler.initScheduler();

    this.wfqEnded = wfQueueRunner.dispatchLoop(this);

    for (const evtRcvr of this.eventReceivers) {
      await evtRcvr.initialize(this);
    }
  }

  async deactivateEventReceivers() {
    this.logger.debug('Deactivating event receivers');
    for (const evtRcvr of this.eventReceivers || []) {
      try {
        await evtRcvr.destroy();
      } catch (err) {
        const e = err as Error;
        this.logger.warn(`Error destroying event receiver: ${e.message}`);
      }
    }
    try {
      await this.scheduler?.destroyScheduler();
    } catch (err) {
      const e = err as Error;
      this.logger.warn(`Error destroying scheduler: ${e.message}`);
    }

    try {
      wfQueueRunner.stop();
      await this.wfqEnded;
    } catch (err) {
      const e = err as Error;
      this.logger.warn(`Error destroying wf queue runner: ${e.message}`);
    }
  }

  async executeWorkflowUUID(workflowID: string, startNewWorkflow: boolean = false): Promise<WorkflowHandle<unknown>> {
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowID);
    const inputs = await this.systemDatabase.getWorkflowInputs(workflowID);
    if (!inputs || !wfStatus) {
      this.logger.error(`Failed to find inputs for workflowUUID: ${workflowID}`);
      throw new DBOSError(`Failed to find inputs for workflow UUID: ${workflowID}`);
    }
    const parentCtx = this.#getRecoveryContext(workflowID, wfStatus);

    const { wfInfo, configuredInst } = this.getWorkflowInfoByStatus(wfStatus);

    // If starting a new workflow, assign a new UUID. Otherwise, use the workflow's original UUID.
    const workflowStartID = startNewWorkflow ? undefined : workflowID;

    if (wfInfo) {
      return this.workflow(
        wfInfo.workflow,
        {
          workflowUUID: workflowStartID,
          parentCtx: parentCtx,
          configuredInstance: configuredInst,
          queueName: wfStatus.queueName,
          executeWorkflow: true,
        },
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ...inputs,
      );
    }

    // Should be temporary workflows. Parse the name of the workflow.
    const wfName = wfStatus.workflowName;
    const nameArr = wfName.split('-');
    if (!nameArr[0].startsWith(DBOSExecutor.tempWorkflowName)) {
      // CB - Doesn't this happen if the user changed the function name in their code?
      throw new DBOSError(
        `This should never happen! Cannot find workflow info for a non-temporary workflow! UUID ${workflowID}, name ${wfName}`,
      );
    }

    let temp_workflow: Workflow<unknown[], unknown>;
    if (nameArr[1] === TempWorkflowType.transaction) {
      const { txnInfo, clsInst } = this.getTransactionInfoByNames(
        wfStatus.workflowClassName,
        nameArr[2],
        wfStatus.workflowConfigName,
      );
      if (!txnInfo) {
        this.logger.error(`Cannot find transaction info for UUID ${workflowID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }

      return await this.startTransactionTempWF(
        txnInfo.transaction,
        {
          workflowUUID: workflowStartID,
          parentCtx: parentCtx ?? undefined,
          configuredInstance: clsInst,
          queueName: wfStatus.queueName,
          executeWorkflow: true,
        },
        undefined,
        undefined,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ...inputs,
      );
    } else if (nameArr[1] === TempWorkflowType.step) {
      const { commInfo, clsInst } = this.getStepInfoByNames(
        wfStatus.workflowClassName,
        nameArr[2],
        wfStatus.workflowConfigName,
      );
      if (!commInfo) {
        this.logger.error(`Cannot find step info for UUID ${workflowID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      return await this.startStepTempWF(
        commInfo.step,
        {
          workflowUUID: workflowStartID,
          parentCtx: parentCtx ?? undefined,
          configuredInstance: clsInst,
          queueName: wfStatus.queueName, // Probably null
          executeWorkflow: true,
        },
        undefined,
        undefined,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ...inputs,
      );
    } else if (nameArr[1] === TempWorkflowType.send) {
      temp_workflow = async (ctxt: WorkflowContext, ...args: unknown[]) => {
        return await ctxt.send<unknown>(args[0] as string, args[1], args[2] as string); // id, value, topic
      };
      return this.workflow(
        temp_workflow,
        {
          workflowUUID: workflowStartID,
          parentCtx: parentCtx ?? undefined,
          tempWfType: TempWorkflowType.send,
          queueName: wfStatus.queueName,
          executeWorkflow: true,
        },
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ...inputs,
      );
    } else {
      this.logger.error(`Unrecognized temporary workflow! UUID ${workflowID}, name ${wfName}`);
      throw new DBOSNotRegisteredError(wfName);
    }
  }

  async getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSEventReceiverState | undefined> {
    return await this.systemDatabase.getEventDispatchState(svc, wfn, key);
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

  async cancelWorkflow(workflowID: string): Promise<void> {
    await this.systemDatabase.cancelWorkflow(workflowID);
    this.logger.info(`Cancelling workflow ${workflowID}`);
    this.workflowCancellationMap.set(workflowID, true);
  }

  async getWorkflowSteps(workflowID: string): Promise<step_info[]> {
    const outputs = await this.systemDatabase.getAllOperationResults(workflowID);
    return outputs.map((row) => {
      return {
        function_id: row.function_id,
        function_name: row.function_name ?? '<unknown>',
        child_workflow_id: row.child_workflow_id,
        output: row.output !== null ? (DBOSJSON.parse(row.output) as unknown) : null,
        error: row.error !== null ? deserializeError(DBOSJSON.parse(row.error as unknown as string)) : null,
      };
    });
  }

  async listWorkflowSteps(workflowID: string): Promise<step_info[]> {
    const steps = await this.getWorkflowSteps(workflowID);
    const transactions = await this.getTransactions(workflowID);

    const merged = [...steps, ...transactions];

    merged.sort((a, b) => a.function_id - b.function_id);

    return merged;
  }

  async resumeWorkflow(workflowID: string): Promise<WorkflowHandle<unknown>> {
    await this.systemDatabase.resumeWorkflow(workflowID);
    this.workflowCancellationMap.delete(workflowID);
    return await this.executeWorkflowUUID(workflowID, false);
  }

  logRegisteredHTTPUrls() {
    this.logger.info('HTTP endpoints supported:');
    this.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as HandlerRegistrationBase;
      if (ro.apiURL) {
        this.logger.info('    ' + ro.apiType.padEnd(6) + '  :  ' + ro.apiURL);
        const roles = ro.getRequiredRoles();
        if (roles.length > 0) {
          this.logger.info('        Required Roles: ' + DBOSJSON.stringify(roles));
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

  /**
    An application's version is computed from a hash of the source of its workflows.
    This is guaranteed to be stable given identical source code because it uses an MD5 hash
    and because it iterates through the workflows in sorted order.
    This way, if the app's workflows are updated (which would break recovery), its version changes.
    App version can be manually set through the DBOS__APPVERSION environment variable.
   */
  computeAppVersion(): string {
    const hasher = crypto.createHash('md5');
    const sortedWorkflowSource = Array.from(this.workflowInfoMap.values())
      .map((i) => i.workflowOrigFunction.toString())
      .sort();
    for (const sourceCode of sortedWorkflowSource) {
      hasher.update(sourceCode);
    }
    return hasher.digest('hex');
  }
}
