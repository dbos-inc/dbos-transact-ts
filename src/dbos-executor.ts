/* eslint-disable @typescript-eslint/no-explicit-any */
import { Span } from '@opentelemetry/sdk-trace-base';
import {
  DBOSError,
  DBOSInitializationError,
  DBOSWorkflowConflictError,
  DBOSNotRegisteredError,
  DBOSDebuggerError,
  DBOSFailedSqlTransactionError,
  DBOSMaxStepRetriesError,
  DBOSWorkflowCancelledError,
  DBOSUnexpectedStepError,
  DBOSInvalidQueuePriorityError,
  DBOSAwaitedWorkflowCancelledError,
  DBOSInvalidWorkflowTransitionError,
  DBOSFailLoadOperationsError,
} from './error';
import {
  InvokedHandle,
  type WorkflowHandle,
  type WorkflowParams,
  RetrievedHandle,
  StatusString,
  type WorkflowStatus,
  type GetQueuedWorkflowsInput,
  type StepInfo,
  WorkflowConfig,
  DEFAULT_MAX_RECOVERY_ATTEMPTS,
} from './workflow';

import { IsolationLevel, type TransactionConfig } from './transaction';
import { type StepConfig } from './step';
import { TelemetryCollector } from './telemetry/collector';
import { Tracer } from './telemetry/traces';
import { DBOSContextualLogger, GlobalLogger } from './telemetry/logs';
import { TelemetryExporter } from './telemetry/exporters';
import type { TelemetryConfig } from './telemetry';
import { Pool, type PoolClient, type PoolConfig, type QueryResultRow } from 'pg';
import {
  type SystemDatabase,
  PostgresSystemDatabase,
  type WorkflowStatusInternal,
  type SystemDatabaseStoredResult,
} from './system_database';
import { randomUUID } from 'node:crypto';
import {
  PGNodeUserDatabase,
  PrismaUserDatabase,
  type UserDatabase,
  TypeORMDatabase,
  UserDatabaseName,
  KnexUserDatabase,
  DrizzleUserDatabase,
  type UserDatabaseClient,
  pgNodeIsKeyConflictError,
  UserDatabaseQuery,
} from './user_database';
import {
  MethodRegistration,
  getRegisteredFunctionFullName,
  getRegisteredFunctionClassName,
  getRegisteredFunctionName,
  getConfiguredInstance,
  ConfiguredInstance,
  getNameForClass,
  getClassRegistrationByName,
  getAllRegisteredClassNames,
  getLifecycleListeners,
  UntypedAsyncFunction,
  TypedAsyncFunction,
  getFunctionRegistrationByName,
  getAllRegisteredFunctions,
  getFunctionRegistration,
  MethodRegistrationBase,
} from './decorators';
import type { step_info } from '../schemas/system_db_schema';
import { context, SpanStatusCode, trace } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import {
  runInStepContext,
  getNextWFID,
  functionIDGetIncrement,
  runWithParentContext,
  getCurrentContextStore,
  isInWorkflowCtx,
  DBOSLocalCtx,
  runWithTopContext,
} from './context';
import { HandlerRegistrationBase } from './httpServer/handler';
import { deserializeError, ErrorObject, serializeError } from 'serialize-error';
import { globalParams, DBOSJSON, sleepms, INTERNAL_QUEUE_NAME } from './utils';
import path from 'node:path';
import fs from 'node:fs';
import { pathToFileURL } from 'url';
import { StoredProcedureConfig } from './procedure';
import { NoticeMessage } from 'pg-protocol/dist/messages';
import { GetWorkflowsInput, InitContext } from '.';

import { wfQueueRunner, WorkflowQueue } from './wfqueue';
import { debugTriggerPoint, DEBUG_TRIGGER_WORKFLOW_ENQUEUE } from './debugpoint';
import { ScheduledReceiver } from './scheduler/scheduler';
import { transaction_outputs } from '../schemas/user_db_schema';
import * as crypto from 'crypto';
import {
  forkWorkflow,
  listQueuedWorkflows,
  listWorkflows,
  listWorkflowSteps,
  toWorkflowStatus,
} from './dbos-runtime/workflow_management';
import { getClientConfig } from './utils';
import { ensurePGDatabase, maskDatabaseUrl } from './datasource';

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface DBOSNull {}
const dbosNull: DBOSNull = {};

export const DBOS_QUEUE_MIN_PRIORITY = 1;
export const DBOS_QUEUE_MAX_PRIORITY = 2 ** 31 - 1; // 2,147,483,647

/* Interface for DBOS configuration */
export interface DBOSConfig {
  name?: string;

  databaseUrl?: string;
  userDatabaseClient?: UserDatabaseName;
  userDatabasePoolSize?: number;

  systemDatabaseUrl?: string;
  systemDatabasePoolSize?: number;

  logLevel?: string;
  addContextMetadata?: boolean;
  otlpTracesEndpoints?: string[];
  otlpLogsEndpoints?: string[];
  adminPort?: number;
  runAdminServer?: boolean;
}

export interface DBOSRuntimeConfig {
  port: number;
  admin_port: number;
  runAdminServer: boolean;
  start: string[];
  setup: string[];
}

export type DBOSConfigInternal = {
  name?: string;

  databaseUrl: string;
  userDbPoolSize?: number;
  userDbClient?: UserDatabaseName;

  systemDatabaseUrl: string;
  sysDbPoolSize?: number;

  telemetry: TelemetryConfig;

  http?: {
    cors_middleware?: boolean;
    credentials?: boolean;
    allowed_origins?: string[];
  };
};

export interface InternalWorkflowParams extends WorkflowParams {
  readonly tempWfType?: string;
  readonly tempWfName?: string;
  readonly tempWfClass?: string;
}

export const OperationType = {
  HANDLER: 'handler',
  WORKFLOW: 'workflow',
  TRANSACTION: 'transaction',
  STEP: 'step',
  PROCEDURE: 'procedure',
} as const;

export const TempWorkflowType = {
  transaction: 'transaction',
  procedure: 'procedure',
  step: 'step',
  send: 'send',
} as const;

type QueryFunction = <T>(sql: string, args: unknown[]) => Promise<T[]>;

/**
 * State item to be kept in the DBOS system database on behalf of clients
 */
export interface DBOSExternalState {
  /** Name of event receiver service */
  service: string;
  /** Fully qualified function name for which state is kept */
  workflowFnName: string;
  /** subkey within the service+workflowFnName */
  key: string;
  /** Value kept for the service+workflowFnName+key combination */
  value?: string;
  /** Updated time (used to version the value) */
  updateTime?: number;
  /** Updated sequence number (used to version the value) */
  updateSeq?: bigint;
}

export interface DBOSExecutorOptions {
  systemDatabase?: SystemDatabase;
  debugMode?: boolean;
}

export class DBOSExecutor {
  initialized: boolean;
  // User Database
  #userDatabase: UserDatabase | undefined = undefined;
  readonly #procedurePool: Pool | undefined = undefined;

  // System Database
  readonly systemDatabase: SystemDatabase;

  // Temporary workflows are created by calling transaction/send/recv directly from the executor class
  static readonly #tempWorkflowName = 'temp_workflow';

  readonly telemetryCollector: TelemetryCollector;

  static readonly defaultNotificationTimeoutSec = 60;

  readonly #debugMode: boolean;

  static systemDBSchemaName = 'dbos';

  readonly logger: GlobalLogger;
  readonly ctxLogger: DBOSContextualLogger;
  readonly tracer: Tracer;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  #typeormEntities: Function[] = [];
  #drizzleEntities: { [key: string]: object } = {};

  #scheduler = new ScheduledReceiver();
  #wfqEnded?: Promise<void> = undefined;

  readonly executorID: string = globalParams.executorID;

  static globalInstance: DBOSExecutor | undefined = undefined;

  static async loadClasses(entrypoints: string[]): Promise<object[]> {
    type ModuleExports = Record<string, unknown>;

    const allClasses: object[] = [];
    for (const entrypoint of entrypoints) {
      const operations = path.isAbsolute(entrypoint) ? entrypoint : path.join(process.cwd(), entrypoint);
      let exports: ModuleExports;
      if (fs.existsSync(operations)) {
        const operationsURL = pathToFileURL(operations).href;
        exports = (await import(operationsURL)) as ModuleExports;
      } else {
        throw new DBOSFailLoadOperationsError(`Failed to load operations from the entrypoint ${entrypoint}`);
      }
      const classes: object[] = [];
      for (const key in exports) {
        const $export = exports[key];
        if (isObject($export)) {
          classes.push($export);
        }
      }
      allClasses.push(...classes);
    }
    if (allClasses.length === 0) {
      throw new DBOSFailLoadOperationsError('operations not found');
    }
    return allClasses;

    function isObject(value: unknown): value is object {
      return typeof value === 'function' || (typeof value === 'object' && value !== null);
    }
  }

  /* WORKFLOW EXECUTOR LIFE CYCLE MANAGEMENT */
  constructor(
    readonly config: DBOSConfigInternal,
    { systemDatabase, debugMode }: DBOSExecutorOptions = {},
  ) {
    this.#debugMode = debugMode ?? false;

    if (config.telemetry.OTLPExporter) {
      const OTLPExporter = new TelemetryExporter(config.telemetry.OTLPExporter);
      this.telemetryCollector = new TelemetryCollector(OTLPExporter);
    } else {
      // We always setup a collector to drain the signals queue, even if we don't have an exporter.
      this.telemetryCollector = new TelemetryCollector();
    }
    this.logger = new GlobalLogger(this.telemetryCollector, this.config.telemetry.logs);
    this.ctxLogger = new DBOSContextualLogger(this.logger, () => trace.getActiveSpan() as Span);
    this.tracer = new Tracer(this.telemetryCollector);

    if (this.#debugMode) {
      this.logger.info('Running in debug mode!');
    }

    this.#procedurePool = this.config.userDbClient ? new Pool(getClientConfig(this.config.databaseUrl)) : undefined;

    if (systemDatabase) {
      this.logger.debug('Using provided system database'); // XXX print the name or something
      this.systemDatabase = systemDatabase;
    } else {
      this.logger.debug('Using Postgres system database');
      this.systemDatabase = new PostgresSystemDatabase(
        this.config.systemDatabaseUrl,
        this.logger,
        this.config.sysDbPoolSize,
      );
    }

    this.initialized = false;
    DBOSExecutor.globalInstance = this;
  }

  get appName(): string | undefined {
    return this.config.name;
  }

  #configureDbClient() {
    const userDbClient = this.config.userDbClient;
    const userDBConfig: PoolConfig = getClientConfig(this.config.databaseUrl);
    userDBConfig.max = this.config.userDbPoolSize ?? 20;
    if (userDbClient === UserDatabaseName.PRISMA) {
      // TODO: make Prisma work with debugger proxy.
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const { PrismaClient } = require(path.join(process.cwd(), 'node_modules', '@prisma', 'client')); // Find the prisma client in the node_modules of the current project
      this.#userDatabase = new PrismaUserDatabase(
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
        this.#userDatabase = new TypeORMDatabase(
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
          new DataSourceExports.DataSource({
            type: 'postgres',
            url: userDBConfig.connectionString,
            connectTimeoutMS: userDBConfig.connectionTimeoutMillis,
            entities: this.#typeormEntities,
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
        connection: getClientConfig(this.config.databaseUrl),
        pool: {
          min: 0,
          max: userDBConfig.max,
        },
      };
      this.#userDatabase = new KnexUserDatabase(knex(knexConfig));
      this.logger.debug('Loaded Knex user database');
    } else if (userDbClient === UserDatabaseName.DRIZZLE) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-require-imports
      const DrizzleExports = require('drizzle-orm/node-postgres');
      const drizzlePool = new Pool(userDBConfig);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
      const drizzle = DrizzleExports.drizzle(drizzlePool, { schema: this.#drizzleEntities });
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      this.#userDatabase = new DrizzleUserDatabase(drizzlePool, drizzle);
      this.logger.debug('Loaded Drizzle user database');
    } else {
      this.#userDatabase = new PGNodeUserDatabase(userDBConfig);
      this.logger.debug('Loaded Postgres user database');
    }
  }

  async init(classes?: object[]): Promise<void> {
    if (this.initialized) {
      this.logger.error('Workflow executor already initialized!');
      return;
    }

    let classnames: string[] = [];
    if (!classes || !classes.length) {
      classnames = getAllRegisteredClassNames();
    } else {
      classnames = classes.map((c) => getNameForClass(c as AnyConstructor));
    }

    type AnyConstructor = new (...args: unknown[]) => object;
    try {
      let length; // Track the length of the array (or number of keys of the object)
      for (const clsname of classnames) {
        const reg = getClassRegistrationByName(clsname);
        /**
         * With TSORM, we take an array of entities (Function[]) and add them to this.entities:
         */
        if (Array.isArray(reg.ormEntities)) {
          this.#typeormEntities = this.#typeormEntities.concat(reg.ormEntities as any[]);
          length = reg.ormEntities.length;
        } else {
          /**
           * With Drizzle, we need to take an object of entities, since the object keys are used to access the entities from ctx.client.query:
           */
          this.#drizzleEntities = { ...this.#drizzleEntities, ...reg.ormEntities };
          length = Object.keys(reg.ormEntities).length;
        }
        this.logger.debug(`Loaded ${length} ORM entities`);
      }

      if (this.config.userDbClient) {
        if (!this.#debugMode) {
          const res = await ensurePGDatabase({
            urlToEnsure: this.config.databaseUrl,
            logger: (msg: string) => this.logger.debug(msg),
          });
          if (res.status === 'connection_error') {
            this.logger.warn(
              `Failed to check or create connection to application database ${maskDatabaseUrl(this.config.databaseUrl)}: ${res.hint ?? ''}\n  ${res.notes.join('\n')}`,
            );
          }
          if (res.status === 'failed') {
            this.logger.warn(
              `Application database does not exist and could not be created: ${maskDatabaseUrl(this.config.databaseUrl)}: ${res.hint ?? ''}\n  ${res.notes.join('\n')}`,
            );
          }
        }
        this.#configureDbClient();

        if (!this.#userDatabase) {
          this.logger.error('No user database configured!');
          throw new DBOSInitializationError('No user database configured!');
        }

        // Debug mode doesn't need to initialize the DBs. Everything should appear to be read-only.
        await this.#userDatabase.init(this.#debugMode);
      }

      // Debug mode doesn't initialize the sys db
      await this.systemDatabase.init(this.#debugMode);
    } catch (err) {
      if (err instanceof DBOSInitializationError) {
        throw err;
      }
      this.logger.error(err);
      let message = 'Failed to initialize workflow executor: ';
      if (err instanceof AggregateError) {
        for (const error of err.errors as Error[]) {
          message += `${error.message}; `;
        }
      } else if (err instanceof Error) {
        message += err.message;
      } else {
        message += String(err);
      }
      throw new DBOSInitializationError(message, err instanceof Error ? err : undefined);
    }
    this.initialized = true;

    // Only execute init code if under non-debug mode
    if (!this.#debugMode) {
      for (const cls of classnames) {
        // Init its configurations
        const creg = getClassRegistrationByName(cls);
        for (const [_cfgname, cfg] of creg.configuredInstances) {
          await cfg.initialize(new InitContext());
        }
      }

      for (const v of getAllRegisteredFunctions()) {
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
      this.logger.info(`Initializing DBOS (v${globalParams.dbosVersion})`);
      this.logger.info(`Executor ID: ${this.executorID}`);
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
      await this.systemDatabase.awaitRunningWorkflows();
      await this.systemDatabase.destroy();
      await this.#userDatabase?.destroy();
      await this.#procedurePool?.end();
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

  async createUserSchema() {
    if (!this.#userDatabase) {
      throw new Error('User database not enabled.');
    }
    await this.#userDatabase.createSchema();
  }

  async dropUserSchema() {
    if (!this.#userDatabase) {
      throw new Error('User database not enabled.');
    }
    await this.#userDatabase.dropSchema();
  }
  async queryUserDbFunction<C extends UserDatabaseClient, R, T extends unknown[]>(
    queryFunction: UserDatabaseQuery<C, R, T>,
    ...params: T
  ): Promise<R> {
    if (!this.#userDatabase) {
      throw new Error('User database not enabled.');
    }
    return await this.#userDatabase.queryFunction(queryFunction, ...params);
  }

  // This could return WF, or the function underlying a temp wf
  #getFunctionInfoFromWFStatus(wf: WorkflowStatusInternal) {
    const methReg = getFunctionRegistrationByName(wf.workflowClassName, wf.workflowName);
    return { methReg, configuredInst: getConfiguredInstance(wf.workflowClassName, wf.workflowConfigName) };
  }

  static reviveResultOrError<R = unknown>(r: SystemDatabaseStoredResult, success?: boolean) {
    if (success === true || !r.error) {
      return DBOSJSON.parse(r.output ?? null) as R;
    } else {
      throw deserializeError(DBOSJSON.parse(r.error));
    }
  }

  async workflow<T extends unknown[], R>(
    wf: TypedAsyncFunction<T, R>,
    params: InternalWorkflowParams,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    return this.internalWorkflow(wf, params, undefined, undefined, ...args);
  }

  // If callerWFID and functionID are set, it means the workflow is invoked from within a workflow.
  async internalWorkflow<T extends unknown[], R>(
    wf: TypedAsyncFunction<T, R>,
    params: InternalWorkflowParams,
    callerID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    const workflowID: string = params.workflowUUID ? params.workflowUUID : randomUUID();
    const presetID: boolean = params.workflowUUID ? true : false;
    const timeoutMS = params.timeoutMS;
    // If a timeout is explicitly specified, use it over any propagated deadline
    const deadlineEpochMS = params.timeoutMS
      ? // Queued workflows are assigned a deadline on dequeue. Otherwise, compute the deadline immediately
        params.queueName
        ? undefined
        : Date.now() + params.timeoutMS
      : // if no timeout is specified, use the propagated deadline (if any)
        params.deadlineEpochMS;

    const priority = params?.enqueueOptions?.priority;
    if (priority !== undefined && (priority < DBOS_QUEUE_MIN_PRIORITY || priority > DBOS_QUEUE_MAX_PRIORITY)) {
      throw new DBOSInvalidQueuePriorityError(priority, DBOS_QUEUE_MIN_PRIORITY, DBOS_QUEUE_MAX_PRIORITY);
    }

    // If the workflow is called on a queue with a priority but the queue is not configured with a priority, print a warning.
    if (params.queueName) {
      const wfqueue = this.#getQueueByName(params.queueName);
      if (!wfqueue.priorityEnabled && priority !== undefined) {
        this.logger.warn(
          `Priority is not enabled for queue ${params.queueName}. Setting priority will not have any effect.`,
        );
      }
    }

    const pctx = { ...getCurrentContextStore() }; // function ID was already incremented...

    let wConfig: WorkflowConfig = {};
    const wInfo = getFunctionRegistration(wf);

    if (wf.name !== DBOSExecutor.#tempWorkflowName) {
      if (!wInfo || !wInfo.workflowConfig) {
        throw new DBOSNotRegisteredError(wf.name);
      }
      wConfig = wInfo.workflowConfig;
    }

    const maxRecoveryAttempts = wConfig.maxRecoveryAttempts
      ? wConfig.maxRecoveryAttempts
      : DEFAULT_MAX_RECOVERY_ATTEMPTS;

    const wfname = wf.name; // TODO: Should be what was registered in wfInfo...

    const span = this.tracer.startSpan(wfname, {
      status: StatusString.PENDING,
      operationUUID: workflowID,
      operationType: OperationType.WORKFLOW,
      operationName: wInfo?.name ?? wf.name,
      authenticatedUser: pctx?.authenticatedUser ?? '',
      authenticatedRoles: pctx?.authenticatedRoles ?? [],
      assumedRole: pctx?.assumedRole ?? '',
    });

    const isTempWorkflow = DBOSExecutor.#tempWorkflowName === wfname;

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowID,
      status: params.queueName !== undefined ? StatusString.ENQUEUED : StatusString.PENDING,
      workflowName: getRegisteredFunctionName(wf),
      workflowClassName: isTempWorkflow ? '' : getRegisteredFunctionClassName(wf),
      workflowConfigName: params.configuredInstance?.name || '',
      queueName: params.queueName,
      output: null,
      error: null,
      authenticatedUser: pctx?.authenticatedUser || '',
      assumedRole: pctx?.assumedRole || '',
      authenticatedRoles: pctx?.authenticatedRoles || [],
      request: pctx?.request || {},
      executorId: globalParams.executorID,
      applicationVersion: globalParams.appVersion,
      applicationID: globalParams.appID,
      createdAt: Date.now(), // Remember the start time of this workflow,
      timeoutMS: timeoutMS,
      deadlineEpochMS: deadlineEpochMS,
      input: DBOSJSON.stringify(args),
      deduplicationID: params.enqueueOptions?.deduplicationID,
      priority: priority ?? 0,
    };

    if (isTempWorkflow) {
      internalStatus.workflowName = `${DBOSExecutor.#tempWorkflowName}-${params.tempWfType}-${params.tempWfName}`;
      internalStatus.workflowClassName = params.tempWfClass ?? '';
    }

    let status: string | undefined = undefined;
    let $deadlineEpochMS: number | undefined = undefined;

    // Synchronously set the workflow's status to PENDING and record workflow inputs.
    // We have to do it for all types of workflows because operation_outputs table has a foreign key constraint on workflow status table.
    if (this.#debugMode) {
      const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowID);
      if (!wfStatus) {
        throw new DBOSDebuggerError(`Failed to find inputs for workflow UUID ${workflowID}`);
      }

      // Make sure we use the same input.
      if (DBOSJSON.stringify(args) !== wfStatus.input) {
        throw new DBOSDebuggerError(
          `Detected different inputs for workflow UUID ${workflowID}.\n Received: ${DBOSJSON.stringify(args)}\n Original: ${wfStatus.input}`,
        );
      }
      status = wfStatus.status;
    } else {
      if (callerFunctionID !== undefined && callerID !== undefined) {
        const result = await this.systemDatabase.getOperationResultAndThrowIfCancelled(callerID, callerFunctionID);
        if (result) {
          return new RetrievedHandle(this.systemDatabase, result.childWorkflowID!);
        }
      }
      const ires = await this.systemDatabase.initWorkflowStatus(internalStatus, maxRecoveryAttempts);

      if (callerFunctionID !== undefined && callerID !== undefined) {
        await this.systemDatabase.recordOperationResult(callerID, callerFunctionID, internalStatus.workflowName, true, {
          childWorkflowID: workflowID,
        });
      }

      status = ires.status;
      $deadlineEpochMS = ires.deadlineEpochMS;
      await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_ENQUEUE);
    }

    async function callPromiseWithTimeout(
      callPromise: Promise<R>,
      deadlineEpochMS: number,
      sysdb: SystemDatabase,
    ): Promise<R> {
      let timeoutID: ReturnType<typeof setTimeout> | undefined = undefined;
      const timeoutResult = {};
      const timeoutPromise = new Promise<R>((_, reject) => {
        timeoutID = setTimeout(reject, deadlineEpochMS - Date.now(), timeoutResult);
      });

      try {
        return await Promise.race([callPromise, timeoutPromise]);
      } catch (err) {
        if (err === timeoutResult) {
          await sysdb.cancelWorkflow(workflowID);
          await callPromise.catch(() => {});
          throw new DBOSWorkflowCancelledError(workflowID);
        }
        throw err;
      } finally {
        clearTimeout(timeoutID);
      }
    }

    async function handleWorkflowError(err: Error, exec: DBOSExecutor) {
      // Record the error.
      const e = err as Error & { dbos_already_logged?: boolean };
      exec.logger.error(e);
      e.dbos_already_logged = true;
      internalStatus.error = DBOSJSON.stringify(serializeError(e));
      internalStatus.status = StatusString.ERROR;
      if (!exec.#debugMode) {
        await exec.systemDatabase.recordWorkflowError(workflowID, internalStatus);
      }
      span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
    }

    const runWorkflow = async () => {
      let result: R;

      // Execute the workflow.
      try {
        const callResult = await context.with(trace.setSpan(context.active(), span), async () => {
          return await runWithParentContext(
            pctx,
            {
              presetID,
              workflowTimeoutMS: undefined, // Becomes deadline
              deadlineEpochMS,
              workflowId: workflowID,
              logger: this.ctxLogger,
              curWFFunctionId: undefined,
            },
            () => {
              const callPromise = wf.call(params.configuredInstance, ...args);

              if ($deadlineEpochMS === undefined) {
                return callPromise;
              } else {
                return callPromiseWithTimeout(callPromise, $deadlineEpochMS, this.systemDatabase);
              }
            },
          );
        });

        if (this.#debugMode) {
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
        if (!this.#debugMode) {
          await this.systemDatabase.recordWorkflowOutput(workflowID, internalStatus);
        }
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        if (err instanceof DBOSWorkflowConflictError) {
          // Retrieve the handle and wait for the result.
          const retrievedHandle = this.retrieveWorkflow<R>(workflowID);
          result = await retrievedHandle.getResult();
          span.setAttribute('cached', true);
          span.setStatus({ code: SpanStatusCode.OK });
        } else if (err instanceof DBOSWorkflowCancelledError) {
          span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
          internalStatus.error = err.message;
          if (err.workflowID === workflowID) {
            internalStatus.status = StatusString.CANCELLED;
            throw err;
          } else {
            const e = new DBOSAwaitedWorkflowCancelledError(err.workflowID);
            await handleWorkflowError(e as Error, this);
            throw e;
          }
        } else {
          await handleWorkflowError(err as Error, this);
          throw err;
        }
      } finally {
        this.tracer.endSpan(span);
      }
      return result;
    };

    if (
      this.#debugMode ||
      (status !== 'SUCCESS' && status !== 'ERROR' && (params.queueName === undefined || params.executeWorkflow))
    ) {
      const workflowPromise: Promise<R> = runWorkflow();

      this.systemDatabase.registerRunningWorkflow(workflowID, workflowPromise);

      // Return the normal handle that doesn't capture errors.
      return new InvokedHandle(this.systemDatabase, workflowPromise, workflowID, wf.name);
    } else {
      return new RetrievedHandle(this.systemDatabase, workflowID);
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
    if (this.#debugMode) {
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
        throw new DBOSWorkflowConflictError(workflowUUID);
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
    if (this.#debugMode) {
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
        throw new DBOSWorkflowConflictError(workflowUUID);
      } else {
        throw error;
      }
    }
  }

  async getTransactions(workflowUUID: string): Promise<step_info[]> {
    if (this.#userDatabase) {
      const rows = await this.#userDatabase.query<step_info, [string]>(
        `SELECT function_id, function_name, output, error FROM ${DBOSExecutor.systemDBSchemaName}.transaction_outputs WHERE workflow_uuid=$1`,
        workflowUUID,
      );

      for (const row of rows) {
        row.output = row.output !== null ? DBOSJSON.parse(row.output as string) : null;
        row.error = row.error !== null ? deserializeError(DBOSJSON.parse(row.error as unknown as string)) : null;
      }

      return rows;
    } else {
      return [];
    }
  }

  async runTransactionTempWF<T extends unknown[], R>(
    txn: (...args: T) => Promise<R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<R> {
    return await (await this.startTransactionTempWF(txn, params, undefined, undefined, ...args)).getResult();
  }

  async startTransactionTempWF<T extends unknown[], R>(
    txn: (...args: T) => Promise<R>,
    params: InternalWorkflowParams,
    callerWFID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    // Create a workflow and call transaction.
    const temp_workflow = async (...args: T) => {
      return await this.callTransactionFunction(txn, params.configuredInstance ?? null, ...args);
    };
    return await this.internalWorkflow(
      temp_workflow,
      {
        ...params,
        tempWfType: TempWorkflowType.transaction,
        tempWfName: getRegisteredFunctionName(txn),
        tempWfClass: getRegisteredFunctionClassName(txn),
      },
      callerWFID,
      callerFunctionID,
      ...args,
    );
  }

  async callTransactionFunction<T extends unknown[], R>(
    txn: TypedAsyncFunction<T, R>,
    clsinst: ConfiguredInstance | null,
    ...args: T
  ): Promise<R> {
    const userDB = this.#userDatabase;
    if (!userDB) {
      throw new Error('No user database configured for transactions.');
    }

    const txnReg = getFunctionRegistration(txn);
    if (!txnReg || !txnReg.txnConfig) {
      throw new DBOSNotRegisteredError(txn.name);
    }

    const funcId = functionIDGetIncrement();
    const pctx = { ...getCurrentContextStore()! };
    const wfid = pctx.workflowId!;

    await this.systemDatabase.checkIfCanceled(wfid);

    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.
    const span: Span = this.tracer.startSpan(txn.name, {
      operationUUID: wfid,
      operationType: OperationType.TRANSACTION,
      operationName: txn.name,
      authenticatedUser: pctx.authenticatedUser ?? '',
      assumedRole: pctx.assumedRole ?? '',
      authenticatedRoles: pctx.authenticatedRoles ?? [],
      isolationLevel: txnReg.txnConfig.isolationLevel,
    });

    while (true) {
      await this.systemDatabase.checkIfCanceled(wfid);

      let txn_snapshot = 'invalid';
      let prevResultFound = false;
      const wrappedTransaction = async (client: UserDatabaseClient): Promise<R> => {
        // If the UUID is preset, it is possible this execution previously happened. Check, and return its original result if it did.
        // Note: It is possible to retrieve a generated ID from a workflow handle, run a concurrent execution, and cause trouble for yourself. We recommend against this.
        let prevResult: R | Error | DBOSNull = dbosNull;
        const queryFunc = <T>(sql: string, args: unknown[]) => userDB.queryWithClient<T>(client, sql, ...args);
        if (pctx.presetID) {
          const executionResult = await this.#checkExecution<R>(queryFunc, wfid, funcId, txn.name);
          prevResult = executionResult.result;
          txn_snapshot = executionResult.txn_snapshot;
          if (prevResult !== dbosNull) {
            prevResultFound = true;
            span.setAttribute('cached', true);

            // Return/throw the previous result
            if (prevResult instanceof Error) {
              throw prevResult;
            } else {
              return prevResult as R;
            }
          }
        } else {
          // Collect snapshot information for read-only transactions and non-preset UUID transactions, if not already collected above
          txn_snapshot = await DBOSExecutor.#retrieveSnapshot(queryFunc);
        }

        if (this.#debugMode && prevResult === dbosNull) {
          throw new DBOSDebuggerError(
            `Failed to find the recorded output for the transaction: workflow UUID ${wfid}, step number ${funcId}`,
          );
        }

        // Execute the user's transaction.
        const ctxlog = this.ctxLogger;
        const result = await (async function () {
          try {
            return await context.with(trace.setSpan(context.active(), span), async () => {
              return await runWithParentContext(
                pctx,
                {
                  authenticatedRoles: pctx?.authenticatedRoles,
                  authenticatedUser: pctx?.authenticatedUser,
                  workflowId: wfid,
                  curTxFunctionId: funcId,
                  parentCtx: pctx,
                  sqlClient: client,
                  logger: ctxlog,
                },
                async () => {
                  const tf = txn as unknown as (...args: T) => Promise<R>;
                  return await tf.call(clsinst, ...args);
                },
              );
            });
          } catch (e) {
            return e instanceof Error ? e : new Error(`${e as any}`);
          }
        })();

        if (this.#debugMode) {
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
            wfid,
            funcId,
            txn_snapshot,
            result,
            (error) => userDB.isKeyConflictError(error),
            txn.name,
          );
          span.setAttribute('pg_txn_id', pg_txn_id);
        } catch (error) {
          if (userDB.isFailedSqlTransactionError(error)) {
            this.logger.error(
              `Postgres aborted the ${txn.name} @DBOS.transaction of Workflow ${wfid}, but the function did not raise an exception.  Please ensure that the @DBOS.transaction method raises an exception if the database transaction is aborted.`,
            );
            throw new DBOSFailedSqlTransactionError(wfid, txn.name);
          } else {
            throw error;
          }
        }

        return result;
      };

      try {
        const result = await userDB.transaction(wrappedTransaction, txnReg.txnConfig);
        span.setStatus({ code: SpanStatusCode.OK });
        this.tracer.endSpan(span);
        return result;
      } catch (err) {
        const e: Error = err as Error;
        if (!prevResultFound && !this.#debugMode && !(e instanceof DBOSUnexpectedStepError)) {
          if (userDB.isRetriableTransactionError(err)) {
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
          await userDB.transaction(
            async (client: UserDatabaseClient) => {
              const func = <T>(sql: string, args: unknown[]) => userDB.queryWithClient<T>(client, sql, ...args);
              await this.#recordError(
                func,
                wfid,
                funcId,
                txn_snapshot,
                e,
                (error) => userDB.isKeyConflictError(error),
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

  async runProcedureTempWF<T extends unknown[], R>(
    proc: (...args: T) => Promise<R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<R> {
    // Create a workflow and call procedure.
    const temp_workflow = async (...args: T) => {
      return this.callProcedureFunction(proc, ...args);
    };
    return await (
      await this.workflow(
        temp_workflow,
        {
          ...params,
          tempWfType: TempWorkflowType.procedure,
          tempWfName: getRegisteredFunctionName(proc),
          tempWfClass: getRegisteredFunctionClassName(proc),
        },
        ...args,
      )
    ).getResult();
  }

  async callProcedureFunction<T extends unknown[], R>(proc: (...args: T) => Promise<R>, ...args: T): Promise<R> {
    const procInfo = getFunctionRegistration(proc);
    if (!procInfo || !procInfo.procConfig) {
      throw new DBOSNotRegisteredError(proc.name);
    }
    const procConfig = procInfo.procConfig as StoredProcedureConfig;

    const pctx = getCurrentContextStore()!;
    const wfid = pctx.workflowId!;

    await this.systemDatabase.checkIfCanceled(wfid);

    const executeLocally = this.#debugMode || (procConfig.executeLocally ?? false);
    const funcId = functionIDGetIncrement();
    const span: Span = this.tracer.startSpan(proc.name, {
      operationUUID: wfid,
      operationType: OperationType.PROCEDURE,
      operationName: proc.name,
      authenticatedUser: pctx.authenticatedUser ?? '',
      assumedRole: pctx.assumedRole ?? '',
      authenticatedRoles: pctx.authenticatedRoles ?? [],
      isolationLevel: procInfo.procConfig.isolationLevel,
      executeLocally,
    });

    try {
      const result = executeLocally
        ? await this.#callProcedureFunctionLocal(proc, args, span, procInfo, funcId)
        : await this.#callProcedureFunctionRemote(proc, args, span, procConfig, funcId);
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
    proc: (...args: T) => Promise<R>,
    args: T,
    span: Span,
    procInfo: MethodRegistrationBase,
    funcId: number,
  ): Promise<R> {
    const procPool = this.#procedurePool;
    const userDB = this.#userDatabase;
    if (!procPool || !userDB) {
      throw new Error('User database not enabled.');
    }

    let retryWaitMillis = 1;
    const backoffFactor = 1.5;
    const maxRetryWaitMs = 2000; // Maximum wait 2 seconds.

    const pctx = { ...getCurrentContextStore()! };
    const wfid = pctx.workflowId!;

    while (true) {
      await this.systemDatabase.checkIfCanceled(wfid);

      let txn_snapshot = 'invalid';
      const wrappedProcedure = async (client: PoolClient): Promise<R> => {
        let prevResult: R | Error | DBOSNull = dbosNull;
        const queryFunc = <T>(sql: string, args: unknown[]) => procPool.query(sql, args).then((v) => v.rows as T[]);
        if (pctx.presetID) {
          const executionResult = await this.#checkExecution<R>(queryFunc, wfid, funcId, proc.name);
          prevResult = executionResult.result;
          txn_snapshot = executionResult.txn_snapshot;
          if (prevResult !== dbosNull) {
            span.setAttribute('cached', true);

            // Return/throw the previous result
            if (prevResult instanceof Error) {
              throw prevResult;
            } else {
              return prevResult as R;
            }
          }
        } else {
          // Collect snapshot information for read-only transactions and non-preset UUID transactions, if not already collected above
          txn_snapshot = await DBOSExecutor.#retrieveSnapshot(queryFunc);
        }

        if (this.#debugMode && prevResult === dbosNull) {
          throw new DBOSDebuggerError(
            `Failed to find the recorded output for the procedure: workflow UUID ${wfid}, step number ${funcId}`,
          );
        }

        // Execute the user's transaction.
        const ctxlog = this.ctxLogger;
        const result = await (async function () {
          try {
            // Check we are in a workflow context and not in a step / transaction already
            if (!pctx) throw new DBOSInvalidWorkflowTransitionError();
            if (!isInWorkflowCtx(pctx)) throw new DBOSInvalidWorkflowTransitionError();
            return await context.with(trace.setSpan(context.active(), span), async () => {
              return await runWithParentContext(
                pctx,
                {
                  curTxFunctionId: funcId,
                  parentCtx: pctx,
                  isInStoredProc: true,
                  sqlClient: client,
                  logger: ctxlog,
                },
                async () => {
                  const pf = proc as unknown as TypedAsyncFunction<T, R>;
                  return await pf(...args);
                },
              );
            });
          } catch (e) {
            return e instanceof Error ? e : new Error(`${e as any}`);
          }
        })();

        if (this.#debugMode) {
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
          wfid,
          funcId,
          txn_snapshot,
          result,
          pgNodeIsKeyConflictError,
          proc.name,
        );

        // const pg_txn_id = await wfCtx.recordOutputProc<R>(client, funcId, txn_snapshot, result);
        span.setAttribute('pg_txn_id', pg_txn_id);

        return result;
      };

      try {
        const result = await this.invokeStoredProcFunction(wrappedProcedure, {
          isolationLevel: procInfo.procConfig!.isolationLevel,
        });
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err) {
        if (!this.#debugMode) {
          if (userDB.isRetriableTransactionError(err)) {
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
              await this.#recordError(func, wfid, funcId, txn_snapshot, e, pgNodeIsKeyConflictError, proc.name);
            },
            { isolationLevel: IsolationLevel.ReadCommitted },
          );

          await userDB.transaction(
            async (client: UserDatabaseClient) => {
              const func = <T>(sql: string, args: unknown[]) => userDB.queryWithClient<T>(client, sql, ...args);
              await this.#recordError(
                func,
                wfid,
                funcId,
                txn_snapshot,
                e,
                (error) => userDB.isKeyConflictError(error),
                proc.name,
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
    proc: (...args: T) => Promise<R>,
    args: T,
    span: Span,
    config: StoredProcedureConfig,
    funcId: number,
  ): Promise<R> {
    if (this.#debugMode) {
      throw new DBOSDebuggerError("Can't invoke stored procedure in debug mode.");
    }

    const pctx = getCurrentContextStore()!;
    const wfid = pctx.workflowId!;

    await this.systemDatabase.checkIfCanceled(wfid);

    const $jsonCtx = {
      request: pctx.request,
      authenticatedUser: pctx.authenticatedUser,
      authenticatedRoles: pctx.authenticatedRoles,
      assumedRole: pctx.assumedRole,
    };

    // TODO (Qian/Harry): remove this unshift when we remove the resultBuffer argument
    // Note, node-pg converts JS arrays to postgres array literals, so must call JSON.strigify on
    // args and bufferedResults before being passed to #invokeStoredProc
    const $args = [wfid, funcId, pctx.presetID, $jsonCtx, null, JSON.stringify(args)] as unknown[];

    const readonly = config.readOnly ?? false;
    if (!readonly) {
      $args.unshift(null);
    }

    type ReturnValue = {
      return_value: { output?: R; error?: unknown; txn_id?: string; txn_snapshot?: string; created_at?: number };
    };
    const [{ return_value }] = await this.#invokeStoredProc<ReturnValue>(proc as UntypedAsyncFunction, $args);

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

  async #invokeStoredProc<R extends QueryResultRow = any>(proc: UntypedAsyncFunction, args: unknown[]): Promise<R[]> {
    if (!this.#procedurePool) {
      throw new Error('User Database not enabled.');
    }
    const client = await this.#procedurePool.connect();
    const log = (msg: NoticeMessage) => this.#logNotice(msg);

    const procname = getRegisteredFunctionFullName(proc);
    const plainProcName = `${procname.className}_${procname.name}_p`;
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
    if (!this.#procedurePool) {
      throw new Error('User Database not enabled.');
    }
    const client = await this.#procedurePool.connect();
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

  async runStepTempWF<T extends unknown[], R>(
    stepFn: TypedAsyncFunction<T, R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<R> {
    return await (await this.startStepTempWF(stepFn, params, undefined, undefined, ...args)).getResult();
  }

  async startStepTempWF<T extends unknown[], R>(
    stepFn: TypedAsyncFunction<T, R>,
    params: InternalWorkflowParams,
    callerWFID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    // Create a workflow and call external.
    const temp_workflow = async (...args: T) => {
      return await this.callStepFunction(stepFn, undefined, undefined, params.configuredInstance ?? null, ...args);
    };

    return await this.internalWorkflow(
      temp_workflow,
      {
        ...params,
        tempWfType: TempWorkflowType.step,
        tempWfName: getRegisteredFunctionName(stepFn),
        tempWfClass: getRegisteredFunctionClassName(stepFn),
      },
      callerWFID,
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
    stepFn: TypedAsyncFunction<T, R>,
    stepFnName: string | undefined,
    stepConfig: StepConfig | undefined,
    clsInst: object | null,
    ...args: T
  ): Promise<R> {
    stepFnName = stepFnName ?? stepFn.name ?? '<unnamed>';
    if (!stepConfig) {
      const stepReg = getFunctionRegistration(stepFn);
      stepConfig = stepReg?.stepConfig;
    }
    if (stepConfig === undefined) {
      throw new DBOSNotRegisteredError(stepFnName);
    }

    // Intentionally advance the function ID before any awaits, then work with a copy of the context.
    const funcID = functionIDGetIncrement();
    const lctx = { ...getCurrentContextStore()! };
    const wfid = lctx.workflowId!;

    await this.systemDatabase.checkIfCanceled(wfid);

    const maxRetryIntervalSec = 3600; // Maximum retry interval: 1 hour

    const span: Span = this.tracer.startSpan(stepFnName, {
      operationUUID: wfid,
      operationType: OperationType.STEP,
      operationName: stepFnName,
      authenticatedUser: lctx.authenticatedUser ?? '',
      assumedRole: lctx.assumedRole ?? '',
      authenticatedRoles: lctx.authenticatedRoles ?? [],
      retriesAllowed: stepConfig.retriesAllowed,
      intervalSeconds: stepConfig.intervalSeconds,
      maxAttempts: stepConfig.maxAttempts,
      backoffRate: stepConfig.backoffRate,
    });

    // Check if this execution previously happened, returning its original result if it did.
    const checkr = await this.systemDatabase.getOperationResultAndThrowIfCancelled(wfid, funcID);
    if (checkr) {
      if (checkr.functionName !== stepFnName) {
        throw new DBOSUnexpectedStepError(wfid, funcID, stepFnName, checkr.functionName ?? '?');
      }
      const check = DBOSExecutor.reviveResultOrError<R>(checkr);
      span.setAttribute('cached', true);
      span.setStatus({ code: SpanStatusCode.OK });
      this.tracer.endSpan(span);
      return check;
    }

    if (this.#debugMode) {
      throw new DBOSDebuggerError(
        `Failed to find the recorded output for the step: workflow UUID: ${wfid}, step number: ${funcID}`,
      );
    }

    const maxAttempts = stepConfig.maxAttempts ?? 3;

    // Execute the step function.  If it throws an exception, retry with exponential backoff.
    // After reaching the maximum number of retries, throw an DBOSError.
    let result: R | DBOSNull = dbosNull;
    let err: Error | DBOSNull = dbosNull;
    const errors: Error[] = [];
    if (stepConfig.retriesAllowed) {
      let attemptNum = 0;
      let intervalSeconds: number = stepConfig.intervalSeconds ?? 1;
      if (intervalSeconds > maxRetryIntervalSec) {
        this.logger.warn(
          `Step config interval exceeds maximum allowed interval, capped to ${maxRetryIntervalSec} seconds!`,
        );
      }
      while (result === dbosNull && attemptNum++ < (maxAttempts ?? 3)) {
        try {
          await this.systemDatabase.checkIfCanceled(wfid);

          let cresult: R | undefined;
          await runInStepContext(lctx, funcID, maxAttempts, attemptNum, async () => {
            const sf = stepFn as unknown as (...args: T) => Promise<R>;
            cresult = await sf.call(clsInst, ...args);
          });
          result = cresult!;
        } catch (error) {
          const e = error as Error;
          errors.push(e);
          this.logger.warn(
            `Error in step being automatically retried. Attempt ${attemptNum} of ${maxAttempts}. ${e.stack}`,
          );
          span.addEvent(
            `Step attempt ${attemptNum + 1} failed`,
            { retryIntervalSeconds: intervalSeconds, error: (error as Error).message },
            performance.now(),
          );
          if (attemptNum < maxAttempts) {
            // Sleep for an interval, then increase the interval by backoffRate.
            // Cap at the maximum allowed retry interval.
            await sleepms(intervalSeconds * 1000);
            intervalSeconds *= stepConfig.backoffRate ?? 2;
            intervalSeconds = intervalSeconds < maxRetryIntervalSec ? intervalSeconds : maxRetryIntervalSec;
          }
        }
      }
    } else {
      try {
        let cresult: R | undefined;
        await context.with(trace.setSpan(context.active(), span), async () => {
          await runInStepContext(lctx, funcID, maxAttempts, undefined, async () => {
            const sf = stepFn as unknown as (...args: T) => Promise<R>;
            cresult = await sf.call(clsInst, ...args);
          });
        });
        result = cresult!;
      } catch (error) {
        err = error as Error;
      }
    }

    // `result` can only be dbosNull when the step timed out
    if (result === dbosNull) {
      // Record the error, then throw it.
      err = err === dbosNull ? new DBOSMaxStepRetriesError(stepFnName, maxAttempts, errors) : err;
      await this.systemDatabase.recordOperationResult(wfid, funcID, stepFnName, true, {
        error: DBOSJSON.stringify(serializeError(err)),
      });
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      this.tracer.endSpan(span);
      throw err as Error;
    } else {
      // Record the execution and return.
      await this.systemDatabase.recordOperationResult(wfid, funcID, stepFnName, true, {
        output: DBOSJSON.stringify(result),
      });
      span.setStatus({ code: SpanStatusCode.OK });
      this.tracer.endSpan(span);
      return result as R;
    }
  }

  async runSendTempWF<T>(destinationId: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    // Create a workflow and call send.
    const temp_workflow = async (destinationId: string, message: T, topic?: string) => {
      const ctx = getCurrentContextStore();
      const functionID: number = functionIDGetIncrement();
      await this.systemDatabase.send(ctx!.workflowId!, functionID, destinationId, DBOSJSON.stringify(message), topic);
    };
    const workflowUUID = idempotencyKey ? destinationId + idempotencyKey : undefined;
    return (
      await this.workflow(
        temp_workflow,
        {
          workflowUUID: workflowUUID,
          tempWfType: TempWorkflowType.send,
          configuredInstance: null,
        },
        destinationId,
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
   * Fork a workflow.
   * The forked workflow will be assigned a new ID.
   */
  forkWorkflow(
    workflowID: string,
    startStep: number,
    options: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number } = {},
  ): Promise<string> {
    const newWorkflowID = options.newWorkflowID ?? getNextWFID(undefined);
    return forkWorkflow(this.systemDatabase, this.#userDatabase, workflowID, startStep, { ...options, newWorkflowID });
  }

  /**
   * Retrieve a handle for a workflow UUID.
   */
  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R> {
    return new RetrievedHandle(this.systemDatabase, workflowID);
  }

  async runInternalStep<T>(
    callback: () => Promise<T>,
    functionName: string,
    workflowID: string,
    functionID: number,
    childWfId?: string,
  ): Promise<T> {
    const result = await this.systemDatabase.getOperationResultAndThrowIfCancelled(workflowID, functionID);
    if (result) {
      if (result.functionName !== functionName) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, functionName, result.functionName!);
      }
      return DBOSExecutor.reviveResultOrError<T>(result);
    }
    try {
      const output: T = await callback();
      await this.systemDatabase.recordOperationResult(workflowID, functionID, functionName, true, {
        output: DBOSJSON.stringify(output),
        childWorkflowID: childWfId,
      });
      return output;
    } catch (e) {
      await this.systemDatabase.recordOperationResult(workflowID, functionID, functionName, false, {
        error: DBOSJSON.stringify(serializeError(e)),
        childWorkflowID: childWfId,
      });

      throw e;
    }
  }

  async getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null> {
    // use sysdb getWorkflowStatus directly in order to support caller ID/FN params
    const status = await this.systemDatabase.getWorkflowStatus(workflowID, callerID, callerFN);
    return status ? toWorkflowStatus(status) : null;
  }

  async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    return listWorkflows(this.systemDatabase, input);
  }

  async listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatus[]> {
    return listQueuedWorkflows(this.systemDatabase, input);
  }

  async listWorkflowSteps(workflowID: string): Promise<StepInfo[] | undefined> {
    return listWorkflowSteps(this.systemDatabase, this.#userDatabase, workflowID);
  }

  async queryUserDB(sql: string, params?: unknown[]) {
    if (!this.#userDatabase) {
      throw new Error('User database not enabled.');
    }
    if (params !== undefined) {
      return await this.#userDatabase.query(sql, ...params);
    } else {
      return await this.#userDatabase.query(sql);
    }
  }

  /* INTERNAL HELPERS */
  /**
   * A recovery process that by default runs during executor init time.
   * It runs to completion all pending workflows that were executing when the previous executor failed.
   */
  async recoverPendingWorkflows(executorIDs: string[] = ['local']): Promise<WorkflowHandle<unknown>[]> {
    if (this.#debugMode) {
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
    this.#wfqEnded = wfQueueRunner.dispatchLoop(this);

    for (const lcl of getLifecycleListeners()) {
      await lcl.initialize?.();
    }
  }

  async deactivateEventReceivers(stopQueueThread: boolean = true) {
    this.logger.debug('Deactivating lifecycle listeners');
    for (const lcl of getLifecycleListeners()) {
      try {
        await lcl.destroy?.();
      } catch (err) {
        const e = err as Error;
        this.logger.warn(`Error destroying lifecycle listener: ${e.message}`);
      }
    }

    this.logger.debug('Deactivating queue runner');
    if (stopQueueThread) {
      try {
        wfQueueRunner.stop();
        await this.#wfqEnded;
      } catch (err) {
        const e = err as Error;
        this.logger.warn(`Error destroying wf queue runner: ${e.message}`);
      }
    }
  }

  async executeWorkflowUUID(workflowID: string, startNewWorkflow: boolean = false): Promise<WorkflowHandle<unknown>> {
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowID);
    if (!wfStatus) {
      this.logger.error(`Failed to find workflow status for workflowUUID: ${workflowID}`);
      throw new DBOSError(`Failed to find workflow status for workflow UUID: ${workflowID}`);
    }

    if (!wfStatus?.input) {
      this.logger.error(`Failed to find inputs for workflowUUID: ${workflowID}`);
      throw new DBOSError(`Failed to find inputs for workflow UUID: ${workflowID}`);
    }
    const inputs = DBOSJSON.parse(wfStatus.input) as unknown[];
    const recoverCtx = this.#getRecoveryContext(workflowID, wfStatus);

    const { methReg, configuredInst } = this.#getFunctionInfoFromWFStatus(wfStatus);

    // If starting a new workflow, assign a new UUID. Otherwise, use the workflow's original UUID.
    const workflowStartID = startNewWorkflow ? undefined : workflowID;

    if (methReg?.workflowConfig) {
      return await runWithTopContext(recoverCtx, async () => {
        return await this.workflow(
          methReg.registeredFunction as UntypedAsyncFunction,
          {
            workflowUUID: workflowStartID,
            configuredInstance: configuredInst,
            queueName: wfStatus.queueName,
            executeWorkflow: true,
            deadlineEpochMS: wfStatus.deadlineEpochMS,
          },
          ...inputs,
        );
      });
    }

    // Should be temporary workflows. Parse the name of the workflow.
    const wfName = wfStatus.workflowName;
    const nameArr = wfName.split('-');
    if (!nameArr[0].startsWith(DBOSExecutor.#tempWorkflowName)) {
      throw new DBOSError(
        `Cannot find workflow function for a non-temporary workflow, ID ${workflowID}, class '${wfStatus.workflowClassName}', function '${wfName}'; did you change your code?`,
      );
    }

    if (nameArr[1] === TempWorkflowType.transaction) {
      const txnReg = getFunctionRegistrationByName(wfStatus.workflowClassName, nameArr[2]);
      if (!txnReg?.txnConfig) {
        this.logger.error(`Cannot find transaction info for ID ${workflowID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }

      return await runWithTopContext(recoverCtx, async () => {
        return await this.startTransactionTempWF(
          txnReg.registeredFunction as UntypedAsyncFunction,
          {
            workflowUUID: workflowStartID,
            configuredInstance: configuredInst,
            queueName: wfStatus.queueName,
            executeWorkflow: true,
          },
          undefined,
          undefined,
          ...inputs,
        );
      });
    } else if (nameArr[1] === TempWorkflowType.step) {
      const stepReg = getFunctionRegistrationByName(wfStatus.workflowClassName, nameArr[2]);
      if (!stepReg?.stepConfig) {
        this.logger.error(`Cannot find step info for ID ${workflowID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      return await runWithTopContext(recoverCtx, async () => {
        return await this.startStepTempWF(
          stepReg.registeredFunction as UntypedAsyncFunction,
          {
            workflowUUID: workflowStartID,
            configuredInstance: configuredInst,
            queueName: wfStatus.queueName, // Probably null
            executeWorkflow: true,
          },
          undefined,
          undefined,
          ...inputs,
        );
      });
    } else if (nameArr[1] === TempWorkflowType.send) {
      const swf = async (destinationID: string, message: unknown, topic?: string) => {
        const ctx = getCurrentContextStore();
        const functionID: number = functionIDGetIncrement();
        await this.systemDatabase.send(ctx!.workflowId!, functionID, destinationID, DBOSJSON.stringify(message), topic);
      };
      const temp_workflow = swf as UntypedAsyncFunction;
      return await runWithTopContext(recoverCtx, async () => {
        return this.workflow(
          temp_workflow,
          {
            workflowUUID: workflowStartID,
            tempWfType: TempWorkflowType.send,
            queueName: wfStatus.queueName,
            executeWorkflow: true,
          },
          ...inputs,
        );
      });
    } else {
      this.logger.error(`Unrecognized temporary workflow! UUID ${workflowID}, name ${wfName}`);
      throw new DBOSNotRegisteredError(wfName);
    }
  }

  async getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSExternalState | undefined> {
    return await this.systemDatabase.getEventDispatchState(svc, wfn, key);
  }
  async upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState> {
    return await this.systemDatabase.upsertEventDispatchState(state);
  }

  #getRecoveryContext(_workflowID: string, status: WorkflowStatusInternal): DBOSLocalCtx {
    // Note: this doesn't inherit the original parent context's span.
    const oc: DBOSLocalCtx = {};
    oc.request = status.request;
    oc.authenticatedUser = status.authenticatedUser;
    oc.authenticatedRoles = status.authenticatedRoles;
    oc.assumedRole = status.assumedRole;
    return oc;
  }

  async cancelWorkflow(workflowID: string): Promise<void> {
    await this.systemDatabase.cancelWorkflow(workflowID);
    this.logger.info(`Cancelling workflow ${workflowID}`);
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

  async resumeWorkflow(workflowID: string): Promise<void> {
    await this.systemDatabase.resumeWorkflow(workflowID);
  }

  logRegisteredHTTPUrls() {
    this.logger.info('HTTP endpoints supported:');
    getAllRegisteredFunctions().forEach((registeredOperation) => {
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

  /**
    An application's version is computed from a hash of the source of its workflows.
    This is guaranteed to be stable given identical source code because it uses an MD5 hash
    and because it iterates through the workflows in sorted order.
    This way, if the app's workflows are updated (which would break recovery), its version changes.
    App version can be manually set through the DBOS__APPVERSION environment variable.
   */
  computeAppVersion(): string {
    const hasher = crypto.createHash('md5');
    const sortedWorkflowSource = Array.from(getAllRegisteredFunctions())
      .filter((e) => e.workflowConfig)
      .map((i) => i.origFunction.toString())
      .sort();
    // Different DBOS versions should produce different hashes.
    sortedWorkflowSource.push(globalParams.dbosVersion);
    for (const sourceCode of sortedWorkflowSource) {
      hasher.update(sourceCode);
    }
    return hasher.digest('hex');
  }

  static internalQueue: WorkflowQueue | undefined = undefined;

  static createInternalQueue() {
    if (DBOSExecutor.internalQueue !== undefined) {
      return;
    }
    DBOSExecutor.internalQueue = new WorkflowQueue(INTERNAL_QUEUE_NAME, {});
  }
}
