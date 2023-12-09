/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  DBOSError,
  DBOSInitializationError,
  DBOSWorkflowConflictUUIDError,
  DBOSNotRegisteredError,
  DBOSDebuggerError,
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
} from './workflow';

import { IsolationLevel, Transaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, Communicator } from './communicator';
import { TelemetryCollector } from './telemetry/collector';
import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import { TelemetryConfig } from './telemetry';
import { PoolConfig } from 'pg';
import { SystemDatabase, PostgresSystemDatabase } from './system_database';
import { v4 as uuidv4 } from 'uuid';
import {
  PGNodeUserDatabase,
  PrismaUserDatabase,
  UserDatabase,
  TypeORMDatabase,
  UserDatabaseName,
  KnexUserDatabase,
  UserDatabaseClient,
} from './user_database';
import { MethodRegistrationBase, getRegisteredOperations, getOrCreateClassRegistration, MethodRegistration } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import { DBOSContextImpl, InitContext } from './context';
import { HandlerRegistration } from './httpServer/handler';
import { WorkflowContextDebug } from './debugger/debug_workflow';

export interface DBOSNull { }
export const dbosNull: DBOSNull = {};

/* Interface for DBOS configuration */
export interface DBOSConfig {
  readonly poolConfig: PoolConfig;
  readonly userDbclient?: UserDatabaseName;
  readonly telemetry?: TelemetryConfig;
  readonly system_database: string;
  readonly observability_database?: string;
  readonly application?: any;
  readonly dbClientMetadata?: any;
  readonly debugProxy?: string;
}

interface WorkflowInfo<T extends any[], R> {
  workflow: Workflow<T, R>;
  config: WorkflowConfig;
}

export class DBOSExecutor {
  initialized: boolean;
  // User Database
  userDatabase: UserDatabase = null as unknown as UserDatabase;
  // System Database
  readonly systemDatabase: SystemDatabase;

  // Temporary workflows are created by calling transaction/send/recv directly from the executor class
  readonly tempWorkflowName = "temp_workflow";

  readonly workflowInfoMap: Map<string, WorkflowInfo<any, any>> = new Map([
    // We initialize the map with an entry for temporary workflows.
    [
      this.tempWorkflowName,
      {
        // eslint-disable-next-line @typescript-eslint/require-await
        workflow: async () => this.logger.error("UNREACHABLE: Indirect invoke of temp workflow"),
        config: {},
      },
    ],
  ]);
  readonly transactionConfigMap: Map<string, TransactionConfig> = new Map();
  readonly communicatorConfigMap: Map<string, CommunicatorConfig> = new Map();
  readonly registeredOperations: Array<MethodRegistrationBase> = [];
  readonly pendingWorkflowMap: Map<string, Promise<unknown>> = new Map(); // Map from workflowUUID to workflow promise
  readonly pendingAsyncWrites: Map<string, Promise<unknown>> = new Map(); // Map from workflowUUID to asynchronous write promise

  readonly telemetryCollector: TelemetryCollector;
  readonly flushBufferIntervalMs: number = 1000;
  readonly flushBufferID: NodeJS.Timeout;

  static readonly defaultNotificationTimeoutSec = 60;

  readonly debugMode: boolean;

  readonly logger: Logger;
  readonly tracer: Tracer;
  // eslint-disable-next-line @typescript-eslint/ban-types
  entities: Function[] = [];

  /* WORKFLOW EXECUTOR LIFE CYCLE MANAGEMENT */
  constructor(readonly config: DBOSConfig, systemDatabase?: SystemDatabase) {
    this.debugMode = config.debugProxy ? true : false;
    this.telemetryCollector = new TelemetryCollector([]);
    this.logger = new Logger(this.telemetryCollector, this.config.telemetry?.logs);
    this.tracer = new Tracer(this.telemetryCollector);

    if (this.debugMode) {
      this.logger.info("Running in debug mode!")
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

    if (systemDatabase) {
      this.logger.debug("Using provided system database"); // XXX print the name or something
      this.systemDatabase = systemDatabase;
    } else {
      this.logger.debug("Using Postgres system database");
      this.systemDatabase = new PostgresSystemDatabase(this.config.poolConfig, this.config.system_database, this.logger);
    }

    this.flushBufferID = setInterval(() => {
      if (!this.debugMode) {
        void this.flushWorkflowStatusBuffer();
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
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
      const { PrismaClient } = require("@prisma/client");
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call
      this.userDatabase = new PrismaUserDatabase(new PrismaClient());
      this.logger.debug("Loaded Prisma user database");
    } else if (userDbClient === UserDatabaseName.TYPEORM) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
      const DataSourceExports = require("typeorm");
      try {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
        this.userDatabase = new TypeORMDatabase(new DataSourceExports.DataSource({
          type: "postgres", // perhaps should move to config file
          host: userDBConfig.host,
          port: userDBConfig.port,
          username: userDBConfig.user,
          password: userDBConfig.password,
          database: userDBConfig.database,
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
          entities: this.entities
        }))
      } catch (s) {
        this.logger.error("Error loading TypeORM user database");
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
        }
      }
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
        const wf = ro.registeredFunction as Workflow<any, any>;
        this.#registerWorkflow(wf, ro.workflowConfig);
        this.logger.debug(`Registered workflow ${ro.name}`);
      } else if (ro.txnConfig) {
        const tx = ro.registeredFunction as Transaction<any, any>;
        this.#registerTransaction(tx, ro.txnConfig);
        this.logger.debug(`Registered transaction ${ro.name}`);
      } else if (ro.commConfig) {
        const comm = ro.registeredFunction as Communicator<any, any>;
        this.#registerCommunicator(comm, ro.commConfig);
        this.logger.debug(`Registered communicator ${ro.name}`);
      }
    }
  }

  async init(...classes: object[]): Promise<void> {
    if (this.initialized) {
      this.logger.error("Workflow executor already initialized!");
      return;
    }

    try {
      type AnyConstructor = new (...args: unknown[]) => object;
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
      if (!this.debugMode) {
        await this.systemDatabase.init();
        await this.telemetryCollector.init(this.registeredOperations);
        await this.userDatabase.init(); // Skip user DB init because we're using the proxy.
        await this.recoverPendingWorkflows();
      }
    } catch (err) {
      this.logger.error(new Error(`failed to initialize workflow executor: ${(err as Error).message}`));
      throw new DBOSInitializationError(`failed to initialize workflow executor: ${(err as Error).message}`);
    }
    this.initialized = true;

    // Only execute init code if under non-debug mode
    if (!this.debugMode) {
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

  async destroy() {
    if (this.pendingWorkflowMap.size > 0) {
      this.logger.info("Waiting for pending workflows to finish.");
      await Promise.allSettled(this.pendingWorkflowMap.values());
    }
    if (this.pendingAsyncWrites.size > 0) {
      this.logger.info("Waiting for pending buffer flushes to finish");
      await Promise.allSettled(this.pendingAsyncWrites.values());
    }
    clearInterval(this.flushBufferID);
    await this.flushWorkflowStatusBuffer();
    await this.systemDatabase.destroy();
    await this.userDatabase.destroy();
    await this.telemetryCollector.destroy();
  }

  /* WORKFLOW OPERATIONS */

  #registerWorkflow<T extends any[], R>(wf: Workflow<T, R>, config: WorkflowConfig = {}) {
    if (wf.name === this.tempWorkflowName || this.workflowInfoMap.has(wf.name)) {
      throw new DBOSError(`Repeated workflow name: ${wf.name}`);
    }
    const workflowInfo: WorkflowInfo<T, R> = {
      workflow: wf,
      config: config,
    };
    this.workflowInfoMap.set(wf.name, workflowInfo);
  }

  #registerTransaction<T extends any[], R>(txn: Transaction<T, R>, params: TransactionConfig = {}) {
    if (this.transactionConfigMap.has(txn.name)) {
      throw new DBOSError(`Repeated Transaction name: ${txn.name}`);
    }
    this.transactionConfigMap.set(txn.name, params);
  }

  #registerCommunicator<T extends any[], R>(comm: Communicator<T, R>, params: CommunicatorConfig = {}) {
    if (this.communicatorConfigMap.has(comm.name)) {
      throw new DBOSError(`Repeated Commmunicator name: ${comm.name}`);
    }
    this.communicatorConfigMap.set(comm.name, params);
  }

  async workflow<T extends any[], R>(wf: Workflow<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    if (this.debugMode) {
      return this.debugWorkflow(wf, params, undefined, undefined, ...args);
    }
    return this.internalWorkflow(wf, params, undefined, undefined, ...args);
  }

  // If callerUUID and functionID are set, it means the workflow is invoked from within a workflow.
  async internalWorkflow<T extends any[], R>(wf: Workflow<T, R>, params: WorkflowParams, callerUUID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>> {
    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();
    const presetUUID: boolean = params.workflowUUID ? true : false;

    const wInfo = this.workflowInfoMap.get(wf.name);
    if (wInfo === undefined) {
      throw new DBOSNotRegisteredError(wf.name);
    }
    const wConfig = wInfo.config;

    const wCtxt: WorkflowContextImpl = new WorkflowContextImpl(this, params.parentCtx, workflowUUID, wConfig, wf.name, presetUUID);
    wCtxt.span.setAttributes({ args: JSON.stringify(args) }); // TODO enforce skipLogging & request for hashing

    // Synchronously set the workflow's status to PENDING and record workflow inputs.
    if (!wCtxt.isTempWorkflow) {
      args = await this.systemDatabase.initWorkflowStatus(workflowUUID, wf.name, wCtxt.authenticatedUser, wCtxt.assumedRole, wCtxt.authenticatedRoles, wCtxt.request, args);
    } else {
      // For temporary workflows, instead asynchronously record inputs.
      const setWorkflowInputs: Promise<void> = this.systemDatabase.setWorkflowInputs<T>(workflowUUID, args)
        .catch(error => { this.logger.error('Error asynchronously setting workflow inputs', error) })
        .finally(() => { this.pendingAsyncWrites.delete(workflowUUID) })
      this.pendingAsyncWrites.set(workflowUUID, setWorkflowInputs);
    }
    const runWorkflow = async () => {
      let result: R;
      // Execute the workflow.
      try {
        result = await wf(wCtxt, ...args);
        this.systemDatabase.bufferWorkflowOutput(workflowUUID, result);
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
          await this.systemDatabase.recordWorkflowError(workflowUUID, e);
          // TODO: Log errors, but not in the tests when they're expected.
          wCtxt.span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
          throw err;
        }
      } finally {
        this.tracer.endSpan(wCtxt.span);
      }
      // Asynchronously flush the result buffer.
      const resultBufferFlush: Promise<void> = this.userDatabase.transaction(async (client: UserDatabaseClient) => {
        if (wCtxt.resultBuffer.size > 0) {
          await wCtxt.flushResultBuffer(client);
        }
      }, { isolationLevel: IsolationLevel.ReadCommitted })
        .catch(error => { this.logger.error('Error asynchronously flushing result buffer', error) })
        .finally(() => { this.pendingAsyncWrites.delete(workflowUUID) })
      this.pendingAsyncWrites.set(workflowUUID, resultBufferFlush);
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
  // eslint-disable-next-line @typescript-eslint/require-await
  async debugWorkflow<T extends any[], R>(wf: Workflow<T, R>, params: WorkflowParams, callerUUID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>> {
    // In debug mode, we must have a specific workflow UUID.
    if (!params.workflowUUID) {
      throw new DBOSDebuggerError("Workflow UUID not found!");
    }
    const workflowUUID = params.workflowUUID;
    const wInfo = this.workflowInfoMap.get(wf.name);
    if (wInfo === undefined) {
      throw new DBOSDebuggerError("Workflow unregistered! " + wf.name);
    }
    const wConfig = wInfo.config;

    const wCtxt = new WorkflowContextDebug(this, params.parentCtx, workflowUUID, wConfig, wf.name);

    // TODO: maybe also check the input args against the recorded ones.
    const runWorkflow = async () => {
      // A non-temp debug workflow must have run before.
      const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
      if (!wCtxt.isTempWorkflow && !wfStatus) {
        throw new DBOSDebuggerError("Workflow status not found! UUID: " + workflowUUID);
      }

      // Execute the workflow. We don't need to record anything.
      return wf(wCtxt, ...args);
    };
    const workflowPromise: Promise<R> = runWorkflow();

    return new InvokedHandle(this.systemDatabase, workflowPromise, workflowUUID, wf.name, callerUUID, callerFunctionID);
  }


  async transaction<T extends any[], R>(txn: Transaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.transaction(txn, ...args);
    };
    return (await this.workflow(temp_workflow, params, ...args)).getResult();
  }

  async external<T extends any[], R>(commFn: Communicator<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call external.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.external(commFn, ...args);
    };
    return (await this.workflow(temp_workflow, params, ...args)).getResult();
  }

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    // Create a workflow and call send.
    const temp_workflow = async (ctxt: WorkflowContext, destinationUUID: string, message: T, topic?: string) => {
      return await ctxt.send<T>(destinationUUID, message, topic);
    };
    const workflowUUID = idempotencyKey ? destinationUUID + idempotencyKey : undefined;
    return (await this.workflow(temp_workflow, { workflowUUID: workflowUUID }, destinationUUID, message, topic)).getResult();
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
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
  async recoverPendingWorkflows(executorIDs: string[] = ["local"]): Promise<WorkflowHandle<any>[]> {
    const pendingWorkflows: string[] = [];
    for (const execID of executorIDs) {
      this.logger.debug(`Recovering workflows of executor: ${execID}`);
      const wIDs = await this.systemDatabase.getPendingWorkflows(execID);
      pendingWorkflows.push(...wIDs);
    }

    const handlerArray: WorkflowHandle<any>[] = [];
    for (const workflowUUID of pendingWorkflows) {
      try {
        handlerArray.push(await this.executeWorkflowUUID(workflowUUID));
      } catch (e) {
        this.logger.warn(`Recovery of workflow ${workflowUUID} failed: ${(e as Error).message}`);
      }
    }
    return handlerArray;
  }

  async executeWorkflowUUID(workflowUUID: string): Promise<WorkflowHandle<unknown>> {
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
    const inputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
    if (!inputs || !wfStatus) {
      this.logger.error(`Failed to find inputs for workflowUUID: ${workflowUUID}`);
      throw new DBOSError(`Failed to find inputs for workflow UUID: ${workflowUUID}`);
    }
    const wfInfo: WorkflowInfo<any, any> | undefined = this.workflowInfoMap.get(wfStatus.workflowName);

    // If wfInfo is undefined, then it means it's a temporary workflow.
    // TODO: we need to find the name of that function and run it.
    if (!wfInfo) {
      throw new DBOSError(`Cannot find workflow info for UUID ${workflowUUID}`);
    }
    const parentCtx = this.#getRecoveryContext(workflowUUID, wfStatus);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return this.workflow(wfInfo.workflow, { workflowUUID: workflowUUID, parentCtx: parentCtx ?? undefined }, ...inputs);
  }

  #getRecoveryContext(workflowUUID: string, status: WorkflowStatus): DBOSContextImpl {
    const span = this.tracer.startSpan(status.workflowName);
    span.setAttributes({
      operationName: status.workflowName,
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
  async flushWorkflowStatusBuffer() {
    if (this.initialized) {
      await this.systemDatabase.flushWorkflowStatusBuffer();
    }
  }

  logRegisteredHTTPUrls() {
    this.logger.info("HTTP endpoints supported:");
    this.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as HandlerRegistration<unknown, unknown[], unknown>;
      if (ro.apiURL) {
        this.logger.info("    " + ro.apiType.padEnd(4) + "  :  " + ro.apiURL);
        const roles = ro.getRequiredRoles();
        if (roles.length > 0) {
          this.logger.info("        Required Roles: " + JSON.stringify(roles));
        }
      }
    });
  }
}
