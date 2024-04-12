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
  StatusString,
  BufferedResult,
} from './workflow';

import { Transaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, Communicator } from './communicator';
import { TelemetryCollector } from './telemetry/collector';
import { Tracer } from './telemetry/traces';
import { GlobalLogger as Logger } from './telemetry/logs';
import { TelemetryExporter } from './telemetry/exporters';
import { TelemetryConfig } from './telemetry';
import { PoolConfig } from 'pg';
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
import { MethodRegistrationBase, getRegisteredOperations, getOrCreateClassRegistration, MethodRegistration } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import { DBOSContextImpl, InitContext } from './context';
import { HandlerRegistration } from './httpServer/handler';
import { WorkflowContextDebug } from './debugger/debug_workflow';
import { serializeError } from 'serialize-error';

export interface DBOSNull { }
export const dbosNull: DBOSNull = {};

/* Interface for DBOS configuration */
export interface DBOSConfig {
  readonly poolConfig: PoolConfig;
  readonly userDbclient?: UserDatabaseName;
  readonly telemetry?: TelemetryConfig;
  readonly system_database: string;
  readonly env?: Record<string, string>
  readonly application?: object;
  readonly dbClientMetadata?: any;
  readonly debugProxy?: string;
  readonly debugMode?: boolean;
  readonly http?: {
    readonly cors_middleware?: boolean;
    readonly credentials?: boolean;
    readonly allowed_origins?: string[];
  };
}

interface WorkflowInfo {
  workflow: Workflow<any, any>;
  config: WorkflowConfig;
}

interface TransactionInfo {
  transaction: Transaction<any, any>;
  config: TransactionConfig;
}

interface CommunicatorInfo {
  communicator: Communicator<any, any>;
  config: CommunicatorConfig;
}

interface InternalWorkflowParams extends WorkflowParams {
  readonly tempWfType?: string;
  readonly tempWfName?: string;
}

export const OperationType = {
  HANDLER: "handler",
  WORKFLOW: "workflow",
  TRANSACTION: "transaction",
  COMMUNICATOR: "communicator",
} as const;

const TempWorkflowType = {
  transaction: "transaction",
  external: "external",
  send: "send",
} as const;

export class DBOSExecutor {
  initialized: boolean;
  // User Database
  userDatabase: UserDatabase = null as unknown as UserDatabase;
  // System Database
  readonly systemDatabase: SystemDatabase;

  // Temporary workflows are created by calling transaction/send/recv directly from the executor class
  static readonly tempWorkflowName = "temp_workflow";

  readonly workflowInfoMap: Map<string, WorkflowInfo> = new Map([
    // We initialize the map with an entry for temporary workflows.
    [
      DBOSExecutor.tempWorkflowName,
      {
        // eslint-disable-next-line @typescript-eslint/require-await
        workflow: async () => this.logger.error("UNREACHABLE: Indirect invoke of temp workflow"),
        config: {},
      },
    ],
  ]);
  readonly transactionInfoMap: Map<string, TransactionInfo> = new Map();
  readonly communicatorInfoMap: Map<string, CommunicatorInfo> = new Map();
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
      // We always an collector to drain the signals queue, even if we don't have an exporter.
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
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
      const { PrismaClient } = require("@prisma/client");
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call
      this.userDatabase = new PrismaUserDatabase(new PrismaClient());
      this.logger.debug("Loaded Prisma user database");
    } else if (userDbClient === UserDatabaseName.TYPEORM) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
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
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
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
      await this.userDatabase.init(this.debugMode);
      if (!this.debugMode) {
        await this.systemDatabase.init();
        await this.recoverPendingWorkflows();
      }
    } catch (err) {
      (err as Error).message = `failed to initialize workflow executor: ${(err as Error).message}`
      throw new DBOSInitializationError(`${(err as Error).message}`);
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
    clearInterval(this.flushBufferID);
    await this.flushWorkflowBuffers();
    await this.systemDatabase.destroy();
    await this.userDatabase.destroy();
    await this.logger.destroy();
  }

  /* WORKFLOW OPERATIONS */

  #registerWorkflow<T extends any[], R>(wf: Workflow<T, R>, config: WorkflowConfig = {}) {
    if (wf.name === DBOSExecutor.tempWorkflowName || this.workflowInfoMap.has(wf.name)) {
      throw new DBOSError(`Repeated workflow name: ${wf.name}`);
    }
    const workflowInfo: WorkflowInfo = {
      workflow: wf,
      config: config,
    };
    this.workflowInfoMap.set(wf.name, workflowInfo);
  }

  #registerTransaction<T extends any[], R>(txn: Transaction<T, R>, params: TransactionConfig = {}) {
    if (this.transactionInfoMap.has(txn.name)) {
      throw new DBOSError(`Repeated Transaction name: ${txn.name}`);
    }
    const txnInfo: TransactionInfo = {
      transaction: txn,
      config: params,
    };
    this.transactionInfoMap.set(txn.name, txnInfo);
  }

  #registerCommunicator<T extends any[], R>(comm: Communicator<T, R>, params: CommunicatorConfig = {}) {
    if (this.communicatorInfoMap.has(comm.name)) {
      throw new DBOSError(`Repeated Commmunicator name: ${comm.name}`);
    }
    const commInfo: CommunicatorInfo = {
      communicator: comm,
      config: params,
    };
    this.communicatorInfoMap.set(comm.name, commInfo);
  }

  async workflow<T extends any[], R>(wf: Workflow<T, R>, params: InternalWorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    if (this.debugMode) {
      return this.debugWorkflow(wf, params, undefined, undefined, ...args);
    }
    return this.internalWorkflow(wf, params, undefined, undefined, ...args);
  }

  // If callerUUID and functionID are set, it means the workflow is invoked from within a workflow.
  async internalWorkflow<T extends any[], R>(wf: Workflow<T, R>, params: InternalWorkflowParams, callerUUID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>> {
    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();
    const presetUUID: boolean = params.workflowUUID ? true : false;

    const wInfo = this.workflowInfoMap.get(wf.name);
    if (wInfo === undefined) {
      throw new DBOSNotRegisteredError(wf.name);
    }
    const wConfig = wInfo.config;

    const wCtxt: WorkflowContextImpl = new WorkflowContextImpl(this, params.parentCtx, workflowUUID, wConfig, wf.name, presetUUID, params.tempWfType, params.tempWfName);

    // If running in DBOS Cloud, set the executor ID
    if (process.env.DBOS__VMID) {
      wCtxt.executorID = process.env.DBOS__VMID
    }

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowUUID,
      status: StatusString.PENDING,
      name: wf.name,
      authenticatedUser: wCtxt.authenticatedUser,
      output: undefined,
      error: "",
      assumedRole: wCtxt.assumedRole,
      authenticatedRoles: wCtxt.authenticatedRoles,
      request: wCtxt.request,
      executorID: wCtxt.executorID,
      createdAt: Date.now() // Remember the start time of this workflow
    };

    if (wCtxt.isTempWorkflow) {
      internalStatus.name = `${DBOSExecutor.tempWorkflowName}-${wCtxt.tempWfOperationType}-${wCtxt.tempWfOperationName}`;
    }

    // Synchronously set the workflow's status to PENDING and record workflow inputs (for non single-transaction workflows).
    // We have to do it for all types of workflows because operation_outputs table has a foreign key constraint on workflow status table.
    if (wCtxt.tempWfOperationType !== TempWorkflowType.transaction) {
      args = await this.systemDatabase.initWorkflowStatus(internalStatus, args);
    }

    const runWorkflow = async () => {
      let result: R;
      // Execute the workflow.
      try {
        result = await wf(wCtxt, ...args);
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
          internalStatus.error = JSON.stringify(serializeError(e));
          internalStatus.status = StatusString.ERROR;
          await this.systemDatabase.recordWorkflowError(workflowUUID, internalStatus);
          // TODO: Log errors, but not in the tests when they're expected.
          wCtxt.span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
          throw err;
        }
      } finally {
        this.tracer.endSpan(wCtxt.span);
        if (wCtxt.tempWfOperationType === TempWorkflowType.transaction) {
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

    // A workflow must have run before.
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
    const recordedInputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
    if (!wfStatus || !recordedInputs) {
      throw new DBOSDebuggerError("Workflow status or inputs not found! UUID: " + workflowUUID);
    }

    // Make sure we use the same input.
    if (JSON.stringify(args) !== JSON.stringify(recordedInputs)) {
      throw new DBOSDebuggerError(`Detect different input for the workflow UUID ${workflowUUID}!\n Received: ${JSON.stringify(args)}\n Original: ${JSON.stringify(recordedInputs)}`);
    }

    const workflowPromise: Promise<R> = wf(wCtxt, ...args)
      .then(async (result) => {
        // Check if the result is the same.
        const recordedResult = await this.systemDatabase.getWorkflowResult<R>(workflowUUID);
        if (result === undefined && !recordedResult) {
          return result;
        }
        if (JSON.stringify(result) !== JSON.stringify(recordedResult)) {
          this.logger.error(`Detect different output for the workflow UUID ${workflowUUID}!\n Received: ${JSON.stringify(result)}\n Original: ${JSON.stringify(recordedResult)}`);
        }
        return recordedResult; // Always return the recorded result.
      });
    return new InvokedHandle(this.systemDatabase, workflowPromise, workflowUUID, wf.name, callerUUID, callerFunctionID);
  }

  async transaction<T extends any[], R>(txn: Transaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.transaction(txn, ...args);
    };
    return (await this.workflow(temp_workflow, { ...params, tempWfType: TempWorkflowType.transaction, tempWfName: txn.name }, ...args)).getResult();
  }

  async external<T extends any[], R>(commFn: Communicator<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call external.
    const temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.external(commFn, ...args);
    };
    return (await this.workflow(temp_workflow, { ...params, tempWfType: TempWorkflowType.external, tempWfName: commFn.name }, ...args)).getResult();
  }

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    // Create a workflow and call send.
    const temp_workflow = async (ctxt: WorkflowContext, destinationUUID: string, message: T, topic?: string) => {
      return await ctxt.send<T>(destinationUUID, message, topic);
    };
    const workflowUUID = idempotencyKey ? destinationUUID + idempotencyKey : undefined;
    return (await this.workflow(temp_workflow, { workflowUUID: workflowUUID, tempWfType: TempWorkflowType.send }, destinationUUID, message, topic)).getResult();
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
      if (execID == "local" && process.env.DBOS__VMID) {
        this.logger.debug(`Skip local recovery because it's running in a VM: ${process.env.DBOS__VMID}`);
        continue;
      }
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
    const parentCtx = this.#getRecoveryContext(workflowUUID, wfStatus);

    const wfInfo: WorkflowInfo | undefined = this.workflowInfoMap.get(wfStatus.workflowName);

    if (wfInfo) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      return this.workflow(wfInfo.workflow, { workflowUUID: workflowUUID, parentCtx: parentCtx ?? undefined }, ...inputs);
    }

    // Should be temporary workflows. Parse the name of the workflow.
    const wfName = wfStatus.workflowName;
    const nameArr = wfName.split("-");
    if (!nameArr[0].startsWith(DBOSExecutor.tempWorkflowName)) {
      throw new DBOSError(`This should never happen! Cannot find workflow info for a non-temporary workflow! UUID ${workflowUUID}, name ${wfName}`);
    }

    let temp_workflow: Workflow<any, any>;
    if (nameArr[1] === TempWorkflowType.transaction) {
      const txnInfo: TransactionInfo | undefined = this.transactionInfoMap.get(nameArr[2]);
      if (!txnInfo) {
        this.logger.error(`Cannot find transaction info for UUID ${workflowUUID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      temp_workflow = async (ctxt: WorkflowContext, ...args: any[]) => {
        const ctxtImpl = ctxt as WorkflowContextImpl;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument
        return await ctxtImpl.transaction(txnInfo.transaction, ...args);
      };
    } else if (nameArr[1] === TempWorkflowType.external) {
      const commInfo: CommunicatorInfo | undefined = this.communicatorInfoMap.get(nameArr[2]);
      if (!commInfo) {
        this.logger.error(`Cannot find communicator info for UUID ${workflowUUID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      temp_workflow = async (ctxt: WorkflowContext, ...args: any[]) => {
        const ctxtImpl = ctxt as WorkflowContextImpl;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument
        return await ctxtImpl.external(commInfo.communicator, ...args);
      };
    } else if (nameArr[1] === TempWorkflowType.send) {
      temp_workflow = async (ctxt: WorkflowContext, ...args: any[]) => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        return await ctxt.send<any>(args[0], args[1], args[2]);
      };
    } else {
      this.logger.error(`Unrecognized temporary workflow! UUID ${workflowUUID}, name ${wfName}`)
      throw new DBOSNotRegisteredError(wfName);
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return this.workflow(temp_workflow, { workflowUUID: workflowUUID, parentCtx: parentCtx ?? undefined }, ...inputs);
  }

  // NOTE: this creates a new span, it does not inherit the span from the original workflow
  #getRecoveryContext(workflowUUID: string, status: WorkflowStatus): DBOSContextImpl {
    const span = this.tracer.startSpan(
      status.workflowName,
      {
        operationUUID: workflowUUID,
        operationType: OperationType.WORKFLOW,
        status: status.status,
        authenticatedUser: status.authenticatedUser,
        assumedRole: status.assumedRole,
        authenticatedRoles: status.authenticatedRoles,
      },
    );
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
            const createdAt = recorded.created_at!
            if (paramCnt > 1) {
              sqlStmt += ", ";
            }
            sqlStmt += `($${paramCnt++}, $${paramCnt++}, $${paramCnt++}, $${paramCnt++}, null, $${paramCnt++}, $${paramCnt++})`;
            values.push(workflowUUID, funcID, JSON.stringify(output), JSON.stringify(null), txnSnapshot, createdAt);
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
          this.workflowResultBuffer.set(workflowUUID, wfBuffer)
        }
      }
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
