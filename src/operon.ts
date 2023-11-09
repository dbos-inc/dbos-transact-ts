/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  OperonError,
  OperonInitializationError,
  OperonWorkflowConflictUUIDError,
  OperonNotRegisteredError,
} from './error';
import {
  InvokedHandle,
  OperonWorkflow,
  WorkflowConfig,
  WorkflowContext,
  WorkflowHandle,
  WorkflowParams,
  RetrievedHandle,
  WorkflowContextImpl,
  WorkflowStatus,
} from './workflow';

import { OperonTransaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, OperonCommunicator } from './communicator';
import { JaegerExporter } from './telemetry/exporters';
import { TelemetryCollector } from './telemetry/collector';
import { Tracer } from './telemetry/traces';
import { createGlobalLogger, WinstonLogger as Logger } from './telemetry/logs';
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
} from './user_database';
import { OperonMethodRegistrationBase, getRegisteredOperations, getOrCreateOperonClassRegistration, OperonMethodRegistration } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import { OperonContextImpl, InitContext } from './context';
import { OperonHandlerRegistration } from './httpServer/handler';

export interface OperonNull { }
export const operonNull: OperonNull = {};

/* Interface for Operon configuration */
export interface OperonConfig {
  readonly poolConfig: PoolConfig;
  readonly userDbclient?: UserDatabaseName;
  readonly telemetry?: TelemetryConfig;
  readonly system_database: string;
  readonly observability_database?: string;
  readonly application?: any;
  readonly dbClientMetadata?: any;
}

interface WorkflowInfo<T extends any[], R> {
  workflow: OperonWorkflow<T, R>;
  config: WorkflowConfig;
}

export class Operon {
  initialized: boolean;
  // User Database
  userDatabase: UserDatabase = null as unknown as UserDatabase;
  // System Database
  readonly systemDatabase: SystemDatabase;

  // Temporary workflows are created by calling transaction/send/recv directly from the Operon class
  readonly tempWorkflowName = "operon_temp_workflow";

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
  readonly topicConfigMap: Map<string, string[]> = new Map();
  readonly registeredOperations: Array<OperonMethodRegistrationBase> = [];
  readonly initialEpochTimeMs: number;
  readonly recoveryWorkflowHandles: Array<WorkflowHandle<any>> = [];

  readonly telemetryCollector: TelemetryCollector;
  readonly flushBufferIntervalMs: number = 1000;
  readonly flushBufferID: NodeJS.Timeout;

  static readonly defaultNotificationTimeoutSec = 60;

  readonly logger: Logger;
  readonly tracer: Tracer;
  // eslint-disable-next-line @typescript-eslint/ban-types
  entities: Function[] = []

  /* OPERON LIFE CYCLE MANAGEMENT */
  constructor(readonly config: OperonConfig, systemDatabase?: SystemDatabase) {
    this.logger = createGlobalLogger(this.config.telemetry?.logs);

    if (systemDatabase) {
      this.logger.debug("Using provided system database"); // XXX print the name or something
      this.systemDatabase = systemDatabase;
    } else {
      this.logger.debug("Using Postgres system database");
      this.systemDatabase = new PostgresSystemDatabase(this.config.poolConfig, this.config.system_database);
    }

    this.flushBufferID = setInterval(() => {
      void this.flushWorkflowStatusBuffer();
    }, this.flushBufferIntervalMs);
    this.logger.debug('Started workflow status buffer worker');

    // Add Jaeger exporter if tracing is enabled
    const telemetryExporters = [];
    if (this.config.telemetry?.traces?.enabled) {
      telemetryExporters.push(new JaegerExporter(this.config.telemetry?.traces?.endpoint));
      this.logger.debug("Loaded Jaeger Telemetry Exporter");
    }
    this.telemetryCollector = new TelemetryCollector(telemetryExporters);
    this.tracer = new Tracer(this.telemetryCollector);
    this.initialized = false;
    this.initialEpochTimeMs = Date.now();
  }

  configureDbClient() {
    const userDbClient = this.config.userDbclient;
    if (userDbClient === UserDatabaseName.PRISMA) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
      const { PrismaClient } = require('@prisma/client');
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call
      this.userDatabase = new PrismaUserDatabase(new PrismaClient());
      this.logger.debug("Loaded Prisma user database");
    } else if (userDbClient === UserDatabaseName.TYPEORM) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
      const DataSourceExports = require('typeorm');
      try {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
        this.userDatabase = new TypeORMDatabase(new DataSourceExports.DataSource({
          type: "postgres", // perhaps should move to config file
          host: this.config.poolConfig.host,
          port: this.config.poolConfig.port,
          username: this.config.poolConfig.user,
          password: this.config.poolConfig.password,
          database: this.config.poolConfig.database,
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
          entities: this.entities
        }))
      } catch (s) {
        this.logger.error("Error loading TypeORM user database");
      }
      this.logger.debug("Loaded TypeORM user database");
    } else if (userDbClient === UserDatabaseName.KNEX) {
      const knexConfig: Knex.Config = {
        client: 'postgres',
        connection: {
          host: this.config.poolConfig.host,
          port: this.config.poolConfig.port,
          user: this.config.poolConfig.user,
          password: this.config.poolConfig.password,
          database: this.config.poolConfig.database,
          ssl: this.config.poolConfig.ssl,
        }
      }
      this.userDatabase = new KnexUserDatabase(knex(knexConfig));
      this.logger.debug("Loaded Knex user database");
    } else {
      this.userDatabase = new PGNodeUserDatabase(this.config.poolConfig);
      this.logger.debug("Loaded Postgres user database");
    }
  }

  #registerClass(cls: object) {
    const registeredClassOperations = getRegisteredOperations(cls);
    this.registeredOperations.push(...registeredClassOperations);
    for (const ro of registeredClassOperations) {
      if (ro.workflowConfig) {
        const wf = ro.registeredFunction as OperonWorkflow<any, any>;
        this.#registerWorkflow(wf, ro.workflowConfig);
        this.logger.debug(`Registered workflow ${ro.name}`);
      } else if (ro.txnConfig) {
        const tx = ro.registeredFunction as OperonTransaction<any, any>;
        this.#registerTransaction(tx, ro.txnConfig);
        this.logger.debug(`Registered transaction ${ro.name}`);
      } else if (ro.commConfig) {
        const comm = ro.registeredFunction as OperonCommunicator<any, any>;
        this.#registerCommunicator(comm, ro.commConfig);
        this.logger.debug(`Registered communicator ${ro.name}`);
      }
    }
  }

  async init(...classes: object[]): Promise<void> {
    if (this.initialized) {
      this.logger.error("Operon already initialized!");
      return;
    }

    try {
      type AnyConstructor = new (...args: unknown[]) => object;
      for (const cls of classes) {
        const reg = getOrCreateOperonClassRegistration(cls as AnyConstructor);
        if (reg.ormEntities.length > 0) {
          this.entities = this.entities.concat(reg.ormEntities)
          this.logger.debug(`Loaded ${reg.ormEntities.length} ORM entities`);
        }
      }

      this.configureDbClient();

      if (!this.userDatabase) {
        this.logger.error("No user database configured!");
        throw new OperonInitializationError("No user database configured!");
      }

      for (const cls of classes) {
        this.#registerClass(cls);
      }

      await this.telemetryCollector.init(this.registeredOperations);
      await this.userDatabase.init();
      await this.systemDatabase.init();
    } catch (err) {
      if (err instanceof Error) {
        this.logger.error(`failed to initialize Operon: ${err.message}`);
        throw new OperonInitializationError(err.message);
      }
    }
    const initRecoveryHandles = await this.recoverPendingWorkflows();
    this.recoveryWorkflowHandles.push(...initRecoveryHandles)
    this.initialized = true;

    for ( const v of this.registeredOperations) {
      const m = v as OperonMethodRegistration<unknown, unknown[], unknown> ;
      if (m.init === true) {
        this.logger.debug("Executing init method: " + m.name);
        await m.origFunction(new InitContext(this));
      }

    }

    this.logger.info("Operon initialized");
  }

  async destroy() {
    // Wait for recovery workflows to finish.
    await Promise.allSettled(this.recoveryWorkflowHandles.map((i) => i.getResult()));
    clearInterval(this.flushBufferID);
    await this.flushWorkflowStatusBuffer();
    await this.systemDatabase.destroy();
    await this.userDatabase.destroy();
    await this.telemetryCollector.destroy();
  }

  /* WORKFLOW OPERATIONS */

  #registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, config: WorkflowConfig = {}) {
    if (wf.name === this.tempWorkflowName || this.workflowInfoMap.has(wf.name)) {
      throw new OperonError(`Repeated workflow name: ${wf.name}`);
    }
    const workflowInfo: WorkflowInfo<T, R> = {
      workflow: wf,
      config: config,
    };
    this.workflowInfoMap.set(wf.name, workflowInfo);
  }

  #registerTransaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: TransactionConfig = {}) {
    if (this.transactionConfigMap.has(txn.name)) {
      throw new OperonError(`Repeated Transaction name: ${txn.name}`);
    }
    this.transactionConfigMap.set(txn.name, params);
  }

  #registerCommunicator<T extends any[], R>(comm: OperonCommunicator<T, R>, params: CommunicatorConfig = {}) {
    if (this.communicatorConfigMap.has(comm.name)) {
      throw new OperonError(`Repeated Commmunicator name: ${comm.name}`);
    }
    this.communicatorConfigMap.set(comm.name, params);
  }

  async workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    return this.internalWorkflow(wf, params, undefined, undefined, ...args);
  }

  // If callerUUID and functionID are set, it means the workflow is invoked from within a workflow.
  async internalWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, callerUUID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>> {
    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();

    const wInfo = this.workflowInfoMap.get(wf.name);
    if (wInfo === undefined) {
      throw new OperonNotRegisteredError(wf.name);
    }
    const wConfig = wInfo.config;

    const wCtxt: WorkflowContextImpl = new WorkflowContextImpl(this, params.parentCtx, workflowUUID, wConfig, wf.name);
    wCtxt.span.setAttributes({ args: JSON.stringify(args) }); // TODO enforce skipLogging & request for hashing

    // Synchronously set the workflow's status to PENDING and record workflow inputs.  Not needed for temporary workflows.
    if (!wCtxt.isTempWorkflow) {
      args = await this.systemDatabase.initWorkflowStatus(workflowUUID, wf.name, wCtxt.authenticatedUser, wCtxt.assumedRole, wCtxt.authenticatedRoles, wCtxt.request, args);
    }
    const runWorkflow = async () => {
      // Check if the workflow previously ran.
      const previousOutput = await this.systemDatabase.checkWorkflowOutput(workflowUUID);
      if (previousOutput !== operonNull) {
        wCtxt.span.setAttribute("cached", true);
        wCtxt.span.setStatus({ code: SpanStatusCode.OK });
        this.tracer.endSpan(wCtxt.span);
        return previousOutput as R;
      }
      let result: R;
      // Execute the workflow.
      try {
        result = await wf(wCtxt, ...args);
        this.systemDatabase.bufferWorkflowOutput(workflowUUID, result);
        wCtxt.span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        if (err instanceof OperonWorkflowConflictUUIDError) {
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
      return result!;
    };
    const workflowPromise: Promise<R> = runWorkflow();
    return new InvokedHandle(this.systemDatabase, workflowPromise, workflowUUID, wf.name, callerUUID, callerFunctionID);
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const operon_temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.transaction(txn, ...args);
    };
    return (await this.workflow(operon_temp_workflow, params, ...args)).getResult();
  }

  async external<T extends any[], R>(commFn: OperonCommunicator<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call external.
    const operon_temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      const ctxtImpl = ctxt as WorkflowContextImpl;
      return await ctxtImpl.external(commFn, ...args);
    };
    return (await this.workflow(operon_temp_workflow, params, ...args)).getResult();
  }

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    // Create a workflow and call send.
    const operon_temp_workflow = async (ctxt: WorkflowContext, destinationUUID: string, message: T, topic?: string) => {
      return await ctxt.send<T>(destinationUUID, message, topic);
    };
    const workflowUUID = idempotencyKey ? destinationUUID + idempotencyKey : undefined;
    return (await this.workflow(operon_temp_workflow, { workflowUUID: workflowUUID }, destinationUUID, message, topic)).getResult();
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = Operon.defaultNotificationTimeoutSec): Promise<T | null> {
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
   * A recovery process that by default runs during Operon init time.
   * It runs to completion all pending workflows that were executing when the previous executor failed.
   */
  async recoverPendingWorkflows(executorIDs: string[] = ["local"]): Promise<WorkflowHandle<any>[]> {
    const pendingWorkflows: string[] = [];
    for (const execID of executorIDs) {
      this.logger.debug(`Recovering workflows of executor: ${execID}`)
      const wIDs = await this.systemDatabase.getPendingWorkflows(execID)
      pendingWorkflows.push(...wIDs);
    }

    const handlerArray: WorkflowHandle<any>[] = [];
    for (const workflowUUID of pendingWorkflows) {
      try {
        const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
        const inputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
        if (!inputs || !wfStatus) {
          this.logger.error(`Failed to find inputs during recover, workflow UUID: ${workflowUUID}`);
          continue;
        }
        const wfInfo: WorkflowInfo<any, any> | undefined = this.workflowInfoMap.get(wfStatus.workflowName);

        const parentCtx = this.#getRecoveryContext(workflowUUID, wfStatus);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        handlerArray.push(await this.workflow(wfInfo!.workflow, { workflowUUID: workflowUUID, parentCtx: parentCtx ?? undefined }, ...inputs))
      } catch (e) {
        this.logger.warn(`Recovery of workflow ${workflowUUID} failed:`, e);
      }
    }
    return handlerArray;
  }

  #getRecoveryContext(workflowUUID: string, status: WorkflowStatus): OperonContextImpl {
    const span = this.tracer.startSpan(status.workflowName);
    span.setAttributes({
      operationName: status.workflowName,
    });
    const oc = new OperonContextImpl(status.workflowName, span, this.logger);
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
      const ro = registeredOperation as OperonHandlerRegistration<unknown, unknown[], unknown>;
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
