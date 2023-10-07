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
import {
  ConsoleExporter,
  CONSOLE_EXPORTER,
  PostgresExporter,
  POSTGRES_EXPORTER,
  JAEGER_EXPORTER,
  JaegerExporter,
} from './telemetry/exporters';
import { TelemetryCollector } from './telemetry/collector';
import { Logger } from './telemetry/logs';
import { Tracer } from './telemetry/traces';
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
import { OperonMethodRegistrationBase, getRegisteredOperations, getOrCreateOperonClassRegistration } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';
import knex, { Knex } from 'knex';
import { OperonContextImpl } from './context';

export interface OperonNull { }
export const operonNull: OperonNull = {};

/* Interface for Operon configuration */
export interface OperonConfig {
  readonly poolConfig: PoolConfig;
  readonly userDbclient?: UserDatabaseName;
  readonly telemetryExporters?: string[];
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
        workflow: async () => console.error("UNREACHABLE: Indirect invoke of temp workflow"),
        config: {},
      },
    ],
  ]);
  readonly transactionConfigMap: Map<string, TransactionConfig> = new Map();
  readonly communicatorConfigMap: Map<string, CommunicatorConfig> = new Map();
  readonly topicConfigMap: Map<string, string[]> = new Map();
  readonly registeredOperations: Array<OperonMethodRegistrationBase> = [];
  readonly initialEpochTimeMs: number;

  readonly telemetryCollector: TelemetryCollector;
  readonly flushBufferIntervalMs: number = 1000;
  readonly flushBufferID: NodeJS.Timeout;

  readonly defaultNotificationTimeoutSec = 60;

  readonly logger: Logger;
  readonly tracer: Tracer;
  // eslint-disable-next-line @typescript-eslint/ban-types
  entities: Function[] = []

  /* OPERON LIFE CYCLE MANAGEMENT */
  constructor(readonly config: OperonConfig, systemDatabase?: SystemDatabase) {
    if (systemDatabase) {
      this.systemDatabase = systemDatabase;
    } else {
      this.systemDatabase = new PostgresSystemDatabase(this.config.poolConfig, this.config.system_database);
    }
    this.flushBufferID = setInterval(() => {
      void this.flushWorkflowStatusBuffer();
    }, this.flushBufferIntervalMs);

    // Parse requested exporters
    const telemetryExporters = [];
    if (this.config.telemetryExporters && this.config.telemetryExporters.length > 0) {
      for (const exporter of this.config.telemetryExporters) {
        if (exporter === CONSOLE_EXPORTER) {
          telemetryExporters.push(new ConsoleExporter());
        } else if (exporter === POSTGRES_EXPORTER) {
          telemetryExporters.push(new PostgresExporter(this.config.poolConfig, this.config.observability_database));
        } else if (exporter === JAEGER_EXPORTER) {
          telemetryExporters.push(new JaegerExporter());
        }
      }
    } else {
      // If nothing is configured, enable console exporter by default.
      telemetryExporters.push(new ConsoleExporter());
    }
    this.telemetryCollector = new TelemetryCollector(telemetryExporters);
    this.logger = new Logger(this.telemetryCollector);
    this.tracer = new Tracer(this.telemetryCollector);
    this.initialized = false;
    this.initialEpochTimeMs = Date.now();
  }

  configureDbClient(config: OperonConfig) {
    const userDbClient = config.userDbclient;
    if (userDbClient === UserDatabaseName.PRISMA) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
      const { PrismaClient } = require('@prisma/client');
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call
      this.userDatabase = new PrismaUserDatabase(new PrismaClient());
    } else if (userDbClient === UserDatabaseName.TYPEORM) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-var-requires
      const DataSourceExports = require('typeorm');
      try {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
        this.userDatabase = new TypeORMDatabase(new DataSourceExports.DataSource({
          type: "postgres", // perhaps should move to config file
          host: config.poolConfig.host,
          port: config.poolConfig.port,
          username: config.poolConfig.user,
          password: config.poolConfig.password,
          database: config.poolConfig.database,
          // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
          entities: this.entities
        }))
      } catch (s) {
        console.log(s);
      }
    } else if (userDbClient === UserDatabaseName.KNEX) {
      const knexConfig: Knex.Config = {
        client: 'postgres',
        connection: {
          host: config.poolConfig.host,
          port: config.poolConfig.port,
          user: config.poolConfig.user,
          password: config.poolConfig.password,
          database: config.poolConfig.database,
        }
      }
      this.userDatabase = new KnexUserDatabase(knex(knexConfig));
    } else {
      this.userDatabase = new PGNodeUserDatabase(this.config.poolConfig);
    }
  }

  #registerClass(cls: object) {
    const registeredClassOperations = getRegisteredOperations(cls);
    this.registeredOperations.push(...registeredClassOperations);
    for (const ro of registeredClassOperations) {
      if (ro.workflowConfig) {
        const wf = ro.registeredFunction as OperonWorkflow<any, any>;
        this.#registerWorkflow(wf, ro.workflowConfig);
      } else if (ro.txnConfig) {
        const tx = ro.registeredFunction as OperonTransaction<any, any>;
        this.#registerTransaction(tx, ro.txnConfig);
      } else if (ro.commConfig) {
        const comm = ro.registeredFunction as OperonCommunicator<any, any>;
        this.#registerCommunicator(comm, ro.commConfig);
      }
    }
  }

  async init(...classes: object[]): Promise<void> {

    if (this.initialized) {
      console.log("Operon already initialized!");
      return;
    }
    try {

      type AnyConstructor = new (...args: unknown[]) => object;
      for (const cls of classes) {
        const reg = getOrCreateOperonClassRegistration(cls as AnyConstructor);
        if (reg.ormEntities.length > 0) {
          this.entities = this.entities.concat(reg.ormEntities)
        }

      }

      this.configureDbClient(this.config);

      if (!this.userDatabase) {
        throw new OperonInitializationError("No data source!");
      }

      for (const cls of classes) {
        this.#registerClass(cls);
      }

      await this.telemetryCollector.init(this.registeredOperations);
      await this.userDatabase.init();
      await this.systemDatabase.init();
    } catch (err) {
      if (err instanceof Error) {
        console.error(err);
        throw new OperonInitializationError(err.message);
      }
    }
    void this.recoverPendingWorkflows();
    this.initialized = true;
  }

  async destroy() {
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
      args = await this.systemDatabase.initWorkflowStatus(workflowUUID, wf.name, wCtxt.authenticatedUser, wCtxt.assumedRole, wCtxt.authenticatedRoles, wCtxt.request ?? null, args);
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
    return new InvokedHandle(this.systemDatabase, workflowPromise, workflowUUID, wf.name);
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

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic: string, idempotencyKey?: string): Promise<void> {
    // Create a workflow and call send.
    const operon_temp_workflow = async (ctxt: WorkflowContext, destinationUUID: string, message: T, topic: string) => {
      return await ctxt.send<T>(destinationUUID, message, topic);
    };
    const workflowUUID = idempotencyKey ? destinationUUID + idempotencyKey : undefined;
    return (await this.workflow(operon_temp_workflow, { workflowUUID: workflowUUID }, destinationUUID, message, topic)).getResult();
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = this.defaultNotificationTimeoutSec): Promise<T | null> {
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
   * A recovery process that runs during Operon init time.
   * It runs to completion all pending workflows that were executing when the previous executor failed.
   */
  async recoverPendingWorkflows() {
    const pendingWorkflows = await this.systemDatabase.getPendingWorkflows();
    const handlerArray: WorkflowHandle<any>[] = [];
    for (const workflowUUID of pendingWorkflows) {
      try {
        const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
        const inputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
        if (!inputs || !wfStatus) {
          console.error("Failed to find inputs during recover, workflow UUID: " + workflowUUID);
          continue;
        }
        const wfInfo: WorkflowInfo<any, any> | undefined = this.workflowInfoMap.get(wfStatus.workflowName);

        const parentCtx = this.#getRecoveryContext(workflowUUID, wfStatus);
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        handlerArray.push(await this.workflow(wfInfo!.workflow, { workflowUUID: workflowUUID, parentCtx: parentCtx ?? undefined }, ...inputs))
      } catch (e) {
        console.warn(`Recovery of workflow ${workflowUUID} failed:`, e);
      }
    }
    await Promise.allSettled(handlerArray.map((i) => i.getResult()));
  }

  #getRecoveryContext(workflowUUID: string, status: WorkflowStatus): OperonContextImpl {
    const span = this.tracer.startSpan(status.workflowName);
    span.setAttributes({
      operationName: status.workflowName,
    });
    const oc = new OperonContextImpl(status.workflowName, span, this.logger);
    oc.request = status.request ?? undefined;
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

}
