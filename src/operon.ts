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
} from './workflow';

import { OperonTransaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, OperonCommunicator } from './communicator';
import {
  PostgresExporter,
  POSTGRES_EXPORTER,
  JAEGER_EXPORTER,
  JaegerExporter,
} from './telemetry/exporters';
import { TelemetryCollector } from './telemetry/collector';
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
} from './user_database';
import { OperonMethodRegistrationBase, getRegisteredOperations, getOrCreateOperonClassRegistration } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';

export interface OperonNull { }
export const operonNull: OperonNull = {};

export interface TemporaryLogger {
  info: (input: any) => void;
  warn: (input: any) => void;
  debug: (input: any) => void;
  error: (input: any) => void;
}

/* Interface for Operon configuration */
export interface OperonConfig {
  readonly poolConfig: PoolConfig;
  readonly userDbclient?: UserDatabaseName;
  readonly telemetryExporters?: string[];
  readonly system_database: string;
  readonly observability_database?: string;
  readonly application?: any;
  readonly dbClientMetadata?: any;
  readonly logger: TemporaryLogger;
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

  readonly tracer: Tracer;
// eslint-disable-next-line @typescript-eslint/ban-types
  entities: Function[] = []

  /* OPERON LIFE CYCLE MANAGEMENT */
  constructor(readonly config: OperonConfig, systemDatabase?: SystemDatabase) {
    if (systemDatabase) {
      this.config.logger.debug("Using provided system database"); // XXX print the name or something
      this.systemDatabase = systemDatabase;
    } else {
      this.config.logger.debug("Using Postgres system database");
      this.systemDatabase = new PostgresSystemDatabase(this.config.poolConfig, this.config.system_database);
    }

    this.flushBufferID = setInterval(() => {
      void this.flushWorkflowStatusBuffer();
    }, this.flushBufferIntervalMs);
    this.config.logger.debug(`Started workflow status buffer worker with ID ${this.flushBufferID}`);

    // Parse requested exporters
    const telemetryExporters = [];
    if (this.config.telemetryExporters && this.config.telemetryExporters.length > 0) {
      for (const exporter of this.config.telemetryExporters) {
        if (exporter === POSTGRES_EXPORTER) {
          telemetryExporters.push(new PostgresExporter(this.config.poolConfig, this.config.observability_database));
          this.config.logger.debug("Loaded Postgres Telemetry Exporter");
        } else if (exporter === JAEGER_EXPORTER) {
          telemetryExporters.push(new JaegerExporter());
          this.config.logger.debug("Loaded Jaeger Telemetry Exporter");
        }
      }
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
        const { PrismaClient }  = require('@prisma/client');
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-call
        this.userDatabase = new PrismaUserDatabase(new PrismaClient());
        this.config.logger.debug("Loaded Prisma user database");
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
            // synchronize: true,
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
            // entities: config.dbClientMetadata?.entities
            entities: this.entities
          }))
        } catch (error) {
          this.config.logger.error("Error loading TypeORM user database");
        }
        this.config.logger.debug("Loaded TypeORM user database");
      } else {
        this.userDatabase = new PGNodeUserDatabase(this.config.poolConfig);
        this.config.logger.debug("Loaded Postgres user database");
      }
  }

  #registerClass(cls: object) {
    const registeredClassOperations = getRegisteredOperations(cls);
    this.registeredOperations.push(...registeredClassOperations);
    for (const ro of registeredClassOperations) {
     if (ro.workflowConfig) {
      const wf = ro.registeredFunction as OperonWorkflow<any, any>;
      this.registerWorkflow(wf, ro.workflowConfig);
      this.config.logger.debug(`Registered workflow ${ro.name}`);
     } else if (ro.txnConfig) {
      const tx = ro.registeredFunction as OperonTransaction<any, any>;
      this.registerTransaction(tx, ro.txnConfig);
      this.config.logger.debug(`Registered transaction ${ro.name}`);
     } else if (ro.commConfig) {
      const comm = ro.registeredFunction as OperonCommunicator<any, any>;
      this.registerCommunicator(comm, ro.commConfig);
      this.config.logger.debug(`Registered communicator ${ro.name}`);
     }
    }
  }

  async init(...classes: object[]): Promise<void> {
    if (this.initialized) {
      this.config.logger.debug("Operon already initialized!");
      return;
    }

    try {
      type AnyConstructor = new (...args: unknown[]) => object;
      for (const cls of classes) {
        const reg = getOrCreateOperonClassRegistration(cls as AnyConstructor);
        if (reg.ormEntities.length > 0) {
          this.entities = this.entities.concat(reg.ormEntities)
          this.config.logger.debug(`Loaded ${reg.ormEntities.length} ORM entities`);
        }
      }

      this.configureDbClient();

      if (!this.userDatabase) {
        this.config.logger.error("No user database configured!");
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
        this.config.logger.error(`failed to initialize Operon: ${err.message}`);
        throw new OperonInitializationError(err.message);
      }
    }
    void this.recoverPendingWorkflows();
    this.initialized = true;
    this.config.logger.info("Operon initialized");
  }

  async destroy() {
    clearInterval(this.flushBufferID);
    await this.flushWorkflowStatusBuffer();
    await this.systemDatabase.destroy();
    await this.userDatabase.destroy();
    await this.telemetryCollector.destroy();
  }

  /* WORKFLOW OPERATIONS */

  registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, config: WorkflowConfig = {}) {
    if (wf.name === this.tempWorkflowName || this.workflowInfoMap.has(wf.name)) {
      throw new OperonError(`Repeated workflow name: ${wf.name}`);
    }
    const workflowInfo: WorkflowInfo<T, R> = {
      workflow: wf,
      config: config,
    };
    this.workflowInfoMap.set(wf.name, workflowInfo);
  }

  registerTransaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: TransactionConfig = {}) {
    if (this.transactionConfigMap.has(txn.name)) {
      throw new OperonError(`Repeated Transaction name: ${txn.name}`);
    }
    this.transactionConfigMap.set(txn.name, params);
  }

  registerCommunicator<T extends any[], R>(comm: OperonCommunicator<T, R>, params: CommunicatorConfig = {}) {
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
      args = await this.systemDatabase.initWorkflowStatus(workflowUUID, wf.name, wCtxt.authenticatedUser, wCtxt.assumedRole, wCtxt.authenticatedRoles, args);
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
      return await ctxt.transaction(txn, ...args);
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
      const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowUUID);
      const inputs = await this.systemDatabase.getWorkflowInputs(workflowUUID);
      if (!inputs) {
        console.error("Failed to find inputs during recover, workflow UUID: " + workflowUUID);
        continue;
      }
      const wfInfo: WorkflowInfo<any,any> | undefined = this.workflowInfoMap.get(wfStatus!.workflowName);

      const parentCtx = await this.systemDatabase.getRecoveryContext(this, workflowUUID);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      handlerArray.push(await this.workflow(wfInfo!.workflow, {workflowUUID : workflowUUID, parentCtx: parentCtx ?? undefined}, ...inputs))
    }
    await Promise.allSettled(handlerArray.map((i) => i.getResult()));
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
