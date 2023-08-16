/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  OperonError,
  OperonWorkflowPermissionDeniedError,
  OperonInitializationError,
  OperonWorkflowConflictUUIDError,
  OperonWorkflowUnknownError
} from './error';
import {InvokedHandle, OperonWorkflow, WorkflowConfig, WorkflowContext, WorkflowHandle, WorkflowParams, RetrievedHandle, StatusString } from './workflow';
import { OperonTransaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, OperonCommunicator } from './communicator';
import { readFileSync } from './utils';
import {
  TelemetryCollector,
  ConsoleExporter,
  CONSOLE_EXPORTER,
  PostgresExporter,
  POSTGRES_EXPORTER,
  JAEGER_EXPORTER,
  JaegerExporter,
  Logger,
} from './telemetry';
import { PoolConfig } from 'pg';
import { transaction_outputs } from '../schemas/user_db_schema';
import { SystemDatabase, PostgresSystemDatabase } from './system_database';
import { v4 as uuidv4 } from 'uuid';
import YAML from 'yaml';
import { PGNodeUserDatabase, PrismaClient, PrismaUserDatabase, UserDatabase } from './user_database';
import { OperonMethodRegistration, forEachMethod } from './decorators';
import { SpanStatusCode } from '@opentelemetry/api';
import { Span } from "@opentelemetry/sdk-trace-base";
import { Tracer } from './telemetry/traces';

export interface OperonNull {}
export const operonNull: OperonNull = {};

/* Interface for Operon configuration */
const CONFIG_FILE: string = "operon-config.yaml";

export interface OperonConfig {
  readonly poolConfig: PoolConfig;
  readonly telemetryExporters?: string[];
  readonly system_database: string;
  readonly observability_database?: string;
}

interface ConfigFile {
  database: {
    hostname: string;
    port: number;
    username: string;
    connectionTimeoutMillis: number;
    user_database: string;
    system_database: string;
  };
  telemetryExporters?: string[];
}

interface WorkflowInfo<T extends any[], R> {
  workflow: OperonWorkflow<T, R>;
  config: WorkflowConfig;
}

interface WorkflowInput<T extends any[]> {
  workflow_name: string;
  input: T;
}

export class Operon {
  initialized: boolean;
  readonly config: OperonConfig;
  // User Database
  userDatabase: UserDatabase = null as unknown as UserDatabase;
  // System Database
  readonly systemDatabase: SystemDatabase;
  
  // Temporary workflows are created by calling transaction/send/recv directly from the Operon class
  readonly tempWorkflowName = "operon_temp_workflow";

  readonly recoveryDelayMs: number = 20000;
  readonly recoveryID: NodeJS.Timeout;

  readonly workflowInfoMap: Map<string, WorkflowInfo<any, any>> = new Map([
    // We initialize the map with an entry for temporary workflows.
    [this.tempWorkflowName, 
      {
        // eslint-disable-next-line @typescript-eslint/require-await
        workflow: async () => console.error("UNREACHABLE: Indirect invoke of temp workflow"),
        config: {}
      }]
  ]);
  readonly transactionConfigMap: Map<string, TransactionConfig> = new Map();
  readonly communicatorConfigMap: WeakMap<OperonCommunicator<any, any>, CommunicatorConfig> = new WeakMap();
  readonly topicConfigMap: Map<string, string[]> = new Map();

  readonly initialEpochTimeMs: number;

  readonly telemetryCollector: TelemetryCollector;
  readonly flushBufferIntervalMs: number = 1000;
  readonly flushBufferID: NodeJS.Timeout;

  readonly logger: Logger;
  readonly tracer: Tracer;

  /* OPERON LIFE CYCLE MANAGEMENT */
  constructor(config?: OperonConfig) {
    if (config) {
      this.config = config;
    } else {
      this.config = this.generateOperonConfig();
    }
    this.systemDatabase = new PostgresSystemDatabase(this.config.poolConfig, this.config.system_database);
    this.flushBufferID = setInterval(() => {
      void this.flushWorkflowOutputBuffer();
    }, this.flushBufferIntervalMs) ;
    this.recoveryID = setTimeout(() => {void this.recoverPendingWorkflows()}, this.recoveryDelayMs);

    // Parse requested exporters
    const telemetryExporters = [];
    if (this.config.telemetryExporters) {
      for (const exporter of this.config.telemetryExporters) {
        if (exporter === CONSOLE_EXPORTER) {
          telemetryExporters.push(new ConsoleExporter());
        } else if (exporter === POSTGRES_EXPORTER) {
          telemetryExporters.push(new PostgresExporter(this, config?.observability_database));
        } else if (exporter === JAEGER_EXPORTER) {
          telemetryExporters.push(new JaegerExporter());
        }
      }
    }
    this.telemetryCollector = new TelemetryCollector(telemetryExporters);
    this.logger = new Logger(this.telemetryCollector);
    this.tracer = new Tracer(this.telemetryCollector);
    this.initialized = false;
    this.initialEpochTimeMs = Date.now();

    // Register user declared operations
    forEachMethod((registeredOperation) => {
      const ro = registeredOperation as OperonMethodRegistration<unknown, unknown[], unknown>;
      for (const arg of ro.args) {
        if (arg.argType.name === 'WorkflowContext') {
          const wf = ro.origFunction as OperonWorkflow<any, any>;
          this.registerWorkflow(wf, ro.workflowConfig);
          break;
        } else if (arg.argType.name === 'TransactionContext') {
          const tx = ro.origFunction as OperonTransaction<any, any>;
          this.registerTransaction(tx, ro.txnConfig);
          break;
        }
      }
    });
  }

  useNodePostgres() {
    if (this.userDatabase) {
      throw new OperonInitializationError("Data source already initialized!");
    }
    this.userDatabase = new PGNodeUserDatabase(this.config.poolConfig);
  }

  usePrisma(client: PrismaClient) {
    if (this.userDatabase) {
      throw new OperonInitializationError("Data source already initialized!");
    }
    this.userDatabase = new PrismaUserDatabase(client);
  }

  async init(): Promise<void> {
    if (!this.userDatabase) {
      throw new OperonInitializationError("No data source!");
    }
    if (this.initialized) {
      // TODO add logging when we have a logger
      return;
    }
    try {
      await this.userDatabase.init();
      await this.telemetryCollector.init();
      await this.systemDatabase.init();
    } catch (err) {
      if (err instanceof Error) {
        throw(new OperonInitializationError(err.message));
      }
    }
    this.initialized = true;
  }

  async destroy() {
    clearInterval(this.flushBufferID);
    await this.flushWorkflowOutputBuffer();
    clearTimeout(this.recoveryID);
    await this.systemDatabase.destroy();
    await this.userDatabase.destroy();
    await this.telemetryCollector.destroy();
  }

  generateOperonConfig(): OperonConfig {
    // Load default configuration
    let config: ConfigFile | undefined;
    try {
      const configContent = readFileSync(CONFIG_FILE);
      config = YAML.parse(configContent) as ConfigFile;
    } catch(error) {
      if (error instanceof Error) {
        throw(new OperonInitializationError(`parsing ${CONFIG_FILE}: ${error.message}`));
      }
    }
    if (!config) {
      throw(new OperonInitializationError(`Operon configuration ${CONFIG_FILE} is empty`));
    }

    // Handle "Global" pool config
    if (!config.database) {
      throw(new OperonInitializationError(
        `Operon configuration ${CONFIG_FILE} does not contain database config`
      ));
    }
    const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
    if (!dbPassword) {
      throw(new OperonInitializationError(
        'DB_PASSWORD or PGPASSWORD environment variable not set'
      ));
    }
    const poolConfig: PoolConfig = {
      host: config.database.hostname,
      port: config.database.port,
      user: config.database.username,
      password: dbPassword,
      connectionTimeoutMillis: config.database.connectionTimeoutMillis,
      database: config.database.user_database,
    };

    return {
      poolConfig: poolConfig,
      telemetryExporters: config.telemetryExporters || [],
      system_database: config.database.system_database ?? 'operon_systemdb'
    };
  }

  /* BACKGROUND PROCESSES */

  /**
   * A background process that runs once asynchronously a certain period after initialization.
   * It executes all pending workflows that were last updated before initialization.
   * This recovers and runs to completion any workflows that were still executing when a previous executor failed.
   */
  async recoverPendingWorkflows() {
    // Retrieve a list of workflow UUIDs from the function output table.
    const workflows = await this.userDatabase.query<transaction_outputs>("select workflow_uuid, output from operon.transaction_outputs WHERE function_id = 0;");
    const handlerArray: WorkflowHandle<any>[] = [];
    for (const workflow of workflows) {
      // Check workflow status. If not success or error, then recover.
      const status = await this.retrieveWorkflow(workflow.workflow_uuid).getStatus();
      if ((status.status !== StatusString.SUCCESS) && (status.status !== StatusString.ERROR)) {
        // Retrieve workflow name from the recorded input.
        const wfInput: WorkflowInput<any> = (JSON.parse(workflow.output) as WorkflowInput<any>);
        const wInfo = this.workflowInfoMap.get(wfInput.workflow_name);
        if (wInfo === undefined) {
          throw new OperonWorkflowUnknownError(workflow.workflow_uuid, wfInput.workflow_name);
        }
        handlerArray.push(this.workflow(wInfo.workflow, {workflowUUID: workflow.workflow_uuid}));
      }
    }
    // Wait until all workflows complete.
    await Promise.allSettled(handlerArray.map((i) => i.getResult()));
  }

  /* WORKFLOW OPERATIONS */

  registerTopic(topic: string, rolesThatCanPubSub: string[] = []) {
    this.topicConfigMap.set(topic, rolesThatCanPubSub);
  }

  registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, config: WorkflowConfig={}) {
    if (wf.name === this.tempWorkflowName || this.workflowInfoMap.has(wf.name)) {
      throw new OperonError(`Repeated workflow name: ${wf.name}`)
    }
    const workflowInfo: WorkflowInfo<T, R> = {
      workflow: wf,
      config: config
    }
    this.workflowInfoMap.set(wf.name, workflowInfo);
  }

  registerTransaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: TransactionConfig={}) {
    this.transactionConfigMap.set(txn.name, params);
  }

  registerCommunicator<T extends any[], R>(comm: OperonCommunicator<T, R>, params: CommunicatorConfig={}) {
    this.communicatorConfigMap.set(comm, params);
  }

  workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T): WorkflowHandle<R> {
    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();

    const runWorkflow = async () => {
      const wInfo = this.workflowInfoMap.get(wf.name);
      if (wInfo === undefined) {
        throw new OperonError(`Unregistered Workflow ${wf.name}`);
      }
      const wConfig = wInfo.config;

      // Check if the user has permission to run this workflow.
      if (!params.runAs) {
        params.runAs = "defaultRole";
      }
      const userHasPermission = this.hasPermission(params.runAs, wConfig);
      if (!userHasPermission) {
        throw new OperonWorkflowPermissionDeniedError(params.runAs, wf.name);
      }

      const span: Span = this.tracer.startSpan(wf.name, params.parentSpan);
      const wCtxt: WorkflowContext = new WorkflowContext(this, span, workflowUUID, params.runAs, wConfig, wf.name);
      const workflowInputID = wCtxt.functionIDGetIncrement();
      wCtxt.span.setAttributes({
        'workflowUUID': workflowUUID,
        'operationName': wf.name,
        'runAs': params.runAs,
        'functionID': workflowInputID,
        'args': JSON.stringify(args), // TODO enforce skipLogging & request for hashing
      });

      const checkWorkflowInput = async (input: T) => {
        // The workflow input is always at function ID = 0 in the operon.transaction_outputs table.
        const rows = await this.userDatabase.query<transaction_outputs>("SELECT output FROM operon.transaction_outputs WHERE workflow_uuid=$1 AND function_id=$2",
          workflowUUID, workflowInputID);
        if (rows.length === 0) {
          // This workflow has never executed before, so record the input.
          wCtxt.resultBuffer.set(workflowInputID, {workflow_name: wf.name, input: input});
        } else {
        // Return the old recorded input
          input = (JSON.parse(rows[0].output) as WorkflowInput<T>).input;
        }
        return input;
      }

      const previousOutput = await this.systemDatabase.checkWorkflowOutput(workflowUUID);
      if (previousOutput !== operonNull) {
        wCtxt.span.setAttribute('cached', true);
        wCtxt.span.setStatus({ code: SpanStatusCode.OK });
        this.tracer.endSpan(span);
        return previousOutput as R;
      }
      // Record inputs for OAOO. Not needed for temporary workflows.
      const input = wCtxt.isTempWorkflow ? args: await checkWorkflowInput(args);
      let result: R;
      try {
        result = await wf(wCtxt, ...input);
        await this.systemDatabase.bufferWorkflowOutput(workflowUUID, result);
        wCtxt.span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        if (err instanceof OperonWorkflowConflictUUIDError) {
          // Retrieve the handle and wait for the result.
          const retrievedHandle = this.retrieveWorkflow<R>(workflowUUID);
          result = await retrievedHandle.getResult();
          wCtxt.span.setAttribute('cached', true);
          wCtxt.span.setStatus({ code: SpanStatusCode.OK });
        } else {
          // Record the error.
          const e: Error = err as Error;
          await this.systemDatabase.recordWorkflowError(workflowUUID, e);
          wCtxt.span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
          throw err;
        }
      } finally {
        this.tracer.endSpan(wCtxt.span);
      }
      return result!;
    }
    const workflowPromise: Promise<R> = runWorkflow();
    return new InvokedHandle(this.systemDatabase, workflowPromise, workflowUUID, wf.name);
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const operon_temp_workflow = async (ctxt: WorkflowContext, ...args: T) => {
      return await ctxt.transaction(txn, ...args);
    };
    return await this.workflow(operon_temp_workflow, params, ...args).getResult();
  }

  async send<T extends NonNullable<any>>(params: WorkflowParams, topic: string, key: string, message: T) : Promise<boolean> {
    // Create a workflow and call send.
    const operon_temp_workflow = async (ctxt: WorkflowContext, topic: string, key: string, message: T) => {
      return await ctxt.send<T>(topic, key, message);
    };
    return await this.workflow(operon_temp_workflow, params, topic, key, message).getResult();
  }

  async recv<T extends NonNullable<any>>(params: WorkflowParams, topic: string, key: string, timeoutSeconds: number) : Promise<T | null> {
    // Create a workflow and call recv.
    const operon_temp_workflow = async (ctxt: WorkflowContext, topic: string, key: string, timeoutSeconds: number) => {
      return await ctxt.recv<T>(topic, key, timeoutSeconds);
    };
    return await this.workflow(operon_temp_workflow, params, topic, key, timeoutSeconds).getResult();
  }

  retrieveWorkflow<R>(workflowUUID: string) : WorkflowHandle<R> {
    return new RetrievedHandle(this.systemDatabase, workflowUUID);
  }

  /* INTERNAL HELPERS */
  #generateUUID(): string {
    return uuidv4();
  }

  hasPermission(role: string, workflowConfig: WorkflowConfig): boolean {
    // An empty list of roles in the workflow config means the workflow is permission-less
    if (!workflowConfig.rolesThatCanRun) {
      return true;
    } else {
      // Default role cannot run permissioned workflows
      if (role === "defaultRole") {
        return false;
      }
      // Check if the user's role is in the list of roles that can run the workflow
      for (const roleThatCanRun of workflowConfig.rolesThatCanRun) {
        if (role === roleThatCanRun) {
          return true;
        }
      }
    }
    // Reject by default
    return false;
  }

  /* BACKGROUND PROCESSES */
  /**
   * Periodically flush the workflow output buffer to the system database.
   */
  async flushWorkflowOutputBuffer() {
    if (this.initialized) {
      await this.systemDatabase.flushWorkflowOutputBuffer();
    }
  }
}
