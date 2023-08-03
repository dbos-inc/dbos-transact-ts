/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  OperonError,
  OperonWorkflowPermissionDeniedError,
  OperonInitializationError,
  OperonWorkflowConflictUUIDError
} from './error';
import { WorkflowStatus, InvokedHandle, OperonWorkflow, WorkflowConfig, WorkflowContext, WorkflowHandle, WorkflowParams, RetrievedHandle } from './workflow';
import { OperonTransaction, TransactionConfig, validateTransactionConfig } from './transaction';
import { CommunicatorConfig, OperonCommunicator } from './communicator';
import { readFileSync } from './utils';
import operonSystemDbSchema from '../schemas/operon';

import { Pool, PoolConfig, Client, Notification, PoolClient } from 'pg';
import { v4 as uuidv4 } from 'uuid';
import YAML from 'yaml';
import { deserializeError, serializeError } from 'serialize-error';

/* Interfaces for Operon system data structures */
export interface operon__FunctionOutputs {
  workflow_id: string;
  function_id: number;
  output: string;
  error: string;
}

export interface operon__WorkflowStatus {
  workflow_id: string;
  workflow_name: string;
  status: string;
  output: string;
  error: string;
  last_update: number;  // UNIX timestamp in seconds.
}

export interface operon__Notifications {
  key: string;
  message: string;
}

export interface OperonNull {}
export const operonNull: OperonNull = {};

/* Interface for Operon configuration */
const CONFIG_FILE: string = "operon-config.yaml";

export interface OperonConfig {
  readonly poolConfig: PoolConfig;
}

interface operon__ConfigFile {
  database: operon__DatabaseConfig;
}

interface operon__DatabaseConfig {
  hostname: string;
  port: number;
  username: string;
  connectionTimeoutMillis: number;
  database: string;
}

export class Operon {
  initialized: boolean;
  readonly config: OperonConfig;
  // "Global" pool
  readonly pool: Pool;
  // PG client for interacting with the `postgres` database
  readonly pgSystemClient: Client;
  // PG client for listening to Operon notifications
  readonly pgNotificationsClient: Client;

  readonly listenerMap: Record<string, () => void> = {};

  readonly workflowOutputBuffer: Map<string, string> = new Map();
  readonly flushBufferIntervalMs: number = 1000;
  readonly flushBufferID: NodeJS.Timeout;

  readonly workflowConfigMap: WeakMap<OperonWorkflow<any, any>, WorkflowConfig> = new WeakMap();
  readonly transactionConfigMap: WeakMap<OperonTransaction<any, any>, TransactionConfig> = new WeakMap();
  readonly communicatorConfigMap: WeakMap<OperonCommunicator<any, any>, CommunicatorConfig> = new WeakMap();

  readonly workflowNameMap: Map<string, OperonWorkflow<any, any>> = new Map();

  readonly initialEpochTimeMs: number;  // UNIX timestamp in seconds.

  /* OPERON LIFE CYCLE MANAGEMENT */
  constructor(config?: OperonConfig) {
    if (config) {
      this.config = config;
    } else {
      this.config = this.generateOperonConfig();
    }

    this.pgSystemClient = new Client({
      user: this.config.poolConfig.user,
      port: this.config.poolConfig.port,
      host: this.config.poolConfig.host,
      password: this.config.poolConfig.password,
      database: 'postgres',
    });
    this.pgNotificationsClient = new Client({
      user: this.config.poolConfig.user,
      port: this.config.poolConfig.port,
      host: this.config.poolConfig.host,
      password: this.config.poolConfig.password,
      database: this.config.poolConfig.database,
    });
    this.pool = new Pool(this.config.poolConfig);
    this.flushBufferID = setInterval(() => {
      void this.flushWorkflowOutputBuffer();
    }, this.flushBufferIntervalMs) ;
    this.initialized = false;
    this.initialEpochTimeMs = Date.now();
  }

  async init(): Promise<void> {
    if (this.initialized) {
      // TODO add logging when we have a logger
      return;
    }
    try {
      await this.loadOperonDatabase();
      await this.listenForNotifications();
    } catch (err) {
      if (err instanceof Error) {
        throw(new OperonInitializationError(err.message));
      }
    }
    this.initialized = true;
  }

  // Operon database management
  async loadOperonDatabase() {
    await this.pgSystemClient.connect();
    try {
      const databaseName: string = this.config.poolConfig.database as string;
      // Validate the database name
      const regex = /^[a-z0-9]+$/i;
      if (!regex.test(databaseName)) {
        throw(new Error(`invalid DB name: ${databaseName}`));
      }
      // Check whether the Operon system database exists, create it if needed
      const dbExists = await this.pgSystemClient.query(
        `SELECT FROM pg_database WHERE datname = '${databaseName}'`
      );
      if (dbExists.rows.length === 0) {
        // Create the Operon system database
        await this.pgSystemClient.query(`CREATE DATABASE ${databaseName}`);
      }
      // Load the Operon system schema
      await this.pool.query(operonSystemDbSchema);
    } finally {
      // We want to close the client no matter what
      await this.pgSystemClient.end();
    }
  }

  async destroy() {
    clearInterval(this.flushBufferID);
    await this.flushWorkflowOutputBuffer();
    await this.pgNotificationsClient.removeAllListeners().end();
    await this.pool.end();
  }

  generateOperonConfig(): OperonConfig {
    // Load default configuration
    let configuration: operon__ConfigFile | undefined;
    try {
      const configContent = readFileSync(CONFIG_FILE);
      configuration = YAML.parse(configContent) as operon__ConfigFile;
    } catch(error) {
      if (error instanceof Error) {
        throw(new OperonInitializationError(`parsing ${CONFIG_FILE}: ${error.message}`));
      }
    }
    if (!configuration) {
      throw(new OperonInitializationError(`Operon configuration ${CONFIG_FILE} is empty`));
    }

    // Handle "Global" pool config
    if (!configuration.database) {
      throw(new OperonInitializationError(
        `Operon configuration ${CONFIG_FILE} does not contain database config`
      ));
    }
    const dbConfig: operon__DatabaseConfig = configuration.database;
    const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
    if (!dbPassword) {
      throw(new OperonInitializationError(
        'DB_PASSWORD or PGPASSWORD environment variable not set'
      ));
    }
    const poolConfig: PoolConfig = {
      host: dbConfig.hostname,
      port: dbConfig.port,
      user: dbConfig.username,
      password: dbPassword,
      connectionTimeoutMillis: dbConfig.connectionTimeoutMillis,
      database: dbConfig.database,
    };

    return {
      poolConfig,
    };
  }

  /* BACKGROUND PROCESSES */
  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.no
   */
  async listenForNotifications() {
    await this.pgNotificationsClient.connect();
    await this.pgNotificationsClient.query('LISTEN operon__notificationschannel;');
    const handler = (msg: Notification ) => {
      if (msg.payload && msg.payload in this.listenerMap) {
        this.listenerMap[msg.payload]();
      }
    };
    this.pgNotificationsClient.on('notification', handler);
  }

  /**
   * A background process that periodically flushes the workflow output buffer to the database.
   */
  async flushWorkflowOutputBuffer() {
    if (this.initialized) {
      const localBuffer = new Map(this.workflowOutputBuffer);
      this.workflowOutputBuffer.clear();
      const client: PoolClient = await this.pool.connect();
      await client.query("BEGIN");
      for (const [workflowUUID, output] of localBuffer) {
        await client.query(`INSERT INTO operon__WorkflowStatus (workflow_id, status, output) VALUES($1, $2, $3) ON CONFLICT (workflow_id)
         DO UPDATE SET status=EXCLUDED.status, output=EXCLUDED.output, last_update_epoch_ms=(EXTRACT(EPOCH FROM now())*1000)::bigint;`,
        [workflowUUID, WorkflowStatus.SUCCESS, output]);
      }
      await client.query("COMMIT");
      client.release();
    }
  }

  /**
   * A background process that runs once asynchronously during init and restarts all pending workflows.
   * It then waits for all recovering workflows to finish.
   */
  async recoverPendingWorkflows() {
    const { rows } = await this.pool.query<operon__WorkflowStatus>("SELECT * FROM operon__WorkflowStatus WHERE status=$1 AND last_update_epoch_ms<$2", 
      [WorkflowStatus.PENDING, this.initialEpochTimeMs]);
    const handlerArray: WorkflowHandle<any>[] = [];
    for (const row of rows) {
      const wf = this.workflowNameMap.get(row.workflow_name);
      if (wf === undefined) {
        throw new OperonError(`Workflow unregistered during recovery: ${row.workflow_name}`);
      }
      handlerArray.push(this.workflow(wf, {workflowUUID: row.workflow_id}));
    }
    await Promise.allSettled(handlerArray.map((i) => i.getResult()));
  }

  /* OPERON INTERFACE */
  registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, config: WorkflowConfig={}) {
    // TODO: fix anonymous workflow names in the next PR.
    if ((wf.name !== 'anonWf') && this.workflowNameMap.has(wf.name)) {
      throw new OperonError(`Repeated workflow name: ${wf.name}`)
    }
    this.workflowNameMap.set(wf.name, wf);
    this.workflowConfigMap.set(wf, config);
  }

  registerTransaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: TransactionConfig={}) {
    validateTransactionConfig(params);
    this.transactionConfigMap.set(txn, params);
  }

  registerCommunicator<T extends any[], R>(comm: OperonCommunicator<T, R>, params: CommunicatorConfig={}) {
    this.communicatorConfigMap.set(comm, params);
  }

  workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T): WorkflowHandle<R> {
    
    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();

    const runWorkflow = async () => {
      const wConfig = this.workflowConfigMap.get(wf);
      if (wConfig === undefined) {
        throw new OperonError(`Unregistered Workflow ${wf.name}`);
      }
      const wCtxt: WorkflowContext = new WorkflowContext(this, workflowUUID, wConfig, wf.name);

      const workflowInputID = wCtxt.functionIDGetIncrement();

      // Check if the user has permission to run this workflow.
      if (!params.runAs) {
        params.runAs = "defaultRole";
      }
      const userHasPermission = this.hasPermission(params.runAs, wConfig);
      if (!userHasPermission) {
        throw new OperonWorkflowPermissionDeniedError(params.runAs, wf.name);
      }

      const checkWorkflowOutput = async () => {
        const { rows } = await this.pool.query<operon__WorkflowStatus>("SELECT status, output, error FROM operon__WorkflowStatus WHERE workflow_id=$1",
          [workflowUUID]);
        if ((rows.length === 0) || (rows[0].status === WorkflowStatus.PENDING)) {
          return operonNull;
        } else if (rows[0].status === WorkflowStatus.ERROR) {
          throw deserializeError(JSON.parse(rows[0].error));
        } else {
          return JSON.parse(rows[0].output) as R;  // Could be null.
        }
      }

      const recordWorkflowOutput = (output: R) => {
        this.workflowOutputBuffer.set(workflowUUID, JSON.stringify(output));
      }

      const recordWorkflowError = async (err: Error) => {
        const serialErr = JSON.stringify(serializeError(err));
        await this.pool.query(`INSERT INTO operon__WorkflowStatus (workflow_id, status, error) VALUES($1, $2, $3) ON CONFLICT (workflow_id) 
        DO UPDATE SET status=EXCLUDED.status, error=EXCLUDED.error, last_update_epoch_ms=(EXTRACT(EPOCH FROM now())*1000)::bigint;`, 
        [workflowUUID, WorkflowStatus.ERROR, serialErr]);
      }

      const checkWorkflowInput = async (input: T) => {
      // The workflow input is always at function ID = 0 in the operon__FunctionOutputs table.
        const { rows } = await this.pool.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
          [workflowUUID, workflowInputID]);
        if (rows.length === 0) {
          // This workflow has never executed before, so record the input.
          wCtxt.resultBuffer.set(workflowInputID, JSON.stringify(input));
          // TODO: Also indicate this workflow is pending now.
        } else {
        // Return the old recorded input
          input = JSON.parse(rows[0].output) as T;
        }
        return input;
      }

      const previousOutput = await checkWorkflowOutput();
      if (previousOutput !== operonNull) {
        return previousOutput as R;
      }
      const input = await checkWorkflowInput(args);
      let result: R;
      try {
        result = await wf(wCtxt, ...input);
        recordWorkflowOutput(result);
      } catch (err) {
        if (err instanceof OperonWorkflowConflictUUIDError) {
          // TODO: Retrieve, then return await result.
          throw err;
        } else {
          // Record the error.
          await recordWorkflowError(err as Error);
          throw err;
        }
      }
      return result!;
    }
    const workflowPromise: Promise<R> = runWorkflow();
    return new InvokedHandle(this.pool, workflowPromise, workflowUUID);
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const anonWf = async (ctxt: WorkflowContext, ...args: T) => {
      return await ctxt.transaction(txn, ...args);
    };
    this.registerWorkflow(anonWf);
    return await this.workflow(anonWf, params, ...args).getResult();
  }

  async send<T extends NonNullable<any>>(params: WorkflowParams, key: string, message: T) : Promise<boolean> {
    // Create a workflow and call send.
    const anonWf = async (ctxt: WorkflowContext, key: string, message: T) => {
      return await ctxt.send<T>(key, message);
    };
    this.registerWorkflow(anonWf);
    return await this.workflow(anonWf, params, key, message).getResult();
  }

  async recv<T extends NonNullable<any>>(params: WorkflowParams, key: string, timeoutSeconds: number) : Promise<T | null> {
    // Create a workflow and call recv.
    const anonWf = async (ctxt: WorkflowContext, key: string, timeoutSeconds: number) => {
      return await ctxt.recv<T>(key, timeoutSeconds);
    };
    this.registerWorkflow(anonWf);
    return await this.workflow(anonWf, params, key, timeoutSeconds).getResult();
  }

  async retrieveWorkflow<R>(workflowUUID: string) : Promise<WorkflowHandle<R> | null> {
    const { rows } = await this.pool.query<operon__WorkflowStatus>("SELECT status FROM operon__WorkflowStatus WHERE workflow_id=$1", [workflowUUID]);
    if (rows.length === 0) {
      return null;
    }
    return new RetrievedHandle(this.pool, workflowUUID);
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
}
