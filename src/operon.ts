/* eslint-disable @typescript-eslint/no-explicit-any */
import { OperonConfig } from './operon.config';
import { OperonError, OperonWorkflowPermissionDeniedError } from './error';
import { OperonWorkflow, WorkflowConfig, WorkflowContext, WorkflowParams } from './workflow';
import { OperonTransaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, OperonCommunicator } from './communicator';

import { Pool, Client, Notification } from 'pg';
import { v4 as uuidv4 } from 'uuid';

export interface operon__FunctionOutputs {
    workflow_id: string;
    function_id: number;
    output: string;
    error: string;
}

export interface operon__Notifications {
  key: string;
  message: string;
}

export class Operon {
  readonly config: OperonConfig;
  readonly pool: Pool;
  readonly pgSystemClient: Client;
  readonly notificationsClient: Client;
  readonly listenerMap: Record<string, () => void> = {};

  /* OPERON LIFE CYCLE MANAGEMENT */
  constructor() {
    this.config = new OperonConfig();
    this.pgSystemClient = new Client({
      user: this.config.poolConfig.user,
      port: this.config.poolConfig.port,
      host: this.config.poolConfig.host,
      password: this.config.poolConfig.password,
      database: 'postgres',
    });
    this.notificationsClient = new Client({
      user: this.config.poolConfig.user,
      port: this.config.poolConfig.port,
      host: this.config.poolConfig.host,
      password: this.config.poolConfig.password,
      database: this.config.poolConfig.database,
    });
    this.pool = new Pool(this.config.poolConfig);
  }

  async init(): Promise<void> {
    await this.loadOperonDatabase()
    await this.listenForNotifications()
  }

  // Operon database management
  async loadOperonDatabase() {
    await this.pgSystemClient.connect();
    // Check whether the Operon system database exists, create it if needed
    const dbExists = await this.pgSystemClient.query(`SELECT FROM pg_database WHERE datname = '${this.config.poolConfig.database}'`);
    if (dbExists.rows.length === 0) {
      const createDbStatement = `CREATE DATABASE ${this.config.poolConfig.database}`;
      await this.pgSystemClient.query(createDbStatement);
    }
    // We could end the client at destroy(), but given we are only using it here, do it now.
    await this.pgSystemClient.end();
    // Load the Operon system schema
    await this.pool.query(this.config.operonDbSchema);
  }

  async destroy() {
    await this.notificationsClient.removeAllListeners().end();
    await this.pool.end();
  }

  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  async listenForNotifications() {
    await this.notificationsClient.connect();
    await this.notificationsClient.query('LISTEN operon__notificationschannel;');
    const handler = (msg: Notification ) => {
      if (msg.payload && msg.payload in this.listenerMap) {
        this.listenerMap[msg.payload]();
      }
    };
    this.notificationsClient.on('notification', handler);
  }

  /* Operon Workflows */

  readonly workflowConfigMap: WeakMap<OperonWorkflow<any, any>, WorkflowConfig> = new WeakMap();

  readonly transactionConfigMap: WeakMap<OperonTransaction<any, any>, TransactionConfig> = new WeakMap();

  readonly communicatorConfigMap: WeakMap<OperonCommunicator<any, any>, CommunicatorConfig> = new WeakMap();

  #generateUUID(): string {
    return uuidv4();
  }

  registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, config: WorkflowConfig={}) {
    this.workflowConfigMap.set(wf, config);
  }

  registerTransaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: TransactionConfig={}) {
    this.transactionConfigMap.set(txn, params);
  }

  registerCommunicator<T extends any[], R>(comm: OperonCommunicator<T, R>, params: CommunicatorConfig={}) {
    this.communicatorConfigMap.set(comm, params);
  }

  async workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T) {
    const wConfig = this.workflowConfigMap.get(wf);
    if (wConfig === undefined) {
      throw new OperonError(`Unregistered Workflow ${wf.name}`);
    }

    // Check if the user has permission to run this workflow.
    if (!params.runAs) {
      params.runAs = "defaultRole";
    }
    const userHasPermission = this.hasPermission(params.runAs, wConfig);
    if (!userHasPermission) {
      throw new OperonWorkflowPermissionDeniedError(params.runAs, wf.name);
    }

    // TODO: need to optimize this extra transaction per workflow.
    const recordExecution = async (input: T) => {
      const initFuncID = wCtxt.functionIDGetIncrement();
      const client = await this.pool.connect();
      await client.query("BEGIN;");
      const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [workflowUUID, initFuncID]);
  
      let retInput: T;
      if (rows.length === 0) {
        // This workflow has never executed before, so record the input
        await client.query("INSERT INTO operon__FunctionOutputs (workflow_id, function_id, output) VALUES ($1, $2, $3)",
          [workflowUUID, initFuncID, JSON.stringify(input)]);
        retInput = input;
      } else {
        // Return the old recorded input
        retInput = JSON.parse(rows[0].output) as T;
      }
  
      await client.query("COMMIT");
      client.release();
  
      return retInput;
    }

    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();
    const wCtxt: WorkflowContext = new WorkflowContext(this, workflowUUID, wConfig);

    const input = await recordExecution(args);
    const result: R = await wf(wCtxt, ...input);
    return result;
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const wf = async (ctxt: WorkflowContext, ...args: T) => {
      return await ctxt.transaction(txn, ...args);
    };
    this.registerWorkflow(wf);
    return await this.workflow(wf, params, ...args);
  }

  async send<T extends NonNullable<any>>(params: WorkflowParams, key: string, message: T) : Promise<boolean> {
    // Create a workflow and call send.
    const wf = async (ctxt: WorkflowContext, key: string, message: T) => {
      return await ctxt.send<T>(key, message);
    };
    this.registerWorkflow(wf);
    return await this.workflow(wf, params, key, message);
  }

  async recv<T extends NonNullable<any>>(params: WorkflowParams, key: string, timeoutSeconds: number) : Promise<T | null> {
    // Create a workflow and call recv.
    const wf = async (ctxt: WorkflowContext, key: string, timeoutSeconds: number) => {
      return await ctxt.recv<T>(key, timeoutSeconds);
    };
    this.registerWorkflow(wf);
    return await this.workflow(wf, params, key, timeoutSeconds);
  }

  /* Operon Permissions */

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
