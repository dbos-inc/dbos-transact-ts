/* eslint-disable @typescript-eslint/no-explicit-any */
import { OperonConfig } from './operon.config';
import { OperonError, OperonWorkflowPermissionDeniedError } from './error';
import { OperonWorkflow, WorkflowConfig, WorkflowContext, WorkflowParams } from './workflow';
import { OperonTransaction, TransactionConfig, validateTransactionConfig } from './transaction';
import { CommunicatorConfig, OperonCommunicator } from './communicator';

import { Pool, PoolClient, Notification } from 'pg';
import { v4 as uuidv4 } from 'uuid';

export interface operon__FunctionOutputs {
    workflow_id: string;
    function_id: number;
    output: string;
    error: string;
}

export interface operon__WorkflowOutputs {
  workflow_id: string;
  output: string;
}

export interface operon__Notifications {
  key: string;
  message: string;
}

export interface OperonNull {}
export const operonNull: OperonNull = {};

export class Operon {
  config: OperonConfig;
  readonly pool: Pool;

  readonly notificationsClient: Promise<PoolClient>;
  readonly listenerMap: Record<string, () => void> = {};

  readonly workflowOutputBuffer: Map<string, string> = new Map();
  readonly flushBufferIntervalMs: number = 1000;
  readonly flushBufferID: NodeJS.Timeout;

  readonly workflowConfigMap: WeakMap<OperonWorkflow<any, any>, WorkflowConfig> = new WeakMap();
  readonly transactionConfigMap: WeakMap<OperonTransaction<any, any>, TransactionConfig> = new WeakMap();
  readonly communicatorConfigMap: WeakMap<OperonCommunicator<any, any>, CommunicatorConfig> = new WeakMap();

  constructor() {
    this.config = new OperonConfig();
    this.pool = new Pool(this.config.poolConfig);
    this.notificationsClient = this.pool.connect();
    void this.listenForNotifications();
    this.flushBufferID = setInterval(() => {
      void this.flushWorkflowOutputBuffer();
    }, this.flushBufferIntervalMs)
  }

  async destroy() {
    clearInterval(this.flushBufferID);
    await this.flushWorkflowOutputBuffer();
    (await this.notificationsClient).removeAllListeners().release();
    await this.pool.end();
  }

  async initializeOperonTables() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__FunctionOutputs (
      workflow_id VARCHAR(64) NOT NULL,
      function_id INT NOT NULL,
      output TEXT,
      error TEXT,
      PRIMARY KEY (workflow_id, function_id)
      );`
    );
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__WorkflowOutputs (
      workflow_id VARCHAR(64) PRIMARY KEY,
      output TEXT
      );`
    );
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__Notifications (
      key VARCHAR(255) PRIMARY KEY,
      message TEXT NOT NULL
    );`);
    // Weird node-postgres issue -- channel names must be all-lowercase.
    await this.pool.query(`
        CREATE OR REPLACE FUNCTION operon__NotificationsFunction() RETURNS TRIGGER AS $$
        DECLARE
        BEGIN
            -- Publish a notification for all keys
            PERFORM pg_notify('operon__notificationschannel', NEW.key::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        DO
        $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'operon__notificationstrigger') THEN
              EXECUTE '
                  CREATE TRIGGER operon__notificationstrigger
                  AFTER INSERT ON operon__Notifications
                  FOR EACH ROW EXECUTE FUNCTION operon__NotificationsFunction()';
            END IF;
        END
        $$;
    `);
  }

  async resetOperonTables() {
    await this.pool.query(`DROP TABLE IF EXISTS operon__FunctionOutputs;`);
    await this.pool.query(`DROP TABLE IF EXISTS operon__Notifications`)
    await this.pool.query(`DROP TABLE IF EXISTS operon__WorkflowOutputs`)
    await this.initializeOperonTables();
  }

  #generateUUID(): string {
    return uuidv4();
  }

  /**
   * A background process that listens for notifications from Postgres then signals the appropriate
   * workflow listener by resolving its promise.
   */
  async listenForNotifications() {
    const client = await this.notificationsClient;
    await client.query('LISTEN operon__notificationschannel;');
    const handler = (msg: Notification ) => {
      if (msg.payload && msg.payload in this.listenerMap) {
        this.listenerMap[msg.payload]();
      }
    };
    client.on('notification', handler);
  }

  async flushWorkflowOutputBuffer() {
    const localBuffer = new Map(this.workflowOutputBuffer);
    this.workflowOutputBuffer.clear();
    const client: PoolClient = await this.pool.connect();
    await client.query("BEGIN");
    for (const [workflowUUID, output] of localBuffer) {
      await client.query("INSERT INTO operon__WorkflowOutputs VALUES($1, $2) ON CONFLICT DO NOTHING", [workflowUUID, output]);
    }
    await client.query("COMMIT");
    client.release();
  }

  registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, config: WorkflowConfig={}) {
    this.workflowConfigMap.set(wf, config);
  }

  registerTransaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: TransactionConfig={}) {
    validateTransactionConfig(params);
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
    const workflowUUID: string = params.workflowUUID ? params.workflowUUID : this.#generateUUID();
    const wCtxt: WorkflowContext = new WorkflowContext(this, workflowUUID, wConfig);
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
      const { rows } = await this.pool.query<operon__WorkflowOutputs>("SELECT output FROM operon__WorkflowOutputs WHERE workflow_id=$1",
        [workflowUUID]);
      if (rows.length === 0) {
        return operonNull;
      } else {
        return JSON.parse(rows[0].output) as R;  // Could be null.
      }
    }

    const recordWorkflowOutput = (output: R) => {
      this.workflowOutputBuffer.set(workflowUUID, JSON.stringify(output));
    }

    const checkWorkflowInput = async (input: T) => {
      const { rows } = await this.pool.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [workflowUUID, workflowInputID]);
      if (rows.length === 0) {
        // This workflow has never executed before, so record the input.
        wCtxt.resultBuffer.set(workflowInputID, JSON.stringify(input));
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
    const result = await wf(wCtxt, ...input);
    recordWorkflowOutput(result);
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

  // Permissions management
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
