/* eslint-disable @typescript-eslint/no-explicit-any */
import { OperonConfig } from './operon.config';
import { OperonError, WorkflowOperonPermissionDeniedError } from './error';
import { OperonWorkflow, WorkflowConfig, WorkflowContext, WorkflowParams } from './workflow';
import { OperonTransaction, TransactionConfig } from './transaction';
import { CommunicatorConfig, OperonCommunicator } from './communicator';
import { User } from './users';

import { Pool, PoolClient, Notification, QueryArrayResult, QueryResultRow } from 'pg';
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

export interface operon__Roles {
  [key: string]: boolean;
}

export class Operon {
  config: OperonConfig;
  readonly pool: Pool;
  readonly notificationsClient: Promise<PoolClient>;
  readonly listenerMap: Record<string, () => void> = {};
  readonly roles: operon__Roles = {}; // Convenience map for O(1) lookups

  constructor() {
    this.config = new OperonConfig();
    this.pool = new Pool(this.config.poolConfig);
    for (const role of this.config.operonRoles) {
      this.roles[role] = true;
    }

    this.notificationsClient = this.pool.connect();
    void this.listenForNotifications();
  }

  async destroy() {
    (await this.notificationsClient).removeAllListeners().release();
    await this.pool.end();
  }

  readonly workflowConfigMap: WeakMap<OperonWorkflow<any, any>, WorkflowConfig> = new WeakMap();

  readonly transactionConfigMap: WeakMap<OperonTransaction<any, any>, TransactionConfig> = new WeakMap();

  readonly communicatorConfigMap: WeakMap<OperonCommunicator<any, any>, CommunicatorConfig> = new WeakMap();

  async initializeOperonTables() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__FunctionOutputs (
      workflow_id VARCHAR(64) NOT NULL,
      function_id INT NOT NULL,
      output TEXT,
      error TEXT,
      PRIMARY KEY (workflow_id, function_id)
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
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__Workflows (
      id VARCHAR(64) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`
    );
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__WorkflowPermissions (
        workflow_id VARCHAR(64) NOT NULL,
        role VARCHAR(255) NOT NULL
      );`
    );
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__Users (
        id VARCHAR(64) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        role VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`
    );
  }

  async resetOperonTables() {
    await this.pool.query(`DROP TABLE IF EXISTS operon__FunctionOutputs;`);
    await this.pool.query(`DROP TABLE IF EXISTS operon__Notifications`)
    await this.pool.query(`DROP TABLE IF EXISTS operon__Workflows;`);
    await this.pool.query(`DROP TABLE IF EXISTS operon__WorkflowPermissions;`);
    await this.pool.query(`DROP TABLE IF EXISTS operon__Users;`);
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

  async registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>, config: WorkflowConfig={}) {
    if (!config.id) {
      config.id = this.#generateUUID();
    }

    if (!config.name) {
      // Retrieve the underlying function object `name` property
      config.name = wf.name;
    }

    // Keep track of registered workflows in memory
    this.workflowConfigMap.set(wf, config);

    // Register the workflow and its permitted roles in the database
    const client = await this.pool.connect();
    await client.query("BEGIN;");

    await this.pool.query(
      "INSERT INTO operon__Workflows (id, name) VALUES ($1, $2)",
      [config.id, config.name]
    );

    if (config.rolesThatCanRun) {
      for (const role of config.rolesThatCanRun) {
        if (!this.roles[role]) {
          throw new OperonError(`Role ${role} does not exist`);
        }
        await this.pool.query(
          "INSERT INTO operon__WorkflowPermissions (workflow_id, role) VALUES ($1, $2)",
          [config.id, role]
        );
      }
    }

    await client.query("COMMIT;");
    client.release();
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

    // This checks if the user has permission in the DB.
    if (!params.runAs) {
      params.runAs = {
        name: "defaultUser",
        role: "defaultRole"
      }
    }
    const userHasPermission = await this.hasPermission(params.runAs, wConfig);
    if (!userHasPermission) {
      throw new WorkflowOperonPermissionDeniedError(params.runAs.name, wConfig);
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
    await this.registerWorkflow(wf);
    return await this.workflow(wf, params, ...args);
  }

  async send<T extends NonNullable<any>>(params: WorkflowParams, key: string, message: T) : Promise<boolean> {
    // Create a workflow and call send.
    const wf = async (ctxt: WorkflowContext, key: string, message: T) => {
      return await ctxt.send<T>(key, message);
    };
    await this.registerWorkflow(wf);
    return await this.workflow(wf, params, key, message);
  }

  async recv<T extends NonNullable<any>>(params: WorkflowParams, key: string, timeoutSeconds: number) : Promise<T | null> {
    // Create a workflow and call recv.
    const wf = async (ctxt: WorkflowContext, key: string, timeoutSeconds: number) => {
      return await ctxt.recv<T>(key, timeoutSeconds);
    };
    await this.registerWorkflow(wf);
    return await this.workflow(wf, params, key, timeoutSeconds);
  }

  // Users and roles management
  async registerUser(user: User): Promise<void> {
    const client = await this.pool.connect();
    user.id = this.#generateUUID();
    await this.pool.query(
      "INSERT INTO operon__Users (id, name, role) VALUES ($1, $2, $3)",
      [user.id, user.name, user.role]
    );
    client.release();
  }

  // Permissions management
  async hasPermission(user: User, workflowConfig: WorkflowConfig): Promise<boolean> {
    const client = await this.pool.connect();
    // First retrieve all the roles allowed to run the workflow
    const results: QueryArrayResult = await this.pool.query(
      "SELECT * from operon__WorkflowPermissions WHERE workflow_id=$1",
      [workflowConfig.id]
    );
    client.release();
    // If results is empty the workflow is permisionless and anyone can run it
    if (results.rows.length === 0) {
      return true;
    } else if (results.rows.length > 0) {
      // Check if the user's role is in the list
      for (const row of results.rows) {
        if ((row as QueryResultRow).role === user.role) {
          return true;
        }
      }
    }
    return false;
  }
}
