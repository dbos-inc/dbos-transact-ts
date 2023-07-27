/* eslint-disable @typescript-eslint/no-explicit-any */
import { OperonConfig } from './operon.config';
import { Pool } from 'pg';
import { OperonWorkflow, WorkflowContext, WorkflowParams } from './workflow';
import { v1 as uuidv1 } from 'uuid';
import { OperonTransaction } from './transaction';

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
  readonly pool: Pool;
  config: OperonConfig;
  readonly notificationsClient: Promise<PoolClient>;
  readonly listenerMap: Record<string, () => void> = {};

  constructor() {
    this.config = new OperonConfig();
    this.pool = new Pool(this.config.poolConfig);
    this.notificationsClient = this.pool.connect();
    void this.listenForNotifications();
  }

  async destroy() {
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
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__Notifications (
      key VARCHAR(255) PRIMARY KEY,
      message TEXT NOT NULL
    );`)
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
    await this.initializeOperonTables();
  }

  #generateUUID(): string {
    return uuidv1();
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
  

  async workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T) {
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
    const wCtxt: WorkflowContext = new WorkflowContext(this.pool, this.listenerMap, workflowUUID);
    const input = await recordExecution(args);
    const result: R = await wf(wCtxt, ...input);
    return result;
  }

  async transaction<T extends any[], R>(txn: OperonTransaction<T, R>, params: WorkflowParams, ...args: T): Promise<R> {
    // Create a workflow and call transaction.
    const wf = async (ctxt: WorkflowContext, ...args: T) => {
      return await ctxt.transaction(txn, ...args);
    };
    return await this.workflow(wf, params, ...args);
  }

  async send<T extends NonNullable<any>>(params: WorkflowParams, key: string, message: T) : Promise<boolean> {
    // Create a workflow and call send.
    const wf = async (ctxt: WorkflowContext, key: string, message: T) => {
      return await ctxt.send<T>(key, message);
    };
    return await this.workflow(wf, params, key, message);
  }

  async recv<T extends NonNullable<any>>(params: WorkflowParams, key: string, timeoutSeconds: number) : Promise<T | null> {
    // Create a workflow and call recv.
    const wf = async (ctxt: WorkflowContext, key: string, timeoutSeconds: number) => {
      return await ctxt.recv<T>(key, timeoutSeconds);
    };
    return await this.workflow(wf, params, key, timeoutSeconds);
  }
}
