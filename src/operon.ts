/* eslint-disable @typescript-eslint/no-explicit-any */
import { Pool, PoolConfig } from 'pg';
import { OperonWorkflow, WorkflowContext, WorkflowParams } from './workflow';
import { v1 as uuidv1 } from 'uuid';

export interface operon__FunctionOutputs {
    workflow_id: string;
    function_id: number;
    output: string;
}

export interface operon__Notifications {
  key: string;
  message: string;
}

export class Operon {
  pool: Pool;
  constructor(config: PoolConfig) {
    this.pool = new Pool(config);
  }

  async initializeOperonTables() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS operon__FunctionOutputs (
      workflow_id VARCHAR(64) NOT NULL,
      function_id INT NOT NULL,
      output TEXT NOT NULL,
      PRIMARY KEY (workflow_id, function_id)
      );`
    );
    await this.pool.query(`CREATE TABLE operon__Notifications (
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

        CREATE TRIGGER operon__NotificationsTrigger
        AFTER INSERT ON operon__Notifications
        FOR EACH ROW EXECUTE FUNCTION operon__NotificationsFunction();
    `);
  }

  async resetOperonTables() {
    await this.pool.query(`DROP TABLE IF EXISTS operon__FunctionOutputs;`);
    await this.pool.query(`DROP TABLE IF EXISTS operon__Notifications`)
    await this.initializeOperonTables();
  }

  #generateIdempotencyKey(): string {
    return uuidv1();
  }
  

  async workflow<T extends any[], R>(wf: OperonWorkflow<T, R>, params: WorkflowParams, ...args: T) {
    // TODO: need to optimize this extra transaction per workflow.
    const recordExecution = async (input: T) => {
      const workflowFuncId = wCtxt.functionIDGetIncrement();
      const client = await this.pool.connect();
      await client.query("BEGIN;");
      const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [workflowID, workflowFuncId]);
  
      let retInput: T;
      if (rows.length === 0) {
        // This workflow has never executed before, so record the input
        await client.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)",
          [workflowID, workflowFuncId, JSON.stringify(input)]);
        retInput = input;
      } else {
        // Return the old recorded input
        retInput = JSON.parse(rows[0].output) as T;
      }
  
      await client.query("COMMIT");
      client.release();
  
      return retInput;
    }
  
    const workflowID: string = params.idempotencyKey ? params.idempotencyKey : this.#generateIdempotencyKey();
    const wCtxt: WorkflowContext = new WorkflowContext(this.pool, workflowID);
    const input = await recordExecution(args);
    const result: R = await wf(wCtxt, ...input);
    return result;
  }

  async send<T extends NonNullable<any>>(params: WorkflowParams, key: string, message: T) : Promise<boolean> {
    // Create a simple workflow and call its send.
    const wf = async (ctxt: WorkflowContext, key: string, message: T) => {
      return await ctxt.send<T>(key, message);
    };
    return await this.workflow(wf, params, key, message);
  }

  async recv<T extends NonNullable<any>>(params: WorkflowParams, key: string, timeoutSeconds: number) : Promise<T | null> {
    // Create a simple workflow and call its recv.
    const wf = async (ctxt: WorkflowContext, key: string, timeoutSeconds: number) => {
      return await ctxt.recv<T>(key, timeoutSeconds);
    };
    return await this.workflow(wf, params, key, timeoutSeconds);
  }
}