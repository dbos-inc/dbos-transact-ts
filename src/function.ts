/* eslint-disable @typescript-eslint/no-explicit-any */
import { WorkflowContext } from "./workflow";
import { PoolClient } from 'pg';
import { operon__FunctionOutputs } from "./operon";

export class FunctionContext {
  client: PoolClient;

  #functionAborted: boolean = false;
  readonly functionID: number;

  constructor(client: PoolClient, functionID: number) {
    this.client = client;
    this.functionID = functionID;
  }

  async rollback() {
    await this.client.query("ROLLBACK");
    this.#functionAborted = true;
    this.client.release();
  }

  isAborted() : boolean {
    return this.#functionAborted;
  }


}

export type OperonFunction<T extends any[], R> = (ctxt: FunctionContext, ...args: T) => Promise<R>;
export type RegisteredFunction<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export function registerFunction<T extends any[], R>(fn: OperonFunction<T, R>): RegisteredFunction<T, R> {
  return async function (ctxt: WorkflowContext, ...args: T): Promise<R> {
    let client: PoolClient = await ctxt.pool.connect();
    const fCtxt: FunctionContext = new FunctionContext(client, ctxt.functionIDGetIncrement());

    async function checkExecution() : Promise<R | null> {
      const { rows } = await client.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [ctxt.workflowID, fCtxt.functionID]);
      if (rows.length === 0) {
        return null;
      } else {
        return JSON.parse(rows[0].output) as R;
      }
    }
    
    async function recordExecution(output: R) {
      await client.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)", 
        [ctxt.workflowID, fCtxt.functionID, JSON.stringify(output)]);
    }

    await client.query("BEGIN");

    const check: R | null = await checkExecution();

    if (check !== null) {
      await client.query("ROLLBACK");
      client.release();
      return check; 
    }

    const result: R = await fn(fCtxt, ...args);

    if(fCtxt.isAborted()) {
      client = await ctxt.pool.connect();
    }
    await recordExecution(result);
    await client.query("COMMIT");
    client.release();
    return result;
  };
}