/* eslint-disable @typescript-eslint/no-explicit-any */
import { WorkflowContext } from "./workflow";
import { PoolClient } from 'pg';

export class FunctionContext {
  client: PoolClient;

  #functionAborted: boolean = false;

  constructor(client: PoolClient) {
    this.client = client;
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
    const client: PoolClient = await ctxt.pool.connect();
    const fCtxt: FunctionContext = new FunctionContext(client);

    await client.query("BEGIN");
    const result: R = await fn(fCtxt, ...args);

    if(!fCtxt.isAborted) {
      await client.query("COMMIT");
      client.release();
    }
    
    return result;
  };
}