/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon, operon__FunctionOutputs, operon__IdempotencyKeys } from './operon';
import { Pool } from 'pg';

export interface WorkflowParams {
  idempotencyKey?: string;
}

export class WorkflowContext {
  pool: Pool;

  readonly workflowID: string;
  #functionID: number = 0;

  constructor(pool: Pool, workflowID: string) {
    this.pool = pool;
    this.workflowID = workflowID;
  }

  functionIDGetIncrement() : number {
    return this.#functionID++;
  }
}

async function generateIdempotencyKey(ctxt: Operon): Promise<string> {
  const { rows } = await ctxt.pool.query<operon__IdempotencyKeys>("SELECT nextval('operon__IdempotencyKeys') as idempotency_key;");
  return String(rows[0].idempotency_key);
}

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;
export type RegisteredWorkflow<T extends any[], R> = (ctxt: Operon, params: WorkflowParams, ...args: T) => Promise<R>;

export function registerWorkflow<T extends any[], R>(wf: OperonWorkflow<T, R>): RegisteredWorkflow<T, R> {
  return async function (ctxt: Operon, params: WorkflowParams = {}, ...args: T): Promise<R> {
    // TODO: need to optimize this extra transaction per workflow.
    async function recordExecution(input: T): Promise<T> {
      const workflowFuncId = wCtxt.functionIDGetIncrement();
      const client = await ctxt.pool.connect();
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

    const workflowID: string = params.idempotencyKey ? "o" + params.idempotencyKey : await generateIdempotencyKey(ctxt);
    const wCtxt: WorkflowContext = new WorkflowContext(ctxt.pool, workflowID);
    const input = await recordExecution(args);
    const result: R = await wf(wCtxt, ...input);
    return result;
  };
}