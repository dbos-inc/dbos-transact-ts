/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon } from './operon';
import { Pool } from 'pg';
import { operon__IdempotencyKeys } from './operon';

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
    const workflowID: string = params.idempotencyKey ? "o" + params.idempotencyKey : await generateIdempotencyKey(ctxt);
    const wCtxt: WorkflowContext = new WorkflowContext(ctxt.pool, workflowID);
    const result: R = await wf(wCtxt, ...args);
    return result;
  };
}