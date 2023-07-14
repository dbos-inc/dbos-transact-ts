/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon } from './operon';
import { Pool } from 'pg';

export interface WorkflowParams {
  idempotencyKey?: string;
}

export class WorkflowContext {
  pool: Pool;

  readonly workflowID: string;
  #functionID: number = 0;

  constructor(pool: Pool, params: WorkflowParams) {
    this.pool = pool;
    this.workflowID = params.idempotencyKey ?? "TODO";
  }

  functionIDGetIncrement() : number {
    return this.#functionID++;
  }
}

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => R;
export type RegisteredWorkflow<T extends any[], R> = (ctxt: Operon, params: WorkflowParams, ...args: T) => R;

export function registerWorkflow<T extends any[], R>(fn: OperonWorkflow<T, R>): RegisteredWorkflow<T, R> {
  return function (ctxt: Operon, params: WorkflowParams, ...args: T): R {
    const wCtxt: WorkflowContext = new WorkflowContext(ctxt.pool, params);
    const result: R = fn(wCtxt, ...args);
    return result;
  };
}