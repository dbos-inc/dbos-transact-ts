/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon } from './operon';

export interface WorkflowContext {
  helloWorkflow: () => void;
}

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => R;
export type RegisteredWorkflow<T extends any[], R> = (ctxt: Operon, ...args: T) => R;

export function registerWorkflow<T extends any[], R>(fn: OperonWorkflow<T, R>): RegisteredWorkflow<T, R> {
  return function (ctxt: Operon, ...args: T): R {
    const wCtxt: WorkflowContext = {
      helloWorkflow: ctxt.helloWorld
    }
    const result: R = fn(wCtxt, ...args);
    return result;
  };
}