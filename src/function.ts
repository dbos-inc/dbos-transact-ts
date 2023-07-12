/* eslint-disable @typescript-eslint/no-explicit-any */
import { WorkflowContext } from "./workflow";

export interface FunctionContext {
  helloFunction: () => void;
}

export type OperonFunction<T extends any[], R> = (ctxt: FunctionContext, ...args: T) => R;
export type RegisteredFunction<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => R;

export function registerFunction<T extends any[], R>(fn: OperonFunction<T, R>): RegisteredFunction<T, R> {
  return function (ctxt: WorkflowContext, ...args: T): R {
    const fCtxt: FunctionContext = {
      helloFunction: ctxt.helloWorkflow
    }
    const result: R = fn(fCtxt, ...args);
    return result;
  };
}