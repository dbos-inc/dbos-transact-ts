import { WorkflowContext } from "./workflow";

export interface FunctionContext {
    helloFunction: () => void;
  }

export type OperonFunction<T extends any[], R> = (ctxt: FunctionContext, ...args: T) => R;
export type RegisteredFunction<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => R;


export function registerFunction<T extends any[], R>(fn: OperonFunction<T, R>): RegisteredFunction<T, R> {
    return function (ctxt: WorkflowContext, ...args: T): R {
      // Here you can add logic before the function call
      console.log("Before function call");

      const fCtxt: FunctionContext = {
        helloFunction: ctxt.helloWorkflow
      }
  
      // Call the provided function with the provided arguments
      const result: R = fn(fCtxt, ...args);
  
      // Add logic after the function call
      console.log("After function call");
  
      // Return the result of the function call
      return result;
    };
}