import { Operon } from './operon';

export interface WorkflowContext {
    helloWorkflow: () => void;
  }

export type OperonWorkflow<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => R;
export type RegisteredWorkflow<T extends any[], R> = (ctxt: Operon, ...args: T) => R;

export function registerWorkflow<T extends any[], R>(fn: OperonWorkflow<T, R>): RegisteredWorkflow<T, R> {
    return function (ctxt: Operon, ...args: T): R {
      // Here you can add logic before the function call
      console.log("Before function call");

      const bob: WorkflowContext = {
        helloWorkflow: ctxt.helloWorld
      }
  
      // Call the provided function with the provided arguments
      const result: R = fn(bob, ...args);
  
      // Add logic after the function call
      console.log("After function call");
  
      // Return the result of the function call
      return result;
    };
  }