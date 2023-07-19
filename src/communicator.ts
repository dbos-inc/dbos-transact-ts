/* eslint-disable @typescript-eslint/no-explicit-any */
import { WorkflowContext } from "./workflow";
import { operon__FunctionOutputs } from "./operon";

// Intercepting requests.
export class CommunicatorContext {

  // TODO: decide what goes into this context.

  readonly functionID: number;

  constructor(functionID: number) {
    this.functionID = functionID;
  }

}

export type OperonCommunicator<T extends any[], R> = (ctxt: CommunicatorContext, ...args: T) => Promise<R>;
export type RegisteredCommunicator<T extends any[], R> = (ctxt: WorkflowContext, ...args: T) => Promise<R>;

export function registerCommunicator<T extends any[], R>(fn: OperonCommunicator<T, R>): RegisteredCommunicator<T, R> {
  return async function (ctxt: WorkflowContext, ...args: T): Promise<R> {
    const commCtxt: CommunicatorContext = new CommunicatorContext(ctxt.functionIDGetIncrement());

    async function checkExecution() : Promise<R | null> {
      const { rows } = await ctxt.pool.query<operon__FunctionOutputs>("SELECT output FROM operon__FunctionOutputs WHERE workflow_id=$1 AND function_id=$2",
        [ctxt.workflowID, commCtxt.functionID]);
      if (rows.length === 0) {
        return null;
      } else {
        return JSON.parse(rows[0].output) as R;
      }
    }

    async function recordExecution(output: R) {
      await ctxt.pool.query("INSERT INTO operon__FunctionOutputs VALUES ($1, $2, $3)", 
        [ctxt.workflowID, commCtxt.functionID, JSON.stringify(output)]);
    }

    // Check if this execution previously happened, returning its original result if it did.
    const check: R | null = await checkExecution();
    if (check !== null) {
      return check; 
    }

    // Execute the communicator function.
    const result: R = await fn(commCtxt, ...args);

    // Record the execution and return.
    await recordExecution(result);
    return result;
  };
}