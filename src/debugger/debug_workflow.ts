/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { DBOSExecutor, DBOSNull, dbosNull } from "../dbos-executor";
import { transaction_outputs } from "../../schemas/user_db_schema";
import { IsolationLevel, Transaction, TransactionContext, TransactionContextImpl } from "../transaction";
import { Communicator, CommunicatorContext, CommunicatorContextImpl } from "../communicator";
import { DBOSError, DBOSNotRegisteredError, DBOSWorkflowConflictUUIDError } from "../error";
import { serializeError, deserializeError } from "serialize-error";
import { SystemDatabase } from "../system_database";
import { UserDatabaseClient } from "../user_database";
import { SpanStatusCode } from "@opentelemetry/api";
import { Span } from "@opentelemetry/sdk-trace-base";
import { HTTPRequest, DBOSContext, DBOSContextImpl } from '../context';
import { getRegisteredOperations } from "../decorators";
import { WFInvokeFuncs, Workflow, WorkflowConfig, WorkflowContext, WorkflowHandle } from "../workflow";

/**
 * Context used for debugging a workflow
 */
export class WorkflowContextDebug extends DBOSContextImpl implements WorkflowContext {
  functionID: number = 0;
  readonly #wfe;
  readonly isTempWorkflow: boolean;

  constructor(wfe: DBOSExecutor, parentCtx: DBOSContextImpl | undefined, workflowUUID: string, readonly workflowConfig: WorkflowConfig, workflowName: string) {
    const span = wfe.tracer.startSpan(workflowName, { workflowUUID: workflowUUID }, parentCtx?.span);
    super(workflowName, span, wfe.logger, parentCtx);
    this.workflowUUID = workflowUUID;
    this.#wfe = wfe;
    this.isTempWorkflow = wfe.tempWorkflowName === workflowName;
    if (wfe.config.application) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      this.applicationConfig = wfe.config.application;
    }
  }

  functionIDGetIncrement(): number {
    return this.functionID++;
  }

  invoke<T extends object>(object: T): WFInvokeFuncs<T> {
    const ops = getRegisteredOperations(object);

    const proxy: any = {};
    for (const op of ops) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      proxy[op.name] = op.txnConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.transaction(op.registeredFunction as Transaction<any[], any>, ...args)
        : op.commConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => this.external(op.registeredFunction as Communicator<any[], any>, ...args)
        : undefined;
    }
    return proxy as WFInvokeFuncs<T>;
  }

  async transaction<T extends any[], R>(txn: Transaction<T, R>, ...args: T): Promise<R> {
    throw new Error("Method not implemented");
  }

  external<T extends any[], R>(commFn: Communicator<T, R>, ...args: T): Promise<R> {
    throw new Error("Method not implemented");
  }

  childWorkflow<T extends any[], R>(wf: Workflow<T, R>, ...args: T): Promise<WorkflowHandle<R>> {
    const funcId = this.functionIDGetIncrement();
    const childUUID: string = this.workflowUUID + "-" + funcId;
    return this.#wfe.debugWorkflow(wf, { parentCtx: this, workflowUUID: childUUID }, this.workflowUUID, funcId, ...args);
  }

  send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string | undefined): Promise<void> {
    throw new Error("Method not implemented.");
  }
  recv<T extends NonNullable<any>>(topic?: string | undefined, timeoutSeconds?: number | undefined): Promise<T | null> {
    throw new Error("Method not implemented.");
  }
  setEvent<T extends NonNullable<any>>(key: string, value: T): Promise<void> {
    throw new Error("Method not implemented.");
  }
  getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds?: number | undefined): Promise<T | null> {
    throw new Error("Method not implemented.");
  }
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    throw new Error("Method not implemented.");
  }
}