/* eslint-disable @typescript-eslint/no-explicit-any */
import { IncomingMessage } from "http";
import { OperonCommunicator } from "../communicator";
import { HTTPRequest, OperonContextImpl } from "../context";
import { getRegisteredOperations } from "../decorators";
import { OperonError } from "../error";
import { HandlerWfFuncs } from "../httpServer/handler";
import { OperonHttpServer } from "../httpServer/server";
import { Operon, OperonConfig } from "../operon";
import { parseConfigFile } from "../operon-runtime/config";
import { OperonTransaction } from "../transaction";
import { OperonWorkflow, WFInvokeFuncs, WorkflowHandle, WorkflowParams } from "../workflow";
import { Http2ServerRequest, Http2ServerResponse } from "http2";
import { ServerResponse } from "http";
import { SystemDatabase } from "../system_database";

export async function createTestingRuntime(userClasses: object[], testConfig?: OperonConfig, logLevel: string="info"): Promise<OperonTestingRuntime> {
  const otr = new OperonTestingRuntimeImpl();
  await otr.init(userClasses, testConfig, undefined, logLevel);
  return otr;
}

export interface OperonInvokeParams {
  authenticatedUser?: string;
  authenticatedRoles?: string[];
  request?: HTTPRequest;
}

export interface OperonTestingRuntime {
  send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic: string, idempotencyKey?: string): Promise<void>;
  getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
  invoke<T extends object>(object: T, workflowUUID?: string, params?: OperonInvokeParams): WFInvokeFuncs<T> & HandlerWfFuncs<T>;
  getHandlersCallback(): (req: IncomingMessage | Http2ServerRequest, res: ServerResponse | Http2ServerResponse) => Promise<void>;
  destroy(): Promise<void>; // Release resources after tests.

  // User database operations.
  queryUserDB<R>(sql: string, ...params: any[]): Promise<R[]>; // Execute a raw SQL query on the user database.
  createUserSchema(): Promise<void>; // Only valid if using TypeORM. Create tables based on the provided schema.
  dropUserSchema(): Promise<void>; // Only valid if using TypeORM. Drop all tables created by createUserSchema().
}

/**
 * This class provides a runtime to test Opeorn functions in unit tests.
 */
export class OperonTestingRuntimeImpl implements OperonTestingRuntime {
  #server: OperonHttpServer | null = null;
 
  /**
   * Initialize the testing runtime by loading user functions specified in classes and using the specified config.
   * This should be the first function call before any subsequent calls.
   */
  async init(userClasses: object[], testConfig?: OperonConfig, systemDB?: SystemDatabase, logLevel?: string) {
    const operonConfig = testConfig ? [testConfig] : parseConfigFile({loglevel: logLevel});
    const operon = new Operon(operonConfig[0], systemDB);
    await operon.init(...userClasses);
    this.#server = new OperonHttpServer(operon);
  }

  /**
   * Release resources after tests.
   */
  async destroy() {
    await this.#server?.operon.destroy();
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to invoke workflows, transactions, and communicators;
   */
  invoke<T extends object>(object: T, workflowUUID?: string, params?: OperonInvokeParams): WFInvokeFuncs<T> & HandlerWfFuncs<T> {
    const operon = this.getOperon();
    const ops = getRegisteredOperations(object);

    const proxy: any = {};

    // Creates an Operon context to pass in necessary info.
    const span = operon.tracer.startSpan("test");
    const oc = new OperonContextImpl("test", span, operon.logger);
    oc.authenticatedUser = params?.authenticatedUser ?? "";
    oc.request = params?.request;
    oc.authenticatedRoles = params?.authenticatedRoles ?? [];

    const wfParams: WorkflowParams = { workflowUUID: workflowUUID, parentCtx: oc };
    for (const op of ops) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      proxy[op.name] = op.txnConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => operon.transaction(op.registeredFunction as OperonTransaction<any[], any>, wfParams, ...args)
        : op.workflowConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => operon.workflow(op.registeredFunction as OperonWorkflow<any[], any>, wfParams, ...args)
        : op.commConfig
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        ? (...args: any[]) => operon.external(op.registeredFunction as OperonCommunicator<any[], any>, wfParams, ...args)
        : undefined;
    }
    return proxy as (WFInvokeFuncs<T> & HandlerWfFuncs<T>);
  }

  /**
   * Return a request handler callback for node's native http/http2 server, which includes all registered HTTP endpoints.
   */
  getHandlersCallback() {
    if (!this.#server) {
      throw new OperonError("Uninitialized testing runtime! Did you forget to call init() first?");
    }
    return this.#server.app.callback();
  }

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic: string, idempotencyKey?: string): Promise<void> {
    return this.getOperon().send(destinationUUID, message, topic, idempotencyKey);
  }

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = 60): Promise<T | null> {
    return this.getOperon().getEvent(workflowUUID, key, timeoutSeconds);
  }

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    return this.getOperon().retrieveWorkflow(workflowUUID);
  }

  async queryUserDB<R>(sql: string, ...params: any[]): Promise<R[]> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return this.getOperon().userDatabase.query(sql, ...params);
  }

  async createUserSchema(): Promise<void> {
    return this.getOperon().userDatabase.createSchema();
  }

  dropUserSchema(): Promise<void> {
    return this.getOperon().userDatabase.dropSchema();
  }

  /**
   * For internal tests use only -- return the Operon object.
   */
  getOperon(): Operon {
    if (!this.#server) {
      throw new OperonError("Uninitialized testing runtime! Did you forget to call init() first?");
    }
    return this.#server.operon;
  }
}