/* eslint-disable @typescript-eslint/no-explicit-any */
import { IncomingMessage } from "http";
import { OperonCommunicator } from "../communicator";
import { HTTPRequest, OperonContextImpl } from "../context";
import { getRegisteredOperations } from "../decorators";
import { OperonError } from "../error";
import { InvokeFuncs } from "../httpServer/handler";
import { OperonHttpServer } from "../httpServer/server";
import { Operon, OperonConfig } from "../operon";
import { operonConfigFilePath, parseConfigFile } from "../operon-runtime/config";
import { OperonTransaction } from "../transaction";
import { OperonWorkflow, WorkflowHandle, WorkflowParams } from "../workflow";
import { Http2ServerRequest, Http2ServerResponse } from "http2";
import { ServerResponse } from "http";
import { SystemDatabase } from "../system_database";
import { Client } from "pg";
import { get, has } from "lodash";

/**
 * Create a testing runtime. Warn: this function will drop the existing system DB and create a clean new one. Don't run tests against your production database!
 */
export async function createTestingRuntime(userClasses: object[], configFilePath: string = operonConfigFilePath): Promise<OperonTestingRuntime> {
  const otr = new OperonTestingRuntimeImpl();
  const [ operonConfig ] = parseConfigFile({configfile: configFilePath});

  // Drop system database
  const pgSystemClient = new Client({
    user: operonConfig.poolConfig.user,
    port: operonConfig.poolConfig.port,
    host: operonConfig.poolConfig.host,
    password: operonConfig.poolConfig.password,
    database: operonConfig.poolConfig.database,
  });
  await pgSystemClient.connect();
  await pgSystemClient.query(`DROP DATABASE IF EXISTS ${operonConfig.system_database};`);
  await pgSystemClient.end();

  // Initialize the runtime.
  await otr.init(userClasses, operonConfig);
  return otr;
}

export interface OperonInvokeParams {
  readonly authenticatedUser?: string; // The user who ran the function.
  readonly authenticatedRoles?: string[]; // Roles the authenticated user has.
  readonly request?: HTTPRequest; // The originating HTTP request.
}

export interface OperonTestingRuntime {
  invoke<T extends object>(targetClass: T, workflowUUID?: string, params?: OperonInvokeParams): InvokeFuncs<T>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
  send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;

  getHandlersCallback(): (req: IncomingMessage | Http2ServerRequest, res: ServerResponse | Http2ServerResponse) => Promise<void>;

  getConfig<T>(key: string): T; // Get application configuration.

  // User database operations.
  queryUserDB<R>(sql: string, ...params: any[]): Promise<R[]>; // Execute a raw SQL query on the user database.
  createUserSchema(): Promise<void>; // Only valid if using TypeORM. Create tables based on the provided schema.
  dropUserSchema(): Promise<void>; // Only valid if using TypeORM. Drop all tables created by createUserSchema().

  destroy(): Promise<void>; // Release resources after tests.
}

/**
 * For internal unit tests only. We do not drop the system database for internal unit tests because we want more control over the behavior.
 */
export async function createInternalTestRuntime(userClasses: object[], testConfig?: OperonConfig, systemDB?: SystemDatabase): Promise<OperonTestingRuntime> {
  const otr = new OperonTestingRuntimeImpl();
  await otr.init(userClasses, testConfig, systemDB);
  return otr;
}

/**
 * This class provides a runtime to test Opeorn functions in unit tests.
 */
export class OperonTestingRuntimeImpl implements OperonTestingRuntime {
  #server: OperonHttpServer | null = null;
  #applicationConfig: unknown = undefined;
 
  /**
   * Initialize the testing runtime by loading user functions specified in classes and using the specified config.
   * This should be the first function call before any subsequent calls.
   */
  async init(userClasses: object[], testConfig?: OperonConfig, systemDB?: SystemDatabase) {
    const operonConfig = testConfig ? [testConfig] : parseConfigFile();
    const operon = new Operon(operonConfig[0], systemDB);
    await operon.init(...userClasses);
    this.#server = new OperonHttpServer(operon);
    this.#applicationConfig = operon.config.application;
  }

  /**
   * Release resources after tests.
   */
  async destroy() {
    await this.#server?.operon.destroy();
  }

  /**
   * Get Application Configuration.
  */
  getConfig(key: string): any {
    if (!this.#applicationConfig) {
      return undefined;
    }
    if (!has(this.#applicationConfig, key)) {
      return undefined;
    }
    return get(this.#applicationConfig, key);
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to invoke workflows, transactions, and communicators;
   */
  invoke<T extends object>(object: T, workflowUUID?: string, params?: OperonInvokeParams): InvokeFuncs<T> {
    const operon = this.getOperon();
    const ops = getRegisteredOperations(object);

    const proxy: any = {};

    // Creates an Operon context to pass in necessary info.
    const span = operon.tracer.startSpan("test");
    const oc = new OperonContextImpl("test", span, operon.logger);
    oc.authenticatedUser = params?.authenticatedUser ?? "";
    oc.request = params?.request ?? {};
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
    return proxy as InvokeFuncs<T>;
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

  async send<T extends NonNullable<any>>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    return this.getOperon().send(destinationUUID, message, topic, idempotencyKey);
  }

  async getEvent<T extends NonNullable<any>>(workflowUUID: string, key: string, timeoutSeconds: number = Operon.defaultNotificationTimeoutSec): Promise<T | null> {
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