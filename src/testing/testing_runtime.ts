/* eslint-disable @typescript-eslint/no-explicit-any */
import { IncomingMessage } from "http";
import { Communicator } from "../communicator";
import { HTTPRequest, DBOSContextImpl } from "../context";
import { ConfiguredInstance, getRegisteredOperations } from "../decorators";
import { DBOSConfigKeyTypeError, DBOSError } from "../error";
import { AsyncHandlerWfFuncs, AsyncHandlerWfFuncInst, InvokeFuncs, InvokeFuncsInst, SyncHandlerWfFuncs, SyncHandlerWfFuncsInst } from "../httpServer/handler";
import { DBOSHttpServer } from "../httpServer/server";
import { DBOSExecutor, DBOSConfig } from "../dbos-executor";
import { dbosConfigFilePath, parseConfigFile } from "../dbos-runtime/config";
import { Transaction } from "../transaction";
import { Workflow, WorkflowHandle, WorkflowParams } from "../workflow";
import { Http2ServerRequest, Http2ServerResponse } from "http2";
import { ServerResponse } from "http";
import { SystemDatabase } from "../system_database";
import { get, set } from "lodash";
import { Client } from "pg";
import { DBOSScheduler } from "../scheduler/scheduler";
import { StoredProcedure } from "../procedure";

/**
 * Create a testing runtime. Warn: this function will drop the existing system DB and create a clean new one. Don't run tests against your production database!
 */
export async function createTestingRuntime(userClasses: object[] | undefined = undefined, configFilePath: string = dbosConfigFilePath, dropSysDB: boolean = true): Promise<TestingRuntime> {
  const [dbosConfig] = parseConfigFile({ configfile: configFilePath });

  if (dropSysDB) {
    // Drop system database. Testing runtime always uses Postgres for local testing.
    const pgSystemClient = new Client({
      user: dbosConfig.poolConfig.user,
      port: dbosConfig.poolConfig.port,
      host: dbosConfig.poolConfig.host,
      password: dbosConfig.poolConfig.password,
      database: dbosConfig.poolConfig.database,
    });
    await pgSystemClient.connect();
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${dbosConfig.system_database};`);
    await pgSystemClient.end();
  }

  const otr = createInternalTestRuntime(userClasses, dbosConfig, undefined)
  return otr;
}

export interface WorkflowInvokeParams {
  readonly authenticatedUser?: string; // The user who ran the function.
  readonly authenticatedRoles?: string[]; // Roles the authenticated user has.
  readonly request?: HTTPRequest; // The originating HTTP request.
}

export interface TestingRuntime {
  invoke<T extends ConfiguredInstance>(targetInst: T, workflowUUID?: string, params?: WorkflowInvokeParams): InvokeFuncsInst<T>;
  invoke<T extends object>(targetClass: T, workflowUUID?: string, params?: WorkflowInvokeParams): InvokeFuncs<T>;
  invokeWorkflow<T extends ConfiguredInstance>(targetCfg: T, workflowUUID?: string, params?: WorkflowInvokeParams): SyncHandlerWfFuncsInst<T>;
  invokeWorkflow<T extends object>(targetClass: T, workflowUUID?: string, params?: WorkflowInvokeParams): SyncHandlerWfFuncs<T>;
  startWorkflow<T extends ConfiguredInstance>(targetCfg: T, workflowUUID?: string, params?: WorkflowInvokeParams): AsyncHandlerWfFuncInst<T>;
  startWorkflow<T extends object>(targetClass: T, workflowUUID?: string, params?: WorkflowInvokeParams): AsyncHandlerWfFuncs<T>;
  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R>;
  send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void>;
  getEvent<T>(workflowUUID: string, key: string, timeoutSeconds?: number): Promise<T | null>;

  getHandlersCallback(): (req: IncomingMessage | Http2ServerRequest, res: ServerResponse | Http2ServerResponse) => Promise<void>;
  getAdminCallback(): (req: IncomingMessage | Http2ServerRequest, res: ServerResponse | Http2ServerResponse) => Promise<void>;

  getConfig<T>(key: string): T | undefined; // Get application configuration.
  getConfig<T>(key: string, defaultValue: T): T;
  setConfig<T>(key: string, newValue: T): void;

  // User database operations.
  queryUserDB<R>(sql: string, ...params: any[]): Promise<R[]>; // Execute a raw SQL query on the user database.
  createUserSchema(): Promise<void>; // Only valid if using TypeORM. Create tables based on the provided schema.
  dropUserSchema(): Promise<void>; // Only valid if using TypeORM. Drop all tables created by createUserSchema().

  destroy(): Promise<void>; // Release resources after tests.
}

/**
 * For internal unit tests which allows us to provide different system DB and control its behavior.
 */
export async function createInternalTestRuntime(userClasses: object[] | undefined, testConfig: DBOSConfig, systemDB?: SystemDatabase): Promise<TestingRuntime> {
  const otr = new TestingRuntimeImpl();
  await otr.init(userClasses, testConfig, systemDB);
  return otr;
}

/**
 * This class provides a runtime to test DBOS functions in unit tests.
 */
export class TestingRuntimeImpl implements TestingRuntime {
  #server: DBOSHttpServer | null = null;
  #scheduler: DBOSScheduler | null = null;
  #applicationConfig: object = {};
  #isInitialized = false;

  /**
   * Initialize the testing runtime by loading user functions specified in classes and using the specified config.
   * This should be the first function call before any subsequent calls.
   */
  async init(userClasses?: object[], testConfig?: DBOSConfig, systemDB?: SystemDatabase) {
    const dbosConfig = testConfig ? [testConfig] : parseConfigFile();
    const dbosExec = new DBOSExecutor(dbosConfig[0], systemDB);
    await dbosExec.init(userClasses);
    this.#server = new DBOSHttpServer(dbosExec);
    for (const evtRcvr of dbosExec.eventReceivers) {
      await evtRcvr.initialize(dbosExec);
    }
    this.#scheduler = new DBOSScheduler(dbosExec);
    this.#scheduler.initScheduler();
    this.#applicationConfig = dbosExec.config.application ?? {};
    this.#isInitialized = true;
  }

  /**
   * Release resources after tests.
   */
  async destroy() {
    // Only release once.
    if (this.#isInitialized) {
      await this.#scheduler?.destroyScheduler();
      for (const evtRcvr of this.#server?.dbosExec?.eventReceivers || []) {
        await evtRcvr.destroy();
      }
      await this.#server?.dbosExec.destroy();
      this.#isInitialized = false;
    }
  }

  /**
   * Get Application Configuration.
  */
  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const value = get(this.#applicationConfig, key, defaultValue);
    if (value && defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }
    return value;
  }

  setConfig<T>(key: string, newValue: T): void {
    set(this.#applicationConfig, key, newValue);
  }

  /**
   * Generate a proxy object for the provided class that wraps direct calls (i.e. OpClass.someMethod(param))
   * to invoke workflows, transactions, and communicators;
   */
  mainInvoke<T extends object>(object: T, workflowUUID: string | undefined, params: WorkflowInvokeParams | undefined, asyncWf: boolean,
    clsinst: ConfiguredInstance | null): InvokeFuncs<T>
  {
    const dbosExec = this.getDBOSExec();

    const ops = getRegisteredOperations(clsinst ? clsinst : object);

    const proxy: Record<string, unknown> = {};

    // Creates a context to pass in necessary info.
    const span = dbosExec.tracer.startSpan("test");
    const oc = new DBOSContextImpl("test", span, dbosExec.logger);
    oc.authenticatedUser = params?.authenticatedUser ?? "";
    oc.request = params?.request ?? {};
    oc.authenticatedRoles = params?.authenticatedRoles ?? [];

    const wfParams: WorkflowParams = { workflowUUID: workflowUUID, parentCtx: oc, configuredInstance: clsinst };
    for (const op of ops) {
      if (asyncWf) {
        proxy[op.name] = op.txnConfig
          ? (...args: unknown[]) => dbosExec.transaction(op.registeredFunction as Transaction<unknown[], unknown>, wfParams, ...args)
          : op.workflowConfig
            ? (...args: unknown[]) => dbosExec.workflow(op.registeredFunction as Workflow<unknown[], unknown>, wfParams, ...args)
            : op.commConfig
              ? (...args: unknown[]) => dbosExec.external(op.registeredFunction as Communicator<unknown[], unknown>, wfParams, ...args)
              : op.procConfig
                ? (...args: unknown[]) => dbosExec.procedure(op.registeredFunction as StoredProcedure<unknown>, wfParams, ...args)
                : undefined;
      } else {
        proxy[op.name] = op.workflowConfig
          ? (...args: unknown[]) => dbosExec.workflow(op.registeredFunction as Workflow<unknown[], unknown>, wfParams, ...args).then((handle) => handle.getResult())
          : undefined;
      }
    }
    return proxy as InvokeFuncs<T>;
  }

  invoke<T extends object>(object: T | ConfiguredInstance, workflowUUID?: string, params?: WorkflowInvokeParams): InvokeFuncs<T> | InvokeFuncsInst<T> {
    if (typeof object === 'function') {
      return this.mainInvoke(object, workflowUUID, params, true, null);
    }
    else {
      const targetInst = object as ConfiguredInstance;
      return this.mainInvoke(targetInst.constructor, workflowUUID, params, true, targetInst) as unknown as InvokeFuncsInst<T>;
    }
  }

  startWorkflow<T extends object>(object: T, workflowUUID?: string, params?: WorkflowInvokeParams)
    : AsyncHandlerWfFuncs<T> | AsyncHandlerWfFuncInst<T>
  {
    if (typeof object === 'function') {
      return this.mainInvoke(object, workflowUUID, params, true, null);
    }
    else {
      const targetInst = object as ConfiguredInstance;
      return this.mainInvoke(targetInst.constructor, workflowUUID, params, true, targetInst) as unknown as AsyncHandlerWfFuncInst<T>;
    }
  }

  invokeWorkflow<T extends object>(object: T | ConfiguredInstance, workflowUUID?: string, params?: WorkflowInvokeParams)
    : SyncHandlerWfFuncs<T> | SyncHandlerWfFuncsInst<T>
  {
    if (typeof object === 'function') {
      return this.mainInvoke(object, workflowUUID, params, false, null) as unknown as SyncHandlerWfFuncs<T>;
    }
    else {
      const targetInst = object as ConfiguredInstance;
      return this.mainInvoke(targetInst.constructor, workflowUUID, params, false, targetInst) as unknown as SyncHandlerWfFuncsInst<T>;
    }
  }

  /**
   * Return a request handler callback for node's native http/http2 server, which includes all registered HTTP endpoints.
   */
  getHandlersCallback() {
    if (!this.#server) {
      throw new DBOSError("Uninitialized testing runtime! Did you forget to call init() first?");
    }
    return this.#server.app.callback();
  }

  getAdminCallback() {
    if (!this.#server) {
      throw new DBOSError("Uninitialized testing runtime! Did you forget to call init() first?");
    }
    return this.#server.adminApp.callback();
  }

  async send<T>(destinationUUID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    return this.getDBOSExec().send(destinationUUID, message, topic, idempotencyKey);
  }

  async getEvent<T>(workflowUUID: string, key: string, timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec): Promise<T | null> {
    return this.getDBOSExec().getEvent(workflowUUID, key, timeoutSeconds);
  }

  retrieveWorkflow<R>(workflowUUID: string): WorkflowHandle<R> {
    return this.getDBOSExec().retrieveWorkflow(workflowUUID);
  }

  async queryUserDB<R>(sql: string, ...params: any[]): Promise<R[]> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return this.getDBOSExec().userDatabase.query(sql, ...params);
  }

  async createUserSchema(): Promise<void> {
    return this.getDBOSExec().userDatabase.createSchema();
  }

  dropUserSchema(): Promise<void> {
    return this.getDBOSExec().userDatabase.dropSchema();
  }

  /**
   * For internal tests use only -- return the workflow executor object.
   */
  getDBOSExec(): DBOSExecutor {
    if (!this.#server) {
      throw new DBOSError("Uninitialized testing runtime! Did you forget to call init() first?");
    }
    return this.#server.dbosExec;
  }
}
