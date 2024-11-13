import { Span } from "@opentelemetry/sdk-trace-base";
import {
  assertCurrentDBOSContext,
  getCurrentContextStore,
  getCurrentDBOSContext,
  HTTPRequest,
  runWithTopContext
} from "./context";
import { DBOSConfig, DBOSExecutor, InternalWorkflowParams } from "./dbos-executor";
import {
  GetWorkflowQueueInput,
  GetWorkflowQueueOutput,
  GetWorkflowsInput,
  GetWorkflowsOutput,
  WorkflowConfig,
  WorkflowFunction,
  WorkflowHandle,
  WorkflowParams
} from "./workflow";
import { DBOSExecutorContext } from "./eventreceiver";
import { DLogger, GlobalLogger } from "./telemetry/logs";
import { DBOSExecutorNotInitializedError, DBOSInvalidWorkflowTransitionError } from "./error";
import { parseConfigFile } from "./dbos-runtime/config";
import { DBOSRuntimeConfig } from "./dbos-runtime/runtime";
import { DBOSScheduler, ScheduledArgs, SchedulerConfig, SchedulerRegistrationBase } from "./scheduler/scheduler";
import { getOrCreateClassRegistration, MethodRegistration, registerAndWrapContextFreeFunction, registerFunctionWrapper } from "./decorators";
import { sleepms } from "./utils";
import { DBOSHttpServer } from "./httpServer/server";
import { Server } from "http";
import { DrizzleClient, PrismaClient, TypeORMEntityManager, UserDatabaseClient } from "./user_database";
import { TransactionConfig, TransactionContextImpl, TransactionFunction } from "./transaction";

import { PoolClient } from "pg";
import { Knex } from "knex";
import { StepConfig, StepFunction } from "./step";
import { wfQueueRunner } from "./wfqueue";
import {
  WorkflowContext
} from ".";
import { ConfiguredInstance } from ".";

export class DBOS {
  ///////
  // Lifecycle
  ///////
  static adminServer: Server | undefined = undefined;

  static setConfig(config: DBOSConfig, runtimeConfig?: DBOSRuntimeConfig) {
    DBOS.dbosConfig = config;
    DBOS.runtimeConfig = runtimeConfig;
  }

  static async launch() {
    // Do nothing is DBOS is already initialized
    if (DBOSExecutor.globalInstance) return;

    // Initialize the DBOS executor
    if (!DBOS.dbosConfig) {
      const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile();
      DBOS.dbosConfig = dbosConfig;
      DBOS.runtimeConfig = runtimeConfig;
    }
    DBOSExecutor.globalInstance = new DBOSExecutor(DBOS.dbosConfig);
    const executor: DBOSExecutor = DBOSExecutor.globalInstance;
    await executor.init();

    DBOSExecutor.globalInstance.scheduler = new DBOSScheduler(DBOSExecutor.globalInstance);
    DBOSExecutor.globalInstance.scheduler.initScheduler();

    DBOSExecutor.globalInstance.wfqEnded = wfQueueRunner.dispatchLoop(DBOSExecutor.globalInstance);

    for (const evtRcvr of DBOSExecutor.globalInstance.eventReceivers) {
      await evtRcvr.initialize(DBOSExecutor.globalInstance);
    }

    // Start the DBOS admin server
    const logger = DBOS.logger;
    if (DBOS.runtimeConfig) {
      const adminApp = DBOSHttpServer.setupAdminApp(executor);
      await DBOSHttpServer.checkPortAvailabilityIPv4Ipv6(DBOS.runtimeConfig.admin_port, logger as GlobalLogger);

      DBOS.adminServer = adminApp.listen(DBOS.runtimeConfig.admin_port, () => {
        this.logger.info(`DBOS Admin Server is running at http://localhost:${DBOS.runtimeConfig?.admin_port}`);
      });
    }
  }

  static async shutdown() {
    // Stop the admin server
    if (DBOS.adminServer) {
      DBOS.adminServer.close();
      DBOS.adminServer = undefined;
    }

    // Stop the executor
    if (DBOSExecutor.globalInstance) {
      await DBOSExecutor.globalInstance.deactivateEventReceivers();
      await DBOSExecutor.globalInstance.destroy();
      DBOSExecutor.globalInstance = undefined;
    }
  }

  static get executor() {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    return DBOSExecutor.globalInstance as DBOSExecutorContext;
  }

  //////
  // Globals
  //////
  static globalLogger?: DLogger;
  static dbosConfig?: DBOSConfig;
  static runtimeConfig?: DBOSRuntimeConfig = undefined;
  static invokeWrappers: Map<unknown, unknown> = new Map();

  //////
  // Context
  //////
  static get logger(): DLogger {
    const ctx = getCurrentDBOSContext();
    if (ctx) return ctx.logger;
    const executor = DBOS.executor;
    if (executor) return executor.logger;
    return new GlobalLogger();
  }
  static get span(): Span | undefined {
    const ctx = getCurrentDBOSContext();
    if (ctx) return ctx.span;
    return undefined;
  }

  static get request(): HTTPRequest | undefined {
    return getCurrentDBOSContext()?.request;
  }

  static get workflowID(): string | undefined {
    return getCurrentDBOSContext()?.workflowUUID;
  }
  static get authenticatedUser(): string {
    return getCurrentDBOSContext()?.authenticatedUser ?? "";
  }
  static get authenticatedRoles(): string[] {
    return getCurrentDBOSContext()?.authenticatedRoles ?? [];
  }
  static get assumedRole(): string {
    return getCurrentDBOSContext()?.assumedRole ?? "";
  }

  static isInTransaction(): boolean {
    return getCurrentContextStore()?.curTxFunctionId !== undefined;
  }

  static isInStep(): boolean {
    return getCurrentContextStore()?.curStepFunctionId !== undefined;
  }

  static isWithinWorkflow(): boolean {
    return getCurrentContextStore()?.workflowId !== undefined;
  }

  static isInWorkflow(): boolean {
    return DBOS.isWithinWorkflow() && !DBOS.isInTransaction() && !DBOS.isInStep();
  }

  // TODO CTX parent workflow ID

  // sql session (various forms)
  static get sqlClient(): UserDatabaseClient {
    if (!DBOS.isInTransaction()) throw new DBOSInvalidWorkflowTransitionError();
    const ctx = assertCurrentDBOSContext() as TransactionContextImpl<UserDatabaseClient>;
    return ctx.client;
  }

  static get pgClient(): PoolClient {
    const client = DBOS.sqlClient;
    // TODO CTX check!
    return client as PoolClient;
  }

  static get knexClient(): Knex {
    const client = DBOS.sqlClient;
    // TODO CTX check!
    return client as Knex;
  }

  static get prismaClient(): PrismaClient {
    const client = DBOS.sqlClient;
    // TODO CTX check!
    return client as PrismaClient;
  }

  static get typeORMClient(): TypeORMEntityManager {
    const client = DBOS.sqlClient;
    // TODO CTX check!
    return client as TypeORMEntityManager;
  }

  static get drizzleClient(): DrizzleClient {
    const client = DBOS.sqlClient;
    // TODO CTX check!
    return client as DrizzleClient;
  }

  static getConfig<T>(key: string): T | undefined;
  static getConfig<T>(key: string, defaultValue: T): T;
  static getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const ctx = getCurrentDBOSContext();
    if (ctx && defaultValue) return ctx.getConfig<T>(key, defaultValue);
    if (ctx) return ctx.getConfig<T>(key);
    if (DBOS.executor) return DBOS.executor.getConfig(key, defaultValue);
    return defaultValue;
  }

  //////
  // Workflow and other operations
  //////
  static getWorkflowStatus(workflowID: string) {
    return DBOS.executor.getWorkflowStatus(workflowID);
  }

  static retrieveWorkflow(workflowID: string) {
    return DBOS.executor.retrieveWorkflow(workflowID);
  }

  static async getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput> {
    return await DBOS.executor.getWorkflows(input);
  }

  static async getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput> {
    return await DBOS.executor.getWorkflowQueue(input);
  }

  static async sleepms(durationMS: number): Promise<void> {
    if (DBOS.isWithinWorkflow()) {
      if (DBOS.isInTransaction() || DBOS.isInStep()) {
        throw new DBOSInvalidWorkflowTransitionError();
      }
      return (getCurrentDBOSContext()! as WorkflowContext).sleepms(durationMS);
    }
    await sleepms(durationMS);
  }
  static async sleep(durationSec: number): Promise<void> {
    return this.sleepms(durationSec * 1000);
  }

  static async withNextWorkflowID<R>(wfid: string, callback: ()=>Promise<R>) : Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      const pcwfid = pctx.idAssignedForNextWorkflow;
      try {
        pctx.idAssignedForNextWorkflow = wfid;
        return callback();
      }
      finally {
        pctx.idAssignedForNextWorkflow = pcwfid;
      }
    }
    else {
      return runWithTopContext({idAssignedForNextWorkflow: wfid}, callback);
    }
  }

  static async withWorkflowQueue<R>(wfq: string, callback: ()=>Promise<R>) : Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      const pcwfq = pctx.queueAssignedForWorkflows;
      try {
        pctx.queueAssignedForWorkflows = wfq;
        return callback();
      }
      finally {
        pctx.queueAssignedForWorkflows = pcwfq;
      }
    }
    else {
      return runWithTopContext({queueAssignedForWorkflows: wfq}, callback);
    }
  }

  // startWorkflow (child or not)
  static async startWorkflow<Args extends unknown[], Return>(func: (...args: Args) => Promise<Return>, ...args: Args)
    : Promise<WorkflowHandle<Return>>
  {
    const pctx = getCurrentContextStore();
    const wfParams: InternalWorkflowParams = {
      workflowUUID: pctx?.idAssignedForNextWorkflow,
      queueName: pctx?.queueAssignedForWorkflows,
      usesContext: false, // TODO: This does not allow interoperation...
    };
    if (DBOS.invokeWrappers.has(func)) {
      return DBOS.executor.workflow(DBOS.invokeWrappers.get(func)! as WorkflowFunction<Args, Return>, wfParams, ...args);
    }
    else {
      return DBOS.executor.workflow(func as unknown as WorkflowFunction<Args, Return>, wfParams, ...args);
    }
  }

  static async send<T>(destinationID: string, message: T, topic?: string): Promise<void> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError();
      }
      return (getCurrentDBOSContext() as WorkflowContext).send(destinationID, message, topic);
    }
    return DBOS.executor.send(destinationID, message, topic);
  }
  
  static async recv<T>(topic?: string, timeoutSeconds?: number): Promise<T | null> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError();
      }
      return (getCurrentDBOSContext() as WorkflowContext).recv<T>(topic, timeoutSeconds);
    }
    throw new DBOSInvalidWorkflowTransitionError(); // Only workflows can recv
  }

  static async setEvent<T>(key: string, value: T): Promise<void> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError();
      }
      return (getCurrentDBOSContext() as WorkflowContext).setEvent(key, value);
    }
    throw new DBOSInvalidWorkflowTransitionError(); // Only workflows can set event
  }

  static async getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError();
      }
      return (getCurrentDBOSContext() as WorkflowContext).getEvent(workflowID, key, timeoutSeconds);
    }
    return DBOS.executor.getEvent(workflowID, key, timeoutSeconds);
  }

  // executeWorkflowId
  // recoverPendingWorkflows

  //////
  // Decorators
  //////
  static scheduled(schedulerConfig: SchedulerConfig) {
    function scheddec<This, Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: ScheduledArgs) => Promise<Return>>
    ) {
      const { descriptor, registration } = registerAndWrapContextFreeFunction(target, propertyKey, inDescriptor);
      const schedRegistration = registration as unknown as SchedulerRegistrationBase;
      schedRegistration.schedulerConfig = schedulerConfig;

      return descriptor;
    }
    return scheddec;
  }

  static workflow(config: WorkflowConfig={})
  {
    function decorator <
      This,
      Args extends unknown[],
      Return
    >
    (
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>
    )
    {
      const { descriptor, registration } = registerAndWrapContextFreeFunction(target, propertyKey, inDescriptor);
      registration.workflowConfig = config;

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        const pctx = getCurrentContextStore();
        let inst: ConfiguredInstance | undefined = undefined;
        if (typeof this === 'function') {
          // This is static          
        }
        else {
          inst = this as ConfiguredInstance;
          if (!("name" in inst)) {
            throw new DBOSInvalidWorkflowTransitionError();
          }
        }
  
        const wfParams: InternalWorkflowParams = {
          workflowUUID: pctx?.idAssignedForNextWorkflow,
          queueName: pctx?.queueAssignedForWorkflows,
          usesContext: false,
          configuredInstance : inst
        };
        if (pctx) {
          pctx.idAssignedForNextWorkflow = undefined;
        }
        const handle = await DBOS.executor.workflow(
          registration.registeredFunction as unknown as WorkflowFunction<Args, Return>,
          wfParams, ...rawArgs
        );
        return await handle.getResult();
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;
      Object.defineProperty(invokeWrapper, "name", {
        value: registration.name,
      });
  
      registerFunctionWrapper(invokeWrapper, registration as MethodRegistration<unknown, unknown[], unknown>);
      // TODO CTX this should not be in here already, or if it is we need to do something different...
      DBOS.invokeWrappers.set(invokeWrapper, registration.registeredFunction);

      return descriptor;
    }
    return decorator;
  }

  static transaction(config: TransactionConfig={}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
    {
      const { descriptor, registration } = registerAndWrapContextFreeFunction(target, propertyKey, inDescriptor);
      registration.txnConfig = config;

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        let inst: ConfiguredInstance | undefined = undefined;
        if (typeof this === 'function') {
          // This is static          
        }
        else {
          inst = this as ConfiguredInstance;
          if (!("name" in inst)) {
            throw new DBOSInvalidWorkflowTransitionError();
          }
        }

        const wfParams: WorkflowParams = {
          configuredInstance: inst
        };
        return await DBOS.executor.transaction(
          registration.registeredFunction as unknown as TransactionFunction<Args, Return>,
          wfParams, ...rawArgs
        );
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;

      Object.defineProperty(invokeWrapper, "name", {
        value: registration.name,
      });

      return descriptor;
    }
    return decorator;
  }

  static step(config: StepConfig={}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
    {
      const { descriptor, registration } = registerAndWrapContextFreeFunction(target, propertyKey, inDescriptor);
      registration.commConfig = config;

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        let inst: ConfiguredInstance | undefined = undefined;
        if (typeof this === 'function') {
          // This is static          
        }
        else {
          inst = this as ConfiguredInstance;
          if (!("name" in inst)) {
            throw new DBOSInvalidWorkflowTransitionError();
          }
        }
        const wfParams: WorkflowParams = {
          configuredInstance: inst
        };
        return  await DBOS.executor.external(
          registration.registeredFunction as unknown as StepFunction<Args, Return>,
          wfParams, ...rawArgs
        );
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;

      Object.defineProperty(invokeWrapper, "name", {
        value: registration.name,
      });

      return descriptor;
    }
    return decorator;
  }

  static defaultRequiredRole(anyOf: string[]) {
    function clsdec<T extends { new (...args: unknown[]) : object }>(ctor: T)
    {
       const clsreg = getOrCreateClassRegistration(ctor);
       clsreg.requiredRole = anyOf;
    }
    return clsdec;
  }

  static requiredRole(anyOf: string[]) {
    function apidec<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
    {
      const {descriptor, registration} = registerAndWrapContextFreeFunction(target, propertyKey, inDescriptor);
      registration.requiredRole = anyOf;
  
      return descriptor;
    }
    return apidec;
  }

  /////
  // Registration, etc
  /////

  // Function registration
  static registerAndWrapContextFreeFunction<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  )
  {
    return registerAndWrapContextFreeFunction(target, propertyKey, descriptor);
  }

  // TODO CTX
  // Middleware ops like setting auth
  // Initializers?  Deploy?  ORM Entities?
}
