import { Span } from "@opentelemetry/sdk-trace-base";
import { assertCurrentDBOSContext, getCurrentContextStore, getCurrentDBOSContext, HTTPRequest } from "./context";
import { DBOSConfig, DBOSExecutor } from "./dbos-executor";
import { WorkflowConfig, WorkflowContext } from "./workflow";
import { DBOSExecutorContext } from "./eventreceiver";
import { DLogger, GlobalLogger } from "./telemetry/logs";
import { DBOSInvalidWorkflowTransitionError } from "./error";
import { parseConfigFile } from "./dbos-runtime/config";
import { DBOSRuntimeConfig } from "./dbos-runtime/runtime";
import { ScheduledArgs, SchedulerConfig, SchedulerRegistrationBase } from "./scheduler/scheduler";
import { registerAndWrapContextFreeFunction } from "./decorators";
import { sleepms } from "./utils";
import { DBOSHttpServer } from "./httpServer/server";
import { Server } from "http";
import { DrizzleClient, PrismaClient, TypeORMEntityManager, UserDatabaseClient } from "./user_database";
import { TransactionContextImpl } from "./transaction";

import { PoolClient } from "pg";
import { Knex } from "knex";

export class DBOS {
  ///////
  // Lifecycle
  ///////
  static adminServer: Server | undefined = undefined;
  static async launch() {
    // Do nothing is DBOS is already initialized
    if (DBOSExecutor.globalInstance) return;

    // Initialize the DBOS executor
    const [dbosConfig, runtimeConfig]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile();
    DBOSExecutor.globalInstance = new DBOSExecutor(dbosConfig);
    const executor: DBOSExecutor = DBOSExecutor.globalInstance;
    await executor.init();

    // Start the DBOS admin server
    const logger = DBOS.logger;
    const adminApp = DBOSHttpServer.setupAdminApp(executor);
    await DBOSHttpServer.checkPortAvailabilityIPv4Ipv6(runtimeConfig.admin_port, logger as GlobalLogger);

    DBOS.adminServer = adminApp.listen(runtimeConfig.admin_port, () => {
      this.logger.info(`DBOS Admin Server is running at http://localhost:${runtimeConfig.admin_port}`);
    });
  }

  static async shutdown() {
    // Stop the admin server
    if (DBOS.adminServer) {
      DBOS.adminServer.close();
    }

    // Stop the executor
    if (DBOSExecutor.globalInstance) {
      await DBOSExecutor.globalInstance.destroy();
    }
  }

  static get executor() {
    return DBOSExecutor.globalInstance as DBOSExecutorContext;
  }

  //////
  // Globals
  //////
  static globalLogger?: DLogger;
  static dbosConfig?: DBOSConfig;

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

  // parent workflow ID

  // sql session (various forms)
  static get sqlClient(): UserDatabaseClient {
    if (!DBOS.isInTransaction()) throw new DBOSInvalidWorkflowTransitionError();
    const ctx = assertCurrentDBOSContext() as TransactionContextImpl<UserDatabaseClient>;
    return ctx.client;
  }

  static get pgClient(): PoolClient {
    const client = DBOS.sqlClient;
    // TODO check!
    return client as PoolClient;
  }

  static get knexClient(): Knex {
    const client = DBOS.sqlClient;
    // TODO check!
    return client as Knex;
  }

  static get prismaClient(): PrismaClient {
    const client = DBOS.sqlClient;
    // TODO check!
    return client as PrismaClient;
  }

  static get typeORMClient(): TypeORMEntityManager {
    const client = DBOS.sqlClient;
    // TODO check!
    return client as TypeORMEntityManager;
  }

  static get drizzleClient(): DrizzleClient {
    const client = DBOS.sqlClient;
    // TODO check!
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

  // This will not be needed ... you will just run the function
  /*
  static async obs_workflow<T extends unknown[], R>(wf: Workflow<T, R>, params: WorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    const executor = DBOS.executor;
    if (!executor) {
      throw new DBOSExecutorNotInitializedError();
    }
    return executor.workflow(wf, params, ...args);
  }
  */

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

  // startWorkflow (child or not)
  // send
  // recv
  // setEvent
  // getEvent
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
      const { descriptor, registration } = DBOS.registerAndWrapContextFreeFunction(target, propertyKey, inDescriptor);
      const schedRegistration = registration as unknown as SchedulerRegistrationBase;
      schedRegistration.schedulerConfig = schedulerConfig;

      return descriptor;
    }
    return scheddec;
  }

  static workflow(config: WorkflowConfig={}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>)
    {
      const { descriptor, registration } = registerAndWrapContextFreeFunction(target, propertyKey, inDescriptor);
      registration.workflowConfig = config;
      return descriptor;
    }
    return decorator;
  }

  //transaction
  //step
  //class
  //required roles
  //etc

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

  // Middleware ops like setting auth
  // Setting next WF id
}
