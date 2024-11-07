import { Span } from "@opentelemetry/sdk-trace-base";
import { getCurrentContextStore, getCurrentDBOSContext, HTTPRequest } from "./context";
import { DBOSConfig, DBOSExecutor, InternalWorkflowParams } from "./dbos-executor";
import { Workflow, WorkflowHandle } from "./workflow";
import { DBOSExecutorContext } from "./eventreceiver";
import { DLogger, GlobalLogger } from "./telemetry/logs";
import { DBOSExecutorNotInitializedError } from "./error";
import { parseConfigFile } from "./dbos-runtime/config";
import { DBOSRuntimeConfig } from "./dbos-runtime/runtime";
import { ScheduledArgs, SchedulerConfig, SchedulerRegistrationBase } from "./scheduler/scheduler";
import { registerAndWrapContextFreeFunction } from "./decorators";

export class DBOS {
  ///////
  // Lifecycle
  ///////
  static async launch() {
    if (DBOSExecutor.globalInstance) return;
    const [dbosConfig, _]: [DBOSConfig, DBOSRuntimeConfig] = parseConfigFile();
    DBOSExecutor.globalInstance = new DBOSExecutor(dbosConfig);
    await DBOSExecutor.globalInstance.init();
    // This needs to start the admin server as well
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

  // sql session
  // parent workflow ID

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

  static async workflow<T extends unknown[], R>(wf: Workflow<T, R>, params: InternalWorkflowParams, ...args: T): Promise<WorkflowHandle<R>> {
    const executor = DBOS.executor;
    if (!executor) {
      throw new DBOSExecutorNotInitializedError();
    }
    return executor.workflow(wf, params, ...args);
  }

  // startWorkflow (child or not)
  // send
  // recv
  // sleep
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

  //workflow
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
