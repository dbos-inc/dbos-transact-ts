import { Span } from "@opentelemetry/sdk-trace-base";
import { getCurrentDBOSContext, HTTPRequest } from "./context";
import { DBOSConfig, DBOSExecutor, InternalWorkflowParams } from "./dbos-executor";
import { Workflow, WorkflowHandle } from "./workflow";
import { DBOSExecutorContext } from "./eventreceiver";
import { DLogger, GlobalLogger } from "./telemetry/logs";
import { DBOSExecutorNotInitializedError } from "./error";
import { parseConfigFile } from "./dbos-runtime/config";
import { DBOSRuntimeConfig } from "./dbos-runtime/runtime";
import { DBOSHttpServer } from "./httpServer/server";
import { Server } from "http";

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

    const adminServer = adminApp.listen(runtimeConfig.admin_port, () => {
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
  //workflow
  //transaction
  //step
  //class
  //required roles
  //scheduled
  //etc

  // Function registration
  // Middleware ops like setting auth
  // Setting next WF id
}
