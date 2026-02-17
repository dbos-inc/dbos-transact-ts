import {
  getCurrentContextStore,
  HTTPRequest,
  runWithTopContext,
  getNextWFID,
  StepStatus,
  DBOSContextOptions,
  functionIDGetIncrement,
  functionIDGet,
} from './context';
import { DBOSConfig, DBOSExecutor, DBOSExternalState, InternalWorkflowParams } from './dbos-executor';
import { DBOSSpan, getActiveSpan, installTraceContextManager, isTraceContextWorking, Tracer } from './telemetry/traces';
import {
  GetWorkflowsInput,
  InternalWFHandle,
  isWorkflowActive,
  RetrievedHandle,
  StepInfo,
  WorkflowConfig,
  WorkflowHandle,
  WorkflowParams,
  WorkflowSerializationFormat,
  WorkflowStatus,
} from './workflow';
import { DLogger, GlobalLogger } from './telemetry/logs';
import {
  DBOSError,
  DBOSExecutorNotInitializedError,
  DBOSInvalidWorkflowTransitionError,
  DBOSNotRegisteredError,
  DBOSAwaitedWorkflowCancelledError,
  DBOSConflictingRegistrationError,
  DBOSAwaitedWorkflowExceededMaxRecoveryAttempts,
} from './error';
import {
  getDbosConfig,
  getRuntimeConfig,
  overwriteConfigForDBOSCloud,
  readConfigFile,
  translateDbosConfig,
  translateRuntimeConfig,
} from './config';
import { ScheduledArgs, ScheduledReceiver, SchedulerConfig } from './scheduler/scheduler_decorator';
import {
  AlertHandler,
  associateClassWithExternal,
  associateMethodWithExternal,
  ClassAuthDefaults,
  DBOS_AUTH,
  ExternalRegistration,
  getAlertHandler,
  getLifecycleListeners,
  getRegisteredOperations,
  getFunctionRegistration,
  getRegistrationsForExternal,
  insertAllMiddleware,
  MethodAuth,
  MethodRegistration,
  recordDBOSLaunch,
  recordDBOSShutdown,
  registerFunctionWrapper,
  registerLifecycleCallback,
  setAlertHandler,
  transactionalDataSources,
  registerMiddlewareInstaller,
  MethodRegistrationBase,
  TypedAsyncFunction,
  UntypedAsyncFunction,
  FunctionName,
  wrapDBOSFunctionAndRegisterByUniqueName,
  wrapDBOSFunctionAndRegisterByTarget,
  wrapDBOSFunctionAndRegister,
  ensureDBOSIsLaunched,
  ConfiguredInstance,
  DBOSMethodMiddlewareInstaller,
  DBOSLifecycleCallback,
  associateParameterWithExternal,
  finalizeClassRegistrations,
  getClassRegistration,
  clearAllRegistrations,
  getRegisteredFunctionFullName,
  getFunctionRegistrationByName,
} from './decorators';
import { defaultEnableOTLP, globalParams, sleepms, INTERNAL_QUEUE_NAME } from './utils';
import {
  deserializeValue,
  JSONValue,
  registerSerializationRecipe,
  SerializationRecipe,
  serializeValue,
} from './serialization';
import { DBOSAdminServer } from './adminserver';
import { Server } from 'http';

import { randomUUID } from 'node:crypto';

import { StepConfig } from './step';
import { Conductor } from './conductor/conductor';
import {
  EnqueueOptions,
  DBOS_STREAM_CLOSED_SENTINEL,
  DBOS_FUNCNAME_WRITESTREAM,
  WorkflowScheduleInternal,
} from './system_database';
import { WorkflowSchedule, ScheduledWorkflowFn, toWorkflowSchedule, createScheduleId } from './scheduler/scheduler';
import { validateCrontab, TimeMatcher } from './scheduler/crontab';
import { wfQueueRunner } from './wfqueue';
import { registerAuthChecker } from './authdecorators';
import assert from 'node:assert';

type AnyConstructor = new (...args: unknown[]) => object;

// Declare all the options a user can pass to the DBOS object during launch()
export interface DBOSLaunchOptions {
  // For DBOS Conductor
  conductorURL?: string;
  conductorKey?: string;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type PossiblyWFFunc = (...args: any[]) => Promise<unknown>;
type InvokeFunctionsAsync<T> =
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  T extends Function
    ? {
        [P in keyof T]: T[P] extends PossiblyWFFunc
          ? (...args: Parameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>>
          : never;
      }
    : never;

type InvokeFunctionsAsyncInst<T> = T extends ConfiguredInstance
  ? {
      [P in keyof T]: T[P] extends PossiblyWFFunc
        ? (...args: Parameters<T[P]>) => Promise<WorkflowHandle<Awaited<ReturnType<T[P]>>>>
        : never;
    }
  : never;

export interface StartWorkflowParams {
  workflowID?: string;
  queueName?: string;
  timeoutMS?: number | null;
  enqueueOptions?: EnqueueOptions;
}

/**
 * Options for `DBOS.send`
 */
export interface SendOptions {
  /** Serialization format override, allows cross-language send/recv */
  serializationType?: WorkflowSerializationFormat;
}

/**
 * Options for `DBOS.writeStream`
 */
export interface WriteStreamOptions {
  /** Serialization format override, allows cross-language writeStream / readStream */
  serializationType?: WorkflowSerializationFormat;
}

/**
 * Options for `DBOS.setEvent`
 */
export interface SetEventOptions {
  /**
   * Serialization format override, allows event to be read by
   *   workflows or clients written in other languages
   */
  serializationType?: WorkflowSerializationFormat;
}

export function getExecutor() {
  if (!DBOSExecutor.globalInstance) {
    throw new DBOSExecutorNotInitializedError();
  }
  return DBOSExecutor.globalInstance;
}

export function runInternalStep<T>(
  callback: () => Promise<T>,
  funcName: string,
  childWFID?: string,
  assignedFuncID?: number,
): Promise<T> {
  if (DBOS.isWithinWorkflow()) {
    if (DBOS.isInStep()) {
      // OK to use directly
      return callback();
    } else if (DBOS.isInWorkflow()) {
      return DBOSExecutor.globalInstance!.runInternalStep<T>(
        callback,
        funcName,
        DBOS.workflowID!,
        assignedFuncID ?? functionIDGetIncrement(),
        childWFID,
      );
    } else {
      throw new DBOSInvalidWorkflowTransitionError(
        `Invalid call to \`${funcName}\` inside a \`transaction\` or \`procedure\``,
      );
    }
  }
  return callback();
}

export class DBOS {
  ///////
  // Lifecycle
  ///////
  static adminServer: Server | undefined = undefined;
  static conductor: Conductor | undefined = undefined;

  /**
   * Set configuration of `DBOS` prior to `launch`
   * @param config - configuration of services needed by DBOS
   */
  static setConfig(config: DBOSConfig) {
    assert(!DBOS.isInitialized(), 'Cannot call DBOS.setConfig after DBOS.launch');
    DBOS.#dbosConfig = config;
  }

  /**
   * Check if DBOS has been `launch`ed (and not `shutdown`)
   * @returns `true` if DBOS has been launched, or `false` otherwise
   */
  static isInitialized(): boolean {
    return !!DBOSExecutor.globalInstance?.initialized;
  }

  /**
   * Launch DBOS, starting recovery and request handling
   * @param options - Launch options for connecting to DBOS Conductor
   */
  static async launch(options?: DBOSLaunchOptions): Promise<void> {
    const configFile = await readConfigFile();

    let internalConfig = DBOS.#dbosConfig ? translateDbosConfig(DBOS.#dbosConfig) : getDbosConfig(configFile);
    let runtimeConfig = DBOS.#dbosConfig ? translateRuntimeConfig(DBOS.#dbosConfig) : getRuntimeConfig(configFile);

    if (process.env.DBOS__CLOUD === 'true') {
      [internalConfig, runtimeConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, configFile);
    }

    globalParams.enableOTLP = DBOS.#dbosConfig?.enableOTLP ?? defaultEnableOTLP();

    if (!isTraceContextWorking()) installTraceContextManager(internalConfig.name);

    // Do nothing if DBOS is already initialized
    if (DBOS.isInitialized()) {
      return;
    }

    finalizeClassRegistrations();
    insertAllMiddleware();

    // Globally set the application version and executor ID.
    // In DBOS Cloud, instead use the value supplied through environment variables.
    if (process.env.DBOS__CLOUD !== 'true') {
      if (DBOS.#dbosConfig?.applicationVersion) {
        globalParams.appVersion = DBOS.#dbosConfig.applicationVersion;
      } else if (DBOS.#dbosConfig?.enablePatching) {
        globalParams.appVersion = 'PATCHING_ENABLED';
      }
      if (DBOS.#dbosConfig?.executorID) {
        globalParams.executorID = DBOS.#dbosConfig.executorID;
      }
    }
    if (options?.conductorKey) {
      // Always use a generated executor ID in Conductor.
      globalParams.executorID = randomUUID();
    }

    DBOSExecutor.createDebouncerWorkflow();
    DBOSExecutor.createInternalQueue();
    DBOSExecutor.globalInstance = new DBOSExecutor(internalConfig);

    recordDBOSLaunch();

    const executor: DBOSExecutor = DBOSExecutor.globalInstance;
    await executor.init();

    await DBOSExecutor.globalInstance.initEventReceivers(this.#dbosConfig?.listenQueues || null);
    for (const [_n, ds] of transactionalDataSources) {
      await ds.initialize();
    }

    if (options?.conductorKey) {
      if (!options.conductorURL) {
        const dbosDomain = process.env.DBOS_DOMAIN || 'cloud.dbos.dev';
        options.conductorURL = `wss://${dbosDomain}/conductor/v1alpha1`;
      }
      DBOS.conductor = new Conductor(DBOSExecutor.globalInstance, options.conductorKey, options.conductorURL);
      DBOS.conductor.dispatchLoop();
    }

    // Start the DBOS admin server
    const logger = DBOS.logger;
    if (runtimeConfig.runAdminServer) {
      const adminApp = DBOSAdminServer.setupAdminApp(executor);
      try {
        await DBOSAdminServer.checkPortAvailabilityIPv4Ipv6(runtimeConfig.admin_port, logger as GlobalLogger);
        // Wrap the listen call in a promise to properly catch errors
        DBOS.adminServer = await new Promise((resolve, reject) => {
          const server = adminApp.listen(runtimeConfig?.admin_port, () => {
            DBOS.logger.debug(`DBOS Admin Server is running at http://localhost:${runtimeConfig?.admin_port}`);
            resolve(server);
          });
          server.on('error', (err) => {
            reject(err);
          });
        });
      } catch (e) {
        logger.warn(`Unable to start DBOS admin server on port ${runtimeConfig.admin_port}`);
      }
    }
  }

  /**
   * Logs all workflows that can be invoked externally, rather than directly by the applicaton.
   * This includes:
   *   All DBOS event receiver entrypoints (message queues, URLs, etc.)
   *   Scheduled workflows
   *   Queues
   */
  static logRegisteredEndpoints(): void {
    if (!DBOSExecutor.globalInstance) return;
    wfQueueRunner.logRegisteredEndpoints(DBOSExecutor.globalInstance);
    for (const lcl of getLifecycleListeners()) {
      lcl.logRegisteredEndpoints?.();
    }
  }

  /**
   * Shut down DBOS processing:
   *   Stops receiving external workflow requests
   *   Disconnects from administration / Conductor
   *   Stops workflow processing and disconnects from databases
   * @param options Optional shutdown options.
   * @param options.deregister
   *   If true, clear the DBOS workflow, queue, instance, data source, listener, and other registries.
   *   This is available for testing and development purposes only.
   *   Functions may then be registered before the next call to DBOS.launch().
   *   Decorated / registered functions created prior to `clearRegistry` may no longer be used.
   *     Fresh wrappers may be created from the original functions.
   */
  static async shutdown(options?: { deregister?: boolean }) {
    // Stop the admin server
    if (DBOS.adminServer) {
      DBOS.adminServer.close();
      DBOS.adminServer = undefined;
    }

    // Stop the conductor
    if (DBOS.conductor) {
      DBOS.conductor.stop();
      while (!DBOS.conductor.isClosed) {
        await sleepms(500);
      }
      DBOS.conductor = undefined;
    }

    // Stop the executor
    if (DBOSExecutor.globalInstance) {
      await DBOSExecutor.globalInstance.deactivateEventReceivers();
      await DBOSExecutor.globalInstance.destroy();
      DBOSExecutor.globalInstance = undefined;
    }

    for (const [_n, ds] of transactionalDataSources) {
      await ds.destroy();
    }

    // Reset the global app version and executor ID
    globalParams.appVersion = process.env.DBOS__APPVERSION || '';
    globalParams.wasComputed = false;
    globalParams.appID = process.env.DBOS__APPID || '';
    globalParams.executorID = process.env.DBOS__VMID || 'local';

    recordDBOSShutdown();

    if (options?.deregister) {
      DBOS.clearRegistry();
    }
  }

  /**
   * Clear the DBOS workflow, queue, instance, data source, listener, and other registries.
   *   This is available for testing and development purposes only.
   *   This may only be done while DBOS is not launch()ed.
   *   Decorated / registered functions created prior to `clearRegistry` may no longer be used.
   *     Fresh wrappers may be created from the original functions.
   */
  private static clearRegistry() {
    assert(!DBOS.isInitialized(), 'Cannot call DBOS.clearRegistry after DBOS.launch');
    clearAllRegistrations();
    wfQueueRunner.clearRegistrations();
    DBOSExecutor.debouncerWorkflow = undefined;
    DBOSExecutor.internalQueue = undefined;
  }

  /** Stop listening for external events (for testing) */
  static async deactivateEventReceivers() {
    return DBOSExecutor.globalInstance?.deactivateEventReceivers();
  }

  /** Start listening for external events (for testing) */
  static async initEventReceivers() {
    return DBOSExecutor.globalInstance?.initEventReceivers(this.#dbosConfig?.listenQueues || null);
  }

  // Global DBOS executor instance
  static get #executor() {
    return getExecutor();
  }

  //////
  // Globals
  //////
  static #dbosConfig?: DBOSConfig;

  //////
  // Context
  //////
  /** Get the current DBOS Logger, appropriate to the current context */
  static get logger(): DLogger {
    const lctx = getCurrentContextStore();
    if (lctx?.logger) return lctx.logger;
    const executor = DBOSExecutor.globalInstance;
    if (executor) return executor.logger;
    return new GlobalLogger();
  }

  /** Get the current DBOS Tracer, for starting spans */
  static get tracer(): Tracer | undefined {
    const executor = DBOSExecutor.globalInstance;
    if (executor) return executor.tracer;
  }

  /** Get the current DBOS tracing span, appropriate to the current context */
  static get span(): DBOSSpan | undefined {
    return getActiveSpan();
  }

  /**
   * Get the current request object (such as an HTTP request)
   * This is intended for use in event libraries that know the type of the current request,
   *  and set it using `withTracedContext` or `runWithContext`
   */
  static requestObject(): object | undefined {
    return getCurrentContextStore()?.request;
  }

  /** Get the current HTTP request (within `@DBOS.getApi` et al) */
  static getRequest(): HTTPRequest | undefined {
    return this.requestObject() as HTTPRequest | undefined;
  }

  /** Get the current HTTP request (within `@DBOS.getApi` et al) */
  static get request(): HTTPRequest {
    const r = DBOS.getRequest();
    if (!r) throw new DBOSError('`DBOS.request` accessed from outside of HTTP requests');
    return r;
  }

  /** Get the current application version */
  static get applicationVersion(): string {
    return globalParams.appVersion;
  }

  /** Get the current executor ID */
  static get executorID(): string {
    return globalParams.executorID;
  }

  /** Get the current workflow ID */
  static get workflowID(): string | undefined {
    return getCurrentContextStore()?.workflowId;
  }

  /** Use portable serialization by default? */
  static get defaultSerializationType(): WorkflowSerializationFormat | undefined {
    return getCurrentContextStore()?.serializationType;
  }

  /** Get the current step number, within the current workflow */
  static get stepID(): number | undefined {
    if (DBOS.isInStep()) {
      return getCurrentContextStore()?.curStepFunctionId;
    } else if (DBOS.isInTransaction()) {
      return getCurrentContextStore()?.curTxFunctionId;
    } else {
      return undefined;
    }
  }

  static get stepStatus(): StepStatus | undefined {
    return getCurrentContextStore()?.stepStatus;
  }

  /** Get the current authenticated user */
  static get authenticatedUser(): string {
    return getCurrentContextStore()?.authenticatedUser ?? '';
  }
  /** Get the roles granted to the current authenticated user */
  static get authenticatedRoles(): string[] {
    return getCurrentContextStore()?.authenticatedRoles ?? [];
  }
  /** Get the role assumed by the current user giving authorization to execute the current function */
  static get assumedRole(): string {
    return getCurrentContextStore()?.assumedRole ?? '';
  }

  /** @returns true if called from within a transaction, false otherwise */
  static isInTransaction(): boolean {
    return getCurrentContextStore()?.curTxFunctionId !== undefined;
  }

  /** @returns true if called from within a step, false otherwise */
  static isInStep(): boolean {
    return getCurrentContextStore()?.curStepFunctionId !== undefined;
  }

  /**
   * @returns true if called from within a workflow
   *  (regardless of whether the workflow is currently executing a step,
   *   transaction, or procedure), false otherwise
   */
  static isWithinWorkflow(): boolean {
    return getCurrentContextStore()?.workflowId !== undefined;
  }

  /**
   * @returns true if called from within a workflow that is not currently executing
   *  a step, transaction, or procedure, or false otherwise
   */
  static isInWorkflow(): boolean {
    return DBOS.isWithinWorkflow() && !DBOS.isInTransaction() && !DBOS.isInStep();
  }

  //////
  // Access to system DB, for event receivers etc.
  //////
  /**
   * Get a state item from the system database, which provides a key/value store interface for event dispatchers.
   *   The full key for the database state should include the service, function, and item.
   *   Values are versioned.  A version can either be a sequence number (long integer), or a time (high precision floating point).
   *       If versions are in use, any upsert is discarded if the version field is less than what is already stored.
   *
   * Examples of state that could be kept:
   *   Offsets into kafka topics, per topic partition
   *   Last time for which a scheduling service completed schedule dispatch
   *
   * @param service - should be unique to the event receiver keeping state, to separate from others
   * @param workflowFnName - function name; should be the fully qualified / unique function name dispatched
   * @param key - The subitem kept by event receiver service for the function, allowing multiple values to be stored per function
   * @returns The latest system database state for the specified service+workflow+item
   */
  static async getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSExternalState | undefined> {
    ensureDBOSIsLaunched('getEventDispatchState');
    return await DBOS.#executor.getEventDispatchState(svc, wfn, key);
  }
  /**
   * Set a state item into the system database, which provides a key/value store interface for event dispatchers.
   *   The full key for the database state should include the service, function, and item; these fields are part of `state`.
   *   Values are versioned.  A version can either be a sequence number (long integer), or a time (high precision floating point).
   *     If versions are in use, any upsert is discarded if the version field is less than what is already stored.
   *
   * @param state - the service, workflow, item, version, and value to write to the database
   * @returns The upsert returns the current record, which may be useful if it is more recent than the `state` provided.
   */
  static async upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState> {
    ensureDBOSIsLaunched('upsertEventDispatchState');
    return await DBOS.#executor.upsertEventDispatchState(state);
  }

  //////
  // Workflow and other operations
  //////

  /**
   * Get the workflow status given a workflow ID
   * @param workflowID - ID of the workflow
   * @returns status of the workflow as `WorkflowStatus`, or `null` if there is no workflow with `workflowID`
   */
  static getWorkflowStatus(workflowID: string): Promise<WorkflowStatus | null> {
    ensureDBOSIsLaunched('getWorkflowStatus');
    if (DBOS.isWithinWorkflow()) {
      if (DBOS.isInStep()) {
        // OK to use directly
        return DBOS.#executor.getWorkflowStatus(workflowID);
      } else if (DBOS.isInWorkflow()) {
        return DBOS.#executor.getWorkflowStatus(workflowID, DBOS.workflowID, functionIDGetIncrement());
      } else {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `getWorkflowStatus` inside a `transaction` or `procedure`',
        );
      }
    }
    return DBOS.#executor.getWorkflowStatus(workflowID);
  }

  /**
   * Get the workflow result, given a workflow ID
   * @param workflowID - ID of the workflow
   * @param timeoutSeconds - Maximum time to wait for result; if not provided, the operation does not time out
   * @returns The return value of the workflow, or throws the exception thrown by the workflow, or `null` if times out
   */
  static async getResult<T>(workflowID: string, timeoutSeconds?: number): Promise<T | null> {
    ensureDBOSIsLaunched('getResult');
    let timerFuncID: number | undefined = undefined;
    if (DBOS.isWithinWorkflow() && timeoutSeconds !== undefined) {
      timerFuncID = functionIDGetIncrement();
    }
    return await DBOS.getResultInternal(workflowID, timeoutSeconds, timerFuncID, undefined);
  }

  static async getResultInternal<T>(
    workflowID: string,
    timeoutSeconds?: number,
    timerFuncID?: number,
    assignedFuncID?: number,
  ): Promise<T | null> {
    return await runInternalStep(
      async () => {
        const rres = await DBOSExecutor.globalInstance!.systemDatabase.awaitWorkflowResult(
          workflowID,
          timeoutSeconds,
          DBOS.workflowID,
          timerFuncID,
        );
        if (!rres) return null;
        if (rres?.cancelled) {
          throw new DBOSAwaitedWorkflowCancelledError(workflowID);
        }
        if (rres?.maxRecoveryAttemptsExceeded) {
          throw new DBOSAwaitedWorkflowExceededMaxRecoveryAttempts(workflowID);
        }
        return DBOSExecutor.reviveResultOrError<T>(rres, DBOS.#executor.serializer);
      },
      'DBOS.getResult',
      workflowID,
      assignedFuncID,
    );
  }

  /**
   * Create a workflow handle with a given workflow ID.
   * This call always returns a handle, even if the workflow does not exist.
   * The resulting handle will check the database to provide any workflow information.
   * @param workflowID - ID of the workflow
   * @returns `WorkflowHandle` that can be used to poll for the status or result of any workflow with `workflowID`
   */
  static retrieveWorkflow<T = unknown>(workflowID: string): WorkflowHandle<Awaited<T>> {
    ensureDBOSIsLaunched('retrieveWorkflow');
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `retrieveWorkflow` inside a `transaction` or `step`',
        );
      }
      return new RetrievedHandle(DBOSExecutor.globalInstance!.systemDatabase, workflowID);
    }
    return DBOS.#executor.retrieveWorkflow(workflowID);
  }

  /**
   * Query the system database for all workflows matching the provided predicate
   * @param input - `GetWorkflowsInput` predicate for filtering returned workflows
   * @returns `WorkflowStatus` array containing details of the matching workflows
   */
  static async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    ensureDBOSIsLaunched('listWorkflows');
    return await runInternalStep(async () => {
      return await DBOS.#executor.listWorkflows(input);
    }, 'DBOS.listWorkflows');
  }

  /**
   * Query the system database for all queued workflows matching the provided predicate
   * @param input - `GetQueuedWorkflowsInput` predicate for filtering returned workflows
   * @returns `WorkflowStatus` array containing details of the matching workflows
   */
  static async listQueuedWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    ensureDBOSIsLaunched('listQueuedWorkflows');
    return await runInternalStep(async () => {
      return await DBOS.#executor.listQueuedWorkflows(input);
    }, 'DBOS.listQueuedWorkflows');
  }

  /**
   * Retrieve the steps of a workflow
   * @param workflowID - ID of the workflow
   * @returns `StepInfo` array listing the executed steps of the workflow. If the workflow is not found, `undefined` is returned.
   */
  static async listWorkflowSteps(workflowID: string): Promise<StepInfo[] | undefined> {
    ensureDBOSIsLaunched('listWorkflowSteps');
    return await runInternalStep(async () => {
      return await DBOS.#executor.listWorkflowSteps(workflowID);
    }, 'DBOS.listWorkflowSteps');
  }

  /**
   * Cancel a workflow given its ID.
   * If the workflow is currently running, `DBOSWorkflowCancelledError` will be
   *   thrown from its next DBOS call.
   * @param workflowID - ID of the workflow
   */
  static async cancelWorkflow(workflowID: string): Promise<void> {
    ensureDBOSIsLaunched('cancelWorkflow');
    return await runInternalStep(async () => {
      return await DBOS.#executor.cancelWorkflow(workflowID);
    }, 'DBOS.cancelWorkflow');
  }

  /**
   * Resume a workflow given its ID.
   * @param workflowID - ID of the workflow
   */
  static async resumeWorkflow<T>(workflowID: string): Promise<WorkflowHandle<Awaited<T>>> {
    ensureDBOSIsLaunched('resumeWorkflow');
    await runInternalStep(async () => {
      return await DBOS.#executor.resumeWorkflow(workflowID);
    }, 'DBOS.resumeWorkflow');
    return this.retrieveWorkflow(workflowID);
  }

  /**
   * Delete a workflow and optionally all its child workflows.
   * This permanently removes the workflow from the system database.
   *
   * WARNING: This operation is irreversible.
   *
   * @param workflowID - ID of the workflow to delete
   * @param deleteChildren - If true, also delete all child workflows recursively (default: false)
   */
  static async deleteWorkflow(workflowID: string, deleteChildren: boolean = false): Promise<void> {
    ensureDBOSIsLaunched('deleteWorkflow');
    return await runInternalStep(async () => {
      return await DBOS.#executor.deleteWorkflow(workflowID, deleteChildren);
    }, 'DBOS.deleteWorkflow');
  }

  /**
   * Fork a workflow given its ID.
   * @param workflowID - ID of the workflow
   * @param startStep - Step ID to start the forked workflow from
   * @param applicationVersion - Version of the application to use for the forked workflow
   * @returns A handle to the forked workflow
   * @throws DBOSInvalidStepIDError if the `startStep` is greater than the maximum step ID of the workflow
   */
  static async forkWorkflow<T>(
    workflowID: string,
    startStep: number,
    options?: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number },
  ): Promise<WorkflowHandle<Awaited<T>>> {
    ensureDBOSIsLaunched('forkWorkflow');
    const forkedID = await runInternalStep(async () => {
      return await DBOS.#executor.forkWorkflow(workflowID, startStep, options);
    }, 'DBOS.forkWorkflow');

    return this.retrieveWorkflow(forkedID);
  }

  /**
   * Sleep for the specified amount of time.
   * If called from within a workflow, the sleep is "durable",
   *   meaning that the workflow will sleep until the wakeup time
   *   (calculated by adding `durationMS` to the original invocation time),
   *   regardless of workflow recovery.
   * @param durationMS - Length of sleep, in milliseconds.
   */
  static async sleepms(durationMS: number): Promise<void> {
    if (DBOS.isWithinWorkflow() && !DBOS.isInStep()) {
      if (DBOS.isInTransaction()) {
        throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.sleep` inside a `transaction`');
      }
      const functionID = functionIDGetIncrement();
      if (durationMS <= 0) {
        return;
      }
      return await DBOSExecutor.globalInstance!.systemDatabase.durableSleepms(DBOS.workflowID!, functionID, durationMS);
    }
    await sleepms(durationMS);
  }
  /** @see sleepms */
  static async sleepSeconds(durationSec: number): Promise<void> {
    return DBOS.sleepms(durationSec * 1000);
  }
  /** @see sleepms */
  static async sleep(durationMS: number): Promise<void> {
    return DBOS.sleepms(durationMS);
  }

  /**
   * Get the current time in milliseconds, similar to `Date.now()`.
   * This function is deterministic and can be used within workflows.
   */
  static async now(): Promise<number> {
    if (DBOS.isInWorkflow()) {
      return runInternalStep(async () => Promise.resolve(Date.now()), 'DBOS.now');
    }
    return Date.now();
  }

  /**
   * Generate a random (v4) UUUID, similar to `node:crypto.randomUUID`.
   * This function is deterministic and can be used within workflows.
   */
  static async randomUUID(): Promise<string> {
    if (DBOS.isInWorkflow()) {
      return runInternalStep(async () => Promise.resolve(randomUUID()), 'DBOS.randomUUID');
    }
    return randomUUID();
  }

  /**
   * Use the provided `workflowID` as the identifier for first workflow started
   *   within the `callback` function.
   * @param workflowID - ID to assign to the first workflow started
   * @param callback - Function to run, which would start a workflow
   * @returns - Return value from `callback`
   */
  static async withNextWorkflowID<R>(workflowID: string, callback: () => Promise<R>): Promise<R> {
    ensureDBOSIsLaunched('workflows');
    return DBOS.#withTopContext({ idAssignedForNextWorkflow: workflowID }, callback);
  }

  /**
   * Use the provided `authedUser` and `authedRoles` as the authenticated user for
   *   any security checks or calls to `DBOS.authenticatedUser`
   *   or `DBOS.authenticatedRoles` placed within the `callback` function.
   * @param authedUser - Authenticated user
   * @param authedRoles - Authenticated roles
   * @param callback - Function to run with authentication context in place
   * @returns - Return value from `callback`
   */
  static async withAuthedContext<R>(authedUser: string, authedRoles: string[], callback: () => Promise<R>): Promise<R> {
    ensureDBOSIsLaunched('auth');
    return DBOS.#withTopContext(
      {
        authenticatedUser: authedUser,
        authenticatedRoles: authedRoles,
      },
      callback,
    );
  }

  /**
   * This generic setter helps users calling DBOS operation to pass a name,
   *   later used in seeding a parent OTel span for the operation.
   * @param callerName - Tracing caller name
   * @param callback - Function to run with tracing context in place
   * @returns - Return value from `callback`
   */
  static async withNamedContext<R>(callerName: string, callback: () => Promise<R>): Promise<R> {
    ensureDBOSIsLaunched('tracing');
    return DBOS.#withTopContext({ operationCaller: callerName }, callback);
  }

  /**
   * Use queue named `queueName` for any workflows started within the `callback`.
   * @param queueName - Name of queue upon which all workflows called or started within `callback` will be run
   * @param callback - Function to run, which would call or start workflows
   * @returns - Return value from `callback`
   */
  static async withWorkflowQueue<R>(queueName: string, callback: () => Promise<R>): Promise<R> {
    ensureDBOSIsLaunched('workflows');
    return DBOS.#withTopContext({ queueAssignedForWorkflows: queueName }, callback);
  }

  /**
   * Specify workflow timeout for any workflows started within the `callback`.
   * @param timeoutMS - timeout length for all workflows started within `callback` will be run
   * @param callback - Function to run, which would call or start workflows
   * @returns - Return value from `callback`
   */
  static async withWorkflowTimeout<R>(timeoutMS: number | null, callback: () => Promise<R>): Promise<R> {
    ensureDBOSIsLaunched('workflows');
    return DBOS.#withTopContext({ workflowTimeoutMS: timeoutMS }, callback);
  }

  /**
   * Run a workflow with the option to set any of the contextual items
   *
   * @param options - Overrides for options
   * @param callback - Function to run, which would call or start workflows
   * @returns - Return value from `callback`
   */
  static async runWithContext<R>(options: DBOSContextOptions, callback: () => Promise<R>): Promise<R> {
    ensureDBOSIsLaunched('contexts');
    return DBOS.#withTopContext(options, callback);
  }

  static async #withTopContext<R>(options: DBOSContextOptions, callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      // Save existing values and overwrite with new; hard to do cleanly but is actually type correct
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const existing: any = {};
      for (const k of Object.keys(options) as (keyof DBOSContextOptions)[]) {
        if (Object.hasOwn(pctx, k))
          // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
          existing[k] = options[k];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
        (pctx as any)[k] = options[k];
      }

      try {
        return await callback();
      } finally {
        for (const k of Object.keys(options) as (keyof DBOSContextOptions)[]) {
          // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
          if (Object.hasOwn(existing, k))
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
            (pctx as any)[k] = existing[k];
          else delete pctx[k];
        }
      }
    } else {
      return await runWithTopContext(options, callback);
    }
  }

  /**
   * Start a workflow in the background, returning a handle that can be used to check status,
   *   await the result, or otherwise interact with the workflow.
   * The full syntax is:
   * `handle = await DBOS.startWorkflow(<target function>, <params>)(<args>);`
   * @param func - The function to start.
   * @param params - `StartWorkflowParams` which may specify the ID, queue, or other parameters for starting the workflow
   * @returns `WorkflowHandle` which can be used to interact with the started workflow
   */
  static startWorkflow<Args extends unknown[], Return>(
    target: (...args: Args) => Promise<Return>,
    params?: StartWorkflowParams,
  ): (...args: Args) => Promise<WorkflowHandle<Return>>;
  /**
   * Start a workflow in the background, returning a handle that can be used to check status, await a result,
   *   or otherwise interact with the workflow.
   * The full syntax is:
   * `handle = await DBOS.startWorkflow(<target object>, <params>).<target method>(<args>);`
   * @param target - Object (which must be a `ConfiguredInstance`) containing the instance method to invoke
   * @param params - `StartWorkflowParams` which may specify the ID, queue, or other parameters for starting the workflow
   * @returns - `WorkflowHandle` which can be used to interact with the workflow
   */
  static startWorkflow<T extends ConfiguredInstance>(
    target: T,
    params?: StartWorkflowParams,
  ): InvokeFunctionsAsyncInst<T>;
  /**
   * Start a workflow in the background, returning a handle that can be used to check status, await a result,
   *   or otherwise interact with the workflow.
   * The full syntax is:
   * `handle = await DBOS.startWorkflow(<target class>, <params>).<target method>(<args>);`
   * @param target - Class containing the static method to invoke
   * @param params - `StartWorkflowParams` which may specify the ID, queue, or other parameters for starting the workflow
   * @returns - `WorkflowHandle` which can be used to interact with the workflow
   */
  static startWorkflow<T extends object>(targetClass: T, params?: StartWorkflowParams): InvokeFunctionsAsync<T>;
  static startWorkflow(
    target: UntypedAsyncFunction | ConfiguredInstance | object,
    params?: StartWorkflowParams,
  ): unknown {
    ensureDBOSIsLaunched('workflows');
    const instance = typeof target === 'function' ? null : (target as ConfiguredInstance);
    if (instance && typeof instance !== 'function' && !(instance instanceof ConfiguredInstance)) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Attempt to call `startWorkflow` on an object that is not a `ConfiguredInstance`',
      );
    }

    if (params && params.queueName) {
      // Validate partition key usage
      const wfqueue = this.#executor.getQueueByName(params.queueName);
      const queuePartitionKey = params.enqueueOptions?.queuePartitionKey;
      if (wfqueue.partitionQueue && !queuePartitionKey) {
        throw Error(`A workflow cannot be enqueued on partitioned queue ${params.queueName} without a partition key`);
      }
      if (queuePartitionKey && !wfqueue.partitionQueue) {
        throw Error(
          `You can only use a partition key on a partition-enabled queue. Key ${queuePartitionKey} was used with non-partitioned queue ${params.queueName}`,
        );
      }
      if (queuePartitionKey && params.enqueueOptions?.deduplicationID) {
        throw Error('Deduplication is not supported for partitioned queues');
      }
    }

    const regOps = getRegisteredOperations(target);

    const handler: ProxyHandler<object> = {
      apply(target, _thisArg, args) {
        const regOp = getFunctionRegistration(target);
        if (!regOp) {
          // eslint-disable-next-line @typescript-eslint/no-base-to-string
          const name = typeof target === 'function' ? target.name : target.toString();
          throw new DBOSNotRegisteredError(name, `${name} is not a registered DBOS workflow function`);
        }
        return DBOS.#invokeWorkflow(instance, regOp, args, params);
      },
      get(target, p, receiver) {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const func = Reflect.get(target, p, receiver);
        const regOp = getFunctionRegistration(func) ?? regOps.find((op) => op.name === p);
        if (regOp) {
          return (...args: unknown[]) => DBOS.#invokeWorkflow(instance, regOp, args, params);
        }

        const name = typeof p === 'string' ? p : String(p);
        throw new DBOSNotRegisteredError(name, `${name} is not a registered DBOS workflow function`);
      },
    };

    return new Proxy(target, handler);
  }

  /**
   * Send `message` on optional `topic` to the workflow with `destinationID`
   *  This can be done from inside or outside of DBOS workflow functions
   *  Use the optional `idempotencyKey` to guarantee that the message is sent exactly once
   * @see `DBOS.recv`
   *
   * @param destinationID - ID of the workflow that will `recv` the message
   * @param message - Message to send, which must be serializable as JSON
   * @param topic - Optional topic; if specified the `recv` command can specify the same topic to receive selectively
   * @param idempotencyKey - Optional key for sending the message exactly once
   * @param options - `SendOptions`
   */
  static async send<T>(
    destinationID: string,
    message: T,
    topic?: string,
    idempotencyKey?: string,
    options?: SendOptions,
  ): Promise<void> {
    ensureDBOSIsLaunched('send');
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.send` inside a `step` or `transaction`');
      }
      if (idempotencyKey) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.send` with an idempotency key from within a workflow',
        );
      }
      const functionID: number = functionIDGetIncrement();
      const sermsg = serializeValue(
        message,
        DBOS.#executor.serializer,
        options?.serializationType ?? DBOS.defaultSerializationType,
      );
      return await DBOSExecutor.globalInstance!.systemDatabase.send(
        DBOS.workflowID!,
        functionID,
        destinationID,
        sermsg.serializedValue,
        topic,
        sermsg.serialization,
      );
    }
    return DBOS.#executor.runSendTempWF(
      destinationID,
      message,
      topic,
      idempotencyKey,
      options?.serializationType ?? DBOS.defaultSerializationType,
    ); // Temp WF variant
  }

  /**
   * Receive a message on optional `topic` from within a workflow.
   *  This must be called from within a workflow; this workflow's ID is used to check for messages sent by `DBOS.send`
   *  This can be configured to time out.
   *  Messages are received in the order in which they are sent (per-sender / causal order).
   * @see `DBOS.send`
   *
   * @param topic - Optional topic; if specified the `recv` command can specify the same topic to receive selectively
   * @param timeoutSeconds - If no message is received before the timeout (default 60 seconds), `null` will be returned
   * @template T - The type of message that is expected to be received
   * @returns Any message received, or `null` if the timeout expires
   */
  static async recv<T>(topic?: string, timeoutSeconds?: number): Promise<T | null> {
    ensureDBOSIsLaunched('recv');
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.recv` inside a `step` or `transaction`');
      }
      const functionID: number = functionIDGetIncrement();
      const timeoutFunctionID: number = functionIDGetIncrement();
      const msg = await DBOSExecutor.globalInstance!.systemDatabase.recv(
        DBOS.workflowID!,
        functionID,
        timeoutFunctionID,
        topic,
        timeoutSeconds,
      );

      return deserializeValue(msg.serializedValue, msg.serialization, DBOS.#executor.serializer) as T;
    }
    throw new DBOSInvalidWorkflowTransitionError('Attempt to call `DBOS.recv` outside of a workflow'); // Only workflows can recv
  }

  /**
   * Set an event, from within a DBOS workflow.  This value can be retrieved with `DBOS.getEvent`.
   * If the event `key` already exists, its `value` is updated.
   * This function can only be called from within a workflow.
   * @see `DBOS.getEvent`
   *
   * @param key - The key for the event; at most one value is associated with a key at any given time.
   * @param value - The value to associate with `key`
   * @param options - `SetEventOptions` allowing control of the recorded event
   */
  static async setEvent<T>(key: string, value: T, options?: SetEventOptions): Promise<void> {
    ensureDBOSIsLaunched('setEvent');
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.setEvent` inside a `step` or `transaction`',
        );
      }
      const functionID = functionIDGetIncrement();
      const serevt = serializeValue(
        value,
        DBOS.#executor.serializer,
        options?.serializationType ?? DBOS.defaultSerializationType,
      );
      return DBOSExecutor.globalInstance!.systemDatabase.setEvent(
        DBOS.workflowID!,
        functionID,
        key,
        serevt.serializedValue,
        serevt.serialization,
      );
    }
    throw new DBOSInvalidWorkflowTransitionError('Attempt to call `DBOS.setEvent` outside of a workflow'); // Only workflows can set event
  }

  /**
   * Get the value of a workflow event, or wait for it to be set.
   * This function can be called inside or outside of DBOS workflow functions.
   * If this function is called from within a workflow, its result is durably checkpointed.
   * @see `DBOS.setEvent`
   *
   * @param workflowID - The ID of the workflow with the corresponding `setEvent`
   * @param key - The key for the event; at most one value is associated with a key at any given time.
   * @param timeoutSeconds - If a value for `key` is not available before the timeout (default 60 seconds), `null` will be returned
   * @template T - The expected type for the value assigned to `key`
   * @returns The value to associate with `key`, or `null` if the timeout is hit
   */
  static async getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null> {
    ensureDBOSIsLaunched('getEvent');
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.getEvent` inside a `step` or `transaction`',
        );
      }
      const functionID: number = functionIDGetIncrement();
      const timeoutFunctionID = functionIDGetIncrement();
      const params = {
        workflowID: DBOS.workflowID!,
        functionID,
        timeoutFunctionID,
      };
      const evt = await DBOSExecutor.globalInstance!.systemDatabase.getEvent(
        workflowID,
        key,
        timeoutSeconds ?? DBOSExecutor.defaultNotificationTimeoutSec,
        params,
      );
      return deserializeValue(evt.serializedValue, evt.serialization, DBOS.#executor.serializer) as T;
    }
    return DBOS.#executor.getEvent(workflowID, key, timeoutSeconds);
  }

  /**
   * Write a value to a stream.
   * @param key - The stream key/name within the workflow
   * @param value - A serializable value to write to the stream
   * @param options - `WriteStreamOptions` for controlling serialization
   */
  static async writeStream<T>(key: string, value: T, options: WriteStreamOptions = {}): Promise<void> {
    ensureDBOSIsLaunched('writeStream');
    if (DBOS.isWithinWorkflow()) {
      const serval = serializeValue(
        value,
        DBOS.#executor.serializer,
        options.serializationType ?? DBOS.defaultSerializationType,
      );
      if (DBOS.isInWorkflow()) {
        const functionID: number = functionIDGetIncrement();
        return await DBOSExecutor.globalInstance!.systemDatabase.writeStreamFromWorkflow(
          DBOS.workflowID!,
          functionID,
          key,
          serval.serializedValue!,
          serval.serialization,
          DBOS_FUNCNAME_WRITESTREAM,
        );
      } else if (DBOS.isInStep()) {
        return await DBOSExecutor.globalInstance!.systemDatabase.writeStreamFromStep(
          DBOS.workflowID!,
          DBOS.stepID!,
          key,
          serval.serializedValue!,
          serval.serialization,
        );
      } else {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.writeStream` outside of a workflow or step',
        );
      }
    } else {
      throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.writeStream` outside of a workflow or step');
    }
  }

  /**
   * Close a stream by writing a sentinel value.
   * @param key - The stream key/name within the workflow
   */
  static async closeStream(key: string): Promise<void> {
    ensureDBOSIsLaunched('closeStream');
    if (DBOS.isWithinWorkflow()) {
      if (DBOS.isInWorkflow()) {
        const functionID: number = functionIDGetIncrement();
        return await DBOSExecutor.globalInstance!.systemDatabase.closeStream(DBOS.workflowID!, functionID, key);
      } else {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.closeStream` outside of a workflow or step',
        );
      }
    } else {
      throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.closeStream` outside of a workflow');
    }
  }

  /**
   * Read values from a stream as an async generator.
   * This function reads values from a stream identified by the workflowID and key,
   * yielding each value in order until the stream is closed or the workflow terminates.
   * @param workflowID - The workflow instance ID that owns the stream
   * @param key - The stream key/name within the workflow
   * @returns An async generator that yields each value in the stream until the stream is closed
   */
  static async *readStream<T>(workflowID: string, key: string): AsyncGenerator<T, void, unknown> {
    ensureDBOSIsLaunched('readStream');
    let offset = 0;

    while (true) {
      try {
        const value = await DBOSExecutor.globalInstance!.systemDatabase.readStream(workflowID, key, offset);
        if (value.serializedValue === DBOS_STREAM_CLOSED_SENTINEL) {
          break;
        }
        yield deserializeValue(
          value.serializedValue,
          value.serialization,
          DBOSExecutor.globalInstance!.serializer,
        ) as T;
        offset += 1;
      } catch (error: unknown) {
        if (error instanceof Error && error.message.includes('No value found')) {
          // Poll the offset until a value arrives or the workflow terminates
          const status = await DBOS.getWorkflowStatus(workflowID);
          if (!status || !isWorkflowActive(status.status)) {
            break;
          }
          await sleepms(1000); // 1 second polling interval
          continue;
        }
        throw error;
      }
    }
  }

  /**
   * registers a workflow method or function with an invocation schedule
   * @param func - The workflow method or function to register with an invocation schedule
   * @param options - Configuration information for the scheduled workflow
   */
  static registerScheduled<This, Return>(
    func: (this: This, ...args: ScheduledArgs) => Promise<Return>,
    config: SchedulerConfig & FunctionName,
  ) {
    ScheduledReceiver.registerScheduled(func, config);
  }

  //////
  // Decorators
  //////

  /**
   * Allow a class to be assigned a name
   */
  static className(name: string) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    function clsdec<T extends { new (...args: any[]): object }>(ctor: T) {
      const clsreg = getClassRegistration(ctor, true);
      if (clsreg.reg?.name && clsreg.reg.name !== name && clsreg.reg.name !== ctor.name) {
        throw new DBOSConflictingRegistrationError(
          `Attempt to assign name ${name} to class ${ctor.name}, which has already been aliased to ${clsreg.reg.name}`,
        );
      }
      clsreg.reg!.name = name;
    }
    return clsdec;
  }

  /**
   * Decorator associating a class static method with an invocation schedule
   * @param config - The schedule, consisting of a crontab and policy for "make-up work"
   */
  static scheduled(config: SchedulerConfig) {
    function methodDecorator<This, Return>(
      target: object,
      propertyKey: PropertyKey,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: ScheduledArgs) => Promise<Return>>,
    ) {
      if (descriptor.value) {
        DBOS.registerScheduled(descriptor.value, {
          ...config,
          ctorOrProto: target,
          name: String(propertyKey),
        });
      }
      return descriptor;
    }
    return methodDecorator;
  }

  /**
   * Decorator designating a method as a DBOS workflow
   *   Durable execution will be applied within calls to the workflow function
   *   This also registers the function so that it is available during recovery
   * @param config - Configuration information for the workflow
   */
  static workflow(config: WorkflowConfig = {}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = wrapDBOSFunctionAndRegisterByTarget(
        target,
        propertyKey,
        config.name ?? propertyKey,
        inDescriptor,
      );
      const invoker = DBOS.#getWorkflowInvoker(registration, config);

      descriptor.value = invoker;
      registration.wrappedFunction = invoker;
      registerFunctionWrapper(invoker, registration);

      return descriptor;
    }
    return decorator;
  }

  /**
   * Create a DBOS workflow function from a provided function.
   *   Similar to the DBOS.workflow, but without requiring a decorator
   *   Durable execution will be applied to calls to the function returned by registerWorkflow
   *   This also registers the function so that it is available during recovery
   * @param func - The function to register as a workflow
   * @param name - The name of the registered workflow
   * @param options - Configuration information for the registered workflow
   */
  static registerWorkflow<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config?: FunctionName & WorkflowConfig,
  ): (this: This, ...args: Args) => Promise<Return> {
    const registration = wrapDBOSFunctionAndRegisterByUniqueName(
      config?.ctorOrProto,
      config?.className,
      config?.name ?? func.name,
      config?.name ?? func.name,
      func,
    );
    return DBOS.#getWorkflowInvoker(registration, config);
  }

  static async #invokeWorkflow<This, Args extends unknown[], Return>(
    $this: This,
    regOP: MethodRegistrationBase,
    args: Args,
    params: StartWorkflowParams = {},
    startWfFuncId?: number,
  ): Promise<InternalWFHandle<Return>> {
    ensureDBOSIsLaunched('workflows');
    const wfId = getNextWFID(params.workflowID);
    const ppctx = getCurrentContextStore();

    const queueName = params.queueName ?? ppctx?.queueAssignedForWorkflows;
    const timeoutMS = params.timeoutMS ?? ppctx?.workflowTimeoutMS;

    const instance = $this === undefined || typeof $this === 'function' ? undefined : ($this as ConfiguredInstance);
    if (instance && !(instance instanceof ConfiguredInstance)) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Attempt to call a `workflow` function on an object that is not a `ConfiguredInstance`',
      );
    }

    // If this is called from within a workflow, this is a child workflow,
    //  For OAOO, we will need a consistent ID formed from the parent WF and call number
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to a `workflow` function from within a `step` or `transaction`',
        );
      }

      const funcId = startWfFuncId ?? functionIDGetIncrement();
      const pctx = getCurrentContextStore()!;
      const pwfid = pctx.workflowId!;
      const wfParams: WorkflowParams = {
        workflowUUID: wfId || pwfid + '-' + funcId,
        configuredInstance: instance,
        queueName,
        timeoutMS,
        // Detach child deadline if a null timeout is configured
        deadlineEpochMS:
          params.timeoutMS === null || pctx?.workflowTimeoutMS === null ? undefined : pctx?.deadlineEpochMS,
        enqueueOptions: params.enqueueOptions,
      };

      return await invokeRegOp(wfParams, pwfid, funcId);
    } else {
      const wfParams: InternalWorkflowParams = {
        workflowUUID: wfId,
        queueName,
        enqueueOptions: params.enqueueOptions,
        configuredInstance: instance,
        timeoutMS,
      };

      return await invokeRegOp(wfParams, undefined, undefined);
    }

    function invokeRegOp(wfParams: WorkflowParams, workflowID: string | undefined, funcNum: number | undefined) {
      if (regOP.workflowConfig) {
        const func = regOP.registeredFunction as TypedAsyncFunction<Args, Return>;
        return DBOSExecutor.globalInstance!.internalWorkflow(func, wfParams, workflowID, funcNum, ...args);
      }
      if (regOP.stepConfig) {
        const func = regOP.registeredFunction as TypedAsyncFunction<Args, Return>;
        return DBOSExecutor.globalInstance!.startStepTempWF(func, wfParams, workflowID, funcNum, ...args);
      }

      throw new DBOSNotRegisteredError(
        regOP.name,
        `${regOP.name} is not a registered DBOS workflow, step, or transaction function`,
      );
    }
  }

  static #getWorkflowInvoker<This, Args extends unknown[], Return>(
    registration: MethodRegistration<This, Args, Return>,
    config: WorkflowConfig | undefined,
  ): (this: This, ...args: Args) => Promise<Return> {
    registration.setWorkflowConfig(config ?? {});
    const invoker = async function (this: This, ...rawArgs: Args): Promise<Return> {
      ensureDBOSIsLaunched('workflows');
      if (DBOS.isInWorkflow()) {
        const startWfFuncId = functionIDGetIncrement();
        const getResFuncID = functionIDGetIncrement();
        const handle = await DBOS.#invokeWorkflow<This, Args, Return>(
          this,
          registration,
          rawArgs,
          undefined,
          startWfFuncId,
        );
        return await handle.getResult(getResFuncID);
      }
      const handle = await DBOS.#invokeWorkflow<This, Args, Return>(this, registration, rawArgs);
      return await handle.getResult();
    };
    registerFunctionWrapper(invoker, registration as MethodRegistration<unknown, unknown[], unknown>);
    Object.defineProperty(invoker, 'name', {
      value: registration.name,
    });
    return invoker;
  }

  /**
   * Decorator designating a method as a DBOS step.
   *   A durable checkpoint will be made after the step completes
   *   This ensures "at least once" execution of the step, and that the step will not
   *    be executed again once the checkpoint is recorded
   *
   * @param config - Configuration information for the step, particularly the retry policy
   */
  static step(config: StepConfig = {}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = wrapDBOSFunctionAndRegisterByTarget(
        target,
        propertyKey,
        config.name,
        inDescriptor,
      );
      registration.setStepConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        ensureDBOSIsLaunched('steps');
        let inst: ConfiguredInstance | undefined = undefined;
        if (this === undefined || typeof this === 'function') {
          // This is static
        } else {
          inst = this as ConfiguredInstance;
          if (!(inst instanceof ConfiguredInstance)) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Attempt to call a `step` function on an object that is not a `ConfiguredInstance`',
            );
          }
        }

        if (DBOS.isWithinWorkflow()) {
          if (DBOS.isInTransaction()) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Invalid call to a `step` function from within a `transaction`',
            );
          }
          if (DBOS.isInStep()) {
            // There should probably be checks here about the compatibility of the StepConfig...
            return registration.registeredFunction!.call(this, ...rawArgs);
          }
          return await DBOSExecutor.globalInstance!.callStepFunction(
            registration.registeredFunction as unknown as TypedAsyncFunction<Args, Return>,
            undefined,
            undefined,
            inst ?? null,
            ...rawArgs,
          );
        }

        const wfId = getNextWFID(undefined);

        const wfParams: WorkflowParams = {
          configuredInstance: inst,
          workflowUUID: wfId,
        };

        return await DBOS.#executor.runStepTempWF(
          registration.registeredFunction as TypedAsyncFunction<Args, Return>,
          wfParams,
          ...rawArgs,
        );
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;
      registerFunctionWrapper(invokeWrapper, registration);

      Object.defineProperty(invokeWrapper, 'name', {
        value: registration.name,
      });

      return descriptor;
    }
    return decorator;
  }

  /**
   * Create a check pointed DBOS step function from  a provided function
   *   Similar to the DBOS.step decorator, but without requiring a decorator
   *   A durable checkpoint will be made after the step completes
   *   This ensures "at least once" execution of the step, and that the step will not
   *    be executed again once the checkpoint is recorded
   * @param func - The function to register as a step
   * @param config - Configuration information for the step, particularly the retry policy and name
   */
  static registerStep<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    config: StepConfig & FunctionName = {},
  ): (this: This, ...args: Args) => Promise<Return> {
    const name = config.name ?? func.name;

    const reg = wrapDBOSFunctionAndRegister(config?.ctorOrProto, config?.className, name, name, func);

    const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
      ensureDBOSIsLaunched('steps');

      // eslint-disable-next-line @typescript-eslint/no-this-alias
      const inst = this;
      const callFunc = reg.registeredFunction ?? reg.origFunction;

      if (DBOS.isWithinWorkflow()) {
        if (DBOS.isInTransaction()) {
          throw new DBOSInvalidWorkflowTransitionError('Invalid call to a `step` function from within a `transaction`');
        }
        if (DBOS.isInStep()) {
          // There should probably be checks here about the compatibility of the StepConfig...
          return callFunc.call(this, ...rawArgs);
        }
        return await DBOSExecutor.globalInstance!.callStepFunction(
          callFunc as TypedAsyncFunction<Args, Return>,
          name,
          config,
          inst ?? null,
          ...rawArgs,
        );
      }

      if (getNextWFID(undefined)) {
        throw new DBOSInvalidWorkflowTransitionError(
          `Invalid call to step '${name}' outside of a workflow; with directive to start a workflow.`,
        );
      }
      return callFunc.call(this, ...rawArgs);
    };

    registerFunctionWrapper(invokeWrapper, reg);

    Object.defineProperty(invokeWrapper, 'name', { value: name });
    return invokeWrapper;
  }

  /**
   * Run the enclosed `callback` as a checkpointed step within a DBOS workflow
   * @param callback - function containing code to run
   * @param config - Configuration information for the step, particularly the retry policy
   * @param config.name - The name of the step; if not provided, the function name will be used
   * @returns - result (either obtained from invoking function, or retrieved if run before)
   */
  static runStep<Return>(func: () => Promise<Return>, config: StepConfig & { name?: string } = {}): Promise<Return> {
    ensureDBOSIsLaunched('steps');

    const name = config.name ?? func.name;
    if (DBOS.isWithinWorkflow()) {
      if (DBOS.isInTransaction()) {
        throw new DBOSInvalidWorkflowTransitionError('Invalid call to a runStep from within a `transaction`');
      }
      if (DBOS.isInStep()) {
        // There should probably be checks here about the compatibility of the StepConfig...
        return func();
      }
      return DBOSExecutor.globalInstance!.callStepFunction<[], Return>(
        func as unknown as TypedAsyncFunction<[], Return>,
        name,
        config,
        null,
      );
    }

    if (getNextWFID(undefined)) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Invalid call to step '${name}' outside of a workflow; with directive to start a workflow.`,
      );
    }

    return func();
  }

  /**
   * Register serialization recipe; this is used to save/retrieve objects from the DBOS system
   *  database.  This includes workflow inputs, function return values, messages, and events.
   */
  static registerSerialization<T, S extends JSONValue>(serReg: SerializationRecipe<T, S>) {
    if (DBOS.isInitialized()) {
      throw new TypeError(`Serializers/deserializers should not be registered after DBOS.launch()`);
    }
    registerSerializationRecipe(serReg);
  }

  /**
   * Decorate a class with the default list of required roles.
   *   This class-level default can be overridden on a per-function basis with `requiredRole`.
   * @param anyOf - The list of roles allowed access; authorization is granted if the authenticated user has any role on the list
   */
  static defaultRequiredRole(anyOf: string[]) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    function clsdec<T extends { new (...args: any[]): object }>(ctor: T) {
      const clsreg = associateClassWithExternal(DBOS_AUTH, ctor) as ClassAuthDefaults;
      clsreg.requiredRole = anyOf;
      registerAuthChecker();
    }
    return clsdec;
  }

  /**
   * Decorate a method with the default list of required roles.
   * @see `DBOS.defaultRequiredRole`
   * @param anyOf - The list of roles allowed access; authorization is granted if the authenticated user has any role on the list
   */
  static requiredRole(anyOf: string[]) {
    function apidec<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const rr = associateMethodWithExternal(DBOS_AUTH, target, undefined, propertyKey.toString(), inDescriptor.value!);

      (rr.regInfo as MethodAuth).requiredRole = anyOf;
      registerAuthChecker();

      inDescriptor.value = rr.registration.wrappedFunction ?? rr.registration.registeredFunction;

      return inDescriptor;
    }
    return apidec;
  }

  /////
  // Patching
  /////

  /**
   * Check if a workflow execution has been patched.
   *
   * Patching allows reexecution of workflows to accommate changes to the workflow logic.
   *
   * Patches check the system database to see which code branch to take.  As this adds overhead,
   *  they may eventually be removed; see `deprecatePatch`.
   *
   * @param patchName Name of the patch to check.
   * @returns true if this is the patched(new) workflow variant, or false if the execution predates the patch
   */
  static async patch(patchName: string): Promise<boolean> {
    if (!DBOS.isInWorkflow()) {
      throw new DBOSInvalidWorkflowTransitionError(
        '`DBOS.patch` must be called from a workflow, and not within a step',
      );
    }

    if (!DBOS.#dbosConfig?.enablePatching) {
      throw new DBOSInvalidWorkflowTransitionError('Patching is not enabled.  See `enablePatching` in `DBOSConfig`');
    }

    const patched = await DBOSExecutor.globalInstance!.systemDatabase.checkPatch(
      DBOS.workflowID!,
      functionIDGet(),
      patchName,
      false,
    );
    if (patched.hasEntry) {
      functionIDGetIncrement();
    }
    return patched.isPatched;
  }

  /**
   * Check if a workflow execution has been patched, within a plan to eventually remove the unpatched (old) variant.
   *
   * `patch` may be changed to `deprecatePatch` after all unpatched workflows have completed and will not be reexecuted.
   * Once all workflows started with `patch` have completed (in favor of those using `deprecatePatch`), the `deprecatePatch` may then be removed.
   *
   * @param patchName Name of the patch to check.
   * @returns true if this is the patched(new) workflow variant, which it should always be if all unpatched workflows have been retired
   */
  static async deprecatePatch(patchName: string): Promise<boolean> {
    if (!DBOS.isInWorkflow()) {
      throw new DBOSInvalidWorkflowTransitionError(
        '`DBOS.deprecatePatch` must be called from a workflow, and not within a step',
      );
    }

    if (!DBOS.#dbosConfig?.enablePatching) {
      throw new DBOSInvalidWorkflowTransitionError('Patching is not enabled.  See `enablePatching` in `DBOSConfig`');
    }

    const patched = await DBOSExecutor.globalInstance!.systemDatabase.checkPatch(
      DBOS.workflowID!,
      functionIDGet(),
      patchName,
      true,
    );
    if (patched.hasEntry) {
      functionIDGetIncrement();
    }
    return patched.isPatched;
  }

  /////
  // Registration, etc
  /////

  /**
   * Register a lifecycle listener
   */
  static registerLifecycleCallback(lcl: DBOSLifecycleCallback) {
    registerLifecycleCallback(lcl);
  }

  /**
   * Register a middleware provider
   */
  static registerMiddlewareInstaller(mwp: DBOSMethodMiddlewareInstaller) {
    registerMiddlewareInstaller(mwp);
  }

  /**
   * Register information to be associated with a DBOS class
   */
  static associateClassWithInfo(external: AnyConstructor | object | string, cls: AnyConstructor | string): object {
    return associateClassWithExternal(external, cls);
  }

  /**
   * Register information to be associated with a DBOS function
   */
  static associateFunctionWithInfo<This, Args extends unknown[], Return>(
    external: AnyConstructor | object | string,
    func: (this: This, ...args: Args) => Promise<Return>,
    target: FunctionName,
  ) {
    return associateMethodWithExternal(external, target.ctorOrProto, target.className, target.name ?? func.name, func);
  }

  /**
   * Register information to be associated with a DBOS function
   */
  static associateParamWithInfo<This, Args extends unknown[], Return>(
    external: AnyConstructor | object | string,
    func: ((this: This, ...args: Args) => Promise<Return>) | undefined,
    target: FunctionName & {
      param: number | string;
    },
  ) {
    return associateParameterWithExternal(
      external,
      target.ctorOrProto,
      target.className,
      target.name ?? func?.name ?? '<unknown>',
      func,
      target.param,
    );
  }

  /** Get registrations */
  static getAssociatedInfo(
    external: AnyConstructor | object | string,
    cls?: object | string,
    funcName?: string,
  ): readonly ExternalRegistration[] {
    return getRegistrationsForExternal(external, cls, funcName);
  }

  /////
  // Alert Handling
  /////

  /**
   * Register an alert handler to receive alerts from DBOS Conductor.
   * Only one handler can be registered, and it must be registered before DBOS.launch().
   * If no handler is registered, alerts will be logged.
   *
   * @param handler - Function to handle incoming alerts
   * @throws Error if DBOS is already initialized or if a handler is already registered
   *
   * @example
   * ```typescript
   * DBOS.setAlertHandler(async (name: string, message: string, metadata: Record<string, string>) => {
   *   console.log(`Alert: ${name} - ${message}`, metadata);
   *   // Send to monitoring service, etc.
   * });
   * ```
   */
  static setAlertHandler(handler: AlertHandler): void {
    if (DBOS.isInitialized()) {
      throw new DBOSError('Cannot set alert handler after DBOS.launch()');
    }
    if (getAlertHandler()) {
      throw new DBOSError('Alert handler is already registered. Only one handler is allowed.');
    }
    setAlertHandler(handler);
  }

  /////
  // Dynamic Workflow Schedules
  /////

  static async createSchedule(options: {
    scheduleName: string;
    workflowFn: ScheduledWorkflowFn;
    schedule: string;
    context?: unknown;
  }): Promise<void> {
    ensureDBOSIsLaunched('createSchedule');
    const { className, name: funcName } = getRegisteredFunctionFullName(options.workflowFn);
    validateCrontab(options.schedule);

    const serializer = DBOSExecutor.globalInstance!.serializer;
    const schedInternal: WorkflowScheduleInternal = {
      scheduleId: createScheduleId(),
      scheduleName: options.scheduleName,
      workflowName: funcName,
      workflowClassName: className,
      schedule: options.schedule,
      status: 'ACTIVE',
      context: serializer.stringify(options.context !== undefined ? options.context : null),
    };

    await runInternalStep(
      () => DBOSExecutor.globalInstance!.systemDatabase.createSchedule(schedInternal),
      'DBOS.createSchedule',
    );
  }

  static async listSchedules(filters?: {
    status?: string;
    workflowName?: string;
    scheduleNamePrefix?: string;
  }): Promise<WorkflowSchedule[]> {
    ensureDBOSIsLaunched('listSchedules');
    const serializer = DBOSExecutor.globalInstance!.serializer;
    const results = await runInternalStep(
      () => DBOSExecutor.globalInstance!.systemDatabase.listSchedules(filters),
      'DBOS.listSchedules',
    );
    return results.map((r) => toWorkflowSchedule(r, serializer));
  }

  static async getSchedule(name: string): Promise<WorkflowSchedule | null> {
    ensureDBOSIsLaunched('getSchedule');
    const serializer = DBOSExecutor.globalInstance!.serializer;
    const result = await runInternalStep(
      () => DBOSExecutor.globalInstance!.systemDatabase.getSchedule(name),
      'DBOS.getSchedule',
    );
    return result ? toWorkflowSchedule(result, serializer) : null;
  }

  static async deleteSchedule(name: string): Promise<void> {
    ensureDBOSIsLaunched('deleteSchedule');
    await runInternalStep(
      () => DBOSExecutor.globalInstance!.systemDatabase.deleteSchedule(name),
      'DBOS.deleteSchedule',
    );
  }

  static async pauseSchedule(name: string): Promise<void> {
    ensureDBOSIsLaunched('pauseSchedule');
    await runInternalStep(
      () => DBOSExecutor.globalInstance!.systemDatabase.setScheduleStatus(name, 'PAUSED'),
      'DBOS.pauseSchedule',
    );
  }

  static async resumeSchedule(name: string): Promise<void> {
    ensureDBOSIsLaunched('resumeSchedule');
    await runInternalStep(
      () => DBOSExecutor.globalInstance!.systemDatabase.setScheduleStatus(name, 'ACTIVE'),
      'DBOS.resumeSchedule',
    );
  }

  static async applySchedules(
    schedules: Array<{
      scheduleName: string;
      workflowFn: ScheduledWorkflowFn;
      schedule: string;
      context?: unknown;
    }>,
  ): Promise<void> {
    ensureDBOSIsLaunched('applySchedules');
    if (DBOS.isWithinWorkflow()) {
      throw new DBOSError('applySchedules cannot be called from within a workflow');
    }

    const serializer = DBOSExecutor.globalInstance!.serializer;
    const internals: WorkflowScheduleInternal[] = [];
    for (const sched of schedules) {
      const { className, name: funcName } = getRegisteredFunctionFullName(sched.workflowFn);
      validateCrontab(sched.schedule);
      internals.push({
        scheduleId: createScheduleId(),
        scheduleName: sched.scheduleName,
        workflowName: funcName,
        workflowClassName: className,
        schedule: sched.schedule,
        status: 'ACTIVE',
        context: serializer.stringify(sched.context !== undefined ? sched.context : null),
      });
    }

    const systemDb = DBOSExecutor.globalInstance!.systemDatabase;
    for (const sched of internals) {
      await systemDb.deleteSchedule(sched.scheduleName);
      await systemDb.createSchedule(sched);
    }
  }

  static async triggerSchedule(name: string): Promise<WorkflowHandle<unknown>> {
    ensureDBOSIsLaunched('triggerSchedule');
    if (DBOS.isWithinWorkflow()) {
      throw new DBOSError('triggerSchedule cannot be called from within a workflow');
    }

    const executor = DBOSExecutor.globalInstance!;
    const sched = await executor.systemDatabase.getSchedule(name);
    if (!sched) {
      throw new DBOSError(`Schedule "${name}" not found`);
    }

    const methReg = getFunctionRegistrationByName(sched.workflowClassName, sched.workflowName);
    if (!methReg || !methReg.registeredFunction) {
      throw new DBOSNotRegisteredError(
        sched.workflowName,
        `Workflow "${sched.workflowClassName}.${sched.workflowName}" for schedule "${name}" is not registered`,
      );
    }

    let context: unknown;
    try {
      context = executor.serializer.parse(sched.context);
    } catch {
      context = null;
    }

    const now = new Date();
    const workflowID = `sched-${name}-trigger-${now.toISOString()}`;
    const wfParams = { workflowID, queueName: INTERNAL_QUEUE_NAME };
    return await DBOS.startWorkflow(methReg.registeredFunction as ScheduledWorkflowFn, wfParams)(now, context);
  }

  static async backfillSchedule(name: string, start: Date, end: Date): Promise<WorkflowHandle<unknown>[]> {
    ensureDBOSIsLaunched('backfillSchedule');
    if (DBOS.isWithinWorkflow()) {
      throw new DBOSError('backfillSchedule cannot be called from within a workflow');
    }

    const executor = DBOSExecutor.globalInstance!;
    const sched = await executor.systemDatabase.getSchedule(name);
    if (!sched) {
      throw new DBOSError(`Schedule "${name}" not found`);
    }

    const methReg = getFunctionRegistrationByName(sched.workflowClassName, sched.workflowName);
    if (!methReg || !methReg.registeredFunction) {
      throw new DBOSNotRegisteredError(
        sched.workflowName,
        `Workflow "${sched.workflowClassName}.${sched.workflowName}" for schedule "${name}" is not registered`,
      );
    }

    let context: unknown;
    try {
      context = executor.serializer.parse(sched.context);
    } catch {
      context = null;
    }

    const timeMatcher = new TimeMatcher(sched.schedule);
    const handles: WorkflowHandle<unknown>[] = [];
    let current = start.getTime();

    while (current < end.getTime()) {
      const next = timeMatcher.nextWakeupTime(current);
      if (next.getTime() >= end.getTime()) {
        break;
      }

      const workflowID = `sched-${name}-${next.toISOString()}`;

      // Idempotency: skip if already exists
      const existing = await DBOS.getWorkflowStatus(workflowID);
      if (!existing) {
        const wfParams = { workflowID, queueName: INTERNAL_QUEUE_NAME };
        const handle = await DBOS.startWorkflow(methReg.registeredFunction as ScheduledWorkflowFn, wfParams)(
          next,
          context,
        );
        handles.push(handle);
      }

      current = next.getTime();
    }

    return handles;
  }
}
