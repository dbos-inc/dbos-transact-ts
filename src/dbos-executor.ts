import {
  DBOSError,
  DBOSInitializationError,
  DBOSWorkflowConflictError,
  DBOSNotRegisteredError,
  DBOSMaxStepRetriesError,
  DBOSWorkflowCancelledError,
  DBOSUnexpectedStepError,
  DBOSInvalidQueuePriorityError,
  DBOSAwaitedWorkflowCancelledError,
  DBOSQueueDuplicatedError,
} from './error';
import {
  InvokedHandle,
  type WorkflowHandle,
  type WorkflowParams,
  RetrievedHandle,
  StatusString,
  type WorkflowStatus,
  type StepInfo,
  WorkflowConfig,
  DEFAULT_MAX_RECOVERY_ATTEMPTS,
  WorkflowSerializationFormat,
} from './workflow';

import { type StepConfig } from './step';
import { TelemetryCollector } from './telemetry/collector';
import { getActiveSpan, runWithTrace, SpanStatusCode, Tracer } from './telemetry/traces';
import { DBOSContextualLogger, GlobalLogger } from './telemetry/logs';
import { TelemetryExporter } from './telemetry/exporters';
import { SystemDatabase, type WorkflowStatusInternal, type SystemDatabaseStoredResult } from './system_database';
import { randomUUID } from 'node:crypto';
import {
  getRegisteredFunctionClassName,
  getRegisteredFunctionName,
  getConfiguredInstance,
  getLifecycleListeners,
  UntypedAsyncFunction,
  TypedAsyncFunction,
  getFunctionRegistrationByName,
  getAllRegisteredFunctions,
  getFunctionRegistration,
  getAllRegisteredClassNames,
  getClassRegistrationByName,
  getRegisteredFunctionFullName,
} from './decorators';
import { JsonWorkflowArgs, type step_info } from '../schemas/system_db_schema';
import {
  runInStepContext,
  getNextWFID,
  functionIDGetIncrement,
  runWithParentContext,
  getCurrentContextStore,
  DBOSLocalCtx,
  runWithTopContext,
} from './context';
import { deserializeError, serializeError } from 'serialize-error';
import { globalParams, sleepms, INTERNAL_QUEUE_NAME, DEBOUNCER_WORKLOW_NAME as DEBOUNCER_WORKLOW_NAME } from './utils';
import {
  DBOSPortableJSON,
  DBOSSerializer,
  deserializePositionalArgs,
  deserializeResError,
  deserializeValue,
  serializeFunctionInputOutput,
  serializeFunctionInputOutputWithSerializer,
  serializeResError,
  serializeResErrorWithSerializer,
  serializeValue,
} from './serialization';
import { DBOS, GetWorkflowsInput } from '.';

import { wfQueueRunner, WorkflowQueue } from './wfqueue';
import { debugTriggerPoint, DEBUG_TRIGGER_WORKFLOW_ENQUEUE } from './debugpoint';
import { ScheduledReceiver } from './scheduler/scheduler_decorator';
import { DynamicSchedulerLoop } from './scheduler/scheduler';
import * as crypto from 'crypto';
import {
  forkWorkflow,
  listQueuedWorkflows,
  listWorkflows,
  listWorkflowSteps,
  toWorkflowStatus,
} from './workflow_management';
import { maskDatabaseUrl } from './database_utils';
import { debouncerWorkflowFunction } from './debouncer';
import { Pool } from 'pg';

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface DBOSNull {}
const dbosNull: DBOSNull = {};

export const DBOS_QUEUE_MIN_PRIORITY = 1;
export const DBOS_QUEUE_MAX_PRIORITY = 2 ** 31 - 1; // 2,147,483,647

/* Interface for DBOS configuration */
export interface DBOSConfig {
  name?: string;

  systemDatabaseUrl?: string;
  systemDatabasePoolSize?: number;
  systemDatabasePool?: Pool;
  systemDatabaseSchemaName?: string;

  enableOTLP?: boolean;
  logLevel?: string;
  addContextMetadata?: boolean;
  otlpTracesEndpoints?: string[];
  otlpLogsEndpoints?: string[];

  adminPort?: number;
  runAdminServer?: boolean;

  applicationVersion?: string;
  executorID?: string;

  serializer?: DBOSSerializer;
  enablePatching?: boolean;
  listenQueues?: WorkflowQueue[];
  schedulerPollingIntervalMs?: number;
  useListenNotify?: boolean;
}

export interface DBOSRuntimeConfig {
  admin_port: number;
  runAdminServer: boolean;
  start: string[];
  setup: string[];
}

export interface TelemetryConfig {
  logs?: LoggerConfig;
  OTLPExporter?: OTLPExporterConfig;
}

export interface OTLPExporterConfig {
  logsEndpoint?: string[];
  tracesEndpoint?: string[];
}

export interface LoggerConfig {
  logLevel?: string;
  silent?: boolean;
  addContextMetadata?: boolean;
  forceConsole?: boolean;
}

export type DBOSConfigInternal = {
  name?: string;

  systemDatabaseUrl: string;
  sysDbPoolSize?: number;
  systemDatabasePool?: Pool;
  systemDatabaseSchemaName: string;
  serializer: DBOSSerializer;

  telemetry: TelemetryConfig;

  schedulerPollingIntervalMs?: number;
  useListenNotify: boolean;

  http?: {
    cors_middleware?: boolean;
    credentials?: boolean;
    allowed_origins?: string[];
  };
};

export interface InternalWorkflowParams extends WorkflowParams {
  readonly tempWfType?: string;
  readonly tempWfName?: string;
  readonly tempWfClass?: string;
  readonly isRecoveryDispatch?: boolean;
  readonly isQueueDispatch?: boolean;
}

export const OperationType = {
  HANDLER: 'handler',
  WORKFLOW: 'workflow',
  TRANSACTION: 'transaction',
  STEP: 'step',
} as const;

export const TempWorkflowType = {
  step: 'step',
  send: 'send',
} as const;

/**
 * State item to be kept in the DBOS system database on behalf of clients
 */
export interface DBOSExternalState {
  /** Name of event receiver service */
  service: string;
  /** Fully qualified function name for which state is kept */
  workflowFnName: string;
  /** subkey within the service+workflowFnName */
  key: string;
  /** Value kept for the service+workflowFnName+key combination */
  value?: string;
  /** Updated time (used to version the value) */
  updateTime?: number;
  /** Updated sequence number (used to version the value) */
  updateSeq?: bigint;
}

export interface DBOSExecutorOptions {
  systemDatabase?: SystemDatabase;
}

export class DBOSExecutor {
  initialized: boolean;
  // System Database
  readonly systemDatabase: SystemDatabase;

  // Temporary workflows are created by calling transaction/send/recv directly from the executor class
  static readonly #tempWorkflowName = 'temp_workflow';

  readonly telemetryCollector: TelemetryCollector;

  static readonly defaultNotificationTimeoutSec = 60;

  readonly systemDBSchemaName: string;

  readonly logger: GlobalLogger;
  readonly ctxLogger: DBOSContextualLogger;
  readonly tracer: Tracer;
  readonly serializer: DBOSSerializer;

  #wfqEnded?: Promise<void> = undefined;

  readonly executorID: string = globalParams.executorID;

  static globalInstance: DBOSExecutor | undefined = undefined;

  /* WORKFLOW EXECUTOR LIFE CYCLE MANAGEMENT */
  constructor(
    readonly config: DBOSConfigInternal,
    { systemDatabase }: DBOSExecutorOptions = {},
  ) {
    this.systemDBSchemaName = config.systemDatabaseSchemaName;

    if (config.telemetry.OTLPExporter) {
      const OTLPExporter = new TelemetryExporter(config.telemetry.OTLPExporter);
      this.telemetryCollector = new TelemetryCollector(OTLPExporter);
    } else {
      // We always setup a collector to drain the signals queue, even if we don't have an exporter.
      this.telemetryCollector = new TelemetryCollector();
    }
    this.logger = new GlobalLogger(this.telemetryCollector, this.config.telemetry.logs, this.appName);
    this.ctxLogger = new DBOSContextualLogger(this.logger, () => getActiveSpan());
    this.tracer = new Tracer(this.telemetryCollector, this.appName);
    this.serializer = config.serializer;

    if (systemDatabase) {
      this.logger.debug('Using provided system database'); // XXX print the name or something
      this.systemDatabase = systemDatabase;
    } else {
      this.logger.debug('Using Postgres system database');
      this.systemDatabase = new SystemDatabase(
        this.config.systemDatabaseUrl,
        this.logger,
        this.serializer,
        this.config.sysDbPoolSize,
        this.config.systemDatabasePool,
        this.systemDBSchemaName,
        this.config.useListenNotify,
      );
    }

    new ScheduledReceiver(); // Create the scheduler, which registers itself.
    new DynamicSchedulerLoop(config.schedulerPollingIntervalMs); // Create the dynamic scheduler, which registers itself.

    this.initialized = false;
    DBOSExecutor.globalInstance = this;
  }

  get appName(): string | undefined {
    return this.config.name;
  }

  async init(): Promise<void> {
    if (this.initialized) {
      this.logger.error('Workflow executor already initialized!');
      return;
    }
    try {
      await this.systemDatabase.init();
    } catch (err) {
      if (err instanceof DBOSInitializationError) {
        throw err;
      }
      this.logger.error(err);
      let message = 'Failed to initialize workflow executor: ';
      if (err instanceof AggregateError) {
        for (const error of err.errors as Error[]) {
          message += `${error.message}; `;
        }
      } else if (err instanceof Error) {
        message += err.message;
      } else {
        message += String(err);
      }
      throw new DBOSInitializationError(message, err instanceof Error ? err : undefined);
    }
    this.initialized = true;

    // Compute the application version if not provided
    if (globalParams.appVersion === '') {
      globalParams.appVersion = this.computeAppVersion();
      globalParams.wasComputed = true;
    }

    // Any initialization hooks
    const classnames = getAllRegisteredClassNames();
    for (const cls of classnames) {
      // Init its configurations
      const creg = getClassRegistrationByName(cls);
      for (const [_cfgname, cfg] of creg.configuredInstances) {
        await cfg.initialize();
      }
    }

    this.logger.info(`Initializing DBOS (v${globalParams.dbosVersion})`);
    this.logger.info(`System Database URL: ${maskDatabaseUrl(this.config.systemDatabaseUrl)}`);
    this.logger.info(`Executor ID: ${this.executorID}`);
    this.logger.info(`Application version: ${globalParams.appVersion}`);

    await this.recoverPendingWorkflows([this.executorID]);

    this.logger.info('DBOS launched!');
  }

  async destroy() {
    try {
      await this.systemDatabase.awaitRunningWorkflows();
      await this.systemDatabase.destroy();
      await this.logger.destroy();
    } catch (err) {
      const e = err as Error;
      this.logger.error(e);
      throw err;
    }
  }

  // This could return WF, or the function underlying a temp wf
  #getFunctionInfoFromWFStatus(wf: WorkflowStatusInternal) {
    const methReg = getFunctionRegistrationByName(wf.workflowClassName, wf.workflowName);
    return { methReg, configuredInst: getConfiguredInstance(wf.workflowClassName, wf.workflowConfigName) };
  }

  static reviveResultOrError<R = unknown>(r: SystemDatabaseStoredResult, serializer: DBOSSerializer): R {
    if (r.error) {
      throw deserializeResError(r.error, r.serialization ?? null, serializer);
    }
    return deserializeValue(r.output ?? null, r.serialization ?? null, serializer) as R;
  }

  async workflow<T extends unknown[], R>(
    wf: TypedAsyncFunction<T, R>,
    params: InternalWorkflowParams,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    return this.internalWorkflow(wf, params, undefined, undefined, ...args);
  }

  // If callerWFID and functionID are set, it means the workflow is invoked from within a workflow.
  async internalWorkflow<T extends unknown[], R>(
    wf: TypedAsyncFunction<T, R>,
    params: InternalWorkflowParams,
    callerID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    const workflowID: string = params.workflowUUID ? params.workflowUUID : randomUUID();
    const presetID: boolean = params.workflowUUID ? true : false;
    const timeoutMS = params.timeoutMS ?? undefined;
    // If a timeout is explicitly specified, use it over any propagated deadline
    const deadlineEpochMS = params.timeoutMS
      ? // Queued workflows are assigned a deadline on dequeue. Otherwise, compute the deadline immediately
        params.queueName
        ? undefined
        : Date.now() + params.timeoutMS
      : // if no timeout is specified, use the propagated deadline (if any)
        params.deadlineEpochMS;

    const priority = params?.enqueueOptions?.priority;
    if (priority !== undefined && (priority < DBOS_QUEUE_MIN_PRIORITY || priority > DBOS_QUEUE_MAX_PRIORITY)) {
      throw new DBOSInvalidQueuePriorityError(priority, DBOS_QUEUE_MIN_PRIORITY, DBOS_QUEUE_MAX_PRIORITY);
    }

    // If the workflow is called on a queue with a priority but the queue is not configured with a priority, print a warning.
    if (params.queueName) {
      const wfqueue = this.getQueueByName(params.queueName);
      if (!wfqueue.priorityEnabled && priority !== undefined) {
        throw Error(
          `Priority is not enabled for queue ${params.queueName}. Setting priority will not have any effect.`,
        );
      }
    }

    const pctx = { ...getCurrentContextStore() }; // function ID was already incremented...

    let wConfig: WorkflowConfig = {};
    const wInfo = getFunctionRegistration(wf);
    const wfNames = getRegisteredFunctionFullName(wf);
    let wfname = wfNames.name;
    let wfclassname = wfNames.className;

    const isTempWorkflow = DBOSExecutor.#tempWorkflowName === wfname || !!params.tempWfType;

    if (!isTempWorkflow) {
      if (!wInfo || !wInfo.workflowConfig) {
        throw new DBOSNotRegisteredError(wf.name);
      }
      wConfig = wInfo.workflowConfig;
    } else if (params.tempWfName) {
      wfname = params.tempWfName;
      wfclassname = params.tempWfClass ?? '';
    }

    const maxRecoveryAttempts = wConfig.maxRecoveryAttempts
      ? wConfig.maxRecoveryAttempts
      : DEFAULT_MAX_RECOVERY_ATTEMPTS;

    const span = this.tracer.startSpan(wfname, {
      status: StatusString.PENDING,
      operationUUID: workflowID,
      operationType: OperationType.WORKFLOW,
      operationName: wInfo?.name ?? wf.name,
      authenticatedUser: pctx?.authenticatedUser ?? '',
      authenticatedRoles: pctx?.authenticatedRoles ?? [],
      assumedRole: pctx?.assumedRole ?? '',
    });

    let serializationType = wInfo?.workflowConfig?.serialization;
    const funcArgs = serializeFunctionInputOutput(
      serializationType === 'portable' ? ({ positionalArgs: args } as JsonWorkflowArgs) : args,
      [wfname, '<arguments>'],
      this.serializer,
      serializationType,
    );
    args =
      serializationType === 'portable'
        ? ((funcArgs.deserialized as JsonWorkflowArgs).positionalArgs! as T)
        : (funcArgs.deserialized as T);

    const internalStatus: WorkflowStatusInternal = {
      workflowUUID: workflowID,
      status: params.queueName !== undefined ? StatusString.ENQUEUED : StatusString.PENDING,
      workflowName: wfname,
      workflowClassName: wfclassname,
      workflowConfigName: params.configuredInstance?.name || '',
      queueName: params.queueName,
      output: null,
      error: null,
      authenticatedUser: pctx?.authenticatedUser || '',
      assumedRole: pctx?.assumedRole || '',
      authenticatedRoles: pctx?.authenticatedRoles || [],
      request: pctx?.request || {},
      executorId: globalParams.executorID,
      applicationVersion: globalParams.appVersion,
      applicationID: globalParams.appID,
      createdAt: Date.now(), // Remember the start time of this workflow,
      timeoutMS: timeoutMS,
      deadlineEpochMS: deadlineEpochMS,
      input: funcArgs.stringified,
      deduplicationID: params.enqueueOptions?.deduplicationID,
      priority: priority ?? 0,
      queuePartitionKey: params.enqueueOptions?.queuePartitionKey,
      parentWorkflowID: callerID,
      serialization: funcArgs.sername,
    };

    if (isTempWorkflow) {
      internalStatus.workflowName = `${DBOSExecutor.#tempWorkflowName}-${params.tempWfType}-${params.tempWfName}`;
    }

    let $deadlineEpochMS: number | undefined = undefined;
    let shouldExecute: boolean | undefined = undefined;

    // Synchronously set the workflow's status to PENDING and record workflow inputs.
    // We have to do it for all types of workflows because operation_outputs table has a foreign key constraint on workflow status table.
    if (callerFunctionID !== undefined && callerID !== undefined) {
      const result = await this.systemDatabase.getOperationResultAndThrowIfCancelled(callerID, callerFunctionID);
      if (result) {
        if (result.error) {
          throw deserializeResError(result.error, result.serialization ?? null, this.serializer);
        }
        return new RetrievedHandle(this.systemDatabase, result.childWorkflowID!);
      }
    }
    let ires: Awaited<ReturnType<SystemDatabase['initWorkflowStatus']>>;
    try {
      ires = await this.systemDatabase.initWorkflowStatus(internalStatus, randomUUID(), {
        maxRetries: maxRecoveryAttempts,
        isDequeuedRequest: params.isQueueDispatch,
        isRecoveryRequest: params.isRecoveryDispatch,
      });
      serializationType = ires.serialization === DBOSPortableJSON.name() ? 'portable' : undefined;
    } catch (e) {
      if (e instanceof DBOSQueueDuplicatedError && callerID && callerFunctionID) {
        const sererr = serializeResError(e, this.serializer, undefined); // This is a step result
        await this.systemDatabase.recordOperationResult(
          callerID,
          callerFunctionID,
          internalStatus.workflowName,
          true,
          Date.now(),
          { error: sererr.serializedValue, serialization: sererr.serialization },
        );
      }
      throw e;
    }

    if (callerFunctionID !== undefined && callerID !== undefined) {
      await this.systemDatabase.recordOperationResult(
        callerID,
        callerFunctionID,
        internalStatus.workflowName,
        true,
        Date.now(),
        {
          childWorkflowID: workflowID,
        },
      );
    }

    $deadlineEpochMS = ires.deadlineEpochMS;
    shouldExecute = ires.shouldExecuteOnThisExecutor;
    await debugTriggerPoint(DEBUG_TRIGGER_WORKFLOW_ENQUEUE);

    async function callPromiseWithTimeout(
      callPromise: Promise<R>,
      deadlineEpochMS: number,
      sysdb: SystemDatabase,
    ): Promise<R> {
      let timeoutID: ReturnType<typeof setTimeout> | undefined = undefined;
      const timeoutResult = {};
      const timeoutPromise = new Promise<R>((_, reject) => {
        timeoutID = setTimeout(reject, deadlineEpochMS - Date.now(), timeoutResult);
      });

      try {
        return await Promise.race([callPromise, timeoutPromise]);
      } catch (err) {
        if (err === timeoutResult) {
          await sysdb.cancelWorkflow(workflowID);
          await callPromise.catch(() => {});
          throw new DBOSWorkflowCancelledError(workflowID);
        }
        throw err;
      } finally {
        clearTimeout(timeoutID);
      }
    }

    const eserializer = this.serializer;
    async function handleWorkflowError(err: Error, exec: DBOSExecutor) {
      // Record the error.
      const e = err as Error & { dbos_already_logged?: boolean };
      exec.logger.error(e);
      e.dbos_already_logged = true;
      const sererr = serializeResErrorWithSerializer(e, eserializer, ires.serialization ?? null);
      internalStatus.error = sererr.serializedValue;
      internalStatus.status = StatusString.ERROR;
      await exec.systemDatabase.recordWorkflowError(workflowID, internalStatus);
      span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
      return deserializeResError(sererr.serializedValue, sererr.serialization, eserializer);
    }

    const runWorkflow = async () => {
      let result: R;

      // Execute the workflow.
      try {
        const callResult = await runWithTrace(span, async () => {
          return await runWithParentContext(
            pctx,
            {
              presetID,
              workflowTimeoutMS: undefined, // Becomes deadline
              deadlineEpochMS,
              workflowId: workflowID,
              logger: this.ctxLogger,
              curWFFunctionId: undefined,
              serializationType,
            },
            () => {
              const callPromise = wf.call(params.configuredInstance, ...args);

              if ($deadlineEpochMS === undefined) {
                return callPromise;
              } else {
                return callPromiseWithTimeout(callPromise, $deadlineEpochMS, this.systemDatabase);
              }
            },
          );
        });

        result = callResult!;

        const funcResult = serializeFunctionInputOutputWithSerializer(
          result,
          [wfname, '<result>'],
          this.serializer,
          ires.serialization,
        );
        result = funcResult.deserialized;
        internalStatus.output = funcResult.stringified;
        internalStatus.status = StatusString.SUCCESS;
        await this.systemDatabase.recordWorkflowOutput(workflowID, internalStatus);
        span.setStatus({ code: SpanStatusCode.OK });
      } catch (err) {
        if (err instanceof DBOSWorkflowConflictError) {
          // Retrieve the handle and wait for the result.
          const retrievedHandle = this.retrieveWorkflow<R>(workflowID);
          result = await retrievedHandle.getResult();
          span.setAttribute('cached', true);
          span.setStatus({ code: SpanStatusCode.OK });
        } else if (err instanceof DBOSWorkflowCancelledError) {
          span.setStatus({ code: SpanStatusCode.ERROR, message: err.message });
          internalStatus.error = err.message;
          if (err.workflowID === workflowID) {
            internalStatus.status = StatusString.CANCELLED;
            throw err;
          } else {
            const e = new DBOSAwaitedWorkflowCancelledError(err.workflowID);
            await handleWorkflowError(e as Error, this);
            throw e;
          }
        } else {
          // If we want to be consistent about what is thrown (stored result vs live)
          //  we would have to do this.  It is a breaking change in the sense that it
          //  is a behavior change, but it would "break" things that are already broken
          if (serializationType === 'portable') {
            throw await handleWorkflowError(err as Error, this);
          }
          await handleWorkflowError(err as Error, this);
          throw err;
        }
      } finally {
        this.tracer.endSpan(span);
      }
      return result;
    };

    if (
      shouldExecute &&
      (params.queueName === undefined || params.executeWorkflow) &&
      !this.systemDatabase.checkForRunningWorkflow(workflowID)
    ) {
      const workflowPromise: Promise<R> = runWorkflow();

      this.systemDatabase.registerRunningWorkflow(workflowID, workflowPromise);

      // Return the normal handle that doesn't capture errors.
      return new InvokedHandle(this.systemDatabase, workflowPromise, workflowID, wf.name);
    } else {
      return new RetrievedHandle(this.systemDatabase, workflowID);
    }
  }

  getQueueByName(name: string): WorkflowQueue {
    const q = wfQueueRunner.wfQueuesByName.get(name);
    if (!q) throw new DBOSNotRegisteredError(name, `Workflow queue '${name}' is not defined.`);
    return q;
  }

  async runStepTempWF<T extends unknown[], R>(
    stepFn: TypedAsyncFunction<T, R>,
    params: WorkflowParams,
    ...args: T
  ): Promise<R> {
    return await (await this.startStepTempWF(stepFn, params, undefined, undefined, ...args)).getResult();
  }

  async startStepTempWF<T extends unknown[], R>(
    stepFn: TypedAsyncFunction<T, R>,
    params: InternalWorkflowParams,
    callerWFID?: string,
    callerFunctionID?: number,
    ...args: T
  ): Promise<WorkflowHandle<R>> {
    // Create a workflow and call external.
    const temp_workflow = async (...args: T) => {
      return await this.callStepFunction(stepFn, undefined, undefined, params.configuredInstance ?? null, ...args);
    };

    return await this.internalWorkflow(
      temp_workflow,
      {
        ...params,
        tempWfType: TempWorkflowType.step,
        tempWfName: getRegisteredFunctionName(stepFn),
        tempWfClass: getRegisteredFunctionClassName(stepFn),
      },
      callerWFID,
      callerFunctionID,
      ...args,
    );
  }

  /**
   * Execute a step function.
   * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
   * The step may execute many times, but once it is complete, it will not re-execute.
   */
  async callStepFunction<T extends unknown[], R>(
    stepFn: TypedAsyncFunction<T, R>,
    stepFnName: string | undefined,
    stepConfig: StepConfig | undefined,
    clsInst: object | null,
    ...args: T
  ): Promise<R> {
    stepFnName = stepFnName ?? stepFn.name ?? '<unnamed>';
    const startTime = Date.now();
    if (!stepConfig) {
      const stepReg = getFunctionRegistration(stepFn);
      stepConfig = stepReg?.stepConfig;
    }
    if (stepConfig === undefined) {
      throw new DBOSNotRegisteredError(stepFnName);
    }

    // Intentionally advance the function ID before any awaits, then work with a copy of the context.
    const funcID = functionIDGetIncrement();
    const lctx = { ...getCurrentContextStore()! };
    const wfid = lctx.workflowId!;

    await this.systemDatabase.checkIfCanceled(wfid);

    const maxRetryIntervalSec = 3600; // Maximum retry interval: 1 hour

    const span = this.tracer.startSpan(stepFnName, {
      operationUUID: wfid,
      operationType: OperationType.STEP,
      operationName: stepFnName,
      authenticatedUser: lctx.authenticatedUser ?? '',
      assumedRole: lctx.assumedRole ?? '',
      authenticatedRoles: lctx.authenticatedRoles ?? [],
      retriesAllowed: stepConfig.retriesAllowed,
      intervalSeconds: stepConfig.intervalSeconds,
      maxAttempts: stepConfig.maxAttempts,
      backoffRate: stepConfig.backoffRate,
    });

    // Check if this execution previously happened, returning its original result if it did.
    const checkr = await this.systemDatabase.getOperationResultAndThrowIfCancelled(wfid, funcID);
    if (checkr) {
      if (checkr.functionName !== stepFnName) {
        throw new DBOSUnexpectedStepError(wfid, funcID, stepFnName, checkr.functionName ?? '?');
      }
      const check = DBOSExecutor.reviveResultOrError<R>(checkr, this.serializer);
      span.setAttribute('cached', true);
      span.setStatus({ code: SpanStatusCode.OK });
      this.tracer.endSpan(span);
      return check;
    }

    const maxAttempts = stepConfig.maxAttempts ?? 3;

    // Execute the step function.  If it throws an exception, retry with exponential backoff.
    // After reaching the maximum number of retries, throw an DBOSError.
    let result: R | DBOSNull = dbosNull;
    let err: Error | DBOSNull = dbosNull;
    const errors: Error[] = [];
    if (stepConfig.retriesAllowed) {
      let attemptNum = 0;
      let intervalSeconds: number = stepConfig.intervalSeconds ?? 1;
      if (intervalSeconds > maxRetryIntervalSec) {
        this.logger.warn(
          `Step config interval exceeds maximum allowed interval, capped to ${maxRetryIntervalSec} seconds!`,
        );
      }
      while (result === dbosNull && attemptNum++ < (maxAttempts ?? 3)) {
        try {
          await this.systemDatabase.checkIfCanceled(wfid);

          let cresult: R | undefined;
          await runWithTrace(span, async () => {
            await runInStepContext(lctx, funcID, maxAttempts, attemptNum, async () => {
              const sf = stepFn as unknown as (...args: T) => Promise<R>;
              cresult = await sf.call(clsInst, ...args);
            });
          });
          result = cresult!;
        } catch (error) {
          const e = error as Error;
          errors.push(e);
          this.logger.warn(
            `Error in step being automatically retried. Attempt ${attemptNum} of ${maxAttempts}. ${e.stack}`,
          );
          span.addEvent(
            `Step attempt ${attemptNum + 1} failed`,
            { retryIntervalSeconds: intervalSeconds, error: (error as Error).message },
            performance.now(),
          );
          if (attemptNum < maxAttempts) {
            // Sleep for an interval, then increase the interval by backoffRate.
            // Cap at the maximum allowed retry interval.
            await sleepms(intervalSeconds * 1000);
            intervalSeconds *= stepConfig.backoffRate ?? 2;
            intervalSeconds = intervalSeconds < maxRetryIntervalSec ? intervalSeconds : maxRetryIntervalSec;
          }
        }
      }
    } else {
      try {
        let cresult: R | undefined;
        await runWithTrace(span, async () => {
          await runInStepContext(lctx, funcID, maxAttempts, undefined, async () => {
            const sf = stepFn as unknown as (...args: T) => Promise<R>;
            cresult = await sf.call(clsInst, ...args);
          });
        });
        result = cresult!;
      } catch (error) {
        err = error as Error;
      }
    }

    // `result` can only be dbosNull when the step timed out
    if (result === dbosNull) {
      // Record the error, then throw it.
      err = err === dbosNull ? new DBOSMaxStepRetriesError(stepFnName, maxAttempts, errors) : err;
      await this.systemDatabase.recordOperationResult(wfid, funcID, stepFnName, true, startTime, {
        error: this.serializer.stringify(serializeError(err)),
        serialization: this.serializer.name(),
      });
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      this.tracer.endSpan(span);
      throw err as Error;
    } else {
      // Record the execution and return.
      const funcResult = serializeFunctionInputOutput(result, [stepFnName, '<result>'], this.serializer);
      await this.systemDatabase.recordOperationResult(wfid, funcID, stepFnName, true, startTime, {
        output: funcResult.stringified,
        serialization: funcResult.sername,
      });
      span.setStatus({ code: SpanStatusCode.OK });
      this.tracer.endSpan(span);
      return funcResult.deserialized as R;
    }
  }

  /**
   * Wait for a workflow to emit an event, then return its value.
   */
  async getEvent<T>(
    workflowUUID: string,
    key: string,
    timeoutSeconds: number = DBOSExecutor.defaultNotificationTimeoutSec,
  ): Promise<T | null> {
    const evt = await this.systemDatabase.getEvent(workflowUUID, key, timeoutSeconds);
    return deserializeValue(evt.serializedValue, evt.serialization, this.serializer) as T;
  }

  /**
   * Fork a workflow.
   * The forked workflow will be assigned a new ID.
   */
  forkWorkflow(
    workflowID: string,
    startStep: number,
    options: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number } = {},
  ): Promise<string> {
    const newWorkflowID = options.newWorkflowID ?? getNextWFID(undefined);
    return forkWorkflow(this.systemDatabase, workflowID, startStep, { ...options, newWorkflowID });
  }

  /**
   * Retrieve a handle for a workflow UUID.
   */
  retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R> {
    return new RetrievedHandle(this.systemDatabase, workflowID);
  }

  async runInternalStep<T>(
    callback: () => Promise<T>,
    functionName: string,
    workflowID: string,
    functionID: number,
    childWfId?: string,
  ): Promise<T> {
    const startTime = Date.now();
    const result = await this.systemDatabase.getOperationResultAndThrowIfCancelled(workflowID, functionID);
    if (result) {
      if (result.functionName !== functionName) {
        throw new DBOSUnexpectedStepError(workflowID, functionID, functionName, result.functionName!);
      }
      return DBOSExecutor.reviveResultOrError<T>(result, this.serializer);
    }
    try {
      const output: T = await callback();
      const funcOutput = serializeFunctionInputOutput(output, [functionName, '<result>'], this.serializer);
      await this.systemDatabase.recordOperationResult(workflowID, functionID, functionName, true, startTime, {
        output: funcOutput.stringified,
        childWorkflowID: childWfId,
      });
      return funcOutput.deserialized;
    } catch (e) {
      await this.systemDatabase.recordOperationResult(workflowID, functionID, functionName, false, startTime, {
        error: this.serializer.stringify(serializeError(e)),
        childWorkflowID: childWfId,
      });

      throw e;
    }
  }

  async getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null> {
    // use sysdb getWorkflowStatus directly in order to support caller ID/FN params
    const status = await this.systemDatabase.getWorkflowStatus(workflowID, callerID, callerFN);
    return status ? toWorkflowStatus(status, this.serializer) : null;
  }

  async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    return listWorkflows(this.systemDatabase, input);
  }

  async listQueuedWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    return listQueuedWorkflows(this.systemDatabase, input);
  }

  async listWorkflowSteps(workflowID: string): Promise<StepInfo[] | undefined> {
    return listWorkflowSteps(this.systemDatabase, workflowID);
  }

  /* INTERNAL HELPERS */
  /**
   * A recovery process that by default runs during executor init time.
   * It runs to completion all pending workflows that were executing when the previous executor failed.
   */
  async recoverPendingWorkflows(executorIDs: string[] = ['local']): Promise<WorkflowHandle<unknown>[]> {
    const handlerArray: WorkflowHandle<unknown>[] = [];
    for (const execID of executorIDs) {
      this.logger.debug(`Recovering workflows assigned to executor: ${execID}`);
      const pendingWorkflows = await this.systemDatabase.getPendingWorkflows(execID, globalParams.appVersion);
      if (pendingWorkflows.length > 0) {
        this.logger.info(
          `Recovering ${pendingWorkflows.length} workflows from application version ${globalParams.appVersion}`,
        );
      } else {
        this.logger.info(`No workflows to recover from application version ${globalParams.appVersion}`);
      }
      for (const pendingWorkflow of pendingWorkflows) {
        this.logger.debug(
          `Recovering workflow: ${pendingWorkflow.workflowUUID}. Queue name: ${pendingWorkflow.queueName}`,
        );
        try {
          // If the workflow is member of a queue, re-enqueue it.
          if (pendingWorkflow.queueName) {
            const cleared = await this.systemDatabase.clearQueueAssignment(pendingWorkflow.workflowUUID);
            if (cleared) {
              handlerArray.push(this.retrieveWorkflow(pendingWorkflow.workflowUUID));
            } else {
              handlerArray.push(
                await this.executeWorkflowId(pendingWorkflow.workflowUUID, { isRecoveryDispatch: true }),
              );
            }
          } else {
            handlerArray.push(await this.executeWorkflowId(pendingWorkflow.workflowUUID, { isRecoveryDispatch: true }));
          }
        } catch (e) {
          this.logger.warn(`Recovery of workflow ${pendingWorkflow.workflowUUID} failed: ${(e as Error).message}`);
        }
      }
    }
    return handlerArray;
  }

  async initEventReceivers(listenQueues: WorkflowQueue[] | null) {
    this.#wfqEnded = wfQueueRunner.dispatchLoop(this, listenQueues);

    for (const lcl of getLifecycleListeners()) {
      await lcl.initialize?.();
    }
  }

  async deactivateEventReceivers(stopQueueThread: boolean = true) {
    this.logger.debug('Deactivating lifecycle listeners');
    for (const lcl of getLifecycleListeners()) {
      try {
        await lcl.destroy?.();
      } catch (err) {
        const e = err as Error;
        this.logger.warn(`Error destroying lifecycle listener: ${e.message}`);
      }
    }

    this.logger.debug('Deactivating queue runner');
    if (stopQueueThread) {
      try {
        wfQueueRunner.stop();
        await this.#wfqEnded;
      } catch (err) {
        const e = err as Error;
        this.logger.warn(`Error destroying wf queue runner: ${e.message}`);
      }
    }
  }

  async executeWorkflowId(
    workflowID: string,
    options?: {
      startNewWorkflow?: boolean;
      isRecoveryDispatch?: boolean;
      isQueueDispatch?: boolean;
    },
  ): Promise<WorkflowHandle<unknown>> {
    const wfStatus = await this.systemDatabase.getWorkflowStatus(workflowID);
    if (!wfStatus) {
      this.logger.error(`Failed to find workflow status for workflowUUID: ${workflowID}`);
      throw new DBOSError(`Failed to find workflow status for workflow UUID: ${workflowID}`);
    }

    if (!wfStatus?.input) {
      this.logger.error(`Failed to find inputs for workflowUUID: ${workflowID}`);
      throw new DBOSError(`Failed to find inputs for workflow UUID: ${workflowID}`);
    }
    let inputs: unknown[];
    try {
      inputs = deserializePositionalArgs(wfStatus.input, wfStatus.serialization, this.serializer);
    } catch (err) {
      // If deserialization fails, record the error on the workflow
      // so it transitions to ERROR instead of being stuck in PENDING.
      this.logger.error(`Failed to deserialize inputs for workflow ${workflowID}: ${(err as Error).message}`);
      const sererr = serializeResErrorWithSerializer(err as Error, this.serializer, wfStatus.serialization);
      wfStatus.error = sererr.serializedValue;
      await this.systemDatabase.recordWorkflowError(workflowID, wfStatus);
      throw err;
    }
    const recoverCtx = this.#getRecoveryContext(workflowID, wfStatus);

    const { methReg, configuredInst } = this.#getFunctionInfoFromWFStatus(wfStatus);

    // If starting a new workflow, assign a new UUID. Otherwise, use the workflow's original UUID.
    const workflowStartID = !!options?.startNewWorkflow ? undefined : workflowID;

    if (methReg?.workflowConfig) {
      return await runWithTopContext(recoverCtx, async () => {
        return await this.workflow(
          methReg.registeredFunction as UntypedAsyncFunction,
          {
            workflowUUID: workflowStartID,
            configuredInstance: configuredInst,
            queueName: wfStatus.queueName,
            executeWorkflow: true,
            deadlineEpochMS: wfStatus.deadlineEpochMS,
            isRecoveryDispatch: !!options?.isRecoveryDispatch,
            isQueueDispatch: !!options?.isQueueDispatch,
          },
          ...inputs,
        );
      });
    }

    // Should be temporary workflows. Parse the name of the workflow.
    const wfName = wfStatus.workflowName;
    const nameArr = wfName.split('-');
    if (!nameArr[0].startsWith(DBOSExecutor.#tempWorkflowName)) {
      throw new DBOSError(
        `Cannot find workflow function for a non-temporary workflow, ID ${workflowID}, class '${wfStatus.workflowClassName}', function '${wfName}'; did you change your code?`,
      );
    }

    if (nameArr[1] === TempWorkflowType.step) {
      const stepReg = getFunctionRegistrationByName(wfStatus.workflowClassName, nameArr[2]);
      if (!stepReg?.stepConfig) {
        this.logger.error(`Cannot find step info for ID ${workflowID}, name ${nameArr[2]}`);
        throw new DBOSNotRegisteredError(nameArr[2]);
      }
      return await runWithTopContext(recoverCtx, async () => {
        return await this.startStepTempWF(
          stepReg.registeredFunction as UntypedAsyncFunction,
          {
            workflowUUID: workflowStartID,
            configuredInstance: configuredInst,
            queueName: wfStatus.queueName, // Probably null
            executeWorkflow: true,
            isRecoveryDispatch: !!options?.isRecoveryDispatch,
            isQueueDispatch: !!options?.isQueueDispatch,
          },
          undefined,
          undefined,
          ...inputs,
        );
      });
    } else if (nameArr[1] === TempWorkflowType.send) {
      // Backwards compatibility: recover send temp workflows created before sendDirect was introduced.
      const swf = async (
        destinationID: string,
        message: unknown,
        topic?: string,
        serialization?: WorkflowSerializationFormat | null,
      ) => {
        const ctx = getCurrentContextStore();
        const functionID: number = functionIDGetIncrement();
        const sermsg = serializeValue(message, this.serializer, serialization ?? undefined);
        await this.systemDatabase.send(
          ctx!.workflowId!,
          functionID,
          destinationID,
          sermsg.serializedValue,
          topic,
          sermsg.serialization,
        );
      };
      const temp_workflow = swf as UntypedAsyncFunction;
      return await runWithTopContext(recoverCtx, async () => {
        return this.workflow(
          temp_workflow,
          {
            tempWfName: nameArr[2],
            tempWfType: TempWorkflowType.send,
            workflowUUID: workflowStartID,
            queueName: wfStatus.queueName,
            executeWorkflow: true,
            isRecoveryDispatch: !!options?.isRecoveryDispatch,
            isQueueDispatch: !!options?.isQueueDispatch,
          },
          ...inputs,
        );
      });
    } else {
      this.logger.error(`Unrecognized temporary workflow! UUID ${workflowID}, name ${wfName}`);
      throw new DBOSNotRegisteredError(wfName);
    }
  }

  async getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSExternalState | undefined> {
    return await this.systemDatabase.getEventDispatchState(svc, wfn, key);
  }
  async upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState> {
    return await this.systemDatabase.upsertEventDispatchState(state);
  }

  #getRecoveryContext(_workflowID: string, status: WorkflowStatusInternal): DBOSLocalCtx {
    // Note: this doesn't inherit the original parent context's span.
    const oc: DBOSLocalCtx = {};
    oc.request = status.request;
    oc.authenticatedUser = status.authenticatedUser;
    oc.authenticatedRoles = status.authenticatedRoles;
    oc.assumedRole = status.assumedRole;
    oc.serializationType = status.serialization === DBOSPortableJSON.name() ? 'portable' : undefined;
    return oc;
  }

  async cancelWorkflow(workflowID: string): Promise<void> {
    await this.systemDatabase.cancelWorkflow(workflowID);
    this.logger.info(`Cancelling workflow ${workflowID}`);
  }

  async getWorkflowSteps(workflowID: string): Promise<step_info[]> {
    const outputs = await this.systemDatabase.getAllOperationResults(workflowID);
    return outputs.map((row) => {
      return {
        function_id: row.function_id,
        function_name: row.function_name ?? '<unknown>',
        child_workflow_id: row.child_workflow_id,
        output: row.output !== null ? this.serializer.parse(row.output) : null,
        error: row.error !== null ? deserializeError(this.serializer.parse(row.error as unknown as string)) : null,
      };
    });
  }

  async resumeWorkflow(workflowID: string): Promise<void> {
    await this.systemDatabase.resumeWorkflow(workflowID);
  }

  async deleteWorkflow(workflowID: string, deleteChildren: boolean = false): Promise<void> {
    await this.systemDatabase.deleteWorkflow(workflowID, deleteChildren);
    this.logger.info(`Deleted workflow ${workflowID}${deleteChildren ? ' and its children' : ''}`);
  }

  /**
    An application's version is computed from a hash of the source of its workflows.
    This is guaranteed to be stable given identical source code because it uses an MD5 hash
    and because it iterates through the workflows in sorted order.
    This way, if the app's workflows are updated (which would break recovery), its version changes.
    App version can be manually set through the DBOS__APPVERSION environment variable.
   */
  computeAppVersion(): string {
    const hasher = crypto.createHash('md5');
    const sortedWorkflowSource = Array.from(getAllRegisteredFunctions())
      .filter((e) => e.workflowConfig)
      .map((i) => i.origFunction.toString())
      .sort();
    // Different DBOS versions should produce different hashes.
    sortedWorkflowSource.push(globalParams.dbosVersion);
    for (const sourceCode of sortedWorkflowSource) {
      hasher.update(sourceCode);
    }
    return hasher.digest('hex');
  }

  static internalQueue: WorkflowQueue | undefined = undefined;

  static createInternalQueue() {
    if (DBOSExecutor.internalQueue !== undefined) {
      return;
    }
    DBOSExecutor.internalQueue = new WorkflowQueue(INTERNAL_QUEUE_NAME, {});
  }

  static debouncerWorkflow: UntypedAsyncFunction | undefined = undefined;

  static createDebouncerWorkflow() {
    if (DBOSExecutor.debouncerWorkflow !== undefined) {
      return;
    }
    DBOSExecutor.debouncerWorkflow = DBOS.registerWorkflow(debouncerWorkflowFunction, {
      name: DEBOUNCER_WORKLOW_NAME,
    }) as UntypedAsyncFunction;
  }
}
