import { Span } from '@opentelemetry/sdk-trace-base';
import {
  assertCurrentDBOSContext,
  assertCurrentWorkflowContext,
  getCurrentContextStore,
  getCurrentDBOSContext,
  HTTPRequest,
  runWithTopContext,
  DBOSContextImpl,
  getNextWFID,
} from './context';
import { DBOSConfig, DBOSExecutor, InternalWorkflowParams } from './dbos-executor';
import {
  GetWorkflowQueueInput,
  GetWorkflowQueueOutput,
  GetWorkflowsInput,
  GetWorkflowsOutput,
  WorkflowConfig,
  WorkflowFunction,
  WorkflowParams,
} from './workflow';
import { DBOSExecutorContext } from './eventreceiver';
import { DLogger, GlobalLogger } from './telemetry/logs';
import { DBOSError, DBOSExecutorNotInitializedError, DBOSInvalidWorkflowTransitionError } from './error';
import { parseConfigFile } from './dbos-runtime/config';
import { DBOSRuntime, DBOSRuntimeConfig } from './dbos-runtime/runtime';
import { DBOSScheduler, ScheduledArgs, SchedulerConfig, SchedulerRegistrationBase } from './scheduler/scheduler';
import {
  configureInstance,
  getOrCreateClassRegistration,
  getRegisteredOperations,
  MethodRegistration,
  registerAndWrapDBOSFunction,
  registerFunctionWrapper,
} from './decorators';
import { globalParams, sleepms } from './utils';
import { DBOSHttpServer } from './httpServer/server';
import { koaTracingMiddleware, expressTracingMiddleware, honoTracingMiddleware } from './httpServer/middleware';
import { Server } from 'http';
import { DrizzleClient, PrismaClient, TypeORMEntityManager, UserDatabaseClient } from './user_database';
import { TransactionConfig, TransactionContextImpl, TransactionFunction } from './transaction';

import Koa from 'koa';
import { Application as ExpressApp } from 'express';
import { INestApplication } from '@nestjs/common';
import { FastifyInstance } from 'fastify';
import _fastifyExpress from '@fastify/express'; // This is for fastify.use()

import { PoolClient } from 'pg';
import { Knex } from 'knex';
import { StepConfig, StepFunction } from './step';
import { wfQueueRunner } from './wfqueue';
import {
  HandlerContext,
  StepContext,
  StoredProcedureContext,
  TransactionContext,
  WorkflowContext,
  WorkflowHandle,
} from '.';
import { ConfiguredInstance } from '.';
import { StoredProcedure, StoredProcedureConfig } from './procedure';
import { APITypes } from './httpServer/handlerTypes';
import { HandlerRegistrationBase } from './httpServer/handler';
import { set } from 'lodash';
import { db_wizard } from './dbos-runtime/db_wizard';
import { Hono } from 'hono';

// Declare all the HTTP applications a user can pass to the DBOS object during launch()
// This allows us to add a DBOS tracing middleware (extract W3C Trace context, set request ID, etc)
export interface DBOSHttpApps {
  koaApp?: Koa;
  expressApp?: ExpressApp;
  nestApp?: INestApplication;
  fastifyApp?: FastifyInstance;
  honoApp?: Hono;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type PossiblyWFFunc = (...args: any[]) => Promise<unknown>;
type InvokeFunctionsAsync<T> =
  // eslint-disable-next-line @typescript-eslint/ban-types
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

// local type declarations for invoking old-style transaction and step function
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type TailParameters<T extends (arg: any, args: any[]) => any> = T extends (arg: any, ...args: infer P) => any
  ? P
  : never;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TxFunc = (ctxt: TransactionContext<any>, ...args: any[]) => Promise<any>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type StepFunc = (ctxt: StepContext, ...args: any[]) => Promise<any>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ProcFunc = (ctxt: StoredProcedureContext, ...args: any[]) => Promise<any>;

// Utility type that only includes transaction/step/proc functions + converts the method signature to exclude the context parameter
type InvokeFuncs<T> = T extends ConfiguredInstance
  ? never
  : {
      [P in keyof T as T[P] extends TxFunc | StepFunc | ProcFunc ? P : never]: T[P] extends TxFunc | StepFunc | ProcFunc
        ? (...args: TailParameters<T[P]>) => ReturnType<T[P]>
        : never;
    };

type InvokeFuncsInst<T> = T extends ConfiguredInstance
  ? {
      [P in keyof T as T[P] extends TxFunc | StepFunc ? P : never]: T[P] extends TxFunc | StepFunc
        ? (...args: TailParameters<T[P]>) => ReturnType<T[P]>
        : never;
    }
  : never;

function httpApiDec(verb: APITypes, url: string) {
  return function apidec<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
    const handlerRegistration = registration as unknown as HandlerRegistrationBase;
    handlerRegistration.apiURL = url;
    handlerRegistration.apiType = verb;
    registration.performArgValidation = true;

    return descriptor;
  };
}

export interface StartWorkflowParams {
  workflowID?: string;
  queueName?: string;
}

export class DBOS {
  ///////
  // Lifecycle
  ///////
  static adminServer: Server | undefined = undefined;
  static appServer: Server | undefined = undefined;

  static setConfig(config: DBOSConfig, runtimeConfig?: DBOSRuntimeConfig) {
    DBOS.dbosConfig = config;
    DBOS.runtimeConfig = runtimeConfig;
  }

  // For unit testing purposes only
  static setAppConfig<T>(key: string, newValue: T): void {
    const conf = DBOS.dbosConfig?.application;
    if (!conf) throw new DBOSExecutorNotInitializedError();
    set(conf, key, newValue);
  }

  // Load files with DBOS classes (running their decorators)
  static async loadClasses(dbosEntrypointFiles: string[]) {
    return await DBOSRuntime.loadClasses(dbosEntrypointFiles);
  }

  static async launch(httpApps?: DBOSHttpApps) {
    // Do nothing is DBOS is already initialized
    if (DBOSExecutor.globalInstance) return;

    const debugWorkflowId = process.env.DBOS_DEBUG_WORKFLOW_ID;
    const isDebugging = debugWorkflowId !== undefined;

    // Initialize the DBOS executor
    if (!DBOS.dbosConfig) {
      const [dbosConfig, runtimeConfig] = parseConfigFile({ forceConsole: isDebugging });
      if (!isDebugging) {
        dbosConfig.poolConfig = await db_wizard(dbosConfig.poolConfig);
      }
      DBOS.dbosConfig = dbosConfig;
      DBOS.runtimeConfig = runtimeConfig;
    }

    DBOSExecutor.globalInstance = new DBOSExecutor(DBOS.dbosConfig);
    const executor: DBOSExecutor = DBOSExecutor.globalInstance;
    await executor.init();

    if (debugWorkflowId) {
      DBOS.logger.info(`Debugging workflow "${debugWorkflowId}"`);
      const handle = await executor.executeWorkflowUUID(debugWorkflowId);
      await handle.getResult();
      DBOS.logger.info(`Workflow Debugging complete. Exiting process.`);
      await executor.destroy();
      process.exit(0);
      return; // return for cases where process.exit is mocked
    }

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

    if (httpApps) {
      if (httpApps.koaApp) {
        DBOS.logger.info('Setting up Koa tracing middleware');
        httpApps.koaApp.use(koaTracingMiddleware);
      }
      if (httpApps.expressApp) {
        DBOS.logger.info('Setting up Express tracing middleware');
        httpApps.expressApp.use(expressTracingMiddleware);
      }
      if (httpApps.fastifyApp) {
        // Fastify can use express or middie under the hood, for middlewares.
        // Middie happens to have the same semantic than express.
        // See https://fastify.dev/docs/latest/Reference/Middleware/
        DBOS.logger.info('Setting up Fastify tracing middleware');
        httpApps.fastifyApp.use(expressTracingMiddleware);
      }
      if (httpApps.nestApp) {
        // Nest.kj can use express or fastify under the hood. With fastify, Nest.js uses middie.
        DBOS.logger.info('Setting up NestJS tracing middleware');
        httpApps.nestApp.use(expressTracingMiddleware);
      }
      if (httpApps.honoApp) {
        DBOS.logger.info('Setting up Hono tracing middleware');
        httpApps.honoApp.use(honoTracingMiddleware);
      }
    }
  }

  static async shutdown() {
    // Stop the app server
    if (DBOS.appServer) {
      DBOS.appServer.close();
      DBOS.appServer = undefined;
    }

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

    // Reset the global app version and executor ID
    globalParams.appVersion = process.env.DBOS__APPVERSION || '';
    globalParams.wasComputed = false;
    globalParams.appID = process.env.DBOS__APPID || '';
    globalParams.executorID = process.env.DBOS__VMID || 'local';
  }

  static get executor() {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    return DBOSExecutor.globalInstance as DBOSExecutorContext;
  }

  static setUpHandlerCallback() {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    // Create the DBOS HTTP server
    //  This may be a no-op if there are no registered endpoints
    const server = new DBOSHttpServer(DBOSExecutor.globalInstance);

    return server;
  }

  static async launchAppHTTPServer() {
    const server = this.setUpHandlerCallback();
    if (DBOS.runtimeConfig) {
      // This will not listen if there's no decorated endpoint
      DBOS.appServer = await server.appListen(DBOS.runtimeConfig.port);
    }
  }

  // This retrieves the HTTP handlers callback for DBOS HTTP.
  //  (This is the one that handles the @DBOS.getApi, etc., methods.)
  // Useful for testing purposes, or to combine the DBOS service with routes.
  // If you are using your own HTTP server, this won't return anything.
  static getHTTPHandlersCallback() {
    if (!DBOSHttpServer.instance) {
      return undefined;
    }
    return DBOSHttpServer.instance.app.callback();
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
    const executor = DBOSExecutor.globalInstance;
    if (executor) return executor.logger;
    return new GlobalLogger();
  }

  static get span(): Span | undefined {
    const ctx = getCurrentDBOSContext();
    if (ctx) return ctx.span;
    return undefined;
  }

  static getRequest(): HTTPRequest | undefined {
    return getCurrentDBOSContext()?.request;
  }

  static get request(): HTTPRequest {
    const r = DBOS.getRequest();
    if (!r) throw new DBOSError('`DBOS.request` accessed from outside of HTTP requests');
    return r;
  }

  static getKoaContext(): Koa.Context | undefined {
    return (getCurrentDBOSContext() as HandlerContext)?.koaContext;
  }

  static get koaContext(): Koa.Context {
    const r = DBOS.getKoaContext();
    if (!r) throw new DBOSError('`DBOS.koaContext` accessed from outside koa request');
    return r;
  }

  static get workflowID(): string | undefined {
    return getCurrentDBOSContext()?.workflowUUID;
  }
  static get authenticatedUser(): string {
    return getCurrentDBOSContext()?.authenticatedUser ?? '';
  }
  static get authenticatedRoles(): string[] {
    return getCurrentDBOSContext()?.authenticatedRoles ?? [];
  }
  static get assumedRole(): string {
    return getCurrentDBOSContext()?.assumedRole ?? '';
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
    if (!DBOS.isInTransaction())
      throw new DBOSInvalidWorkflowTransitionError('Invalid use of `DBOS.sqlClient` outside of a `transaction`');
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
    if (DBOS.isWithinWorkflow() && !DBOS.isInStep()) {
      if (DBOS.isInTransaction()) {
        throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.sleep` inside a `transaction`');
      }
      return (getCurrentDBOSContext()! as WorkflowContext).sleepms(durationMS);
    }
    await sleepms(durationMS);
  }
  static async sleepSeconds(durationSec: number): Promise<void> {
    return this.sleepms(durationSec * 1000);
  }
  static async sleep(durationMS: number): Promise<void> {
    return this.sleepms(durationMS);
  }

  static async withNextWorkflowID<R>(wfid: string, callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      const pcwfid = pctx.idAssignedForNextWorkflow;
      try {
        pctx.idAssignedForNextWorkflow = wfid;
        return callback();
      } finally {
        pctx.idAssignedForNextWorkflow = pcwfid;
      }
    } else {
      return runWithTopContext({ idAssignedForNextWorkflow: wfid }, callback);
    }
  }

  static async withTracedContext<R>(
    callerName: string,
    span: Span,
    request: HTTPRequest,
    callback: () => Promise<R>,
  ): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      pctx.operationCaller = callerName;
      pctx.span = span;
      pctx.request = request;
      return callback();
    } else {
      return runWithTopContext({ span, request }, callback);
    }
  }

  static async withAuthedContext<R>(authedUser: string, authedRoles: string[], callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      pctx.authenticatedUser = authedUser;
      pctx.authenticatedRoles = authedRoles;
      return callback();
    } else {
      return runWithTopContext({ authenticatedUser: authedUser, authenticatedRoles: authedRoles }, callback);
    }
  }

  // This generic setter helps users calling DBOS operation to pass a name, later used in seeding a parent OTel span for the operation.
  static async withNamedContext<R>(callerName: string, callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      pctx.operationCaller = callerName;
      return callback();
    } else {
      return runWithTopContext({ operationCaller: callerName }, callback);
    }
  }

  static async withWorkflowQueue<R>(wfq: string, callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      const pcwfq = pctx.queueAssignedForWorkflows;
      try {
        pctx.queueAssignedForWorkflows = wfq;
        return callback();
      } finally {
        pctx.queueAssignedForWorkflows = pcwfq;
      }
    } else {
      return runWithTopContext({ queueAssignedForWorkflows: wfq }, callback);
    }
  }

  static startWorkflow<T extends ConfiguredInstance>(
    targetClass: T,
    params?: StartWorkflowParams,
  ): InvokeFunctionsAsyncInst<T>;
  static startWorkflow<T extends object>(targetClass: T, params?: StartWorkflowParams): InvokeFunctionsAsync<T>;
  static startWorkflow<T extends object>(target: T, params?: StartWorkflowParams): InvokeFunctionsAsync<T> {
    if (typeof target === 'function') {
      return DBOS.proxyInvokeWF(target, null, params) as unknown as InvokeFunctionsAsync<T>;
    } else {
      return DBOS.proxyInvokeWF(target, target as ConfiguredInstance, params) as unknown as InvokeFunctionsAsync<T>;
    }
  }

  static proxyInvokeWF<T extends object>(
    object: T,
    configuredInstance: ConfiguredInstance | null,
    inParams?: StartWorkflowParams,
  ): InvokeFunctionsAsync<T> {
    const ops = getRegisteredOperations(object);
    const proxy: Record<string, unknown> = {};

    let wfId = getNextWFID(inParams?.workflowID);
    const pctx = getCurrentContextStore();

    // If this is called from within a workflow, this is a child workflow,
    //  For OAOO, we will need a consistent ID formed from the parent WF and call number
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.startWorkflow` from within a `step` or `transaction`',
        );
      }

      const wfctx = assertCurrentWorkflowContext();

      const funcId = wfctx.functionIDGetIncrement();
      wfId = wfId || wfctx.workflowUUID + '-' + funcId;
      const wfParams: WorkflowParams = {
        workflowUUID: wfId,
        parentCtx: wfctx,
        configuredInstance,
        queueName: inParams?.queueName ?? pctx?.queueAssignedForWorkflows,
      };

      for (const op of ops) {
        proxy[op.name] = op.workflowConfig
          ? (...args: unknown[]) =>
              DBOSExecutor.globalInstance!.internalWorkflow(
                op.registeredFunction as WorkflowFunction<unknown[], unknown>,
                wfParams,
                wfctx.workflowUUID,
                funcId,
                ...args,
              )
          : undefined;
      }

      return proxy as InvokeFunctionsAsync<T>;
    }

    // Else, we setup a parent context that includes all the potential metadata the application could have set in DBOSLocalCtx
    let parentCtx: DBOSContextImpl | undefined = undefined;
    if (pctx) {
      // If pctx has no span, e.g., has not been setup through `withTracedContext`, set up a parent span for the workflow here.
      let span = pctx.span;
      if (!span) {
        span = DBOS.executor.tracer.startSpan(pctx.operationCaller || 'startWorkflow', {
          operationUUID: wfId,
          operationType: pctx.operationType,
          authenticatedUser: pctx.authenticatedUser,
          assumedRole: pctx.assumedRole,
          authenticatedRoles: pctx.authenticatedRoles,
        });
      }
      parentCtx = new DBOSContextImpl(pctx.operationCaller || 'startWorkflow', span, DBOS.logger as GlobalLogger);
      parentCtx.request = pctx.request || {};
      parentCtx.authenticatedUser = pctx.authenticatedUser || '';
      parentCtx.assumedRole = pctx.assumedRole || '';
      parentCtx.authenticatedRoles = pctx.authenticatedRoles || [];
      parentCtx.workflowUUID = wfId || '';
    }

    const wfParams: InternalWorkflowParams = {
      workflowUUID: wfId,
      queueName: inParams?.queueName ?? pctx?.queueAssignedForWorkflows,
      configuredInstance,
      parentCtx,
    };

    for (const op of ops) {
      proxy[op.name] = op.workflowConfig
        ? (...args: unknown[]) =>
            DBOS.executor.workflow(op.registeredFunction as WorkflowFunction<unknown[], unknown>, wfParams, ...args)
        : undefined;
    }

    // TODO CTX - should we put helpful errors for any function that may have "compiled" but is not a workflow?

    return proxy as InvokeFunctionsAsync<T>;
  }

  static invoke<T extends ConfiguredInstance>(targetCfg: T): InvokeFuncsInst<T>;
  static invoke<T extends object>(targetClass: T): InvokeFuncs<T>;
  static invoke<T extends object>(object: T | ConfiguredInstance): InvokeFuncs<T> | InvokeFuncsInst<T> {
    if (!DBOS.isWithinWorkflow()) {
      // Run the temp workflow way...
      if (typeof object === 'function') {
        const ops = getRegisteredOperations(object);

        const proxy: Record<string, unknown> = {};
        for (const op of ops) {
          proxy[op.name] = op.txnConfig
            ? (...args: unknown[]) =>
                DBOSExecutor.globalInstance!.transaction(
                  op.registeredFunction as TransactionFunction<unknown[], unknown>,
                  {},
                  ...args,
                )
            : op.stepConfig
              ? (...args: unknown[]) =>
                  DBOSExecutor.globalInstance!.external(
                    op.registeredFunction as StepFunction<unknown[], unknown>,
                    {},
                    ...args,
                  )
              : op.procConfig
                ? (...args: unknown[]) =>
                    DBOSExecutor.globalInstance!.procedure<unknown[], unknown>(
                      op.registeredFunction as StoredProcedure<unknown[], unknown>,
                      {},
                      ...args,
                    )
                : undefined;
        }
        return proxy as InvokeFuncs<T>;
      } else {
        const targetInst = object as ConfiguredInstance;
        const ops = getRegisteredOperations(targetInst);

        const proxy: Record<string, unknown> = {};
        for (const op of ops) {
          proxy[op.name] = op.txnConfig
            ? (...args: unknown[]) =>
                DBOSExecutor.globalInstance!.transaction(
                  op.registeredFunction as TransactionFunction<unknown[], unknown>,
                  { configuredInstance: targetInst },
                  ...args,
                )
            : op.stepConfig
              ? (...args: unknown[]) =>
                  DBOSExecutor.globalInstance!.external(
                    op.registeredFunction as StepFunction<unknown[], unknown>,
                    { configuredInstance: targetInst },
                    ...args,
                  )
              : undefined;
        }
        return proxy as InvokeFuncsInst<T>;
      }
    }
    const wfctx = assertCurrentWorkflowContext();
    if (typeof object === 'function') {
      const ops = getRegisteredOperations(object);

      const proxy: Record<string, unknown> = {};
      for (const op of ops) {
        proxy[op.name] = op.txnConfig
          ? (...args: unknown[]) =>
              DBOSExecutor.globalInstance!.callTransactionFunction(
                op.registeredFunction as TransactionFunction<unknown[], unknown>,
                null,
                wfctx,
                ...args,
              )
          : op.stepConfig
            ? (...args: unknown[]) =>
                DBOSExecutor.globalInstance!.callStepFunction(
                  op.registeredFunction as StepFunction<unknown[], unknown>,
                  null,
                  wfctx,
                  ...args,
                )
            : op.procConfig
              ? (...args: unknown[]) =>
                  DBOSExecutor.globalInstance!.callProcedureFunction(
                    op.registeredFunction as StoredProcedure<unknown[], unknown>,
                    wfctx,
                    ...args,
                  )
              : undefined;
      }
      return proxy as InvokeFuncs<T>;
    } else {
      const targetInst = object as ConfiguredInstance;
      const ops = getRegisteredOperations(targetInst);

      const proxy: Record<string, unknown> = {};
      for (const op of ops) {
        proxy[op.name] = op.txnConfig
          ? (...args: unknown[]) =>
              DBOSExecutor.globalInstance!.callTransactionFunction(
                op.registeredFunction as TransactionFunction<unknown[], unknown>,
                targetInst,
                wfctx,
                ...args,
              )
          : op.stepConfig
            ? (...args: unknown[]) =>
                DBOSExecutor.globalInstance!.callStepFunction(
                  op.registeredFunction as StepFunction<unknown[], unknown>,
                  targetInst,
                  wfctx,
                  ...args,
                )
            : undefined;
      }
      return proxy as InvokeFuncsInst<T>;
    }
  }

  static async send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string): Promise<void> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.send` inside a `step` or `transaction`');
      }
      if (idempotencyKey) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.send` with an idempotency key from within a workflow',
        );
      }
      return (getCurrentDBOSContext() as WorkflowContext).send(destinationID, message, topic);
    }
    return DBOS.executor.send(destinationID, message, topic, idempotencyKey);
  }

  static async recv<T>(topic?: string, timeoutSeconds?: number): Promise<T | null> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.setEvent` inside a `step` or `transaction`',
        );
      }
      return (getCurrentDBOSContext() as WorkflowContext).recv<T>(topic, timeoutSeconds);
    }
    throw new DBOSInvalidWorkflowTransitionError('Attempt to call `DBOS.recv` outside of a workflow'); // Only workflows can recv
  }

  static async setEvent<T>(key: string, value: T): Promise<void> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.setEvent` inside a `step` or `transaction`',
        );
      }
      return (getCurrentDBOSContext() as WorkflowContext).setEvent(key, value);
    }
    throw new DBOSInvalidWorkflowTransitionError('Attempt to call `DBOS.setEvent` outside of a workflow'); // Only workflows can set event
  }

  static async getEvent<T>(workflowID: string, key: string, timeoutSeconds?: number): Promise<T | null> {
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `DBOS.getEvent` inside a `step` or `transaction`',
        );
      }
      return (getCurrentDBOSContext() as WorkflowContext).getEvent(workflowID, key, timeoutSeconds);
    }
    return DBOS.executor.getEvent(workflowID, key, timeoutSeconds);
  }

  //////
  // Decorators
  //////
  static scheduled(schedulerConfig: SchedulerConfig) {
    function scheddec<This, Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: ScheduledArgs) => Promise<Return>>,
    ) {
      const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
      const schedRegistration = registration as unknown as SchedulerRegistrationBase;
      schedRegistration.schedulerConfig = schedulerConfig;

      return descriptor;
    }
    return scheddec;
  }

  static workflow(config: WorkflowConfig = {}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
      registration.setWorkflowConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        const pctx = getCurrentContextStore();
        let inst: ConfiguredInstance | undefined = undefined;
        if (typeof this === 'function') {
          // This is static
        } else {
          inst = this as ConfiguredInstance;
          if (!('name' in inst)) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Attempt to call a `workflow` function on an object that is not a `ConfiguredInstance`',
            );
          }
        }

        let wfId = getNextWFID(undefined);

        // If this is called from within a workflow, this is a child workflow,
        //  For OAOO, we will need a consistent ID formed from the parent WF and call number
        if (DBOS.isWithinWorkflow()) {
          if (!DBOS.isInWorkflow()) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Invalid call to a `workflow` function from within a `step` or `transaction`',
            );
          }

          const wfctx = assertCurrentWorkflowContext();

          const funcId = wfctx.functionIDGetIncrement();
          wfId = wfId || wfctx.workflowUUID + '-' + funcId;
          const params: WorkflowParams = {
            workflowUUID: wfId,
            parentCtx: wfctx,
            configuredInstance: inst,
            queueName: pctx?.queueAssignedForWorkflows,
          };

          const cwfh = await DBOSExecutor.globalInstance!.internalWorkflow(
            registration.registeredFunction as unknown as WorkflowFunction<Args, Return>,
            params,
            wfctx.workflowUUID,
            funcId,
            ...rawArgs,
          );
          return await cwfh.getResult();
        }

        // Else, we setup a parent context that includes all the potential metadata the application could have set in DBOSLocalCtx
        let parentCtx: DBOSContextImpl | undefined = undefined;
        if (pctx) {
          // If pctx has no span, e.g., has not been setup through `withTracedContext`, set up a parent span for the workflow here.
          let span = pctx.span;
          if (!span) {
            span = DBOS.executor.tracer.startSpan(pctx.operationCaller || 'workflowCaller', {
              operationUUID: wfId,
              operationType: pctx.operationType,
              authenticatedUser: pctx.authenticatedUser,
              assumedRole: pctx.assumedRole,
              authenticatedRoles: pctx.authenticatedRoles,
            });
          }
          parentCtx = new DBOSContextImpl(pctx.operationCaller || 'workflowCaller', span, DBOS.logger as GlobalLogger);
          parentCtx.request = pctx.request || {};
          parentCtx.authenticatedUser = pctx.authenticatedUser || '';
          parentCtx.assumedRole = pctx.assumedRole || '';
          parentCtx.authenticatedRoles = pctx.authenticatedRoles || [];
          parentCtx.workflowUUID = wfId || '';
        }

        const wfParams: InternalWorkflowParams = {
          workflowUUID: wfId,
          queueName: pctx?.queueAssignedForWorkflows,
          configuredInstance: inst,
          parentCtx,
        };

        const handle = await DBOS.executor.workflow(
          registration.registeredFunction as unknown as WorkflowFunction<Args, Return>,
          wfParams,
          ...rawArgs,
        );
        return await handle.getResult();
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;
      Object.defineProperty(invokeWrapper, 'name', {
        value: registration.name,
      });

      registerFunctionWrapper(invokeWrapper, registration as MethodRegistration<unknown, unknown[], unknown>);
      // TODO CTX this should not be in here already, or if it is we need to do something different...
      DBOS.invokeWrappers.set(invokeWrapper, registration.registeredFunction);

      return descriptor;
    }
    return decorator;
  }

  static transaction(config: TransactionConfig = {}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
      registration.setTxnConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        let inst: ConfiguredInstance | undefined = undefined;
        if (typeof this === 'function') {
          // This is static
        } else {
          inst = this as ConfiguredInstance;
          if (!('name' in inst)) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Attempt to call a `transaction` function on an object that is not a `ConfiguredInstance`',
            );
          }
        }

        if (DBOS.isWithinWorkflow()) {
          if (DBOS.isInTransaction()) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Invalid call to a `transaction` function from within a `transaction`',
            );
          }
          if (DBOS.isInStep()) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Invalid call to a `transaction` function from within a `step`',
            );
          }

          const wfctx = assertCurrentWorkflowContext();
          return await DBOSExecutor.globalInstance!.callTransactionFunction(
            registration.registeredFunction as unknown as TransactionFunction<Args, Return>,
            inst ?? null,
            wfctx,
            ...rawArgs,
          );
        }

        const wfId = getNextWFID(undefined);

        const pctx = getCurrentContextStore();
        let span = pctx?.span;
        if (!span) {
          span = DBOS.executor.tracer.startSpan(pctx?.operationCaller || 'transactionCaller', {
            operationType: pctx?.operationType,
            authenticatedUser: pctx?.authenticatedUser,
            assumedRole: pctx?.assumedRole,
            authenticatedRoles: pctx?.authenticatedRoles,
          });
        }

        let parentCtx: DBOSContextImpl | undefined = undefined;
        if (pctx) {
          parentCtx = pctx.ctx as DBOSContextImpl;
        }
        if (!parentCtx) {
          parentCtx = new DBOSContextImpl(pctx?.operationCaller || 'workflowCaller', span, DBOS.logger as GlobalLogger);
          parentCtx.request = pctx?.request || {};
          parentCtx.authenticatedUser = pctx?.authenticatedUser || '';
          parentCtx.assumedRole = pctx?.assumedRole || '';
          parentCtx.authenticatedRoles = pctx?.authenticatedRoles || [];
        }
        const wfParams: WorkflowParams = {
          configuredInstance: inst,
          parentCtx,
          workflowUUID: wfId,
        };

        return await DBOS.executor.transaction(
          registration.registeredFunction as unknown as TransactionFunction<Args, Return>,
          wfParams,
          ...rawArgs,
        );
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;

      Object.defineProperty(invokeWrapper, 'name', {
        value: registration.name,
      });

      return descriptor;
    }
    return decorator;
  }

  static storedProcedure(config: StoredProcedureConfig = {}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
      registration.setProcConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        if (typeof this !== 'function') {
          throw new Error('Stored procedures must be static');
        }

        if (DBOS.isWithinWorkflow()) {
          const wfctx = assertCurrentWorkflowContext();
          return await DBOSExecutor.globalInstance!.callProcedureFunction(
            registration.registeredFunction as unknown as StoredProcedure<Args, Return>,
            wfctx,
            ...rawArgs,
          );
        }

        const wfId = getNextWFID(undefined);

        const pctx = getCurrentContextStore();
        let span = pctx?.span;
        if (!span) {
          span = DBOS.executor.tracer.startSpan(pctx?.operationCaller || 'transactionCaller', {
            operationType: pctx?.operationType,
            authenticatedUser: pctx?.authenticatedUser,
            assumedRole: pctx?.assumedRole,
            authenticatedRoles: pctx?.authenticatedRoles,
          });
        }

        let parentCtx: DBOSContextImpl | undefined = undefined;
        if (pctx) {
          parentCtx = pctx.ctx as DBOSContextImpl;
        }
        if (!parentCtx) {
          parentCtx = new DBOSContextImpl(pctx?.operationCaller || 'workflowCaller', span, DBOS.logger as GlobalLogger);
          parentCtx.request = pctx?.request || {};
          parentCtx.authenticatedUser = pctx?.authenticatedUser || '';
          parentCtx.assumedRole = pctx?.assumedRole || '';
          parentCtx.authenticatedRoles = pctx?.authenticatedRoles || [];
        }

        const wfParams: WorkflowParams = {
          parentCtx,
          workflowUUID: wfId,
        };

        return await DBOS.executor.procedure(
          registration.registeredFunction as unknown as StoredProcedure<Args, Return>,
          wfParams,
          ...rawArgs,
        );
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;

      Object.defineProperty(invokeWrapper, 'name', {
        value: registration.name,
      });

      return descriptor;
    }

    return decorator;
  }

  static step(config: StepConfig = {}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
      registration.setStepConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        let inst: ConfiguredInstance | undefined = undefined;
        if (typeof this === 'function') {
          // This is static
        } else {
          inst = this as ConfiguredInstance;
          if (!('name' in inst)) {
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
          const wfctx = assertCurrentWorkflowContext();
          return await DBOSExecutor.globalInstance!.callStepFunction(
            registration.registeredFunction as unknown as StepFunction<Args, Return>,
            inst ?? null,
            wfctx,
            ...rawArgs,
          );
        }

        const wfId = getNextWFID(undefined);

        const pctx = getCurrentContextStore();
        let span = pctx?.span;
        if (!span) {
          span = DBOS.executor.tracer.startSpan(pctx?.operationCaller || 'transactionCaller', {
            operationType: pctx?.operationType,
            authenticatedUser: pctx?.authenticatedUser,
            assumedRole: pctx?.assumedRole,
            authenticatedRoles: pctx?.authenticatedRoles,
          });
        }

        let parentCtx: DBOSContextImpl | undefined = undefined;
        if (pctx) {
          parentCtx = pctx.ctx as DBOSContextImpl;
        }
        if (!parentCtx) {
          parentCtx = new DBOSContextImpl(pctx?.operationCaller || 'workflowCaller', span, DBOS.logger as GlobalLogger);
          parentCtx.request = pctx?.request || {};
          parentCtx.authenticatedUser = pctx?.authenticatedUser || '';
          parentCtx.assumedRole = pctx?.assumedRole || '';
          parentCtx.authenticatedRoles = pctx?.authenticatedRoles || [];
        }
        const wfParams: WorkflowParams = {
          configuredInstance: inst,
          parentCtx,
          workflowUUID: wfId,
        };

        return await DBOS.executor.external(
          registration.registeredFunction as unknown as StepFunction<Args, Return>,
          wfParams,
          ...rawArgs,
        );
      };

      descriptor.value = invokeWrapper;
      registration.wrappedFunction = invokeWrapper;

      Object.defineProperty(invokeWrapper, 'name', {
        value: registration.name,
      });

      return descriptor;
    }
    return decorator;
  }

  static getApi(url: string) {
    return httpApiDec(APITypes.GET, url);
  }

  static postApi(url: string) {
    return httpApiDec(APITypes.POST, url);
  }

  static putApi(url: string) {
    return httpApiDec(APITypes.PUT, url);
  }

  static patchApi(url: string) {
    return httpApiDec(APITypes.PATCH, url);
  }

  static deleteApi(url: string) {
    return httpApiDec(APITypes.DELETE, url);
  }

  static defaultRequiredRole(anyOf: string[]) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    function clsdec<T extends { new (...args: any[]): object }>(ctor: T) {
      const clsreg = getOrCreateClassRegistration(ctor);
      clsreg.requiredRole = anyOf;
    }
    return clsdec;
  }

  static requiredRole(anyOf: string[]) {
    function apidec<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
      registration.requiredRole = anyOf;

      return descriptor;
    }
    return apidec;
  }

  /////
  // Registration, etc
  /////
  static configureInstance<R extends ConfiguredInstance, T extends unknown[]>(
    cls: new (name: string, ...args: T) => R,
    name: string,
    ...args: T
  ): R {
    return configureInstance(cls, name, ...args);
  }

  // Function registration
  static registerAndWrapDBOSFunction<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    return registerAndWrapDBOSFunction(target, propertyKey, descriptor);
  }

  static async executeWorkflowById(
    workflowId: string,
    startNewWorkflow: boolean = false,
  ): Promise<WorkflowHandle<unknown>> {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    return DBOSExecutor.globalInstance.executeWorkflowUUID(workflowId, startNewWorkflow);
  }

  static async recoverPendingWorkflows(executorIDs: string[] = ['local']): Promise<WorkflowHandle<unknown>[]> {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    return DBOSExecutor.globalInstance.recoverPendingWorkflows(executorIDs);
  }

  // TODO CTX
  // Initializers?  Deploy?  ORM Entities?
}
