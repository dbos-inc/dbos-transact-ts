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
  StepStatus,
} from './context';
import {
  DBOSConfig,
  DBOSConfigInternal,
  isDeprecatedDBOSConfig,
  DBOSExecutor,
  DebugMode,
  InternalWorkflowParams,
} from './dbos-executor';
import { Tracer } from './telemetry/traces';
import {
  GetQueuedWorkflowsInput,
  GetWorkflowQueueInput,
  GetWorkflowQueueOutput,
  GetWorkflowsInput,
  GetWorkflowsOutput,
  WorkflowConfig,
  WorkflowFunction,
  WorkflowParams,
  WorkflowStatus,
} from './workflow';
import { DBOSEventReceiverState, DBOSExecutorContext } from './eventreceiver';
import { DLogger, GlobalLogger } from './telemetry/logs';
import {
  DBOSConfigKeyTypeError,
  DBOSError,
  DBOSExecutorNotInitializedError,
  DBOSInvalidWorkflowTransitionError,
  DBOSNotRegisteredError,
  DBOSTargetWorkflowCancelledError,
  DBOSInvalidStepIDError,
} from './error';
import { parseConfigFile, translatePublicDBOSconfig, overwrite_config } from './dbos-runtime/config';
import { DBOSRuntime, DBOSRuntimeConfig } from './dbos-runtime/runtime';
import { ScheduledArgs, SchedulerConfig, SchedulerRegistrationBase } from './scheduler/scheduler';
import {
  configureInstance,
  getOrCreateClassRegistration,
  getRegisteredOperations,
  MethodRegistration,
  recordDBOSLaunch,
  recordDBOSShutdown,
  registerAndWrapDBOSFunction,
  registerAndWrapDBOSFunctionByName,
  registerFunctionWrapper,
} from './decorators';
import { globalParams, sleepms } from './utils';
import { DBOSHttpServer } from './httpServer/server';
import { koaTracingMiddleware, expressTracingMiddleware, honoTracingMiddleware } from './httpServer/middleware';
import { Server } from 'http';
import {
  DrizzleClient,
  PrismaClient,
  TypeORMEntityManager,
  UserDatabaseClient,
  UserDatabaseName,
} from './user_database';
import { TransactionConfig, TransactionContextImpl, TransactionFunction } from './transaction';

import Koa from 'koa';
import { Application as ExpressApp } from 'express';
import { INestApplication } from '@nestjs/common';
import { FastifyInstance } from 'fastify';
import _fastifyExpress from '@fastify/express'; // This is for fastify.use()
import { randomUUID } from 'node:crypto';

import { PoolClient } from 'pg';
import { Knex } from 'knex';
import { StepConfig, StepFunction } from './step';
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
import { Hono } from 'hono';
import { Conductor } from './conductor/conductor';
import { PostgresSystemDatabase } from './system_database';
import { wfQueueRunner } from './wfqueue';

// Declare all the options a user can pass to the DBOS object during launch()
export interface DBOSLaunchOptions {
  // HTTP applications to add DBOS tracing middleware to (extract W3C Trace context, set request ID, etc)
  koaApp?: Koa;
  expressApp?: ExpressApp;
  nestApp?: INestApplication;
  fastifyApp?: FastifyInstance;
  honoApp?: Hono;
  // For DBOS Conductor
  conductorURL?: string;
  conductorKey?: string;
  debugMode?: DebugMode;
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
type WorkflowFunc = (ctxt: WorkflowContext, ...args: any[]) => Promise<any>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ProcFunc = (ctxt: StoredProcedureContext, ...args: any[]) => Promise<any>;

// Utility type that only includes transaction/step/proc functions + converts the method signature to exclude the context parameter
type InvokeFuncs<T> = T extends ConfiguredInstance
  ? never
  : {
      [P in keyof T as T[P] extends TxFunc | StepFunc | ProcFunc | WorkflowFunc ? P : never]: T[P] extends
        | TxFunc
        | StepFunc
        | ProcFunc
        | WorkflowFunc
        ? (...args: TailParameters<T[P]>) => ReturnType<T[P]>
        : never;
    };

type InvokeFuncsInst<T> = T extends ConfiguredInstance
  ? {
      [P in keyof T as T[P] extends TxFunc | StepFunc | WorkflowFunc ? P : never]: T[P] extends
        | TxFunc
        | StepFunc
        | WorkflowFunc
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

// Fill in any proxy functions with error-throwing stubs
//  (Goal being to give a clearer error message)
function augmentProxy(target: object, proxy: Record<string, unknown>) {
  let proto = target;
  while (proto && proto !== Object.prototype) {
    for (const k of Reflect.ownKeys(proto)) {
      if (typeof k === 'symbol') continue;
      if (k === 'constructor' || k === 'caller' || k === 'callee' || k === 'arguments') continue; // Skip constructor
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
        if (typeof (target as any)[k] !== 'function') continue;
        if (!Object.hasOwn(proxy, k)) {
          proxy[k] = (..._args: unknown[]) => {
            throw new DBOSNotRegisteredError(k, `${k} is not a registered DBOS function`);
          };
        }
      } catch (e) {}
    }
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    proto = Object.getPrototypeOf(proto);
  }
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
  static conductor: Conductor | undefined = undefined;

  private static getDebugModeFromEnv(): DebugMode {
    const debugWorkflowId = process.env.DBOS_DEBUG_WORKFLOW_ID;
    const isDebugging = debugWorkflowId !== undefined;
    return isDebugging
      ? process.env.DBOS_DEBUG_TIME_TRAVEL === 'true'
        ? DebugMode.TIME_TRAVEL
        : DebugMode.ENABLED
      : DebugMode.DISABLED;
  }

  /**
   * Set configuration of `DBOS` prior to `launch`
   * @param config - configuration of services needed by DBOS
   * @param runtimeConfig - configuration of runtime access to DBOS
   */
  static setConfig(config: DBOSConfig, runtimeConfig?: DBOSRuntimeConfig) {
    DBOS.dbosConfig = config;
    DBOS.runtimeConfig = runtimeConfig;
    DBOS.translateConfig();
  }

  private static translateConfig() {
    if (DBOS.dbosConfig && !isDeprecatedDBOSConfig(DBOS.dbosConfig)) {
      const isDebugging = DBOS.getDebugModeFromEnv() !== DebugMode.DISABLED;
      [DBOS.dbosConfig, DBOS.runtimeConfig] = translatePublicDBOSconfig(DBOS.dbosConfig, isDebugging);
      if (process.env.DBOS__CLOUD === 'true') {
        [DBOS.dbosConfig, DBOS.runtimeConfig] = overwrite_config(
          DBOS.dbosConfig as DBOSConfigInternal,
          DBOS.runtimeConfig,
        );
      }
    }
  }

  /**
   * @deprecated For unit testing purposes only
   *   Use `setConfig`
   */
  static setAppConfig<T>(key: string, newValue: T): void {
    const conf = DBOS.dbosConfig?.application;
    if (!conf) throw new DBOSExecutorNotInitializedError();
    set(conf, key, newValue);
  }

  /**
   * Drop DBOS system database.
   * USE IN TESTS ONLY - ALL WORKFLOWS, QUEUES, ETC. WILL BE LOST.
   */
  static async dropSystemDB(): Promise<void> {
    if (!DBOS.dbosConfig) {
      DBOS.dbosConfig = parseConfigFile()[0];
    }

    DBOS.translateConfig();
    return PostgresSystemDatabase.dropSystemDB(DBOS.dbosConfig as DBOSConfigInternal);
  }

  /**
   * Use ORMEntities to set up database schema.
   * Only relevant for TypeORM, and for testing purposes only, not production
   */
  static async createUserSchema() {
    return DBOSExecutor.globalInstance?.userDatabase.createSchema();
  }

  /**
   * Use ORMEntities to drop database schema.
   * Only relevant for TypeORM, and for testing purposes only, not production
   */
  static async dropUserSchema() {
    return DBOSExecutor.globalInstance?.userDatabase.dropSchema();
  }

  // Load files with DBOS classes (running their decorators)
  static async loadClasses(dbosEntrypointFiles: string[]) {
    return await DBOSRuntime.loadClasses(dbosEntrypointFiles);
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
    // Do nothing is DBOS is already initialized

    if (DBOS.isInitialized()) {
      return;
    }
    const debugMode = options?.debugMode ?? DBOS.getDebugModeFromEnv();
    const isDebugging = debugMode !== DebugMode.DISABLED;

    if (options?.conductorKey) {
      // Use a generated executor ID.
      globalParams.executorID = randomUUID();
    }

    // Initialize the DBOS executor
    if (!DBOS.dbosConfig) {
      [DBOS.dbosConfig, DBOS.runtimeConfig] = parseConfigFile({ forceConsole: isDebugging });
    } else if (!isDeprecatedDBOSConfig(DBOS.dbosConfig)) {
      DBOS.translateConfig(); // This is a defensive measure for users who'd do DBOS.config = X instead of using DBOS.setConfig()
    }

    if (!DBOS.dbosConfig) {
      throw new DBOSError('DBOS configuration not set');
    }

    DBOSExecutor.createInternalQueue();
    DBOSExecutor.globalInstance = new DBOSExecutor(DBOS.dbosConfig as DBOSConfigInternal, {
      debugMode,
    });

    const executor: DBOSExecutor = DBOSExecutor.globalInstance;
    DBOS.globalLogger = executor.logger;
    await executor.init();

    const debugWorkflowId = process.env.DBOS_DEBUG_WORKFLOW_ID;
    if (debugWorkflowId) {
      DBOS.logger.info(`Debugging workflow "${debugWorkflowId}"`);
      const handle = await executor.executeWorkflowUUID(debugWorkflowId);
      await handle.getResult();
      DBOS.logger.info(`Workflow Debugging complete. Exiting process.`);
      await executor.destroy();
      process.exit(0);
      return; // return for cases where process.exit is mocked
    }

    await DBOSExecutor.globalInstance.initEventReceivers();

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
    if (DBOS.runtimeConfig && DBOS.runtimeConfig.runAdminServer) {
      const adminApp = DBOSHttpServer.setupAdminApp(executor);
      try {
        await DBOSHttpServer.checkPortAvailabilityIPv4Ipv6(DBOS.runtimeConfig.admin_port, logger as GlobalLogger);
        // Wrap the listen call in a promise to properly catch errors
        DBOS.adminServer = await new Promise((resolve, reject) => {
          const server = adminApp.listen(DBOS.runtimeConfig?.admin_port, () => {
            DBOS.logger.debug(`DBOS Admin Server is running at http://localhost:${DBOS.runtimeConfig?.admin_port}`);
            resolve(server);
          });
          server.on('error', (err) => {
            reject(err);
          });
        });
      } catch (e) {
        logger.warn(`Unable to start DBOS admin server on port ${DBOS.runtimeConfig.admin_port}`);
      }
    }

    if (options?.koaApp) {
      DBOS.logger.debug('Setting up Koa tracing middleware');
      options.koaApp.use(koaTracingMiddleware);
    }
    if (options?.expressApp) {
      DBOS.logger.debug('Setting up Express tracing middleware');
      options.expressApp.use(expressTracingMiddleware);
    }
    if (options?.fastifyApp) {
      // Fastify can use express or middie under the hood, for middlewares.
      // Middie happens to have the same semantic than express.
      // See https://fastify.dev/docs/latest/Reference/Middleware/
      DBOS.logger.debug('Setting up Fastify tracing middleware');
      options.fastifyApp.use(expressTracingMiddleware);
    }
    if (options?.nestApp) {
      // Nest.kj can use express or fastify under the hood. With fastify, Nest.js uses middie.
      DBOS.logger.debug('Setting up NestJS tracing middleware');
      options.nestApp.use(expressTracingMiddleware);
    }
    if (options?.honoApp) {
      DBOS.logger.debug('Setting up Hono tracing middleware');
      options.honoApp.use(honoTracingMiddleware);
    }

    recordDBOSLaunch();
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
    DBOSExecutor.globalInstance.logRegisteredHTTPUrls();
    DBOSExecutor.globalInstance.scheduler?.logRegisteredSchedulerEndpoints();
    wfQueueRunner.logRegisteredEndpoints(DBOSExecutor.globalInstance);
    for (const evtRcvr of DBOSExecutor.globalInstance.eventReceivers) {
      evtRcvr.logRegisteredEndpoints();
    }
  }

  /**
   * Shut down DBOS processing:
   *   Stops receiving external workflow requests
   *   Disconnects from administration / Conductor
   *   Stops workflow processing and disconnects from databases
   */
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

    // Reset the global app version and executor ID
    globalParams.appVersion = process.env.DBOS__APPVERSION || '';
    globalParams.wasComputed = false;
    globalParams.appID = process.env.DBOS__APPID || '';
    globalParams.executorID = process.env.DBOS__VMID || 'local';

    recordDBOSShutdown();
  }

  /** Stop listening for external events (for testing) */
  static async deactivateEventReceivers() {
    return DBOSExecutor.globalInstance?.deactivateEventReceivers();
  }

  /** Start listening for external events (for testing) */
  static async initEventReceivers() {
    return DBOSExecutor.globalInstance?.initEventReceivers();
  }

  /**
   * Global DBOS executor instance
   */
  static get executor() {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    return DBOSExecutor.globalInstance as DBOSExecutorContext;
  }

  /**
   * Creates a node.js HTTP handler for all entrypoints registered with `@DBOS.getApi`
   * and other decorators.  The handler can be retrieved with `DBOS.getHTTPHandlersCallback()`.
   * This method does not start listening for requests.  For that, call `DBOS.launchAppHTTPServer()`.
   */
  static setUpHandlerCallback() {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    // Create the DBOS HTTP server
    //  This may be a no-op if there are no registered endpoints
    const server = new DBOSHttpServer(DBOSExecutor.globalInstance);

    return server;
  }

  /**
   * Creates a node.js HTTP handler for all entrypoints registered with `@DBOS.getApi`
   * and other decorators.  This method also starts listening for requests, on the port
   * specified in the `DBOSRuntimeConfig`.
   */
  static async launchAppHTTPServer() {
    const server = DBOS.setUpHandlerCallback();
    if (DBOS.runtimeConfig) {
      // This will not listen if there's no decorated endpoint
      DBOS.appServer = await server.appListen(DBOS.runtimeConfig.port);
    }
  }

  /**
   * Retrieves the HTTP handlers callback for DBOS HTTP.
   *  (This is the one that handles the @DBOS.getApi, etc., methods.)
   *   Useful for testing purposes, or to combine the DBOS service with other
   *   node.js HTTP server frameworks.
   */
  static getHTTPHandlersCallback() {
    if (!DBOSHttpServer.instance) {
      return undefined;
    }
    return DBOSHttpServer.instance.app.callback();
  }

  /** For unit testing of admin server (do not call) */
  static getAdminCallback() {
    if (!DBOSHttpServer.instance) {
      return undefined;
    }
    return DBOSHttpServer.instance.adminApp.callback();
  }

  //////
  // Globals
  //////
  static globalLogger?: DLogger;
  static dbosConfig?: DBOSConfig;
  static runtimeConfig?: DBOSRuntimeConfig = undefined;

  //////
  // Context
  //////
  /** Get the current DBOS Logger, appropriate to the current context */
  static get logger(): DLogger {
    const ctx = getCurrentDBOSContext();
    if (ctx) return ctx.logger;
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
  static get span(): Span | undefined {
    const ctx = getCurrentDBOSContext();
    if (ctx) return ctx.span;
    return undefined;
  }

  /** Get the current HTTP request (within `@DBOS.getApi` et al) */
  static getRequest(): HTTPRequest | undefined {
    return getCurrentDBOSContext()?.request;
  }

  /** Get the current HTTP request (within `@DBOS.getApi` et al) */
  static get request(): HTTPRequest {
    const r = DBOS.getRequest();
    if (!r) throw new DBOSError('`DBOS.request` accessed from outside of HTTP requests');
    return r;
  }

  /** Get the current Koa context (within `@DBOS.getApi` et al) */
  static getKoaContext(): Koa.Context | undefined {
    return (getCurrentDBOSContext() as HandlerContext)?.koaContext;
  }

  /** Get the current Koa context (within `@DBOS.getApi` et al) */
  static get koaContext(): Koa.Context {
    const r = DBOS.getKoaContext();
    if (!r) throw new DBOSError('`DBOS.koaContext` accessed from outside koa request');
    return r;
  }

  /** Get the current workflow ID */
  static get workflowID(): string | undefined {
    return getCurrentDBOSContext()?.workflowUUID;
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
    return getCurrentDBOSContext()?.authenticatedUser ?? '';
  }
  /** Get the roles granted to the current authenticated user */
  static get authenticatedRoles(): string[] {
    return getCurrentDBOSContext()?.authenticatedRoles ?? [];
  }
  /** Get the role assumed by the current user giving authorization to execute the current function */
  static get assumedRole(): string {
    return getCurrentDBOSContext()?.assumedRole ?? '';
  }

  /** @returns true if called from within a transaction, false otherwise */
  static isInTransaction(): boolean {
    return getCurrentContextStore()?.curTxFunctionId !== undefined;
  }

  /** @returns true if called from within a stored procedure, false otherwise */
  static isInStoredProc(): boolean {
    return getCurrentContextStore()?.isInStoredProc ?? false;
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
    return DBOS.isWithinWorkflow() && !DBOS.isInTransaction() && !DBOS.isInStep() && !DBOS.isInStoredProc();
  }

  // sql session (various forms)
  /** @returns the current SQL client; only allowed within `@DBOS.transaction` functions */
  static get sqlClient(): UserDatabaseClient {
    if (!DBOS.isInTransaction())
      throw new DBOSInvalidWorkflowTransitionError('Invalid use of `DBOS.sqlClient` outside of a `transaction`');
    const ctx = assertCurrentDBOSContext() as TransactionContextImpl<UserDatabaseClient>;
    return ctx.client;
  }

  /**
   * @returns the current PG SQL client;
   *  only allowed within `@DBOS.transaction` functions when a `PGNODE` user database is in use
   */
  static get pgClient(): PoolClient {
    const client = DBOS.sqlClient;
    if (!DBOS.isInStoredProc() && DBOS.dbosConfig?.userDbclient !== UserDatabaseName.PGNODE) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.pgClient' but client is configured with type '${DBOS.dbosConfig?.userDbclient}'`,
      );
    }
    return client as PoolClient;
  }

  /**
   * @returns the current Knex SQL client;
   *  only allowed within `@DBOS.transaction` functions when a `KNEX` user database is in use
   */
  static get knexClient(): Knex {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.knexClient' from within a stored procedure`);
    }
    if (DBOS.dbosConfig?.userDbclient !== UserDatabaseName.KNEX) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.knexClient' but client is configured with type '${DBOS.dbosConfig?.userDbclient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as Knex;
  }

  /**
   * @returns the current Prisma SQL client;
   *  only allowed within `@DBOS.transaction` functions when a `PRISMA` user database is in use
   */
  static get prismaClient(): PrismaClient {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.prismaClient' from within a stored procedure`);
    }
    if (DBOS.dbosConfig?.userDbclient !== UserDatabaseName.PRISMA) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.prismaClient' but client is configured with type '${DBOS.dbosConfig?.userDbclient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as PrismaClient;
  }

  /**
   * @returns the current  TypeORM SQL client;
   *  only allowed within `@DBOS.transaction` functions when the `TYPEORM` user database is in use
   */
  static get typeORMClient(): TypeORMEntityManager {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.typeORMClient' from within a stored procedure`);
    }
    if (DBOS.dbosConfig?.userDbclient !== UserDatabaseName.TYPEORM) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.typeORMClient' but client is configured with type '${DBOS.dbosConfig?.userDbclient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as TypeORMEntityManager;
  }

  /**
   * @returns the current Drizzle SQL client;
   *  only allowed within `@DBOS.transaction` functions when the `DRIZZLE` user database is in use
   */
  static get drizzleClient(): DrizzleClient {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.drizzleClient' from within a stored procedure`);
    }
    if (DBOS.dbosConfig?.userDbclient !== UserDatabaseName.DRIZZLE) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.drizzleClient' but client is configured with type '${DBOS.dbosConfig?.userDbclient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as DrizzleClient;
  }

  static getConfig<T>(key: string): T | undefined;
  static getConfig<T>(key: string, defaultValue: T): T;
  /**
   * Gets configuration information from the `application` section
   *  of `DBOSConfig`
   * @param key - name of configuration item
   * @param defaultValue - value to return if `key` does not exist in the configuration
   */
  static getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const ctx = getCurrentDBOSContext();
    if (ctx && defaultValue) return ctx.getConfig<T>(key, defaultValue);
    if (ctx) return ctx.getConfig<T>(key);
    if (DBOS.executor) return DBOS.executor.getConfig(key, defaultValue);
    return defaultValue;
  }

  /**
   * Query the current application database
   * @param sql - parameterized SQL statement (string) to execute
   * @param params - parameter values for `sql`
   * @template T - Type for the returned records
   * @returns Array of records returned by the SQL statement
   */
  static async queryUserDB<T = unknown>(sql: string, params?: unknown[]): Promise<T[]> {
    if (DBOS.isWithinWorkflow() && !DBOS.isInStep()) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Invalid call to `queryUserDB` inside a `workflow`, without being in a `step`',
      );
    }

    return DBOS.executor.queryUserDB(sql, params) as Promise<T[]>;
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
  static async getEventDispatchState(
    svc: string,
    wfn: string,
    key: string,
  ): Promise<DBOSEventReceiverState | undefined> {
    return await DBOS.executor.getEventDispatchState(svc, wfn, key);
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
  static async upsertEventDispatchState(state: DBOSEventReceiverState): Promise<DBOSEventReceiverState> {
    return await DBOS.executor.upsertEventDispatchState(state);
  }

  //////
  // Workflow and other operations
  //////

  static runAsWorkflowStep<T>(callback: () => Promise<T>, funcName: string, childWFID?: string): Promise<T> {
    if (DBOS.isWithinWorkflow()) {
      if (DBOS.isInStep()) {
        // OK to use directly
        return DBOSExecutor.globalInstance!.runAsStep<T>(callback, funcName, undefined, undefined, childWFID);
      } else if (DBOS.isInWorkflow()) {
        const wfctx = assertCurrentWorkflowContext();
        return DBOSExecutor.globalInstance!.runAsStep<T>(
          callback,
          funcName,
          DBOS.workflowID,
          wfctx.functionIDGetIncrement(),
          childWFID,
        );
      } else {
        throw new DBOSInvalidWorkflowTransitionError(
          `Invalid call to \`${funcName}\` inside a \`transaction\` or \`procedure\``,
        );
      }
    }
    return DBOSExecutor.globalInstance!.runAsStep<T>(callback, funcName, undefined, undefined, childWFID);
  }

  /**
   * Get the workflow status given a workflow ID
   * @param workflowID - ID of the workflow
   * @returns status of the workflow as `WorkflowStatus`, or `null` if there is no workflow with `workflowID`
   */
  static getWorkflowStatus(workflowID: string): Promise<WorkflowStatus | null> {
    if (DBOS.isWithinWorkflow()) {
      if (DBOS.isInStep()) {
        // OK to use directly
        return DBOS.executor.getWorkflowStatus(workflowID);
      } else if (DBOS.isInWorkflow()) {
        const wfctx = assertCurrentWorkflowContext();
        return DBOS.executor.getWorkflowStatus(workflowID, DBOS.workflowID, wfctx.functionIDGetIncrement());
      } else {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `getWorkflowStatus` inside a `transaction` or `procedure`',
        );
      }
    }
    return DBOS.executor.getWorkflowStatus(workflowID);
  }

  /**
   * Get the workflow result, given a workflow ID
   * @param workflowID - ID of the workflow
   * @param timeoutSeconds - Maximum time to wait for result
   * @returns The return value of the workflow, or throws the exception thrown by the workflow, or `null` if times out
   */
  static async getResult<T>(workflowID: string, timeoutSeconds?: number): Promise<T | null> {
    let timerFuncID: number | undefined = undefined;
    if (DBOS.isWithinWorkflow() && timeoutSeconds !== undefined) {
      timerFuncID = assertCurrentWorkflowContext().functionIDGetIncrement();
    }
    return await DBOS.runAsWorkflowStep(
      async () => {
        const rres = await DBOSExecutor.globalInstance!.systemDatabase.awaitWorkflowResult(
          workflowID,
          timeoutSeconds,
          DBOS.workflowID,
          timerFuncID,
        );
        if (!rres) return null;
        if (rres?.cancelled) throw new DBOSTargetWorkflowCancelledError(`Workflow ${workflowID} was cancelled`); // TODO: Make semantically meaningful
        return DBOSExecutor.reviveResultOrError<T>(rres);
      },
      'DBOS.getResult',
      workflowID,
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
    if (DBOS.isWithinWorkflow()) {
      if (!DBOS.isInWorkflow()) {
        throw new DBOSInvalidWorkflowTransitionError(
          'Invalid call to `retrieveWorkflow` inside a `transaction` or `step`',
        );
      }
      return (getCurrentDBOSContext()! as WorkflowContext).retrieveWorkflow(workflowID);
    }
    return DBOS.executor.retrieveWorkflow(workflowID);
  }

  /**
   * Query the system database for all workflows matching the provided predicate
   * @param input - `GetWorkflowsInput` predicate for filtering returned workflows
   * @returns `GetWorkflowsOutput` listing the workflow IDs of matching workflows
   * @deprecated Use `DBOS.listWorkflows` instead
   */
  static async getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput> {
    return await DBOS.runAsWorkflowStep(async () => {
      const wfs = await DBOS.executor.listWorkflows(input);
      return { workflowUUIDs: wfs.map((wf) => wf.workflowID) };
    }, 'DBOS.getWorkflows');
  }

  /**
   * Query the system database for all workflows matching the provided predicate
   * @param input - `GetWorkflowsInput` predicate for filtering returned workflows
   * @returns `GetWorkflowsOutput` listing the workflow IDs of matching workflows
   */
  static async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    return await DBOS.runAsWorkflowStep(async () => {
      return await DBOS.executor.listWorkflows(input);
    }, 'DBOS.listWorkflows');
  }

  static async listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatus[]> {
    return await DBOS.runAsWorkflowStep(async () => {
      return await DBOS.executor.listQueuedWorkflows(input);
    }, 'DBOS.listQueuedWorkflows');
  }
  /**
   * Cancel a workflow given its ID.
   * If the workflow is currently running, `DBOSWorkflowCancelledError` will be
   *   thrown from its next DBOS call.
   * @param workflowID - ID of the workflow
   */
  static async cancelWorkflow(workflowID: string) {
    return await DBOS.runAsWorkflowStep(async () => {
      return await DBOS.executor.cancelWorkflow(workflowID);
    }, 'DBOS.cancelWorkflow');
  }

  /**
   * Resume a workflow given its ID.
   * @param workflowID - ID of the workflow
   */
  static async resumeWorkflow<T>(workflowID: string): Promise<WorkflowHandle<Awaited<T>>> {
    await DBOS.runAsWorkflowStep(async () => {
      return await DBOS.executor.resumeWorkflow(workflowID);
    }, 'DBOS.resumeWorkflow');
    return this.retrieveWorkflow(workflowID);
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
    startStep: number = 0,
    applicationVersion?: string,
  ): Promise<WorkflowHandle<Awaited<T>>> {
    const maxStepID = await DBOS.executor.getMaxStepID(workflowID);

    if (startStep > maxStepID) {
      throw new DBOSInvalidStepIDError(workflowID, startStep, maxStepID);
    }

    const forkedID = await DBOS.runAsWorkflowStep(async () => {
      return await DBOS.executor.forkWorkflow(workflowID, startStep, applicationVersion);
    }, 'DBOS.forkWorkflow');

    return this.retrieveWorkflow(forkedID);
  }

  /**
   * Retrieve the contents of a workflow queue.
   * @param input - Filter predicate, containing the queue name and other criteria
   */
  static async getWorkflowQueue(input: GetWorkflowQueueInput): Promise<GetWorkflowQueueOutput> {
    return await DBOS.runAsWorkflowStep(async () => {
      return await DBOS.executor.getWorkflowQueue(input);
    }, 'DBOS.getWorkflowQueue');
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
      return (getCurrentDBOSContext()! as WorkflowContext).sleepms(durationMS);
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
   * Use the provided `workflowID` as the identifier for first workflow started
   *   within the `callback` function.
   * @param workflowID - ID to assign to the first workflow started
   * @param callback - Function to run, which would start a workflow
   * @returns - Return value from `callback`
   */
  static async withNextWorkflowID<R>(workflowID: string, callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      const pcwfid = pctx.idAssignedForNextWorkflow;
      try {
        pctx.idAssignedForNextWorkflow = workflowID;
        return callback();
      } finally {
        pctx.idAssignedForNextWorkflow = pcwfid;
      }
    } else {
      return runWithTopContext({ idAssignedForNextWorkflow: workflowID }, callback);
    }
  }

  /**
   * Use the provided `callerName`, `span`, and `request` as context for any
   *   DBOS functions called within the `callback` function.
   * @param callerName - Tracing caller name
   * @param span - Tracing span
   * @param request - HTTP request that initiated the call
   * @param callback - Function to run with tracing context in place
   * @returns - Return value from `callback`
   */
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
    const pctx = getCurrentContextStore();
    if (pctx) {
      pctx.authenticatedUser = authedUser;
      pctx.authenticatedRoles = authedRoles;
      return callback();
    } else {
      return runWithTopContext({ authenticatedUser: authedUser, authenticatedRoles: authedRoles }, callback);
    }
  }

  /**
   * This generic setter helps users calling DBOS operation to pass a name,
   *   later used in seeding a parent OTel span for the operation.
   * @param callerName - Tracing caller name
   * @param callback - Function to run with tracing context in place
   * @returns - Return value from `callback`
   */
  static async withNamedContext<R>(callerName: string, callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      pctx.operationCaller = callerName;
      return callback();
    } else {
      return runWithTopContext({ operationCaller: callerName }, callback);
    }
  }

  /**
   * Use queue named `queueName` for any workflows started within the `callback`.
   * @param queueName - Name of queue upon which qll workflows called or started within `callback` will be run
   * @param callback - Function to run, which would call or start workflows
   * @returns - Return value from `callback`
   */
  static async withWorkflowQueue<R>(queueName: string, callback: () => Promise<R>): Promise<R> {
    const pctx = getCurrentContextStore();
    if (pctx) {
      const pcwfq = pctx.queueAssignedForWorkflows;
      try {
        pctx.queueAssignedForWorkflows = queueName;
        return callback();
      } finally {
        pctx.queueAssignedForWorkflows = pcwfq;
      }
    } else {
      return runWithTopContext({ queueAssignedForWorkflows: queueName }, callback);
    }
  }

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
        if (op.workflowConfig) {
          proxy[op.name] = (...args: unknown[]) =>
            DBOSExecutor.globalInstance!.internalWorkflow(
              op.registeredFunction as WorkflowFunction<unknown[], unknown>,
              wfParams,
              wfctx.workflowUUID,
              funcId,
              ...args,
            );
        } else if (op.txnConfig) {
          const txn = op.registeredFunction as TransactionFunction<unknown[], unknown>;
          proxy[op.name] = (...args: unknown[]) =>
            DBOSExecutor.globalInstance!.startTransactionTempWF(txn, wfParams, wfctx.workflowUUID, funcId, ...args);
        } else if (op.stepConfig) {
          const step = op.registeredFunction as StepFunction<unknown[], unknown>;
          proxy[op.name] = (...args: unknown[]) => {
            return DBOSExecutor.globalInstance!.startStepTempWF(step, wfParams, wfctx.workflowUUID, funcId, ...args);
          };
        } else {
          proxy[op.name] = (..._args: unknown[]) => {
            throw new DBOSNotRegisteredError(
              op.name,
              `${op.name} is not a registered DBOS workflow, step, or transaction function`,
            );
          };
        }
      }

      augmentProxy(configuredInstance ?? object, proxy);

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
      if (op.workflowConfig) {
        proxy[op.name] = (...args: unknown[]) =>
          DBOS.executor.workflow(op.registeredFunction as WorkflowFunction<unknown[], unknown>, wfParams, ...args);
      } else if (op.txnConfig) {
        const txn = op.registeredFunction as TransactionFunction<unknown[], unknown>;
        proxy[op.name] = (...args: unknown[]) =>
          DBOSExecutor.globalInstance!.startTransactionTempWF(txn, wfParams, undefined, undefined, ...args);
      } else if (op.stepConfig) {
        const step = op.registeredFunction as StepFunction<unknown[], unknown>;
        proxy[op.name] = (...args: unknown[]) =>
          DBOSExecutor.globalInstance!.startStepTempWF(step, wfParams, undefined, undefined, ...args);
      } else {
        proxy[op.name] = (..._args: unknown[]) => {
          throw new DBOSNotRegisteredError(
            op.name,
            `${op.name} is not a registered DBOS workflow, step, or transaction function`,
          );
        };
      }
    }

    augmentProxy(configuredInstance ?? object, proxy);

    return proxy as InvokeFunctionsAsync<T>;
  }

  /** @deprecated Adjust target function to exclude its `DBOSContext` argument, and then call the function directly */
  static invoke<T extends ConfiguredInstance>(targetCfg: T): InvokeFuncsInst<T>;
  static invoke<T extends object>(targetClass: T): InvokeFuncs<T>;
  static invoke<T extends object>(object: T | ConfiguredInstance): InvokeFuncs<T> | InvokeFuncsInst<T> {
    if (!DBOS.isWithinWorkflow()) {
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
      };

      // Run the temp workflow way...
      if (typeof object === 'function') {
        const ops = getRegisteredOperations(object);

        const proxy: Record<string, unknown> = {};
        for (const op of ops) {
          proxy[op.name] = op.txnConfig
            ? (...args: unknown[]) =>
                DBOSExecutor.globalInstance!.transaction(
                  op.registeredFunction as TransactionFunction<unknown[], unknown>,
                  wfParams,
                  ...args,
                )
            : op.stepConfig
              ? (...args: unknown[]) =>
                  DBOSExecutor.globalInstance!.external(
                    op.registeredFunction as StepFunction<unknown[], unknown>,
                    wfParams,
                    ...args,
                  )
              : op.procConfig
                ? (...args: unknown[]) =>
                    DBOSExecutor.globalInstance!.procedure<unknown[], unknown>(
                      op.registeredFunction as StoredProcedure<unknown[], unknown>,
                      wfParams,
                      ...args,
                    )
                : op.workflowConfig
                  ? async (...args: unknown[]) =>
                      (
                        await DBOSExecutor.globalInstance!.workflow<unknown[], unknown>(
                          op.registeredFunction as WorkflowFunction<unknown[], unknown>,
                          wfParams,
                          ...args,
                        )
                      ).getResult()
                  : (..._args: unknown[]) => {
                      throw new DBOSNotRegisteredError(
                        op.name,
                        `${op.name} is not a registered DBOS step, transaction, or procedure`,
                      );
                    };
        }

        augmentProxy(object, proxy);

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
                  { ...wfParams, configuredInstance: targetInst },
                  ...args,
                )
            : op.stepConfig
              ? (...args: unknown[]) =>
                  DBOSExecutor.globalInstance!.external(
                    op.registeredFunction as StepFunction<unknown[], unknown>,
                    { ...wfParams, configuredInstance: targetInst },
                    ...args,
                  )
              : op.workflowConfig
                ? async (...args: unknown[]) =>
                    (
                      await DBOSExecutor.globalInstance!.workflow(
                        op.registeredFunction as WorkflowFunction<unknown[], unknown>,
                        { ...wfParams, configuredInstance: targetInst },
                        ...args,
                      )
                    ).getResult()
                : (..._args: unknown[]) => {
                    throw new DBOSNotRegisteredError(
                      op.name,
                      `${op.name} is not a registered DBOS step or transaction`,
                    );
                  };
        }

        augmentProxy(targetInst, proxy);

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
                  undefined,
                  undefined,
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
              : (..._args: unknown[]) => {
                  throw new DBOSNotRegisteredError(
                    op.name,
                    `${op.name} is not a registered DBOS step, transaction, or procedure`,
                  );
                };
      }

      augmentProxy(object, proxy);

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
                  undefined,
                  undefined,
                  targetInst,
                  wfctx,
                  ...args,
                )
            : undefined;
      }

      augmentProxy(targetInst, proxy);

      return proxy as InvokeFuncsInst<T>;
    }
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
   */
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

  /**
   * Receive a message on optional `topic` from within a workflow.
   *  This must be called from within a workflow; this workflow's ID is used to check for messages sent by `DBOS.send`
   *  This can be configured to time out.
   *  Messages are received in the order in which they are sent (per-sender / causal order).
   * @see `DBOS.send`
   *
   * @param topic - Optional topic; if specified the `recv` command can specify the same topic to receive selectively
   * @param timeoutSeconds - Optional timeout; if no message is received before the timeout, `null` will be returned
   * @template T - The type of message that is expected to be received
   * @returns Any message received, or `null` if the timeout expires
   */
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

  /**
   * Set an event, from within a DBOS workflow.  This value can be retrieved with `DBOS.getEvent`.
   * If the event `key` already exists, its `value` is updated.
   * This function can only be called from within a workflow.
   * @see `DBOS.getEvent`
   *
   * @param key - The key for the event; at most one value is associated with a key at any given time.
   * @param value - The value to associate with `key`
   */
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

  /**
   * Get the value of a workflow event, or wait for it to be set.
   * This function can be called inside or outside of DBOS workflow functions.
   * If this function is called from within a workflow, its result is durably checkpointed.
   * @see `DBOS.setEvent`
   *
   * @param workflowID - The ID of the workflow with the corresponding `setEvent`
   * @param key - The key for the event; at most one value is associated with a key at any given time.
   * @param timeoutSeconds - Optional timeout; if a value for `key` is not set before the timeout, `null` will be returned
   * @template T - The expected type for the value assigned to `key`
   * @returns The value to associate with `key`, or `null` if the timeout is hit
   */
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
  /**
   * Decorator associating a class static method with an invocation schedule
   * @param schedulerConfig - The schedule, consisting of a crontab and policy for "make-up work"
   */
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

  static registerWorkflow<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    target: {
      classOrInst?: object;
      className?: string;
      name: string;
      config?: WorkflowConfig;
    },
  ): (this: This, ...args: Args) => Promise<Return> {
    const { registration } = registerAndWrapDBOSFunctionByName(target.classOrInst, target.className, target.name, func);
    registration.setWorkflowConfig(target.config ?? {});

    const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
      const pctx = getCurrentContextStore();
      let inst: ConfiguredInstance | undefined = undefined;
      if (this) {
        if (typeof this === 'function') {
          // This is static
        } else {
          inst = this as unknown as ConfiguredInstance;
          if (!('name' in inst)) {
            throw new DBOSInvalidWorkflowTransitionError(
              'Attempt to call a `workflow` function on an object that is not a `ConfiguredInstance`',
            );
          }
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

    // TODO: CB - clean up
    registerFunctionWrapper(invokeWrapper, registration as MethodRegistration<unknown, unknown[], unknown>);
    Object.defineProperty(invokeWrapper, 'name', {
      value: registration.name,
    });
    return invokeWrapper;
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

      return descriptor;
    }
    return decorator;
  }

  /**
   * Decorator designating a method as a DBOS transaction, making SQL clients available.
   *   A durable execution checkpoint will be applied to to the underlying database transaction
   * @see `DBOS.sqlClient`
   * @param config - Configuration information for the transaction, particularly its isolation mode
   */
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

  /**
   * Decorator designating a method as a DBOS stored procedure.
   *   Within the procedure, `DBOS.sqlClient` is available for database operations.
   *   A durable execution checkpoint will be applied to to the underlying database transaction
   * @param config - Configuration information for the stored procedure, particularly its execution mode
   */
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

  static registerStep<This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<Return>,
    target: {
      name: string;
      config?: StepConfig;
    },
  ): (this: This, ...args: Args) => Promise<Return> {
    const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
      let inst: ConfiguredInstance | undefined = undefined;
      if (this && typeof this !== 'function') {
        if (Object.hasOwn(this, 'name')) {
          inst = this as unknown as ConfiguredInstance;
        }
      }

      if (DBOS.isWithinWorkflow()) {
        if (DBOS.isInTransaction()) {
          throw new DBOSInvalidWorkflowTransitionError('Invalid call to a `step` function from within a `transaction`');
        }
        if (DBOS.isInStep()) {
          // There should probably be checks here about the compatibility of the StepConfig...
          return func.call(this, ...rawArgs);
        }
        const wfctx = assertCurrentWorkflowContext();
        return await DBOSExecutor.globalInstance!.callStepFunction(
          func as unknown as StepFunction<Args, Return>,
          target.name,
          target?.config ?? {},
          inst ?? this ?? null,
          wfctx,
          ...rawArgs,
        );
      }

      throw new DBOSInvalidWorkflowTransitionError(`Call to step '${target.name}' outside of a workflow`);
    };

    Object.defineProperty(invokeWrapper, 'name', {
      value: target.name,
    });
    return invokeWrapper;
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
            undefined,
            undefined,
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

  /** Decorator indicating that the method is the target of HTTP GET operations for `url` */
  static getApi(url: string) {
    return httpApiDec(APITypes.GET, url);
  }

  /** Decorator indicating that the method is the target of HTTP POST operations for `url` */
  static postApi(url: string) {
    return httpApiDec(APITypes.POST, url);
  }

  /** Decorator indicating that the method is the target of HTTP PUT operations for `url` */
  static putApi(url: string) {
    return httpApiDec(APITypes.PUT, url);
  }

  /** Decorator indicating that the method is the target of HTTP PATCH operations for `url` */
  static patchApi(url: string) {
    return httpApiDec(APITypes.PATCH, url);
  }

  /** Decorator indicating that the method is the target of HTTP DELETE operations for `url` */
  static deleteApi(url: string) {
    return httpApiDec(APITypes.DELETE, url);
  }

  /**
   * Decorate a class with the default list of required roles.
   *   This class-level default can be overridden on a per-function basis with `requiredRole`.
   * @param anyOf - The list of roles allowed access; authorization is granted if the authenticated user has any role on the list
   */
  static defaultRequiredRole(anyOf: string[]) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    function clsdec<T extends { new (...args: any[]): object }>(ctor: T) {
      const clsreg = getOrCreateClassRegistration(ctor);
      clsreg.requiredRole = anyOf;
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
      const { descriptor, registration } = registerAndWrapDBOSFunction(target, propertyKey, inDescriptor);
      registration.requiredRole = anyOf;

      return descriptor;
    }
    return apidec;
  }

  /////
  // Registration, etc
  /////
  /**
   * Construct and register an object.
   * Calling this is not necessary; calling the constructor of any `ConfiguredInstance` subclass is sufficient
   * @deprecated Use `new` directly
   */
  static configureInstance<R extends ConfiguredInstance, T extends unknown[]>(
    cls: new (name: string, ...args: T) => R,
    name: string,
    ...args: T
  ): R {
    return configureInstance(cls, name, ...args);
  }

  // Function registration - for internal use
  static registerAndWrapDBOSFunction<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    return registerAndWrapDBOSFunction(target, propertyKey, descriptor);
  }

  // For internal and testing purposes
  static async executeWorkflowById(
    workflowId: string,
    startNewWorkflow: boolean = false,
  ): Promise<WorkflowHandle<unknown>> {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    return DBOSExecutor.globalInstance.executeWorkflowUUID(workflowId, startNewWorkflow);
  }

  // For internal and testing purposes
  static async recoverPendingWorkflows(executorIDs: string[] = ['local']): Promise<WorkflowHandle<unknown>[]> {
    if (!DBOSExecutor.globalInstance) {
      throw new DBOSExecutorNotInitializedError();
    }
    return DBOSExecutor.globalInstance.recoverPendingWorkflows(executorIDs);
  }
}

/** @deprecated */
export class InitContext {
  createUserSchema(): Promise<void> {
    DBOS.logger.warn(
      'Schema synchronization is deprecated and unsafe for production use. Please use migrations instead: https://typeorm.io/migrations',
    );
    return DBOS.createUserSchema();
  }

  dropUserSchema(): Promise<void> {
    DBOS.logger.warn(
      'Schema synchronization is deprecated and unsafe for production use. Please use migrations instead: https://typeorm.io/migrations',
    );
    return DBOS.dropUserSchema();
  }

  queryUserDB<R>(sql: string, ...params: unknown[]): Promise<R[]> {
    return DBOS.queryUserDB(sql, params);
  }

  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const value = DBOS.getConfig(key, defaultValue);
    // If the key is found and the default value is provided, check whether the value is of the same type.
    if (value && defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }
    return value;
  }
}
