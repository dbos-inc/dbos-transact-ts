import { Span } from '@opentelemetry/sdk-trace-base';
import {
  getCurrentContextStore,
  HTTPRequest,
  runWithTopContext,
  getNextWFID,
  StepStatus,
  DBOSContextOptions,
  functionIDGetIncrement,
} from './context';
import { DBOSConfig, DBOSExecutor, DBOSExternalState, InternalWorkflowParams } from './dbos-executor';
import { Tracer } from './telemetry/traces';
import {
  GetQueuedWorkflowsInput,
  GetWorkflowsInput,
  GetWorkflowsOutput,
  RetrievedHandle,
  StepInfo,
  WorkflowConfig,
  WorkflowParams,
  WorkflowStatus,
} from './workflow';
import { DLogger, GlobalLogger } from './telemetry/logs';
import {
  DBOSError,
  DBOSExecutorNotInitializedError,
  DBOSInvalidWorkflowTransitionError,
  DBOSNotRegisteredError,
  DBOSAwaitedWorkflowCancelledError,
} from './error';
import {
  getDbosConfig,
  getRuntimeConfig,
  overwriteConfigForDBOSCloud,
  readConfigFile,
  translateDbosConfig,
  translateRuntimeConfig,
} from './dbos-runtime/config';
import { DBOSRuntime } from './dbos-runtime/runtime';
import { ScheduledArgs, ScheduledReceiver, SchedulerConfig } from './scheduler/scheduler';
import {
  associateClassWithExternal,
  associateMethodWithExternal,
  associateParameterWithExternal,
  ClassAuthDefaults,
  DBOS_AUTH,
  ExternalRegistration,
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
  transactionalDataSources,
  registerMiddlewareInstaller,
  MethodRegistrationBase,
  TypedAsyncFunction,
  UntypedAsyncFunction,
  FunctionName,
  wrapDBOSFunctionAndRegisterByUniqueName,
  wrapDBOSFunctionAndRegisterByUniqueNameDec,
  wrapDBOSFunctionAndRegister,
  wrapDBOSFunctionAndRegisterDec,
} from './decorators';
import { DBOSJSON, globalParams, sleepms } from './utils';
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
import { TransactionConfig } from './transaction';

import Koa from 'koa';
import { Application as ExpressApp } from 'express';
import { INestApplication } from '@nestjs/common';
import { FastifyInstance } from 'fastify';
import _fastifyExpress from '@fastify/express'; // This is for fastify.use()
import { randomUUID } from 'node:crypto';

import { PoolClient } from 'pg';
import { Knex } from 'knex';
import { StepConfig } from './step';
import { DBOSLifecycleCallback, DBOSMethodMiddlewareInstaller, requestArgValidation, WorkflowHandle } from '.';
import { ConfiguredInstance } from '.';
import { StoredProcedureConfig } from './procedure';
import { APITypes } from './httpServer/handlerTypes';
import { HandlerRegistrationBase } from './httpServer/handler';
import { Hono } from 'hono';
import { Conductor } from './conductor/conductor';
import { EnqueueOptions } from './system_database';
import { wfQueueRunner } from './wfqueue';
import { registerAuthChecker } from './authdecorators';
import assert from 'node:assert';

type AnyConstructor = new (...args: unknown[]) => object;

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
  debugMode?: boolean;
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

function httpApiDec(verb: APITypes, url: string) {
  return function apidec<This, Args extends unknown[], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    const { descriptor, registration } = wrapDBOSFunctionAndRegisterDec(target, propertyKey, inDescriptor);
    const handlerRegistration = registration as unknown as HandlerRegistrationBase;
    handlerRegistration.apiURL = url;
    handlerRegistration.apiType = verb;
    requestArgValidation(registration);

    return descriptor;
  };
}

export interface StartWorkflowParams {
  workflowID?: string;
  queueName?: string;
  timeoutMS?: number | null;
  enqueueOptions?: EnqueueOptions;
}

export function getExecutor() {
  if (!DBOSExecutor.globalInstance) {
    throw new DBOSExecutorNotInitializedError();
  }
  return DBOSExecutor.globalInstance;
}

export function runInternalStep<T>(callback: () => Promise<T>, funcName: string, childWFID?: string): Promise<T> {
  if (DBOS.isWithinWorkflow()) {
    if (DBOS.isInStep()) {
      // OK to use directly
      return callback();
    } else if (DBOS.isInWorkflow()) {
      return DBOSExecutor.globalInstance!.runInternalStep<T>(
        callback,
        funcName,
        DBOS.workflowID!,
        functionIDGetIncrement(),
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
  static appServer: Server | undefined = undefined;
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
   * Use ORMEntities to set up database schema.
   * Only relevant for TypeORM, and for testing purposes only, not production
   * @deprecated - use data source packages such as `@dbos-inc/typeorm-datasource`
   */
  static async createUserSchema() {
    return DBOSExecutor.globalInstance?.userDatabase.createSchema();
  }

  /**
   * Use ORMEntities to drop database schema.
   * Only relevant for TypeORM, and for testing purposes only, not production
   * @deprecated - use data source packages such as `@dbos-inc/typeorm-datasource`
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
    insertAllMiddleware();

    if (DBOS.isInitialized()) {
      return;
    }

    if (options?.conductorKey) {
      // Use a generated executor ID.
      globalParams.executorID = randomUUID();
    }

    const debugMode = options?.debugMode ?? process.env.DBOS_DEBUG_WORKFLOW_ID !== undefined;
    const configFile = readConfigFile();

    const $dbosConfig = DBOS.#dbosConfig;

    let internalConfig = $dbosConfig
      ? translateDbosConfig($dbosConfig, { forceConsole: debugMode })
      : getDbosConfig(configFile);
    let runtimeConfig = $dbosConfig ? translateRuntimeConfig($dbosConfig) : getRuntimeConfig(configFile);

    if (process.env.DBOS__CLOUD === 'true') {
      [internalConfig, runtimeConfig] = overwriteConfigForDBOSCloud(internalConfig, runtimeConfig, configFile);
    }

    DBOS.#port = runtimeConfig.port;
    DBOS.#dbosConfig = {
      name: internalConfig.name,
      databaseUrl: internalConfig.databaseUrl,
      userDatabaseClient: internalConfig.userDbClient,
      userDatabasePoolSize: internalConfig.userDbPoolSize,
      systemDatabaseUrl: internalConfig.systemDatabaseUrl,
      systemDatabasePoolSize: internalConfig.sysDbPoolSize,
      logLevel: internalConfig.telemetry.logs?.logLevel,
      otlpTracesEndpoints: [...(internalConfig.telemetry.OTLPExporter?.tracesEndpoint ?? [])],
      otlpLogsEndpoints: [...(internalConfig.telemetry.OTLPExporter?.logsEndpoint ?? [])],
      adminPort: runtimeConfig.admin_port,
      runAdminServer: runtimeConfig.runAdminServer,
    };

    if (globalParams.appName === '' && DBOS.#dbosConfig.name) {
      globalParams.appName = DBOS.#dbosConfig.name;
    }

    DBOSExecutor.createInternalQueue();
    DBOSExecutor.globalInstance = new DBOSExecutor(internalConfig, { debugMode });

    const executor: DBOSExecutor = DBOSExecutor.globalInstance;
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
      const adminApp = DBOSHttpServer.setupAdminApp(executor);
      try {
        await DBOSHttpServer.checkPortAvailabilityIPv4Ipv6(runtimeConfig.admin_port, logger as GlobalLogger);
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

    for (const [_n, ds] of transactionalDataSources) {
      await ds.destroy();
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

  // Global DBOS executor instance
  static get #executor() {
    return getExecutor();
  }

  /**
   * Creates a node.js HTTP handler for all entrypoints registered with `@DBOS.getApi`
   * and other decorators.  The handler can be retrieved with `DBOS.getHTTPHandlersCallback()`.
   * This method does not start listening for requests.  For that, call `DBOS.launchAppHTTPServer()`.
   * @deprecated - use `@dbos-inc/koa-serve`
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
   * @deprecated - use `@dbos-inc/koa-serve`
   */
  static async launchAppHTTPServer() {
    const server = DBOS.setUpHandlerCallback();
    if (DBOS.#port) {
      // This will not listen if there's no decorated endpoint
      DBOS.appServer = await server.appListen(DBOS.#port);
    }
  }

  /**
   * Retrieves the HTTP handlers callback for DBOS HTTP.
   *  (This is the one that handles the @DBOS.getApi, etc., methods.)
   *   Useful for testing purposes, or to combine the DBOS service with other
   *   node.js HTTP server frameworks.
   * @deprecated - use `@dbos-inc/koa-serve`
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
  static #dbosConfig?: DBOSConfig;
  static #port?: number;

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
  static get span(): Span | undefined {
    return getCurrentContextStore()?.span;
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

  /**
   * Get the current Koa context (within `@DBOS.getApi` et al)
   * @deprecated - use `@dbos-inc/koa-serve`
   */
  static getKoaContext(): Koa.Context | undefined {
    return getCurrentContextStore()?.koaContext;
  }

  /**
   * Get the current Koa context (within `@DBOS.getApi` et al)
   * @deprecated - use `@dbos-inc/koa-serve`
   */
  static get koaContext(): Koa.Context {
    const r = DBOS.getKoaContext();
    if (!r) throw new DBOSError('`DBOS.koaContext` accessed from outside koa request');
    return r;
  }

  /** Get the current workflow ID */
  static get workflowID(): string | undefined {
    return getCurrentContextStore()?.workflowId;
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
  /**
   * @returns the current SQL client; only allowed within `@DBOS.transaction` functions
   * @deprecated - use data source packages such as:
   *  `@dbos-inc/drizzle-datasource`
   *  `@dbos-inc/knex-datasource`
   *  `@dbos-inc/node-pg-datasource`
   *  `@dbos-inc/postgres-datasource`
   *  `@dbos-inc/prisma-datasource`
   *  `@dbos-inc/typeorm-datasource`
   */
  static get sqlClient(): UserDatabaseClient {
    const c = getCurrentContextStore()?.sqlClient;
    if (!DBOS.isInTransaction() || !c)
      throw new DBOSInvalidWorkflowTransitionError('Invalid use of `DBOS.sqlClient` outside of a `transaction`');
    return c;
  }

  /**
   * @returns the current PG SQL client;
   *  only allowed within `@DBOS.transaction` functions when a `PGNODE` user database is in use
   * @deprecated - use data source packages such as `@dbos-inc/node-pg-datasource`
   */
  static get pgClient(): PoolClient {
    const client = DBOS.sqlClient;
    if (!DBOS.isInStoredProc() && DBOS.#dbosConfig?.userDatabaseClient !== UserDatabaseName.PGNODE) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.pgClient' but client is configured with type '${DBOS.#dbosConfig?.userDatabaseClient}'`,
      );
    }
    return client as PoolClient;
  }

  /**
   * @returns the current Knex SQL client;
   *  only allowed within `@DBOS.transaction` functions when a `KNEX` user database is in use
   * @deprecated - use `@dbos-inc/knex-datasource` package
   */
  static get knexClient(): Knex {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.knexClient' from within a stored procedure`);
    }
    if (DBOS.#dbosConfig?.userDatabaseClient !== UserDatabaseName.KNEX) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.knexClient' but client is configured with type '${DBOS.#dbosConfig?.userDatabaseClient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as Knex;
  }

  /**
   * @returns the current Prisma SQL client;
   *  only allowed within `@DBOS.transaction` functions when a `PRISMA` user database is in use
   * @deprecated - use `@dbos-inc/prisma-datasource` package
   */
  static get prismaClient(): PrismaClient {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.prismaClient' from within a stored procedure`);
    }
    if (DBOS.#dbosConfig?.userDatabaseClient !== UserDatabaseName.PRISMA) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.prismaClient' but client is configured with type '${DBOS.#dbosConfig?.userDatabaseClient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as PrismaClient;
  }

  /**
   * @returns the current  TypeORM SQL client;
   *  only allowed within `@DBOS.transaction` functions when the `TYPEORM` user database is in use
   * @deprecated - use `@dbos-inc/typeorm-datasource` package
   */
  static get typeORMClient(): TypeORMEntityManager {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.typeORMClient' from within a stored procedure`);
    }
    if (DBOS.#dbosConfig?.userDatabaseClient !== UserDatabaseName.TYPEORM) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.typeORMClient' but client is configured with type '${DBOS.#dbosConfig?.userDatabaseClient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as TypeORMEntityManager;
  }

  /**
   * @returns the current Drizzle SQL client;
   *  only allowed within `@DBOS.transaction` functions when the `DRIZZLE` user database is in use
   * @deprecated - use `@dbos-inc/drizzle-datasource` package
   */
  static get drizzleClient(): DrizzleClient {
    if (DBOS.isInStoredProc()) {
      throw new DBOSInvalidWorkflowTransitionError(`Requested 'DBOS.drizzleClient' from within a stored procedure`);
    }
    if (DBOS.#dbosConfig?.userDatabaseClient !== UserDatabaseName.DRIZZLE) {
      throw new DBOSInvalidWorkflowTransitionError(
        `Requested 'DBOS.drizzleClient' but client is configured with type '${DBOS.#dbosConfig?.userDatabaseClient}'`,
      );
    }
    const client = DBOS.sqlClient;
    return client as DrizzleClient;
  }

  /**
   * Query the current application database
   * @param sql - parameterized SQL statement (string) to execute
   * @param params - parameter values for `sql`
   * @template T - Type for the returned records
   * @returns Array of records returned by the SQL statement
   * @deprecated - use data source packages such as:
   *  `@dbos-inc/drizzle-datasource`
   *  `@dbos-inc/knex-datasource`
   *  `@dbos-inc/node-pg-datasource`
   *  `@dbos-inc/postgres-datasource`
   *  `@dbos-inc/prisma-datasource`
   *  `@dbos-inc/typeorm-datasource`
   */
  static async queryUserDB<T = unknown>(sql: string, params?: unknown[]): Promise<T[]> {
    if (DBOS.isWithinWorkflow() && !DBOS.isInStep()) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Invalid call to `queryUserDB` inside a `workflow`, without being in a `step`',
      );
    }

    return DBOS.#executor.queryUserDB(sql, params) as Promise<T[]>;
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
   * @param timeoutSeconds - Maximum time to wait for result
   * @returns The return value of the workflow, or throws the exception thrown by the workflow, or `null` if times out
   */
  static async getResult<T>(workflowID: string, timeoutSeconds?: number): Promise<T | null> {
    let timerFuncID: number | undefined = undefined;
    if (DBOS.isWithinWorkflow() && timeoutSeconds !== undefined) {
      timerFuncID = functionIDGetIncrement();
    }
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
      const functionID: number = functionIDGetIncrement();
      return new RetrievedHandle(DBOSExecutor.globalInstance!.systemDatabase, workflowID, DBOS.workflowID, functionID);
    }
    return DBOS.#executor.retrieveWorkflow(workflowID);
  }

  /**
   * Query the system database for all workflows matching the provided predicate
   * @param input - `GetWorkflowsInput` predicate for filtering returned workflows
   * @returns `GetWorkflowsOutput` listing the workflow IDs of matching workflows
   * @deprecated Use `DBOS.listWorkflows` instead
   */
  static async getWorkflows(input: GetWorkflowsInput): Promise<GetWorkflowsOutput> {
    return await runInternalStep(async () => {
      const wfs = await DBOS.#executor.listWorkflows(input);
      return { workflowUUIDs: wfs.map((wf) => wf.workflowID) };
    }, 'DBOS.getWorkflows');
  }

  /**
   * Query the system database for all workflows matching the provided predicate
   * @param input - `GetWorkflowsInput` predicate for filtering returned workflows
   * @returns `WorkflowStatus` array containing details of the matching workflows
   */
  static async listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]> {
    return await runInternalStep(async () => {
      return await DBOS.#executor.listWorkflows(input);
    }, 'DBOS.listWorkflows');
  }

  /**
   * Query the system database for all queued workflows matching the provided predicate
   * @param input - `GetQueuedWorkflowsInput` predicate for filtering returned workflows
   * @returns `WorkflowStatus` array containing details of the matching workflows
   */
  static async listQueuedWorkflows(input: GetQueuedWorkflowsInput): Promise<WorkflowStatus[]> {
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
    return await runInternalStep(async () => {
      return await DBOS.#executor.cancelWorkflow(workflowID);
    }, 'DBOS.cancelWorkflow');
  }

  /**
   * Resume a workflow given its ID.
   * @param workflowID - ID of the workflow
   */
  static async resumeWorkflow<T>(workflowID: string): Promise<WorkflowHandle<Awaited<T>>> {
    await runInternalStep(async () => {
      return await DBOS.#executor.resumeWorkflow(workflowID);
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
    startStep: number,
    options?: { newWorkflowID?: string; applicationVersion?: string; timeoutMS?: number },
  ): Promise<WorkflowHandle<Awaited<T>>> {
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
    return DBOS.#withTopContext({ idAssignedForNextWorkflow: workflowID }, callback);
  }

  /**
   * Use the provided `callerName`, `span`, and `request` as context for any
   *   DBOS functions called within the `callback` function.
   * @param callerName - Tracing caller name
   * @param span - Tracing span
   * @param request - event context (such as HTTP request) that initiated the call
   * @param callback - Function to run with tracing context in place
   * @returns - Return value from `callback`
   */
  static async withTracedContext<R>(
    callerName: string,
    span: Span,
    request: object,
    callback: () => Promise<R>,
  ): Promise<R> {
    return DBOS.#withTopContext(
      {
        operationCaller: callerName,
        span,
        request,
      },
      callback,
    );
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
    return DBOS.#withTopContext({ operationCaller: callerName }, callback);
  }

  /**
   * @deprecated
   * Use queue named `queueName` for any workflows started within the `callback`.
   * @param queueName - Name of queue upon which all workflows called or started within `callback` will be run
   * @param callback - Function to run, which would call or start workflows
   * @returns - Return value from `callback`
   */
  static async withWorkflowQueue<R>(queueName: string, callback: () => Promise<R>): Promise<R> {
    return DBOS.#withTopContext({ queueAssignedForWorkflows: queueName }, callback);
  }

  /**
   * Specify workflow timeout for any workflows started within the `callback`.
   * @param timeoutMS - timeout length for all workflows started within `callback` will be run
   * @param callback - Function to run, which would call or start workflows
   * @returns - Return value from `callback`
   */
  static async withWorkflowTimeout<R>(timeoutMS: number | null, callback: () => Promise<R>): Promise<R> {
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
      const span = options.span;
      if (!options.span) {
        options.span = DBOS.#executor.tracer.startSpan('topContext', {
          operationUUID: options.idAssignedForNextWorkflow,
          operationType: options.operationType,
          authenticatedUser: options.authenticatedUser,
          assumedRole: options.assumedRole,
          authenticatedRoles: options.authenticatedRoles,
        });
      }

      try {
        return await runWithTopContext(options, callback);
      } finally {
        options.span = span;
      }
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
    const instance = typeof target === 'function' ? null : (target as ConfiguredInstance);
    if (instance && typeof instance !== 'function' && !(instance instanceof ConfiguredInstance)) {
      throw new DBOSInvalidWorkflowTransitionError(
        'Attempt to call `startWorkflow` on an object that is not a `ConfiguredInstance`',
      );
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
      const functionID: number = functionIDGetIncrement();
      return await DBOSExecutor.globalInstance!.systemDatabase.send(
        DBOS.workflowID!,
        functionID,
        destinationID,
        DBOSJSON.stringify(message),
        topic,
      );
    }
    return DBOS.#executor.runSendTempWF(destinationID, message, topic, idempotencyKey); // Temp WF variant
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
        throw new DBOSInvalidWorkflowTransitionError('Invalid call to `DBOS.recv` inside a `step` or `transaction`');
      }
      const functionID: number = functionIDGetIncrement();
      const timeoutFunctionID: number = functionIDGetIncrement();
      return DBOSJSON.parse(
        await DBOSExecutor.globalInstance!.systemDatabase.recv(
          DBOS.workflowID!,
          functionID,
          timeoutFunctionID,
          topic,
          timeoutSeconds,
        ),
      ) as T;
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
      const functionID = functionIDGetIncrement();
      return DBOSExecutor.globalInstance!.systemDatabase.setEvent(
        DBOS.workflowID!,
        functionID,
        key,
        DBOSJSON.stringify(value),
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
      const functionID: number = functionIDGetIncrement();
      const timeoutFunctionID = functionIDGetIncrement();
      const params = {
        workflowID: DBOS.workflowID!,
        functionID,
        timeoutFunctionID,
      };
      return DBOSJSON.parse(
        await DBOSExecutor.globalInstance!.systemDatabase.getEvent(
          workflowID,
          key,
          timeoutSeconds ?? DBOSExecutor.defaultNotificationTimeoutSec,
          params,
        ),
      ) as T;
    }
    return DBOS.#executor.getEvent(workflowID, key, timeoutSeconds);
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
      const { descriptor, registration } = wrapDBOSFunctionAndRegisterByUniqueNameDec(
        target,
        propertyKey,
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
      func,
    );
    return DBOS.#getWorkflowInvoker(registration, config);
  }

  static async #invokeWorkflow<This, Args extends unknown[], Return>(
    $this: This,
    regOP: MethodRegistrationBase,
    args: Args,
    params: StartWorkflowParams = {},
  ): Promise<WorkflowHandle<Return>> {
    const wfId = getNextWFID(params.workflowID);
    const pctx = getCurrentContextStore();
    const queueName = params.queueName ?? pctx?.queueAssignedForWorkflows;
    const timeoutMS = params.timeoutMS ?? pctx?.workflowTimeoutMS;

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

      const pctx = getCurrentContextStore()!;
      const pwfid = pctx.workflowId!;
      const funcId = functionIDGetIncrement();
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
      // Else, we setup a parent context that includes all the potential metadata the application could have set in DBOSLocalCtx
      if (pctx) {
        // If pctx has no span, e.g., has not been setup through `withTracedContext`, set up a parent span for the workflow here.
        pctx.span =
          pctx.span ??
          DBOS.#executor.tracer.startSpan(pctx.operationCaller || 'workflowCaller', {
            operationUUID: wfId,
            operationType: pctx.operationType,
            authenticatedUser: pctx.authenticatedUser,
            assumedRole: pctx.assumedRole,
            authenticatedRoles: pctx.authenticatedRoles,
          });
      }

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
      if (regOP.txnConfig) {
        const func = regOP.registeredFunction;
        return DBOSExecutor.globalInstance!.startTransactionTempWF(
          func as (...args: Args) => Promise<Return>,
          wfParams,
          workflowID,
          funcNum,
          ...args,
        );
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
   * Decorator designating a method as a DBOS transaction, making SQL clients available.
   *   A durable execution checkpoint will be applied to to the underlying database transaction
   * @see `DBOS.sqlClient`
   * @param config - Configuration information for the transaction, particularly its isolation mode
   * @deprecated - use data source packages such as:
   *  `@dbos-inc/drizzle-datasource`
   *  `@dbos-inc/knex-datasource`
   *  `@dbos-inc/node-pg-datasource`
   *  `@dbos-inc/postgres-datasource`
   *  `@dbos-inc/prisma-datasource`
   *  `@dbos-inc/typeorm-datasource`
   */
  static transaction(config: TransactionConfig = {}) {
    function decorator<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration } = wrapDBOSFunctionAndRegisterByUniqueNameDec(
        target,
        propertyKey,
        inDescriptor,
      );
      registration.setTxnConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        let inst: ConfiguredInstance | undefined = undefined;
        if (this === undefined || typeof this === 'function') {
          // This is static
        } else {
          inst = this as ConfiguredInstance;
          if (!(inst instanceof ConfiguredInstance)) {
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

          return await DBOSExecutor.globalInstance!.callTransactionFunction(
            registration.registeredFunction as (...args: unknown[]) => Promise<Return>,
            inst ?? null,
            ...rawArgs,
          );
        }

        const wfId = getNextWFID(undefined);

        const pctx = getCurrentContextStore();
        if (pctx) {
          pctx.span =
            pctx.span ??
            DBOS.#executor.tracer.startSpan(pctx?.operationCaller || 'transactionCaller', {
              operationType: pctx?.operationType,
              authenticatedUser: pctx?.authenticatedUser,
              assumedRole: pctx?.assumedRole,
              authenticatedRoles: pctx?.authenticatedRoles,
            });
        }

        const wfParams: WorkflowParams = {
          configuredInstance: inst,
          workflowUUID: wfId,
        };

        return await DBOS.#executor.runTransactionTempWF(
          registration.registeredFunction as (...args: unknown[]) => Promise<Return>,
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
      const { descriptor, registration } = wrapDBOSFunctionAndRegisterByUniqueNameDec(
        target,
        propertyKey,
        inDescriptor,
      );
      registration.setProcConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
        if (typeof this !== 'function') {
          throw new Error('Stored procedures must be static');
        }

        if (DBOS.isWithinWorkflow()) {
          return await DBOSExecutor.globalInstance!.callProcedureFunction(
            registration.registeredFunction as (...args: unknown[]) => Promise<Return>,
            ...rawArgs,
          );
        }

        const wfId = getNextWFID(undefined);

        const pctx = getCurrentContextStore()!;
        if (pctx) {
          pctx.span =
            pctx.span ??
            DBOS.#executor.tracer.startSpan(pctx?.operationCaller || 'transactionCaller', {
              operationType: pctx?.operationType,
              authenticatedUser: pctx?.authenticatedUser,
              assumedRole: pctx?.assumedRole,
              authenticatedRoles: pctx?.authenticatedRoles,
            });
        }

        const wfParams: WorkflowParams = {
          workflowUUID: wfId,
        };

        return await DBOS.#executor.runProcedureTempWF(
          registration.registeredFunction as (...args: Args) => Promise<Return>,
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
      const { descriptor, registration } = wrapDBOSFunctionAndRegisterByUniqueNameDec(
        target,
        propertyKey,
        inDescriptor,
      );
      registration.setStepConfig(config);

      const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
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

        const pctx = getCurrentContextStore();
        if (pctx) {
          pctx.span =
            pctx.span ??
            DBOS.#executor.tracer.startSpan(pctx?.operationCaller || 'transactionCaller', {
              operationType: pctx?.operationType,
              authenticatedUser: pctx?.authenticatedUser,
              assumedRole: pctx?.assumedRole,
              authenticatedRoles: pctx?.authenticatedRoles,
            });
        }

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

    const reg = wrapDBOSFunctionAndRegister(config?.ctorOrProto, config?.className, name, func);

    const invokeWrapper = async function (this: This, ...rawArgs: Args): Promise<Return> {
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
   * Decorator indicating that the method is the target of HTTP GET operations for `url`
   * @deprecated - use `@dbos-inc/koa-serve`
   */
  static getApi(url: string) {
    return httpApiDec(APITypes.GET, url);
  }

  /**
   * Decorator indicating that the method is the target of HTTP POST operations for `url`
   * @deprecated - use `@dbos-inc/koa-serve`
   */
  static postApi(url: string) {
    return httpApiDec(APITypes.POST, url);
  }

  /**
   * Decorator indicating that the method is the target of HTTP PUT operations for `url`
   * @deprecated - use `@dbos-inc/koa-serve`
   */
  static putApi(url: string) {
    return httpApiDec(APITypes.PUT, url);
  }

  /**
   * Decorator indicating that the method is the target of HTTP PATCH operations for `url`
   * @deprecated - use `@dbos-inc/koa-serve`
   */
  static patchApi(url: string) {
    return httpApiDec(APITypes.PATCH, url);
  }

  /**
   * Decorator indicating that the method is the target of HTTP DELETE operations for `url`
   * @deprecated - use `@dbos-inc/koa-serve`
   */
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
    func: (this: This, ...args: Args) => Promise<Return>,
    target: FunctionName & {
      param: number | string;
    },
  ) {
    return associateParameterWithExternal(
      external,
      target.ctorOrProto,
      target.className,
      target.name ?? func.name,
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
}
