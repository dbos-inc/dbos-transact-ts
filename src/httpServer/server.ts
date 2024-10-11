import Koa, { Context } from 'koa';
import Router from '@koa/router';
import { bodyParser } from '@koa/bodyparser';
import cors from "@koa/cors";
import {
  RequestIDHeader,
  HandlerContextImpl,
  HandlerRegistrationBase,
} from "./handler";
import { ArgSources, APITypes } from "./handlerTypes";
import { Transaction } from "../transaction";
import { Workflow } from "../workflow";
import {
  DBOSDataValidationError,
  DBOSError,
  DBOSResponseError,
  isClientError,
} from "../error";
import { DBOSExecutor } from "../dbos-executor";
import { GlobalLogger as Logger } from "../telemetry/logs";
import { MiddlewareDefaults } from './middleware';
import { SpanStatusCode, trace, ROOT_CONTEXT } from '@opentelemetry/api';
import { StepFunction } from '../step';
import * as net from 'net';
import { performance } from 'perf_hooks';
import { DBOSJSON, exhaustiveCheckGuard } from '../utils';

export const WorkflowUUIDHeader = "dbos-idempotency-key";
export const WorkflowRecoveryUrl = "/dbos-workflow-recovery"
export const HealthUrl = "/dbos-healthz"
export const PerfUrl = "/dbos-perf"

export class DBOSHttpServer {
  readonly app: Koa;
  readonly adminApp: Koa;
  readonly applicationRouter: Router;
  readonly adminRouter: Router;
  readonly logger: Logger;

  /**
   * Create a Koa app.
   * @param dbosExec User pass in an DBOS workflow executor instance.
   * TODO: maybe call dbosExec.init() somewhere in this class?
   */
  constructor(readonly dbosExec: DBOSExecutor) {
    this.applicationRouter = new Router();
    this.adminRouter = new Router();
    this.logger = dbosExec.logger;
    this.app = new Koa();
    this.adminApp = new Koa();
    this.adminApp.use(bodyParser());
    this.adminApp.use(cors());

    // Register HTTP endpoints.
    DBOSHttpServer.registerHealthEndpoint(this.dbosExec, this.adminRouter);
    DBOSHttpServer.registerRecoveryEndpoint(this.dbosExec, this.adminRouter);
    DBOSHttpServer.registerPerfEndpoint(this.dbosExec, this.adminRouter);
    this.adminApp.use(this.adminRouter.routes()).use(this.adminRouter.allowedMethods());
    DBOSHttpServer.registerDecoratedEndpoints(this.dbosExec, this.applicationRouter, this.app);
    this.app.use(this.applicationRouter.routes()).use(this.applicationRouter.allowedMethods());
  }

  /**
   * Register HTTP endpoints and attach to the app. Then start the server at the given port.
   * @param port
   */
  async listen(port: number, adminPort: number) {
    try {
      await this.checkPortAvailability(port, "127.0.0.1");
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code === 'EADDRINUSE') {
        this.logger.error(`Port ${port} is already used for IPv4 address "127.0.0.1". Please use the -p option to choose another port.\n${err.message}`);
        process.exit(1);
      } else {
        this.logger.warn(`Error occurred while checking port availability for IPv4 address "127.0.0.1" : ${err.code}\n${err.message}`);
      }
    }

    try {
      await this.checkPortAvailability(port, "::1");
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code === 'EADDRINUSE') {
        this.logger.error(`Port ${port} is already used for IPv6 address "::1". Please use the -p option to choose another port.\n${err.message}`);
        process.exit(1);
      } else {
        this.logger.warn(`Error occurred while checking port availability for IPv6 address "::1" : ${err.code}\n${err.message}`);
      }
    }

    const appServer = this.app.listen(port, () => {
      this.logger.info(`DBOS Server is running at http://localhost:${port}`);
    });

    const adminServer = this.adminApp.listen(adminPort, () => {
      this.logger.info(`DBOS Admin Server is running at http://localhost:${adminPort}`);
    });
    return {appServer: appServer, adminServer: adminServer}
  }

async checkPortAvailability(port: number, host: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
  const server = new net.Server();
  server.on('error', (error: NodeJS.ErrnoException) => {
    reject(error);
  });

  server.on('listening', () => {
    server.close();
    resolve();
  });

  server.listen({port:port, host: host},() => {
    resolve();
  });

  });
}
  /**
   * Health check endpoint.
   */
    static registerHealthEndpoint(dbosExec: DBOSExecutor, router: Router) {
      // Handler function that parses request for recovery.
      const healthHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
        koaCtxt.body = "healthy";
        await koaNext();
      };
      router.get(HealthUrl, healthHandler);
      dbosExec.logger.debug(`DBOS Server Registered Healthz GET ${HealthUrl}`);
    }

  /**
   * Register workflow recovery endpoint.
   * Receives a list of executor IDs and returns a list of workflowUUIDs.
   */
  static registerRecoveryEndpoint(dbosExec: DBOSExecutor, router: Router) {
    // Handler function that parses request for recovery.
    const recoveryHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      const executorIDs = koaCtxt.request.body as string[];
      dbosExec.logger.info("Recovering workflows for executors: " + executorIDs.toString());
      const recoverHandles = await dbosExec.recoverPendingWorkflows(executorIDs);

      // Return a list of workflowUUIDs being recovered.
      koaCtxt.body = await Promise.allSettled(recoverHandles.map((i) => i.getWorkflowUUID())).then((results) =>
        results.filter((i) => i.status === "fulfilled").map((i) => (i as PromiseFulfilledResult<unknown>).value)
      );
      await koaNext();
    };

    router.post(WorkflowRecoveryUrl, recoveryHandler);
    dbosExec.logger.debug(`DBOS Server Registered Recovery POST ${WorkflowRecoveryUrl}`);
  }

  /**
   * Register performance endpoint.
   * Returns information on VM performance since last call.
   */
  static registerPerfEndpoint(dbosExec: DBOSExecutor, router: Router) {
    let lastELU = performance.eventLoopUtilization()
    const perfHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      const currELU = performance.eventLoopUtilization();
      const elu = performance.eventLoopUtilization(currELU, lastELU);
      koaCtxt.body = elu;
      lastELU = currELU;
      await koaNext();
    };
    router.get(PerfUrl, perfHandler);
    dbosExec.logger.debug(`DBOS Server Registered Perf GET ${HealthUrl}`);
  }

  /**
   * Register decorated functions as HTTP endpoints.
   */
  static registerDecoratedEndpoints(dbosExec: DBOSExecutor, router: Router, app: Koa) {
    const globalMiddlewares: Set<Koa.Middleware> = new Set();
    // Register user declared endpoints, wrap around the endpoint with request parsing and response.
    dbosExec.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as HandlerRegistrationBase;
      if (ro.apiURL) {
        if (ro.isInstance) {
          dbosExec.logger.warn(`Operation ${ro.className}/${ro.name} is registered with an endpoint (${ro.apiURL}) but cannot be invoked.`);
          return;
        }
        const defaults = ro.defaults as MiddlewareDefaults;
        // Check if we need to apply a custom CORS
        if (defaults.koaCors) {
          router.all(ro.apiURL, defaults.koaCors); // Use router.all to register with all methods including preflight requests
        } else {
          if (dbosExec.config.http?.cors_middleware ?? true) {
            router.all(ro.apiURL, cors({
              credentials: dbosExec.config.http?.credentials ?? true,
              origin:
                (o: Context)=>{
                  const whitelist = dbosExec.config.http?.allowed_origins;
                  const origin = o.request.header.origin ?? '*';
                  if (whitelist && whitelist.length > 0) {
                    return (whitelist.includes(origin) ? origin : '');
                  }
                  return o.request.header.origin || '*';
                },
              allowMethods: 'GET,HEAD,PUT,POST,DELETE,PATCH,OPTIONS',
              allowHeaders: ['Origin', 'X-Requested-With', 'Content-Type', 'Accept', 'Authorization'],
            }));
          }
        }
        // Check if we need to apply a custom body parser
        if (defaults.koaBodyParser) {
          router.use(ro.apiURL, defaults.koaBodyParser)
        } else {
          router.use(ro.apiURL, bodyParser())
        }
        // Check if we need to apply any Koa middleware.
        if (defaults?.koaMiddlewares) {
          defaults.koaMiddlewares.forEach((koaMiddleware) => {
            dbosExec.logger.debug(`DBOS Server applying middleware ${koaMiddleware.name} to ${ro.apiURL}`);
            router.use(ro.apiURL, koaMiddleware);
          });
        }
        if (defaults?.koaGlobalMiddlewares) {
          defaults.koaGlobalMiddlewares.forEach((koaMiddleware) => {
            if (globalMiddlewares.has(koaMiddleware)) {
              return;
            }
            dbosExec.logger.debug(`DBOS Server applying middleware ${koaMiddleware.name} globally`);
            globalMiddlewares.add(koaMiddleware)
            app.use(koaMiddleware);
          });
        }

        // Wrapper function that parses request and send response.
        const wrappedHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
          const oc: HandlerContextImpl = new HandlerContextImpl(dbosExec, koaCtxt);

          try {
            // Check for auth first
            if (defaults?.authMiddleware) {
              const res = await defaults.authMiddleware({
                name: ro.name,
                requiredRole: ro.getRequiredRoles(),
                koaContext: koaCtxt,
                logger: oc.logger,
                span: oc.span,
                getConfig: (key: string, def) => {
                  return oc.getConfig(key, def);
                },
                query: (query, ...args) => {
                  return dbosExec.userDatabase.queryFunction(query, ...args);
                },
              });
              if (res) {
                oc.authenticatedUser = res.authenticatedUser;
                oc.authenticatedRoles = res.authenticatedRoles;
              }
            }

            // Parse the arguments.
            const args: unknown[] = [];
            ro.args.forEach((marg, idx) => {
              marg.argSource = marg.argSource ?? ArgSources.DEFAULT; // Assign a default value.
              if (idx === 0) {
                return; // Do not parse the context.
              }

              let foundArg = undefined;
              const isQueryMethod = ro.apiType === APITypes.GET || ro.apiType === APITypes.DELETE;
              const isBodyMethod = ro.apiType === APITypes.POST || ro.apiType === APITypes.PUT || ro.apiType === APITypes.PATCH;

              if ((isQueryMethod && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.QUERY) {
                foundArg = koaCtxt.request.query[marg.name];
                if (foundArg !== undefined) {
                  args.push(foundArg);
                }
              } else if ((isBodyMethod && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.BODY) {
                if (!koaCtxt.request.body) {
                  throw new DBOSDataValidationError(`Argument ${marg.name} requires a method body.`);
                }
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
                foundArg = koaCtxt.request.body[marg.name];
                if (foundArg !== undefined) {
                  args.push(foundArg);
                }
              }

              // Try to parse the argument from the URL if nothing found.
              if (foundArg === undefined) {
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
                args.push(koaCtxt.params[marg.name]);
              }

              //console.log(`found arg ${marg.name} ${idx} ${args[idx-1]}`);
            });

            // Extract workflow UUID from headers (if any).
            // We pass in the specified workflow UUID to workflows and transactions, but doesn't restrict how handlers use it.
            const headerWorkflowUUID = koaCtxt.get(WorkflowUUIDHeader);

            // Finally, invoke the transaction/workflow/plain function and properly set HTTP response.
            // If functions return successfully and hasn't set the body, we set the body to the function return value. The status code will be automatically set to 200 or 204 (if the body is null/undefined).
            // In case of an exception:
            // - If a client-side error is thrown, we return 400.
            // - If an error contains a `status` field, we return the specified status code.
            // - Otherwise, we return 500.
            // configuredInstance is currently null; we don't allow configured handlers now.
            const wfParams = { parentCtx: oc, workflowUUID: headerWorkflowUUID, configuredInstance: null };
            if (ro.txnConfig) {
              koaCtxt.body = await dbosExec.transaction(ro.registeredFunction as Transaction<unknown[], unknown>, wfParams, ...args);
            } else if (ro.workflowConfig) {
              koaCtxt.body = await (await dbosExec.workflow(ro.registeredFunction as Workflow<unknown[], unknown>, wfParams, ...args)).getResult();
            } else if (ro.commConfig) {
              koaCtxt.body = await dbosExec.external(ro.registeredFunction as StepFunction<unknown[], unknown>, wfParams, ...args);
            } else {
              // Directly invoke the handler code.
              const retValue = await ro.invoke(undefined, [oc, ...args]);

              // Set the body to the return value unless the body is already set by the handler.
              if (koaCtxt.body === undefined) {
                koaCtxt.body = retValue;
              }
            }
            oc.span.setStatus({ code: SpanStatusCode.OK });
          } catch (e) {
            if (e instanceof Error) {
              const annotated_e = e as Error & {dbos_already_logged?: boolean};
              if (annotated_e.dbos_already_logged !== true) {
                oc.logger.error(e);
              }
              oc.span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
              let st = (e as DBOSResponseError)?.status || 500;
              const dbosErrorCode = (e as DBOSError)?.dbosErrorCode;
              if (dbosErrorCode && isClientError(dbosErrorCode)) {
                st = 400; // Set to 400: client-side error.
              }
              koaCtxt.status = st;
              koaCtxt.message = e.message;
              koaCtxt.body = {
                status: st,
                message: e.message,
                details: e,
              };
            } else {
              // FIXME we should have a standard, user friendly message for errors that are not instances of Error.
              // using stringify() will not produce a pretty output, because our format function uses stringify() too.
              oc.logger.error(DBOSJSON.stringify(e));
              oc.span.setStatus({ code: SpanStatusCode.ERROR, message: DBOSJSON.stringify(e) });
              koaCtxt.body = e;
              koaCtxt.status = 500;
            }
          } finally {
            // Inject trace context into response headers.
            // We cannot use the defaultTextMapSetter to set headers through Koa
            // So we provide a custom setter that sets headers through Koa's context.
            // See https://github.com/open-telemetry/opentelemetry-js/blob/868f75e448c7c3a0efd75d72c448269f1375a996/packages/opentelemetry-core/src/trace/W3CTraceContextPropagator.ts#L74
            interface Carrier {
              context: Koa.Context;
            }
            oc.W3CTraceContextPropagator.inject(
              trace.setSpanContext(ROOT_CONTEXT, oc.span.spanContext()),
              {
                context: koaCtxt,
              },
              {
                set: (carrier: Carrier, key: string, value: string) => {
                  carrier.context.set(key, value);
                },
              }
            );
            dbosExec.tracer.endSpan(oc.span);
            // Add requestID to response headers.
            koaCtxt.set(RequestIDHeader, oc.request.requestID as string);
            await koaNext();
          }
        };
        // Actually register the endpoint.
        switch(ro.apiType) {
          case APITypes.GET:
            router.get(ro.apiURL, wrappedHandler);
            dbosExec.logger.debug(`DBOS Server Registered GET ${ro.apiURL}`);
            break;
          case APITypes.POST:
            router.post(ro.apiURL, wrappedHandler);
            dbosExec.logger.debug(`DBOS Server Registered POST ${ro.apiURL}`);
            break;
          case APITypes.PUT:
            router.put(ro.apiURL, wrappedHandler);
            dbosExec.logger.debug(`DBOS Server Registered PUT ${ro.apiURL}`);
            break;
          case APITypes.PATCH:
            router.patch(ro.apiURL, wrappedHandler);
            dbosExec.logger.debug(`DBOS Server Registered PATCH ${ro.apiURL}`);
            break;
          case APITypes.DELETE:
            router.delete(ro.apiURL, wrappedHandler);
            dbosExec.logger.debug(`DBOS Server Registered DELETE ${ro.apiURL}`);
            break;
          default:
            exhaustiveCheckGuard(ro.apiType)
        }
      }
    });
  }
}
