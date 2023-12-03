import Koa from 'koa';
import Router from '@koa/router';
import { bodyParser } from '@koa/bodyparser';
import cors from "@koa/cors";
import {
  APITypes,
  ArgSources,
  RequestIDHeader,
  HandlerContextImpl,
  HandlerRegistration,
} from "./handler";
import { Transaction } from "../transaction";
import { Workflow } from "../workflow";
import {
  DBOSDataValidationError,
  DBOSError,
  DBOSResponseError,
  isClientError,
} from "../error";
import { DBOSExecutor } from "../dbos-executor";
import { Logger } from "winston";
import { MiddlewareDefaults } from './middleware';
import { SpanStatusCode, trace, ROOT_CONTEXT } from '@opentelemetry/api';
import { Communicator } from '../communicator';

export const WorkflowUUIDHeader = "dbos-workflowuuid";
export const WorkflowRecoveryUrl = "/dbos-workflow-recovery"

export class DBOSHttpServer {
  readonly app: Koa;
  readonly router: Router;
  readonly logger: Logger;

  /**
   * Create a Koa app.
   * @param dbosExec User pass in an DBOS workflow executor instance.
   * TODO: maybe call wfe.init() somewhere in this class?
   */
  constructor(readonly dbosExec: DBOSExecutor, config: { koa?: Koa; router?: Router } = {}) {
    if (!config.router) {
      config.router = new Router();
    }
    this.router = config.router;
    this.logger = dbosExec.logger;

    if (!config.koa) {
      config.koa = new Koa();

      // Note: we definitely need bodyParser.
      // For cors(), it doesn't work if we use it in a router, and thus we have to use it in app.
      config.koa.use(bodyParser());
      config.koa.use(cors());
    }
    this.app = config.koa;

    // Register HTTP endpoints.
    DBOSHttpServer.registerRecoveryEndpoint(this.dbosExec, this.router);
    DBOSHttpServer.registerDecoratedEndpoints(this.dbosExec, this.router);
    this.app.use(this.router.routes()).use(this.router.allowedMethods());
  }

  /**
   * Register HTTP endpoints and attach to the app. Then start the server at the given port.
   * @param port
   */
  listen(port: number) {
    // Start the HTTP server.
    return this.app.listen(port, () => {
      this.logger.info(`DBOS Server is running at http://localhost:${port}`);
    });
  }

  /**
   * Register workflow recovery endpoint.
   * Receives a list of executor IDs and returns a list of workflowUUIDs.
   */
  static registerRecoveryEndpoint(wfe: DBOSExecutor, router: Router) {
    // Handler function that parses request for recovery.
    const recoveryHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      const executorIDs = koaCtxt.request.body as string[];
      wfe.logger.info("Recovering workflows for executors: " + executorIDs.toString());
      const recoverHandles = await wfe.recoverPendingWorkflows(executorIDs);

      // Return a list of workflowUUIDs being recovered.
      koaCtxt.body = await Promise.allSettled(recoverHandles.map((i) => i.getWorkflowUUID())).then((results) =>
        results.filter((i) => i.status === "fulfilled").map((i) => (i as PromiseFulfilledResult<unknown>).value)
      );
      await koaNext();
    };

    router.post(WorkflowRecoveryUrl, recoveryHandler);
    wfe.logger.debug(`DBOS Server Registered Recovery POST ${WorkflowRecoveryUrl}`);
  }

  /**
   * Register decorated functions as HTTP endpoints.
   */
  static registerDecoratedEndpoints(wfe: DBOSExecutor, router: Router) {
    // Register user declared endpoints, wrap around the endpoint with request parsing and response.
    wfe.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as HandlerRegistration<unknown, unknown[], unknown>;
      if (ro.apiURL) {
        // Ignore URL with "/dbos-workflow-recovery" prefix.
        if (ro.apiURL.startsWith(WorkflowRecoveryUrl)) {
          wfe.logger.error(`Invalid URL: ${ro.apiURL} -- should not start with ${WorkflowRecoveryUrl}!`);
          return;
        }

        // Check if we need to apply any Koa middleware.
        const defaults = ro.defaults as MiddlewareDefaults;
        if (defaults?.koaMiddlewares) {
          defaults.koaMiddlewares.forEach((koaMiddleware) => {
            wfe.logger.debug(`DBOS Server applying middleware ${koaMiddleware.name} to ${ro.apiURL}`);
            router.use(ro.apiURL, koaMiddleware);
          });
        }

        // Wrapper function that parses request and send response.
        const wrappedHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
          const oc: HandlerContextImpl = new HandlerContextImpl(wfe, koaCtxt);

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
                  return wfe.userDatabase.queryFunction(query, ...args);
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
              if ((ro.apiType === APITypes.GET && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.QUERY) {
                foundArg = koaCtxt.request.query[marg.name];
                if (foundArg) {
                  args.push(foundArg);
                }
              } else if ((ro.apiType === APITypes.POST && marg.argSource === ArgSources.DEFAULT) || marg.argSource === ArgSources.BODY) {
                if (!koaCtxt.request.body) {
                  throw new DBOSDataValidationError(`Argument ${marg.name} requires a method body.`);
                }
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
                foundArg = koaCtxt.request.body[marg.name];
                if (foundArg) {
                  args.push(foundArg);
                }
              }

              // Try to parse the argument from the URL if nothing found.
              if (!foundArg) {
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
            const wfParams = { parentCtx: oc, workflowUUID: headerWorkflowUUID };
            if (ro.txnConfig) {
              koaCtxt.body = await wfe.transaction(ro.registeredFunction as Transaction<unknown[], unknown>, wfParams, ...args);
            } else if (ro.workflowConfig) {
              koaCtxt.body = await (await wfe.workflow(ro.registeredFunction as Workflow<unknown[], unknown>, wfParams, ...args)).getResult();
            } else if (ro.commConfig) {
              koaCtxt.body = await wfe.external(ro.registeredFunction as Communicator<unknown[], unknown>, wfParams, ...args);
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
              oc.logger.error(e.message);
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
              oc.logger.error(JSON.stringify(e));
              oc.span.setStatus({ code: SpanStatusCode.ERROR, message: JSON.stringify(e) });
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
            wfe.tracer.endSpan(oc.span);
            // Add requestID to response headers.
            console.log(`requestID: ${oc.request.requestID}`);
            koaCtxt.set(RequestIDHeader, oc.request.requestID as string);
            await koaNext();
          }
        };

        // Actually register the endpoint.
        if (ro.apiType === APITypes.GET) {
          router.get(ro.apiURL, wrappedHandler);
          wfe.logger.debug(`DBOS Server Registered GET ${ro.apiURL}`);
        } else if (ro.apiType === APITypes.POST) {
          router.post(ro.apiURL, wrappedHandler);
          wfe.logger.debug(`DBOS Server Registered POST ${ro.apiURL}`);
        }
      }
    });
  }
}
