import Koa from 'koa';
import Router from '@koa/router';
import { bodyParser } from '@koa/bodyparser';
import cors from "@koa/cors";
import {
  APITypes,
  ArgSources,
  HandlerContextImpl,
  OperonHandlerRegistration,
} from "./handler";
import { OperonTransaction } from "../transaction";
import { OperonWorkflow } from "../workflow";
import {
  OperonDataValidationError,
  OperonError,
  OperonResponseError,
  isOperonClientError,
} from "../error";
import { Operon } from "../operon";
import { Logger } from "winston";
import { OperonMiddlewareDefaults } from './middleware';
import { SpanStatusCode, trace, ROOT_CONTEXT } from '@opentelemetry/api';
import { OperonCommunicator } from '../communicator';

export const OperonWorkflowUUIDHeader = "operon-workflowuuid";

export class OperonHttpServer {
  readonly app: Koa;
  readonly router: Router;
  readonly logger: Logger;

  /**
   * Create a Koa app.
   * @param operon User pass in an Operon instance.
   * TODO: maybe call operon.init() somewhere in this class?
   */
  constructor(readonly operon: Operon, config : {
    koa ?: Koa,
    router ?: Router,
  } = {})
  {
    if (!config.router) {
      config.router = new Router();
    }
    this.router = config.router;
    this.logger = operon.logger;

    if (!config.koa) {
      config.koa = new Koa();

      // Note: we definitely need bodyParser.
      // For cors(), it doesn't work if we use it in a router, and thus we have to use it in app.
      config.koa.use(bodyParser());
      config.koa.use(cors());
    }
    this.app = config.koa;

    // Register operon endpoints.
    OperonHttpServer.registerDecoratedEndpoints(this.operon, this.router);
    this.app.use(this.router.routes()).use(this.router.allowedMethods());
  }

  /**
   * Register Operon endpoints and attach to the app. Then start the server at the given port.
   * @param port
   */
  listen(port: number) {
    // Start the HTTP server.
    return this.app.listen(port, () => {
      this.logger.info(`Operon Server is running at http://localhost:${port}`);
    });
  }

  static registerDecoratedEndpoints(operon : Operon, router : Router)
  {
    // Register user declared endpoints, wrap around the endpoint with request parsing and response.
    operon.registeredOperations.forEach((registeredOperation) => {
      const ro = registeredOperation as OperonHandlerRegistration<unknown, unknown[], unknown>;
      if (ro.apiURL) {
        // Check if we need to apply any Koa middleware.
        const defaults = ro.defaults as OperonMiddlewareDefaults;
        if (defaults?.koaMiddlewares) {
          defaults.koaMiddlewares.forEach((koaMiddleware) => {
            operon.logger.debug(`Operon Server applying middleware ${koaMiddleware.name} to ${ro.apiURL}`);
            router.use(ro.apiURL, koaMiddleware);
          })
        }

        // Wrapper function that parses request and send response.
        const wrappedHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
          const oc: HandlerContextImpl = new HandlerContextImpl(operon, koaCtxt);

          try {
            // Check for auth first
            if (defaults?.authMiddleware) {
              const res = await defaults.authMiddleware({name: ro.name, requiredRole: ro.getRequiredRoles(), koaContext: koaCtxt});
              if (res) {
                oc.authenticatedUser = res.authenticatedUser;
                oc.authenticatedRoles = res.authenticatedRoles;
              }
            }

            // Parse the arguments.
            const args: unknown[] = [];
            ro.args.forEach((marg, idx) => {
              marg.argSource = marg.argSource ?? ArgSources.DEFAULT;  // Assign a default value.
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
                  throw new OperonDataValidationError(`Argument ${marg.name} requires a method body.`);
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
            const headerWorkflowUUID = koaCtxt.get(OperonWorkflowUUIDHeader);

            // Finally, invoke the transaction/workflow/plain function and properly set HTTP response.
            // If functions return successfully and hasn't set the body, we set the body to the function return value. The status code will be automatically set to 200 or 204 (if the body is null/undefined).
            // In case of an exception:
            // - If an Operon client-side error is thrown, we return 400.
            // - If an error contains a `status` field, we return the specified status code.
            // - Otherwise, we return 500.
            const wfParams = { parentCtx: oc, workflowUUID: headerWorkflowUUID };
            if (ro.txnConfig) {
              koaCtxt.body = await operon.transaction(ro.registeredFunction as OperonTransaction<unknown[], unknown>, wfParams, ...args);
            } else if (ro.workflowConfig) {
              koaCtxt.body = await (await operon.workflow(ro.registeredFunction as OperonWorkflow<unknown[], unknown>, wfParams, ...args)).getResult();
            } else if (ro.commConfig) {
              koaCtxt.body = await operon.external(ro.registeredFunction as OperonCommunicator<unknown[], unknown>, wfParams, ...args);
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
            const logger = oc.getLogger();
            logger.error(e as Error);
            if (e instanceof Error) {
              oc.span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
              let st = ((e as OperonResponseError)?.status || 500);
              const operonErrorCode = (e as OperonError)?.operonErrorCode;
              if (operonErrorCode && isOperonClientError(operonErrorCode)) {
                st = 400;  // Set to 400: client-side error.
              }
              koaCtxt.status = st;
              koaCtxt.body = {
                status: st,
                message: e.message,
                details: e,
              }
            } else {
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
                }
              },
            );
            operon.tracer.endSpan(oc.span)
            await koaNext();
          }
        };

        // Actually register the endpoint.
        if (ro.apiType === APITypes.GET) {
          router.get(ro.apiURL, wrappedHandler);
          operon.logger.debug(`Operon Server Registered GET ${ro.apiURL}`);
        } else if (ro.apiType === APITypes.POST) {
          router.post(ro.apiURL, wrappedHandler);
          operon.logger.debug(`Operon Server Registered POST ${ro.apiURL}`);
        }
      }
    });
  }
}
