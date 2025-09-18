import Koa from 'koa';
import Router from '@koa/router';
import bodyParser from '@koa/bodyparser';
import cors from '@koa/cors';

import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { SpanStatusCode, trace, defaultTextMapGetter, ROOT_CONTEXT, context } from '@opentelemetry/api';
import { Span } from '@opentelemetry/sdk-trace-base';

import { DBOS, Error as DBOSErrors } from '@dbos-inc/dbos-sdk';
import {
  APITypes,
  ArgSources,
  DBOSHTTPAuthReturn,
  DBOSHTTPBase,
  DBOSHTTPMethodInfo,
  getOrGenerateRequestID,
  isClientRequestError,
  RequestIDHeader,
  WorkflowIDHeader,
} from './dboshttp';
import { AsyncLocalStorage } from 'async_hooks';

// Context for auth middleware
export interface DBOSKoaAuthContext {
  readonly koaContext: Koa.Context;
  readonly name: string; // Method (handler, transaction, workflow) name
  readonly requiredRole: string[]; // Roles required for the invoked operation, if empty perhaps auth is not required
}

/**
 * Authentication middleware that executes before a request reaches a function.
 * This is expected to:
 *   - Validate the request found in the handler context and extract auth information from the request.
 *   - Map the HTTP request to the user identity and roles defined in app.
 * If this succeeds, return the current authenticated user and a list of roles.
 * If any step fails, throw an error.
 */
export type DBOSKoaAuthMiddleware = (ctx: DBOSKoaAuthContext) => Promise<DBOSHTTPAuthReturn | void>;

export interface DBOSKoaConfig {
  corsMiddleware?: boolean;
  credentials?: boolean;
  allowedOrigins?: string[];
}

export interface DBOSKoaClassReg {
  authMiddleware?: DBOSKoaAuthMiddleware;
  koaBodyParser?: Koa.Middleware;
  koaCors?: Koa.Middleware;
  koaMiddlewares?: Koa.Middleware[];
  koaGlobalMiddlewares?: Koa.Middleware[];
}

function exhaustiveCheckGuard(_: never): never {
  throw new Error('Exaustive matching is not applied');
}

interface DBOSKoaLocalCtx {
  koaCtxt: Koa.Context;
}
const asyncLocalCtx = new AsyncLocalStorage<DBOSKoaLocalCtx>();

function getCurrentKoaContextStore(): DBOSKoaLocalCtx | undefined {
  return asyncLocalCtx.getStore();
}

function assertCurrentKoaContextStore(): DBOSKoaLocalCtx {
  const ctx = getCurrentKoaContextStore();
  if (!ctx) throw new TypeError('Invalid use of `DBOSKoa.koaContext` outside of a handler function');
  return ctx;
}

export class DBOSKoa extends DBOSHTTPBase {
  constructor() {
    super();
    DBOS.registerLifecycleCallback(this);
  }

  static get koaContext(): Koa.Context {
    return assertCurrentKoaContextStore().koaCtxt;
  }

  /**
   * Define an authentication function for each endpoint in this class.
   */
  authentication(authMiddleware: DBOSKoaAuthMiddleware) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSKoaClassReg;
      clsreg.authMiddleware = authMiddleware;
    }
    return clsdec;
  }

  koaBodyParser(koaBodyParser: Koa.Middleware) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSKoaClassReg;
      clsreg.koaBodyParser = koaBodyParser;
    }
    return clsdec;
  }

  koaCors(koaCors: Koa.Middleware) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSKoaClassReg;
      clsreg.koaCors = koaCors;
    }
    return clsdec;
  }

  /**
   * Define Koa middleware that is applied in order to each endpoint in this class.
   */
  koaMiddleware(...koaMiddleware: Koa.Middleware[]) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    koaMiddleware.forEach((i) => {
      if (i === undefined) {
        throw new TypeError('undefined argument passed to koaMiddleware');
      }
    });
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSKoaClassReg;
      clsreg.koaMiddlewares = [...(clsreg.koaMiddlewares ?? []), ...koaMiddleware];
    }
    return clsdec;
  }

  /**
   * Define Koa middleware that is applied to all requests, including this class, other classes,
   *   or requests that do not end up in DBOS handlers at all.
   * @deprecated Apply global middleware to your application router directly
   */
  koaGlobalMiddleware(...koaMiddleware: Koa.Middleware[]) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    koaMiddleware.forEach((i) => {
      if (i === undefined) {
        throw new TypeError('undefined argument passed to koaMiddleware');
      }
    });
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSKoaClassReg;
      clsreg.koaGlobalMiddlewares = [...(clsreg.koaGlobalMiddlewares ?? []), ...koaMiddleware];
    }
    return clsdec;
  }

  app?: Koa;
  appRouter?: Router;

  createApp(config?: DBOSKoaConfig) {
    this.app = new Koa();
    this.appRouter = new Router();
    this.registerWithApp(this.app, this.appRouter, config);
  }

  registerWithApp(app: Koa, appRouter: Router, config?: DBOSKoaConfig) {
    const globalMiddlewares: Set<Koa.Middleware> = new Set();

    const eps = DBOS.getAssociatedInfo(this);
    for (const e of eps) {
      const { methodConfig, classConfig, methodReg } = e;
      const defaults = classConfig as DBOSKoaClassReg;
      const httpmethod = methodConfig as DBOSHTTPMethodInfo;

      for (const ro of httpmethod?.registrations ?? []) {
        // What about instance methods?
        //   Those would have to be registered another way that accepted the instances.
        if (methodReg.isInstance) {
          DBOS.logger.warn(
            `Operation ${methodReg.className}/${methodReg.name} is registered with an endpoint (${ro.apiURL}) but cannot be invoked without an instance.`,
          );
          continue;
        }

        // Apply CORS, bodyParser, and other middlewares
        // Check if we need to apply a custom CORS
        if (defaults.koaCors) {
          appRouter.all(ro.apiURL, defaults.koaCors); // Use router.all to register with all methods including preflight requests
        } else {
          if (config?.corsMiddleware ?? true) {
            appRouter.all(
              ro.apiURL,
              cors({
                credentials: config?.credentials ?? true,
                origin: (o: Koa.Context) => {
                  const whitelist = config?.allowedOrigins;
                  const origin = o.request.header.origin ?? '*';
                  if (whitelist && whitelist.length > 0) {
                    return whitelist.includes(origin) ? origin : '';
                  }
                  return o.request.header.origin || '*';
                },
                allowMethods: 'GET,HEAD,PUT,POST,DELETE,PATCH,OPTIONS',
                allowHeaders: ['Origin', 'X-Requested-With', 'Content-Type', 'Accept', 'Authorization'],
              }),
            );
          }
        }

        // Check if we need to apply any Koa global middleware.
        if (defaults?.koaGlobalMiddlewares) {
          defaults.koaGlobalMiddlewares.forEach((koaMiddleware) => {
            if (globalMiddlewares.has(koaMiddleware)) {
              return;
            }
            DBOS.logger.debug(`DBOS Koa Server applying middleware ${koaMiddleware.name} globally`);
            globalMiddlewares.add(koaMiddleware);
            app.use(koaMiddleware);
          });
        }

        const wrappedHandler = async (koaCtxt: Koa.Context, koaNext: Koa.Next) => {
          let authenticatedUser: string | undefined = undefined;
          let authenticatedRoles: string[] | undefined = undefined;
          let span: Span | undefined;
          const httpTracer = new W3CTraceContextPropagator();

          try {
            // Auth first
            if (defaults?.authMiddleware) {
              const res = await defaults.authMiddleware({
                name: methodReg.name,
                requiredRole: methodReg.getRequiredRoles(),
                koaContext: koaCtxt,
              });
              if (res) {
                authenticatedUser = res.authenticatedUser;
                authenticatedRoles = res.authenticatedRoles;
              }
            }

            // Parse the arguments.
            const args: unknown[] = [];
            methodReg.args.forEach((marg) => {
              let foundArg = undefined;
              const isQueryMethod = ro.apiType === APITypes.GET || ro.apiType === APITypes.DELETE;
              const isBodyMethod =
                ro.apiType === APITypes.POST || ro.apiType === APITypes.PUT || ro.apiType === APITypes.PATCH;
              const argSource = this.getArgSource(marg);

              if ((isQueryMethod && argSource !== ArgSources.BODY) || argSource === ArgSources.QUERY) {
                foundArg = koaCtxt.request.query[marg.name];
                if (foundArg !== undefined) {
                  args.push(foundArg);
                } else if (argSource === ArgSources.AUTO) {
                  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
                  foundArg = koaCtxt.request.body?.[marg.name];
                  if (foundArg !== undefined) {
                    args.push(foundArg);
                  }
                }
              } else if (isBodyMethod || argSource === ArgSources.BODY) {
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
                foundArg = koaCtxt.request.body?.[marg.name];
                if (foundArg !== undefined) {
                  args.push(foundArg);
                } else if (argSource === ArgSources.AUTO) {
                  foundArg = koaCtxt.request.query[marg.name];
                  if (foundArg !== undefined) {
                    args.push(foundArg);
                  } else {
                    if (!koaCtxt.request.body) {
                      throw new DBOSErrors.DBOSDataValidationError(`Argument ${marg.name} requires a method body.`);
                    }
                  }
                }
              }

              // Try to parse the argument from the URL if nothing found.
              if (foundArg === undefined) {
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
                args.push(koaCtxt.params[marg.name]);
              }

              //console.log(`found arg ${marg.name} ${idx} ${args[idx-1]}`);
            });

            // Extract workflow ID from headers (if any).
            // We pass in the specified workflow ID in for any workflow started, should handler happen to do so.
            const headerWorkflowID = koaCtxt.get(WorkflowIDHeader);

            // Retrieve or generate the tracing request ID
            const requestID = getOrGenerateRequestID(koaCtxt.request.headers);
            koaCtxt.set(RequestIDHeader, requestID);

            // If present, retrieve the trace context from the request
            const extractedSpanContext = trace.getSpanContext(
              httpTracer.extract(ROOT_CONTEXT, koaCtxt.request.headers, defaultTextMapGetter),
            );
            const spanAttributes = {
              operationType: DBOSHTTPBase.HTTP_OPERATION_TYPE,
              requestID: requestID,
              requestIP: koaCtxt.request.ip,
              requestURL: koaCtxt.request.url,
              requestMethod: koaCtxt.request.method,
            };
            if (extractedSpanContext === undefined) {
              span = DBOS.tracer?.startSpan(koaCtxt.url, spanAttributes);
            } else {
              extractedSpanContext.isRemote = true;
              span = DBOS.tracer?.startSpanWithContext(extractedSpanContext, koaCtxt.url, spanAttributes);
            }

            // Finally, invoke the function and properly set HTTP response.
            // If functions return successfully and hasn't set the body, we set the body to the function return value.
            //   The status code will be automatically set to 200 or 204 (if the body is null/undefined).
            // In case of an exception:
            // - If a client-side error is thrown, we return 400.
            // - If an error contains a `status` field, we return the specified status code.
            // - Otherwise, we return 500.
            const cresult = await context.with(
              span ? trace.setSpan(context.active(), span) : context.active(),
              async () => {
                return await asyncLocalCtx.run({ koaCtxt }, async () => {
                  return await DBOS.runWithContext(
                    {
                      authenticatedUser,
                      authenticatedRoles,
                      idAssignedForNextWorkflow: headerWorkflowID,
                      request: koaCtxt.request,
                    },
                    async () => {
                      return await methodReg.invoke(undefined, args);
                    },
                  );
                });
              },
            );
            const retValue = cresult!;

            // Set the body to the return value unless the body is already set by the handler.
            if (koaCtxt.body === undefined) {
              koaCtxt.body = retValue;
            }
            span?.setStatus({ code: SpanStatusCode.OK });
          } catch (e) {
            if (e instanceof Error) {
              span?.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
              let st = (e as DBOSErrors.DBOSResponseError)?.status || 500;
              if (isClientRequestError(e)) {
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
              // Thrown item was not an Error, which is poor form.
              // using stringify() will not produce a pretty output, because our format function uses stringify() too.
              DBOS.logger.error(e);
              span?.setStatus({ code: SpanStatusCode.ERROR, message: JSON.stringify(e) });

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
            if (span) {
              httpTracer.inject(
                trace.setSpanContext(ROOT_CONTEXT, span.spanContext()),
                {
                  context: koaCtxt,
                },
                {
                  set: (carrier: Carrier, key: string, value: string) => {
                    carrier.context.set(key, value);
                  },
                },
              );
              DBOS.tracer?.endSpan(span);
            }
            await koaNext();
          }
        };

        // Middleware functions are applied directly to router verb methods to prevent duplicate calls.
        const routeMiddlewares = [defaults.koaBodyParser ?? bodyParser()].concat(defaults.koaMiddlewares ?? []);
        switch (ro.apiType) {
          case APITypes.GET:
            appRouter.get(ro.apiURL, ...routeMiddlewares, wrappedHandler);
            DBOS.logger.debug(`DBOS Server Registered GET ${ro.apiURL}`);
            break;
          case APITypes.POST:
            appRouter.post(ro.apiURL, ...routeMiddlewares, wrappedHandler);
            DBOS.logger.debug(`DBOS Server Registered POST ${ro.apiURL}`);
            break;
          case APITypes.PUT:
            appRouter.put(ro.apiURL, ...routeMiddlewares, wrappedHandler);
            DBOS.logger.debug(`DBOS Server Registered PUT ${ro.apiURL}`);
            break;
          case APITypes.PATCH:
            appRouter.patch(ro.apiURL, ...routeMiddlewares, wrappedHandler);
            DBOS.logger.debug(`DBOS Server Registered PATCH ${ro.apiURL}`);
            break;
          case APITypes.DELETE:
            appRouter.delete(ro.apiURL, ...routeMiddlewares, wrappedHandler);
            DBOS.logger.debug(`DBOS Server Registered DELETE ${ro.apiURL}`);
            break;
          default:
            exhaustiveCheckGuard(ro.apiType);
        }
      }
    }

    app.use(appRouter.routes()).use(appRouter.allowedMethods());
  }
}
