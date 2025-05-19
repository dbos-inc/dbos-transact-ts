import Koa, { Context, Middleware } from 'koa';
import Router from '@koa/router';
import bodyParser from '@koa/bodyparser';
import cors from '@koa/cors';

import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { SpanStatusCode, trace, defaultTextMapGetter, ROOT_CONTEXT } from '@opentelemetry/api';
import { Span } from '@opentelemetry/sdk-trace-base';

import { IncomingHttpHeaders } from 'http';
import { randomUUID } from 'node:crypto';

import { DBOS, DBOSConfig, DBOSLifecycleCallback, Error as DBOSErrors } from '../src';

// Test stuff
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import request from 'supertest';

// Basic idea:
//  Web points are decorated - this registers them
//  Middleware can be decorated - or not.

// At some point, in a main file / function, you launch() DBOS
//  and create and start your own web server.
//  Creating the server can consult the DBOS method registries

// How request, auth, and other context works:
//  You register a context provider, this allows it to save and load
//   stuff from the system DB to accompany the workflow

// OK, what I'm doing today:
//  Registration of arbitrary classes/instances -> associated items
//  Class / class name -> registration; registration has key->items
//    This is really so ... aspects work together?  To pass context around?
//  The open goal:
//   DBOSHttp collects up all the functions; this names and registers them
//   DBOSHttp has a list of all its functions (or retrieves it; that's TBD)
//     It can make you a router of these functions
//   Each function:
//     HTTP dispatches it with request context
//     Some glue gets the request/response stuff and makes args; this can do validation
//       (or is validation based on interceptors); this has auth, request, OAOO key, etc...
//       Hence, it runs with the local storage
//         This hits a DBOS workflow (which may be enqueued); that saves the context
//         The dequeued workflow has to run in same context; do we make a function for it... yes
//     For the first rev, we will just use the auth and request fields in the existing record
//         Once this is working, we'll fix it
//   How to do tracing/logging is a bit trickier
// TODO: Also instances; no reason why not.

export enum APITypes {
  GET = 'GET',
  POST = 'POST',
  PUT = 'PUT',
  PATCH = 'PATCH',
  DELETE = 'DELETE',
}

const HTTP_OPERATION_TYPE = 'http';

// Context for auth middleware
export interface DBOSHTTPAuthContext {
  readonly koaContext: Koa.Context;
  readonly name: string; // Method (handler, transaction, workflow) name
  readonly requiredRole: string[]; // Roles required for the invoked operation, if empty perhaps auth is not required
}

export interface DBOSHTTPAuthReturn {
  authenticatedUser: string;
  authenticatedRoles: string[];
}

/**
 * Authentication middleware that executes before a request reaches a function.
 * This is expected to:
 *   - Validate the request found in the handler context and extract auth information from the request.
 *   - Map the HTTP request to the user identity and roles defined in app.
 * If this succeeds, return the current authenticated user and a list of roles.
 * If any step fails, throw an error.
 */
export type DBOSHTTPAuthMiddleware = (ctx: DBOSHTTPAuthContext) => Promise<DBOSHTTPAuthReturn | void>;

interface DBOSHTTPReg {
  apiURL: string;
  apiType: APITypes;
}

export const WorkflowIDHeader = 'dbos-idempotency-key';

export const RequestIDHeader = 'X-Request-ID';
export function getOrGenerateRequestID(headers: IncomingHttpHeaders): string {
  const reqID = headers[RequestIDHeader.toLowerCase()] as string | undefined; // RequestIDHeader is expected to be a single value, so we dismiss the possible string[] returned type.
  if (reqID) {
    return reqID;
  }
  const newID = randomUUID();
  headers[RequestIDHeader.toLowerCase()] = newID; // This does not carry through the response
  return newID;
}

export function isClientRequestError(e: Error) {
  return DBOSErrors.isDataValidationError(e);
}

function exhaustiveCheckGuard(_: never): never {
  throw new Error('Exaustive matching is not applied');
}

interface DBOSHTTPClassReg {
  authMiddleware?: DBOSHTTPAuthMiddleware;
  koaBodyParser?: Koa.Middleware;
  koaCors?: Koa.Middleware;
  koaMiddlewares?: Koa.Middleware[];
  koaGlobalMiddlewares?: Koa.Middleware[];
}

interface DBOSHTTPConfig {
  corsMiddleware?: boolean;
  credentials?: boolean;
  allowedOrigins?: string[];
}

class DBOSHTTPBase extends DBOSLifecycleCallback {
  httpApiDec(verb: APITypes, url: string) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    return function apidec<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { func, registration, regInfo } = DBOS.associateFunctionWithInfo(er, descriptor.value!, {
        classOrInst: target,
        name: propertyKey,
      });
      const handlerRegistration = regInfo as DBOSHTTPReg;
      handlerRegistration.apiURL = url;
      handlerRegistration.apiType = verb;
      registration.performArgValidation = true;

      descriptor.value = func;
      return descriptor;
    };
  }

  /** Decorator indicating that the method is the target of HTTP GET operations for `url` */
  getApi(url: string) {
    return this.httpApiDec(APITypes.GET, url);
  }

  /** Decorator indicating that the method is the target of HTTP POST operations for `url` */
  postApi(url: string) {
    return this.httpApiDec(APITypes.POST, url);
  }

  /** Decorator indicating that the method is the target of HTTP PUT operations for `url` */
  putApi(url: string) {
    return this.httpApiDec(APITypes.PUT, url);
  }

  /** Decorator indicating that the method is the target of HTTP PATCH operations for `url` */
  patchApi(url: string) {
    return this.httpApiDec(APITypes.PATCH, url);
  }

  /** Decorator indicating that the method is the target of HTTP DELETE operations for `url` */
  deleteApi(url: string) {
    return this.httpApiDec(APITypes.DELETE, url);
  }

  /**
   * Define an authentication function for each endpoint in this class.
   */
  authentication(authMiddleware: DBOSHTTPAuthMiddleware) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSHTTPClassReg;
      clsreg.authMiddleware = authMiddleware;
    }
    return clsdec;
  }

  koaBodyParser(koaBodyParser: Koa.Middleware) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSHTTPClassReg;
      clsreg.koaBodyParser = koaBodyParser;
    }
    return clsdec;
  }

  koaCors(koaCors: Koa.Middleware) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSHTTPClassReg;
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
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSHTTPClassReg;
      clsreg.koaMiddlewares = [...(clsreg.koaMiddlewares ?? []), ...koaMiddleware];
    }
    return clsdec;
  }

  /**
   * Define Koa middleware that is applied to all requests, including this class, other classes,
   *   or requests that do not end up in DBOS handlers at all.
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
      const clsreg = DBOS.associateClassWithInfo(er, ctor) as DBOSHTTPClassReg;
      clsreg.koaGlobalMiddlewares = [...(clsreg.koaGlobalMiddlewares ?? []), ...koaMiddleware];
    }
    return clsdec;
  }

  app?: Koa;
  appRouter?: Router;

  createApp(config?: DBOSHTTPConfig) {
    this.app = new Koa();
    this.appRouter = new Router();
    this.registerWithApp(this.app, this.appRouter, config);
  }

  registerWithApp(app: Koa, appRouter: Router, config?: DBOSHTTPConfig) {
    const globalMiddlewares: Set<Koa.Middleware> = new Set();

    const eps = DBOS.getAssociatedInfo(this);
    for (const e of eps) {
      const { methodConfig, classConfig, methodReg } = e;
      const ro = methodConfig as DBOSHTTPReg;
      const defaults = classConfig as DBOSHTTPClassReg;

      // TODO: What about instance methods?
      //   Those would have to be registered another way that accepted the instances.
      if (methodReg.isInstance) {
        DBOS.logger.warn(
          `Operation ${methodReg.className}/${methodReg.name} is registered with an endpoint (${ro.apiURL}) but cannot be invoked without an instance.`,
        );
        return;
      }

      // Apply CORS, bodyParser, and other middlewares
      // Check if we need to apply a custom CORS
      // TODO give this the right home...
      if (defaults.koaCors) {
        appRouter.all(ro.apiURL, defaults.koaCors); // Use router.all to register with all methods including preflight requests
      } else {
        if (config?.corsMiddleware ?? true) {
          appRouter.all(
            ro.apiURL,
            cors({
              credentials: config?.credentials ?? true,
              origin: (o: Context) => {
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

            if (isQueryMethod) {
              foundArg = koaCtxt.request.query[marg.name];
              if (foundArg !== undefined) {
                args.push(foundArg);
              }
            } else if (isBodyMethod) {
              if (!koaCtxt.request.body) {
                throw new DBOSErrors.DBOSDataValidationError(`Argument ${marg.name} requires a method body.`);
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
            operationType: HTTP_OPERATION_TYPE,
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
          const cresult = await DBOS.runWithContext(
            {
              authenticatedUser,
              authenticatedRoles,
              idAssignedForNextWorkflow: headerWorkflowID,
              span,
              request: koaCtxt.request,
            },
            async () => {
              const f = methodReg.wrappedFunction ?? methodReg.registeredFunction ?? methodReg.origFunction;
              return (await f.call(undefined, ...args)) as unknown;
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

    app.use(appRouter.routes()).use(appRouter.allowedMethods());
  }
}

const dhttp = new DBOSHTTPBase();

let middlewareCounter = 0;
const testMiddleware: Middleware = async (_ctx, next) => {
  middlewareCounter++;
  await next();
};

let middlewareCounter2 = 0;
const testMiddleware2: Middleware = async (_ctx, next) => {
  middlewareCounter2 = middlewareCounter2 + 1;
  await next();
};

let middlewareCounterG = 0;
const testMiddlewareG: Middleware = async (_ctx, next) => {
  middlewareCounterG = middlewareCounterG + 1;
  expect(DBOS.logger).toBeDefined();
  await next();
};

@dhttp.koaGlobalMiddleware(testMiddlewareG)
@dhttp.koaGlobalMiddleware(testMiddleware, testMiddleware2)
export class HTTPEndpoints {
  @dhttp.getApi('/foobar')
  static async foobar(arg: string) {
    return Promise.resolve(`ARG: ${arg}`);
  }
}

describe('decoratorless-api-tests', () => {
  let config: DBOSConfig;
  let app: Koa;
  let appRouter: Router;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    middlewareCounter = middlewareCounter2 = middlewareCounterG = 0;
    await DBOS.launch();
    app = new Koa();
    appRouter = new Router();
    dhttp.registerWithApp(app, appRouter);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-functions', async () => {
    const response1 = await request(app.callback()).get('/foobar?arg=A');
    expect(response1.statusCode).toBe(200);
    expect(response1.text).toBe('ARG: A');
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounter2).toBe(1);
    expect(middlewareCounterG).toBe(1);
  });
});
