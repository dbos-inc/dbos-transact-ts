import {
  associateClassWithEventReceiver,
  associateMethodWithEventReceiver,
  DBOS,
  DBOSConfig,
  DBOSExecutorContext,
  type DBOSEventReceiver,
} from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

import Koa, { Middleware } from 'koa';
import Router from '@koa/router';

import request from 'supertest';
import bodyParser from '@koa/bodyparser';
import { exhaustiveCheckGuard } from '../src/utils';
import { DBOSDataValidationError, DBOSError, DBOSResponseError, isClientError } from '../src/error';

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

interface DBOSHTTPReg {
  apiURL: string;
  apiType: APITypes;
}

interface DBOSHTTPClassReg {
  //authMiddleware?: DBOSHttpAuthMiddleware;
  koaBodyParser?: Koa.Middleware;
  koaCors?: Koa.Middleware;
  koaMiddlewares?: Koa.Middleware[];
  koaGlobalMiddlewares?: Koa.Middleware[];
}

class DBOSHTTPBase implements DBOSEventReceiver {
  executor?: DBOSExecutorContext | undefined;

  // TODO: Lifecycle not really relevant...
  async destroy(): Promise<void> {
    return Promise.resolve();
  }

  async initialize(_executor: DBOSExecutorContext): Promise<void> {
    return Promise.resolve();
  }

  logRegisteredEndpoints(): void {
    return;
  }

  // TODO Register all endpoints into a routing tree to make a handler callback
  // TODO All interesting class stuff
  // TODO The dispatch ... it needs its own wrapper over it, or is that central?

  httpApiDec(verb: APITypes, url: string) {
    const er = this as unknown as DBOSEventReceiver;
    return function apidec<This, Args extends unknown[], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { descriptor, registration, receiverInfo } = associateMethodWithEventReceiver(
        er,
        target,
        propertyKey,
        inDescriptor,
      );
      const handlerRegistration = receiverInfo as DBOSHTTPReg;
      handlerRegistration.apiURL = url;
      handlerRegistration.apiType = verb;
      registration.performArgValidation = true;

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
      const clsreg = associateClassWithEventReceiver(er, ctor) as DBOSHTTPClassReg;
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
      const clsreg = associateClassWithEventReceiver(er, ctor) as DBOSHTTPClassReg;
      clsreg.koaGlobalMiddlewares = [...(clsreg.koaGlobalMiddlewares ?? []), ...koaMiddleware];
    }
    return clsdec;
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
  expect(DBOS.globalLogger).toBeDefined();
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
    const eps = DBOSExecutor.globalInstance!.getRegistrationsFor(dhttp);
    app = new Koa();
    appRouter = new Router();

    const globalMiddlewares: Set<Koa.Middleware> = new Set();

    for (const e of eps) {
      const { methodConfig, classConfig, methodReg } = e;
      const ro = methodConfig as DBOSHTTPReg;
      const defaults = classConfig as DBOSHTTPClassReg;

      // TODO: appRouter.all() on CORS, other middlewares
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
        try {
          // TODO: Auth first

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

          // TODO: Extract workflow UUID from headers (if any).

          // Finally, invoke the function and properly set HTTP response.
          // If functions return successfully and hasn't set the body, we set the body to the function return value. The status code will be automatically set to 200 or 204 (if the body is null/undefined).
          // In case of an exception:
          // - If a client-side error is thrown, we return 400.
          // - If an error contains a `status` field, we return the specified status code.
          // - Otherwise, we return 500.
          // TODO: configuredInstance is currently null; we don't allow configured handlers now.
          // TODO: Run with context
          // TODO: Tracing
          const cresult = await methodReg.invoke(undefined, [...args]);
          const retValue = cresult!;

          // Set the body to the return value unless the body is already set by the handler.
          if (koaCtxt.body === undefined) {
            koaCtxt.body = retValue;
          }
        } catch (e) {
          if (e instanceof Error) {
            const annotated_e = e as Error & { dbos_already_logged?: boolean };
            if (annotated_e.dbos_already_logged !== true) {
              DBOS.logger.error(e);
            }
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
            DBOS.logger.error(e);
            koaCtxt.body = e;
            koaCtxt.status = 500;
          }
        } finally {
          // TODO: Inject trace context into response headers.
          // We cannot use the defaultTextMapSetter to set headers through Koa
          // So we provide a custom setter that sets headers through Koa's context.
          // See https://github.com/open-telemetry/opentelemetry-js/blob/868f75e448c7c3a0efd75d72c448269f1375a996/packages/opentelemetry-core/src/trace/W3CTraceContextPropagator.ts#L74
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
