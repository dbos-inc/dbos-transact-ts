import Koa from 'koa';
import { IncomingHttpHeaders } from 'http';

import { ClassRegistration, RegistrationDefaults, getOrCreateClassRegistration } from '../decorators';
import { DBOSContextualLogger } from '../telemetry/logs';
import { UserDatabaseClient } from '../user_database';
import { OperationType } from '../dbos-executor';
import { getExecutor } from '../dbos';
import { HTTPRequest } from '../context';

import { Span } from '@opentelemetry/sdk-trace-base';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { trace, defaultTextMapGetter, ROOT_CONTEXT } from '@opentelemetry/api';
import { randomUUID } from 'node:crypto';

// Middleware context does not extend base context because it runs before handler/workflow operations.
export interface MiddlewareContext {
  readonly koaContext: Koa.Context;
  readonly name: string; // Method (handler, transaction, workflow) name
  readonly requiredRole: string[]; // Roles required for the invoked operation, if empty perhaps auth is not required

  readonly logger: DBOSContextualLogger; // Logger, for logging from middleware
  readonly span: Span; // Existing span

  query<C extends UserDatabaseClient, R, T extends unknown[]>(
    qry: (dbclient: C, ...args: T) => Promise<R>,
    ...args: T
  ): Promise<R>;
}

/**
 * Authentication middleware that executes before a request reaches a function.
 * This is expected to:
 *   - Validate the request found in the handler context and extract auth information from the request.
 *   - Map the HTTP request to the user identity and roles defined in app.
 * If this succeeds, return the current authenticated user and a list of roles.
 * If any step fails, throw an error.
 * @deprecated - use `@dbos-inc/koa-serve`
 */
export type DBOSHttpAuthMiddleware = (ctx: MiddlewareContext) => Promise<DBOSHttpAuthReturn | void>;

export interface DBOSHttpAuthReturn {
  authenticatedUser: string;
  authenticatedRoles: string[];
}

// Class-level decorators
export interface MiddlewareDefaults extends RegistrationDefaults {
  authMiddleware?: DBOSHttpAuthMiddleware;
  koaBodyParser?: Koa.Middleware;
  koaCors?: Koa.Middleware;
  koaMiddlewares?: Koa.Middleware[];
  koaGlobalMiddlewares?: Koa.Middleware[];
}

export class MiddlewareClassRegistration extends ClassRegistration implements MiddlewareDefaults {
  authMiddleware?: DBOSHttpAuthMiddleware;
  koaBodyParser?: Koa.Middleware;
  koaCors?: Koa.Middleware;
  koaMiddlewares?: Koa.Middleware[];
  koaGlobalMiddlewares?: Koa.Middleware[];

  constructor() {
    super();
  }
}

/////////////////////////////////
/* MIDDLEWARE CLASS DECORATORS */
/////////////////////////////////

/**
 * Define an authentication function for each endpoint in this class.
 * @deprecated - use `@dbos-inc/koa-serve`
 */
export function Authentication(authMiddleware: DBOSHttpAuthMiddleware) {
  if (authMiddleware === undefined) {
    throw new TypeError(`'Authentication' received undefined input. Possible circular dependency?`);
  }
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration;
    clsreg.authMiddleware = authMiddleware;
  }
  return clsdec;
}

/**
 * Define a Koa body parser applied before any middleware. If not set, the default @koa/bodyparser is used.
 * @deprecated - use `@dbos-inc/koa-serve`
 */
export function KoaBodyParser(koaBodyParser: Koa.Middleware) {
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration;
    clsreg.koaBodyParser = koaBodyParser;
  }
  return clsdec;
}

/**
 * Define a Koa CORS policy applied before any middleware. If not set, the default @koa/cors (w/ .yaml config) is used.
 * @deprecated - use `@dbos-inc/koa-serve`
 */
export function KoaCors(koaCors: Koa.Middleware) {
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration;
    clsreg.koaCors = koaCors;
  }
  return clsdec;
}

/**
 * Define Koa middleware that is applied in order to each endpoint in this class.
 * @deprecated - use `@dbos-inc/koa-serve`
 */
export function KoaMiddleware(...koaMiddleware: Koa.Middleware[]) {
  koaMiddleware.forEach((i) => {
    if (i === undefined) {
      throw new TypeError(`'KoaMiddleware' received undefined input. Possible circular dependency?`);
    }
  });
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration;
    clsreg.koaMiddlewares = koaMiddleware;
  }
  return clsdec;
}

/**
 * Define Koa middleware that is applied to all requests, including this class, other classes,
 *   or requests that do not end up in DBOS handlers at all.
 * @deprecated - use `@dbos-inc/koa-serve`
 */
export function KoaGlobalMiddleware(...koaMiddleware: Koa.Middleware[]) {
  koaMiddleware.forEach((i) => {
    if (i === undefined) {
      throw new TypeError(`'KoaGlobalMiddleware' received undefined input. Possible circular dependency?`);
    }
  });
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration;
    clsreg.koaGlobalMiddlewares = koaMiddleware;
  }
  return clsdec;
}

/////////////////////////////////
/* HTTP APP TRACING MIDDLEWARES */
/////////////////////////////////

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

export function createHTTPSpan(request: HTTPRequest, httpTracer: W3CTraceContextPropagator): Span {
  // If present, retrieve the trace context from the request
  const extractedSpanContext = trace.getSpanContext(
    httpTracer.extract(ROOT_CONTEXT, request.headers, defaultTextMapGetter),
  );
  let span: Span;
  const spanAttributes = {
    operationType: OperationType.HANDLER,
    requestID: request.requestID,
    requestIP: request.ip,
    requestURL: request.url,
    requestMethod: request.method,
  };
  if (extractedSpanContext === undefined) {
    // request.url should be defined by now. Let's cast it to string
    span = getExecutor().tracer.startSpan(request.url as string, spanAttributes);
  } else {
    extractedSpanContext.isRemote = true;
    span = getExecutor().tracer.startSpanWithContext(extractedSpanContext, request.url as string, spanAttributes);
  }
  return span;
}
