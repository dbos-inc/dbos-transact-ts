import Koa from "koa";
import { Request, Response, NextFunction } from "express";
import { IncomingHttpHeaders } from "http";

import { ClassRegistration, RegistrationDefaults, getOrCreateClassRegistration } from "../decorators";
import { DBOSUndefinedDecoratorInputError } from "../error";
import { Logger as DBOSLogger } from "../telemetry/logs";
import { UserDatabaseClient } from "../user_database";
import { OperationType } from "../dbos-executor";
import { DBOS } from "../dbos";
import { HTTPRequest } from "../context";

import { Span } from "@opentelemetry/sdk-trace-base";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { trace, defaultTextMapGetter, ROOT_CONTEXT, SpanStatusCode } from "@opentelemetry/api";
import { OpenAPIV3 as OpenApi3 } from "openapi-types";
import { v4 as uuidv4 } from "uuid";
import { DBOSJSON } from "../utils";

// Middleware context does not extend base context because it runs before handler/workflow operations.
export interface MiddlewareContext {
  readonly koaContext: Koa.Context;
  readonly name: string; // Method (handler, transaction, workflow) name
  readonly requiredRole: string[]; // Roles required for the invoked operation, if empty perhaps auth is not required

  readonly logger: DBOSLogger; // Logger, for logging from middleware
  readonly span: Span; // Existing span

  getConfig<T>(key: string, deflt: T | undefined): T | undefined; // Access to configuration information

  query<C extends UserDatabaseClient, R, T extends unknown[]>(qry: (dbclient: C, ...args: T) => Promise<R>, ...args: T): Promise<R>;
}

/**
 * Authentication middleware that executes before a request reaches a function.
 * This is expected to:
 *   - Validate the request found in the handler context and extract auth information from the request.
 *   - Map the HTTP request to the user identity and roles defined in app.
 * If this succeeds, return the current authenticated user and a list of roles.
 * If any step fails, throw an error.
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

export class MiddlewareClassRegistration<CT extends { new(...args: unknown[]): object }> extends ClassRegistration<CT> implements MiddlewareDefaults {
  authMiddleware?: DBOSHttpAuthMiddleware;
  koaBodyParser?: Koa.Middleware;
  koaCors?: Koa.Middleware;
  koaMiddlewares?: Koa.Middleware[];
  koaGlobalMiddlewares?: Koa.Middleware[];

  constructor(ctor: CT) {
    super(ctor);
  }
}

/////////////////////////////////
/* MIDDLEWARE CLASS DECORATORS */
/////////////////////////////////

/**
 * Define an authentication function for each endpoint in this class.
 */
export function Authentication(authMiddleware: DBOSHttpAuthMiddleware) {
  if (authMiddleware === undefined) {
    throw new DBOSUndefinedDecoratorInputError("Authentication");
  }
  function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration<T>;
    clsreg.authMiddleware = authMiddleware;
  }
  return clsdec;
}

/**
 * Define a Koa body parser applied before any middleware. If not set, the default @koa/bodyparser is used.
 */
export function KoaBodyParser(koaBodyParser: Koa.Middleware) {
  function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration<T>;
    clsreg.koaBodyParser = koaBodyParser;
  }
  return clsdec;
}

/**
 * Define a Koa CORS policy applied before any middleware. If not set, the default @koa/cors (w/ .yaml config) is used.
 */
export function KoaCors(koaCors: Koa.Middleware) {
  function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration<T>;
    clsreg.koaCors = koaCors;
  }
  return clsdec;
}

/**
 * Define Koa middleware that is applied in order to each endpoint in this class.
 */
export function KoaMiddleware(...koaMiddleware: Koa.Middleware[]) {
  koaMiddleware.forEach((i) => {
    if (i === undefined) {
      throw new DBOSUndefinedDecoratorInputError("KoaMiddleware");
    }
  });
  function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration<T>;
    clsreg.koaMiddlewares = koaMiddleware;
  }
  return clsdec;
}

/**
 * Define Koa middleware that is applied to all requests, including this class, other classes,
 *   or requests that do not end up in DBOS handlers at all.
 */
export function KoaGlobalMiddleware(...koaMiddleware: Koa.Middleware[]) {
  koaMiddleware.forEach((i) => {
    if (i === undefined) {
      throw new DBOSUndefinedDecoratorInputError("KoaGlobalMiddleware");
    }
  });
  function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
    const clsreg = getOrCreateClassRegistration(ctor) as MiddlewareClassRegistration<T>;
    clsreg.koaGlobalMiddlewares = koaMiddleware;
  }
  return clsdec;
}


/////////////////////////////////
/* OPEN API DECORATORS */
/////////////////////////////////

// Note, OAuth2 is not supported yet.
type SecurityScheme = Exclude<OpenApi3.SecuritySchemeObject, OpenApi3.OAuth2SecurityScheme>;

/**
 * Declare an OpenApi Security Scheme (https://spec.openapis.org/oas/v3.0.3#security-scheme-object
 * for the methods of a class. Note, this decorator is only used in OpenApi generation and does not
 * affect runtime behavior of the app.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function OpenApiSecurityScheme(securityScheme: SecurityScheme) {
  return function <T extends { new (...args: unknown[]): object }>(_ctor: T) {};
}

/////////////////////////////////
/* HTTP APP TRACING MIDDLEWARES */
/////////////////////////////////

export const RequestIDHeader = "X-Request-ID";
export function getOrGenerateRequestID(headers: IncomingHttpHeaders): string {
  const reqID = headers[RequestIDHeader.toLowerCase()] as string | undefined; // RequestIDHeader is expected to be a single value, so we dismiss the possible string[] returned type.
  if (reqID) {
    return reqID;
  }
  const newID = uuidv4();
  headers[RequestIDHeader.toLowerCase()] = newID; // This does not carry through the response
  return newID;
}

export function createHTTPSpan(request: HTTPRequest, httpTracer: W3CTraceContextPropagator): Span {
  // If present, retrieve the trace context from the request
  const extractedSpanContext = trace.getSpanContext(httpTracer.extract(ROOT_CONTEXT, request.headers, defaultTextMapGetter));
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
    span = DBOS.executor.tracer.startSpan(request.url as string, spanAttributes);
  } else {
    extractedSpanContext.isRemote = true;
    span = DBOS.executor.tracer.startSpanWithContext(extractedSpanContext, request.url as string, spanAttributes);
  }
  return span;
}

export async function koaTracingMiddleware(ctx: Koa.Context, next: Koa.Next) {
  // Retrieve or generate the request ID
  const requestID = getOrGenerateRequestID(ctx.request.headers);
  // Attach it to the response headers (here through Koa's context)
  ctx.set(RequestIDHeader, requestID);
  const request: HTTPRequest = {
    headers: ctx.request.headers,
    rawHeaders: ctx.req.rawHeaders,
    params: ctx.params,
    body: ctx.request.body,
    rawBody: ctx.request.rawBody,
    query: ctx.request.query,
    querystring: ctx.request.querystring,
    url: ctx.request.url,
    ip: ctx.request.ip,
    method: ctx.request.method,
    requestID,
  };
  const httpTracer = new W3CTraceContextPropagator();
  const span = createHTTPSpan(request, httpTracer);

  try {
    await DBOS.withTracedContext(request.url as string, span, request, next);
    span.setStatus({ code: SpanStatusCode.OK});
  } catch (e) {
    if (e instanceof Error) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
    } else {
      span.setStatus({ code: SpanStatusCode.ERROR, message: DBOSJSON.stringify(e) });
    }
    throw e;
  } finally {
    DBOS.executor.tracer.endSpan(span);
  }
}

export async function expressTracingMiddleware(req: Request, res: Response, next: NextFunction) {
  // Retrieve or generate the request ID
  const requestID = getOrGenerateRequestID(req.headers);
  // Attach it to the response headers (here through Express's response)
  res.setHeader(RequestIDHeader, requestID);
  const request: HTTPRequest = {
    headers: req.headers,
    rawHeaders: req.rawHeaders,
    params: req.params,
    body: req.body,
    rawBody: req.rawBody,
    // query: req.query,
    querystring: req.url.split("?")[1],
    url: req.url,
    ip: req.ip,
    method: req.method,
    requestID,
  };
  const httpTracer = new W3CTraceContextPropagator();
  const span = createHTTPSpan(request, httpTracer);

  try {
    await DBOS.withTracedContext(request.url as string, span, request, next as () => Promise<void>);
    if (res.statusCode >= 400) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: res.statusMessage });
    } else {
      span.setStatus({ code: SpanStatusCode.OK});
    }
  } catch (e) {
    if (e instanceof Error) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
    } else {
      span.setStatus({ code: SpanStatusCode.ERROR, message: DBOSJSON.stringify(e) });
    }
    throw e;
  } finally {
    DBOS.executor.tracer.endSpan(span);
  }
}
