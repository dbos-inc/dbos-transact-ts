import Koa from "koa";
import { ClassRegistration, RegistrationDefaults, getOrCreateClassRegistration } from "../decorators";
import { DBOSUndefinedDecoratorInputError } from "../error";

import { UserDatabaseClient } from "../user_database";

import { Span } from "@opentelemetry/sdk-trace-base";
import { Logger as DBOSLogger } from "../telemetry/logs";
import { OpenAPIV3 as OpenApi3 } from 'openapi-types';

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
  koaMiddlewares?: Koa.Middleware[];
  authMiddleware?: DBOSHttpAuthMiddleware;
}

export class MiddlewareClassRegistration<CT extends { new(...args: unknown[]): object }> extends ClassRegistration<CT> implements MiddlewareDefaults {
  authMiddleware?: DBOSHttpAuthMiddleware;
  koaMiddlewares?: Koa.Middleware[];

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
  return function <T extends { new(...args: unknown[]): object }>(_ctor: T) { }
}
