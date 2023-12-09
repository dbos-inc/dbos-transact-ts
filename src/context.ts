import { Span } from "@opentelemetry/sdk-trace-base";
import { WinstonLogger as Logger, Logger as DBOSLogger } from "./telemetry/logs";
import { has, get } from "lodash";
import { IncomingHttpHeaders } from "http";
import { ParsedUrlQuery } from "querystring";
import { UserDatabase } from "./user_database";
import { DBOSExecutor } from "./dbos-executor";
import { DBOSConfigKeyTypeError } from "./error";

// HTTPRequest includes useful information from http.IncomingMessage and parsed body, URL parameters, and parsed query string.
export interface HTTPRequest {
  readonly headers?: IncomingHttpHeaders;  // A node's http.IncomingHttpHeaders object.
  readonly rawHeaders?: string[];          // Raw headers.
  readonly params?: unknown;               // Parsed path parameters from the URL.
  readonly body?: unknown;                 // parsed HTTP body as an object.
  readonly rawBody?: string;               // Unparsed raw HTTP body string.
  readonly query?: ParsedUrlQuery;         // Parsed query string.
  readonly querystring?: string;           // Unparsed raw query string.
  readonly url?: string;                   // Request URL.
  readonly ip?: string;                    // Request remote address.
  readonly requestID?: string;              // Request ID. Gathered from headers or generated if missing.
}

export interface DBOSContext {
  readonly request: HTTPRequest;
  readonly workflowUUID: string;
  readonly authenticatedUser: string;
  readonly authenticatedRoles: string[];
  readonly assumedRole: string;

  readonly logger: DBOSLogger;
  readonly span: Span;

  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
}

export class DBOSContextImpl implements DBOSContext {
  request: HTTPRequest = {};          // Raw incoming HTTP request.
  authenticatedUser: string = "";     // The user that has been authenticated
  authenticatedRoles: string[] = [];  // All roles the user has according to authentication
  assumedRole: string = "";           // Role in use - that user has and provided authorization to current function
  workflowUUID: string = "";          // Workflow UUID. Empty for HandlerContexts.
  readonly logger: DBOSLogger;      // Wrapper around the global logger for this context.

  constructor(readonly operationName: string, readonly span: Span, logger: Logger, parentCtx?: DBOSContextImpl) {
    if (parentCtx) {
      this.request = parentCtx.request;
      this.authenticatedUser = parentCtx.authenticatedUser;
      this.authenticatedRoles = parentCtx.authenticatedRoles;
      this.assumedRole = parentCtx.assumedRole;
      this.workflowUUID = parentCtx.workflowUUID;
    }
    this.logger = new DBOSLogger(logger, this);
  }

  /*** Application configuration ***/
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  applicationConfig?: any;
  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    // If there is no application config at all, or the key is missing, return the default value or undefined.
    if (!this.applicationConfig || !has(this.applicationConfig, key)) {
      if (defaultValue) {
        return defaultValue;
      }
      return undefined;
    }

    // If the key is found and the default value is provided, check whether the value is of the same type.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const value = get(this.applicationConfig, key);
    if (defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }

    return value as T;
  }
}


/**
 * TODO : move logger and application, getConfig to a BaseContext which is at the root of all contexts
 */
export class InitContext {

  readonly logger: Logger;

  // All private Not exposed
  private userDatabase: UserDatabase;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private application: any;

  constructor(readonly dbosExec: DBOSExecutor) {
    this.logger = dbosExec.logger;
    this.userDatabase = dbosExec.userDatabase;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    this.application = dbosExec.config.application;
  }

  createUserSchema(): Promise<void> {
    return this.userDatabase.createSchema();
  }

  dropUserSchema(): Promise<void> {
    return this.userDatabase.dropSchema();
  }

  queryUserDB<R>(sql: string, ...params: unknown[]): Promise<R[]> {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return this.userDatabase.query(sql, ...params);
  }

  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    // If there is no application config at all, or the key is missing, return the default value or undefined.
    if (!this.application || !has(this.application, key)) {
      if (defaultValue) {
        return defaultValue;
      }
      return undefined;
    }

    // If the key is found and the default value is provided, check whether the value is of the same type.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const value = get(this.dbosExec.config.application, key);
    if (defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }

    return value as T;
  }
}
