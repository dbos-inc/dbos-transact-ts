import { Span } from "@opentelemetry/sdk-trace-base";
import { GlobalLogger as Logger, Logger as DBOSLogger } from "./telemetry/logs";
import { get } from "lodash";
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
  executorID: string = "local";       // Executor ID. Gathered from request headers and "local" otherwise
  readonly logger: DBOSLogger;      // Wrapper around the global logger for this context.

  constructor(readonly operationName: string, readonly span: Span, logger: Logger, parentCtx?: DBOSContextImpl) {
    if (parentCtx) {
      this.request = parentCtx.request;
      this.authenticatedUser = parentCtx.authenticatedUser;
      this.authenticatedRoles = parentCtx.authenticatedRoles;
      this.assumedRole = parentCtx.assumedRole;
      this.workflowUUID = parentCtx.workflowUUID;
      this.executorID = parentCtx.executorID;
    }
    this.logger = new DBOSLogger(logger, this);
  }

  /*** Application configuration ***/
  applicationConfig?: object;
  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const value = get(this.applicationConfig, key, defaultValue);
    // If the key is found and the default value is provided, check whether the value is of the same type.
    if (value && defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }
    return value;
  }
}


/**
 * TODO : move logger and application, getConfig to a BaseContext which is at the root of all contexts
 */
export class InitContext {

  readonly logger: Logger;

  // All private Not exposed
  private userDatabase: UserDatabase;
  private application?: object;

  constructor(readonly dbosExec: DBOSExecutor) {
    this.logger = dbosExec.logger;
    this.userDatabase = dbosExec.userDatabase;
    this.application = dbosExec.config.application;
  }

  createUserSchema(): Promise<void> {
    this.logger.warn("Schema synchronization is deprecated and unsafe for production use. Please use migrations instead: https://typeorm.io/migrations")
    return this.userDatabase.createSchema();
  }

  dropUserSchema(): Promise<void> {
    this.logger.warn("Schema synchronization is deprecated and unsafe for production use. Please use migrations instead: https://typeorm.io/migrations")
    return this.userDatabase.dropSchema();
  }

  queryUserDB<R>(sql: string, ...params: unknown[]): Promise<R[]> {
    return this.userDatabase.query(sql, ...params);
  }

  getConfig<T>(key: string): T | undefined;
  getConfig<T>(key: string, defaultValue: T): T;
  getConfig<T>(key: string, defaultValue?: T): T | undefined {
    const value = get(this.application, key, defaultValue);
    // If the key is found and the default value is provided, check whether the value is of the same type.
    if (value && defaultValue && typeof value !== typeof defaultValue) {
      throw new DBOSConfigKeyTypeError(key, typeof defaultValue, typeof value);
    }
    return value;
  }
}
