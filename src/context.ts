import { Span } from "@opentelemetry/sdk-trace-base";
import { Logger } from "./telemetry/logs";
import { LogSeverity } from "./telemetry/signals";
import { has, get } from "lodash";
import { IncomingHttpHeaders } from "http";
import { ParsedUrlQuery } from "querystring";

// Operon request includes useful information from http.IncomingMessage and parsed body, URL parameters, and parsed query string.
export interface HTTPRequest {
  headers?: IncomingHttpHeaders;  // HTTP headers.
  rawHeaders?: string[];
  params?: unknown; // Parsed argument from URL.
  body?: unknown;  // parsed HTTP body as an object.
  rawBody?: string; // unparsed raw HTTP body string.
  query?: ParsedUrlQuery; // parsed query string.
  querystring?: string; // unparsed query string.
  url?: string; // request url.
  ip?: string; // request remote address.
}

export interface OperonContext {
  request?: HTTPRequest;
  workflowUUID: string;
  authenticatedUser: string;

  span: Span;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getConfig(key: string): any;

  info(message: string): void;
  warn(message: string): void;
  log(message: string): void;
  error(message: string): void;
  debug(message: string): void;
}

export class OperonContextImpl implements OperonContext {
  request?: HTTPRequest; // Raw incoming HTTP request.

  authenticatedUser: string = ""; ///< The user that has been authenticated
  authenticatedRoles: string[] = []; ///< All roles the user has according to authentication
  assumedRole: string = ""; ///< Role in use - that user has and provided authorization to current function

  workflowUUID: string = "";

  constructor(readonly operationName: string, readonly span: Span, private readonly logger: Logger, parentCtx?: OperonContextImpl) {
    if (parentCtx) {
      this.request = parentCtx.request;
      this.authenticatedUser = parentCtx.authenticatedUser;
      this.authenticatedRoles = parentCtx.authenticatedRoles;
      this.assumedRole = parentCtx.assumedRole;
      this.workflowUUID = parentCtx.workflowUUID;
    }
  }

  /*** Application configuration ***/
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  applicationConfig?: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getConfig(key: string): any {
    if (!this.applicationConfig) {
      return undefined;
    }
    if (!has(this.applicationConfig, key)) {
      return undefined;
    }
    return get(this.applicationConfig, key);
  }

  /*** Logging methods ***/
  info(message: string): void {
    this.logger.log(this, LogSeverity.Info, message);
  }

  warn(message: string): void {
    this.logger.log(this, LogSeverity.Warn, message);
  }

  log(message: string): void {
    this.logger.log(this, LogSeverity.Log, message);
  }

  error(message: string): void {
    this.logger.log(this, LogSeverity.Error, message);
  }

  debug(message: string): void {
    this.logger.log(this, LogSeverity.Debug, message);
  }
}
