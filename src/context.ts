import { Span } from "@opentelemetry/sdk-trace-base";
import { Logger } from "winston";
import { Logger as OperonLogger } from "./telemetry/logs";
import { has, get } from "lodash";
import { IncomingHttpHeaders } from "http";
import { ParsedUrlQuery } from "querystring";

// Operon request includes useful information from http.IncomingMessage and parsed body, URL parameters, and parsed query string.
export interface HTTPRequest {
  headers?: IncomingHttpHeaders;  // A node's http.IncomingHttpHeaders object.
  rawHeaders?: string[];          // Raw headers.
  params?: unknown;               // Parsed path parameters from the URL.
  body?: unknown;                 // parsed HTTP body as an object.
  rawBody?: string;               // Unparsed raw HTTP body string.
  query?: ParsedUrlQuery;         // Parsed query string.
  querystring?: string;           // Unparsed raw query string.
  url?: string;                   // Request URL.
  ip?: string;                    // Request remote address.
}

export interface OperonContext {
  readonly request: HTTPRequest;
  readonly workflowUUID: string;
  readonly authenticatedUser: string;

  readonly logger: OperonLogger;
  readonly span: Span;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getConfig(key: string): any;
}

export class OperonContextImpl implements OperonContext {
  request: HTTPRequest = {}; // Raw incoming HTTP request.

  authenticatedUser: string = ""; ///< The user that has been authenticated
  authenticatedRoles: string[] = []; ///< All roles the user has according to authentication
  assumedRole: string = ""; ///< Role in use - that user has and provided authorization to current function

  workflowUUID: string = "";
  readonly logger: OperonLogger;

  constructor(readonly operationName: string, readonly span: Span, logger: Logger, parentCtx?: OperonContextImpl) {
    this.logger = new OperonLogger(logger, this);
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
}
