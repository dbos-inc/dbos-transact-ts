import { Span } from "@opentelemetry/sdk-trace-base";
import { WinstonLogger as Logger, Logger as OperonLogger } from "./telemetry/logs";
import { has, get } from "lodash";
import { IncomingHttpHeaders } from "http";
import { ParsedUrlQuery } from "querystring";
import { UserDatabase, UserDatabaseName } from "./user_database";
import { Operon } from "./operon";

// Operon request includes useful information from http.IncomingMessage and parsed body, URL parameters, and parsed query string.
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

export interface InitContext extends OperonContext {
  readonly db: UserDatabase;
  
}

export class InitContextImpl extends OperonContextImpl implements InitContext {
  
  constructor(readonly db:UserDatabase, readonly operon: Operon) {
    super("",operon.tracer.startSpan("init") , operon.logger) ;
  }

}
