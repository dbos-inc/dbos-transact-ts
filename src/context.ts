import { Span } from "@opentelemetry/sdk-trace-base";
import { IncomingMessage } from "http";
import { LogSeverity } from "./telemetry/signals";
import { has, get } from "lodash";
import { TemporaryLogger } from "./operon";

export interface OperonContext {
  request?: IncomingMessage;
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
  request?: IncomingMessage; // Raw incoming HTTP request.

  authenticatedUser: string = ""; ///< The user that has been authenticated
  authenticatedRoles: string[] = []; ///< All roles the user has according to authentication
  assumedRole: string = ""; ///< Role in use - that user has and provided authorization to current function

  workflowUUID: string = "";

  constructor(readonly operationName: string, readonly span: Span, private readonly logger: TemporaryLogger, parentCtx?: OperonContextImpl) {
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
  // TODO Format the message to add contextual information
  info(message: string): void {
    this.logger.info(message);
  }

  // TODO replace with logger.warn when we have our selected logger
  warn(message: string): void {
    this.logger.info(message);
  }

  // TODO replace with logger.log when we have our selected logger
  log(message: string): void {
    this.logger.info(message);
  }

  error(message: string): void {
    this.logger.error(message);
  }

  debug(message: string): void {
    this.logger.debug(message);
  }
}
