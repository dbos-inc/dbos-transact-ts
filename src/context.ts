import { Span } from "@opentelemetry/sdk-trace-base";
import { IncomingMessage } from "http";
import { Logger } from "winston";
import { Logger as OperonLogger } from "./telemetry/logs";
import { has, get } from "lodash";

export interface OperonContext {
  request?: IncomingMessage;
  workflowUUID: string;
  authenticatedUser: string;

  span: Span;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getConfig(key: string): any;

  getLogger(): OperonLogger;
}

export class OperonContextImpl implements OperonContext {
  request?: IncomingMessage; // Raw incoming HTTP request.

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

  getContextInfo() {
    return {
      workflowUUID: this.workflowUUID,
      authenticatedUser: this.authenticatedUser,
      authenticatedRoles: this.authenticatedRoles,
      assumedRole: this.assumedRole,
    };
  }

  getLogger(): OperonLogger {
    return new OperonLogger(this.logger);
  }
}
