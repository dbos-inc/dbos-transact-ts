import { Span } from "@opentelemetry/sdk-trace-base";
import { IncomingMessage } from "http";
import { Logger } from "./telemetry/logs";
import { LogSeverity } from "./telemetry/signals";
import { has, get } from "lodash";

export class OperonContext {
  request?: IncomingMessage; // Raw incoming HTTP request.

  authenticatedUser: string = ""; ///< The user that has been authenticated
  authenticatedRoles: string[] = []; ///< All roles the user has according to authentication
  assumedRole: string = ""; ///< Role in use - that user has and provided authorization to current function

  workflowUUID: string = "";

  constructor(readonly operationName: string, readonly span: Span, private readonly logger: Logger, parentCtx?: OperonContext) {
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
  applicationConfig?: any; // applicationConfiguration
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
