import { Logger as winstonLogger } from "winston";
import { OperonContext } from "../context";

/* This is a wrapper around a global Winston logger
 * It allows us to embed contextual information into the logs
 **/
export class Logger {
  constructor(private readonly globalLogger: winstonLogger, private readonly ctx: OperonContext) {}

  // Eventually we this object will implement one of our TelemetrySignal interface
  formatContextInfo(): object {
    return {
      workflowUUID: this.ctx.workflowUUID,
      authenticatedUser: this.ctx.authenticatedUser,
      traceId: this.ctx.span.spanContext().traceId,
      spanId: this.ctx.span.spanContext().spanId,
    };
  }

  info(message: string, ctx: boolean = false): void {
    this.globalLogger.info(message, ctx ? this.formatContextInfo() : {});
  }

  debug(message: string, ctx: boolean = false): void {
    this.globalLogger.debug(message, ctx ? this.formatContextInfo() : {});
  }

  warn(message: string, ctx: boolean = false): void {
    this.globalLogger.warn(message, ctx ? this.formatContextInfo() : {});
  }

  emerg(message: string, ctx: boolean = false): void {
    this.globalLogger.emerg(message, ctx ? this.formatContextInfo() : {});
  }

  alert(message: string, ctx: boolean = false): void {
    this.globalLogger.alert(message, ctx ? this.formatContextInfo() : {});
  }

  crit(message: string, ctx: boolean = false): void {
    this.globalLogger.crit(message, ctx ? this.formatContextInfo() : {});
  }

  error(message: string, ctx: boolean = false): void {
    this.globalLogger.error(message, ctx ? this.formatContextInfo() : {});
  }
}
