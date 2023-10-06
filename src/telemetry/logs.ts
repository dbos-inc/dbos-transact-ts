import { Logger as winstonLogger } from "winston";
import { OperonContext } from "../context";

/* This is a wrapper around a global Winston logger
 * It allows us to embed contextual information into the logs
 **/
export class Logger {
  constructor(private readonly globalLogger: winstonLogger) {}

  // Eventually we this object will implement one of our TelemetrySignal interface
  formatMessage(message: string, ctx?: OperonContext): Object {
    if (ctx) {
      return {
        message,
        workflowUUID: ctx.workflowUUID,
        authenticatedUser: ctx.authenticatedUser,
        traceId: ctx.span.spanContext().traceId,
        spanId: ctx.span.spanContext().spanId,
      };
    } else {
      return {
        message,
      };
    }
  }

  info(message: string, ctx?: OperonContext): void {
    const msg = this.formatMessage(message, ctx);
    this.globalLogger.info(msg);
  }

  debug(message: string, ctx?: OperonContext): void {
    const msg = this.formatMessage(message, ctx);
    this.globalLogger.debug(msg);
  }

  warn(message: string, ctx?: OperonContext): void {
    const msg = this.formatMessage(message, ctx);
    this.globalLogger.warn(msg);
  }

  emerg(message: string, ctx?: OperonContext): void {
    const msg = this.formatMessage(message, ctx);
    this.globalLogger.emerg(msg);
  }

  alert(message: string, ctx?: OperonContext): void {
    const msg = this.formatMessage(message, ctx);
    this.globalLogger.alert(msg);
  }

  crit(message: string, ctx?: OperonContext): void {
    const msg = this.formatMessage(message, ctx);
    this.globalLogger.crit(msg);
  }

  error(message: string, ctx?: OperonContext): void {
    const msg = this.formatMessage(message, ctx);
    this.globalLogger.error(msg);
  }
}
