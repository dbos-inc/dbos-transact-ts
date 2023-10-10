import { transports, createLogger, format, Logger as WinstonLogger } from "winston";
import { OperonContext } from "../context";

/* This is a wrapper around a global Winston logger
 * It allows us to embed contextual information into the logs
 **/
export class Logger {
  constructor(private readonly globalLogger: WinstonLogger, private readonly ctx: OperonContext) {}

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

  // We give users the same interface (message: string argument) but create an error to get a stack trace
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error(inputError: any, ctx: boolean = false): void {
    if (inputError instanceof Error) {
      inputError.message = `${inputError.message}${ctx ? " " + JSON.stringify(this.formatContextInfo()) : ""}`;
      this.globalLogger.error(inputError);
    } else if (typeof inputError === "string") {
      const message = new Error(`${inputError}${ctx ? " " + JSON.stringify(this.formatContextInfo()) : ""}`);
      this.globalLogger.error(message);
    } else {
      // If this is neither a string nor an error, we just log it as is an ommit the context
      this.globalLogger.error(inputError);
    }
  }
}

export function createGlobalLogger(logLevel: string, silent: boolean = false): WinstonLogger {
  return createLogger({
    format: consoleFormat,
    transports: [
      new transports.Console({
        level: logLevel,
      }),
    ],
    handleExceptions: true,
    silent,
  });
}

const consoleFormat = format.combine(
  format.errors({ stack: true }),
  format.timestamp(),
  format.colorize(),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  format.printf((info) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const { timestamp, level, message, stack, ...args } = info;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const ts = timestamp.slice(0, 19).replace("T", " ");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const formattedStack = stack?.split("\n").slice(1).join("\n");
    const messageString: string = typeof message === "string" ? message : JSON.stringify(message);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return `${ts} [${level}]: ${messageString} ${Object.keys(args).length ? "\n" + JSON.stringify(args, null, 2) : ""} ${stack ? "\n" + formattedStack : ""}`;
  })
);
