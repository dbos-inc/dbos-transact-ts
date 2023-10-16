import { transports, createLogger, format, Logger as IWinstonLogger } from "winston";
import { OperonContext } from "../context";

export interface LoggerConfig {
  logLevel?: string;
  silent?: boolean;
  addContextMetadata?: boolean;
}

// A simple extension of Winston adding a flag for the addition of context metadata to log entries.
export interface WinstonLogger extends IWinstonLogger {
  addContextMetadata: boolean;
}

/* This is a wrapper around a global Winston logger. It holds a reference to the global logger.
 * This class is expected to be instantiated by new OperonContext such that they can share context information.
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

  info(message: string): void {
    this.globalLogger.info(message, this.globalLogger.addContextMetadata ? this.formatContextInfo() : {});
  }

  debug(message: string): void {
    this.globalLogger.debug(message, this.globalLogger.addContextMetadata ? this.formatContextInfo() : {});
  }

  warn(message: string): void {
    this.globalLogger.warn(message, this.globalLogger.addContextMetadata ? this.formatContextInfo() : {});
  }

  emerg(message: string): void {
    this.globalLogger.emerg(message, this.globalLogger.addContextMetadata ? this.formatContextInfo() : {});
  }

  alert(message: string): void {
    this.globalLogger.alert(message, this.globalLogger.addContextMetadata ? this.formatContextInfo() : {});
  }

  crit(message: string): void {
    this.globalLogger.crit(message, this.globalLogger.addContextMetadata ? this.formatContextInfo() : {});
  }

  // We give users the same interface (message: string argument) but create an error to get a stack trace
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error(inputError: any): void {
    const contextInfo = this.globalLogger.addContextMetadata ? " " + JSON.stringify(this.formatContextInfo()) : ""
    if (inputError instanceof Error) {
      inputError.message = `${inputError.message}${contextInfo}`;
      this.globalLogger.error(inputError);
    } else if (typeof inputError === "string") {
      const message = new Error(`${inputError}${contextInfo}`);
      this.globalLogger.error(message);
    } else {
      // If this is neither a string nor an error, we just log it as is an ommit the context
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      this.globalLogger.error(inputError.toString());
    }
  }
}

export function createGlobalLogger(config?: LoggerConfig): WinstonLogger {
  const logger: WinstonLogger = createLogger({
    format: consoleFormat,
    transports: [
      new transports.Console({
        level: config?.logLevel || "info",
      }),
    ],
    silent: config?.silent || false,
  }) as WinstonLogger;
  logger.addContextMetadata = config?.addContextMetadata || false;
  return logger;
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
