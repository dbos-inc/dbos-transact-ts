import { transports, createLogger, format, Logger as IWinstonLogger } from "winston";
import { getApplicationVersion } from "../dbos-runtime/applicationVersion";
import { DBOSContext } from "../context";

export interface LoggerConfig {
  logLevel?: string;
  silent?: boolean;
  addContextMetadata?: boolean;
}

// A simple extension of Winston adding a flag for the addition of context metadata to log entries.
export interface WinstonLogger extends IWinstonLogger {
  addContextMetadata: boolean;
}

type ContextualMetadata = {
  workflowUUID: string;
  authenticatedUser: string;
  traceId: string;
  spanId: string;
  [key: string]: string | undefined; // Adding an index signature
};

/* This is a wrapper around a global Winston logger. It holds a reference to the global logger.
 * This class is expected to be instantiated by new DBOSContext such that they can share context information.
 **/
export class Logger {
  readonly metadata?: ContextualMetadata;
  constructor(
    private readonly globalLogger: WinstonLogger,
    private readonly ctx: DBOSContext
  ) {
    if (this.globalLogger.addContextMetadata) {
      this.metadata = {
        workflowUUID: this.ctx.workflowUUID,
        authenticatedUser: this.ctx.authenticatedUser,
        traceId: this.ctx.span.spanContext().traceId,
        spanId: this.ctx.span.spanContext().spanId,
      };
    }
  }

  info(message: string): void {
    this.globalLogger.info(message, this.metadata);
  }

  debug(message: string): void {
    this.globalLogger.debug(message, this.metadata);
  }

  warn(message: string): void {
    this.globalLogger.warn(message, this.metadata);
  }

  // We give users the same interface (message: string argument) but create an error to get a stack trace
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error(inputError: any): void {
    if (inputError instanceof Error) {
      this.globalLogger.error(inputError.message, { ...this.metadata, stack: inputError.stack, cause: inputError.cause });
    } else if (typeof inputError === "string") {
      const e = new Error();
      this.globalLogger.error(inputError, { ...this.metadata, stack: e.stack });
    } else {
      // If this is neither a string nor an error, we just log it as is an ommit the context
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      this.globalLogger.error(inputError.toString());
    }
  }
}

export function createGlobalLogger(config?: LoggerConfig): WinstonLogger {
  const logger: WinstonLogger = createLogger({
    transports: [
      new transports.Console({
        format: consoleFormat,
        level: config?.logLevel || "info",
        silent: config?.silent || false,
      }),
    ],
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
    const { timestamp, level, message, stack, workflowUUID, authenticatedUser, traceId, spanId } = info;
    const applicationVersion = getApplicationVersion();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const ts = timestamp.slice(0, 19).replace("T", " ");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const formattedStack = stack?.split("\n").slice(1).join("\n");
    // If we have context metadata, we add it to the messageString
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment
    const contextualMetadata: ContextualMetadata = { workflowUUID, authenticatedUser, traceId, spanId };
    const contextualMetadataString = Object.keys(contextualMetadata).reduce((acc, key) => {
      const value = contextualMetadata[key];
      return value !== undefined && value !== null ? `${acc}, "${key}": "${value}"` : acc;
    }, "");
    const messageString: string = typeof message === "string" ? message : JSON.stringify(message);
    const fullMessageString = `${messageString}${contextualMetadataString ? ` {${contextualMetadataString.substring(2)}}` : ""}`;

    const versionString = applicationVersion ? ` [version ${applicationVersion}]` : "";
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return `${ts}${versionString} [${level}]: ${fullMessageString} ${stack ? "\n" + formattedStack : ""}`;
  })
);
