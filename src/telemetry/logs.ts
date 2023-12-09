import { transports, createLogger, format, Logger as IWinstonLogger } from "winston";
import TransportStream = require("winston-transport");
import { getApplicationVersion } from "../dbos-runtime/applicationVersion";
import { DBOSContext } from "../context";
import { LogRecord, SeverityNumber } from "@opentelemetry/api-logs";

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
  includeContextMetadata: boolean;
  workflowUUID: string;
  authenticatedUser: string;
  traceId: string;
  spanId: string;
  [key: string]: string | boolean | undefined;
};

/* This is a wrapper around a global Winston logger. It holds a reference to the global logger.
 * This class is expected to be instantiated by new DBOSContext such that they can share context information.
 **/
export class Logger {
  readonly metadata: ContextualMetadata;
  constructor(
    private readonly globalLogger: WinstonLogger,
    private readonly ctx: DBOSContext
  ) {
    this.metadata = {
      includeContextMetadata: this.globalLogger.addContextMetadata,
      workflowUUID: this.ctx.workflowUUID,
      authenticatedUser: this.ctx.authenticatedUser,
      traceId: this.ctx.span.spanContext().traceId,
      spanId: this.ctx.span.spanContext().spanId,
    };
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
      new OTLPLogQueueTransport(
      ),
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
    const { timestamp, level, message, stack, includeContextMetadata, workflowUUID, authenticatedUser, traceId, spanId } = info;
    const applicationVersion = getApplicationVersion();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const ts = timestamp.slice(0, 19).replace("T", " ");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const formattedStack = stack?.split("\n").slice(1).join("\n");

    const messageString: string = typeof message === "string" ? message : JSON.stringify(message);
    const contextualMetadata = { workflowUUID, authenticatedUser, traceId, spanId };
    const fullMessageString = `${messageString}${includeContextMetadata ? ` ${JSON.stringify(contextualMetadata)}` : ""}`;

    const versionString = applicationVersion ? ` [version ${applicationVersion}]` : "";
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return `${ts}${versionString} [${level}]: ${fullMessageString} ${stack ? "\n" + formattedStack : ""}`;
  })
);

class OTLPLogQueueTransport extends TransportStream {
  readonly name = "OTLPLogQueueTransport";

  log(info: any, callback: () => void): void {
    const { level, message, stack, workflowUUID, authenticatedUser, traceId, spanId } = info;

    const levelToSeverityNumber: { [key: string]: SeverityNumber } = {
      error: SeverityNumber.ERROR,
      warn: SeverityNumber.WARN,
      info: SeverityNumber.INFO,
      debug: SeverityNumber.DEBUG,
    };

    const logRecord: LogRecord = {
      severityNumber: levelToSeverityNumber[level],
      severityText: level,
      body: message,
      timestamp: new Date().getTime(),
      attributes: { workflowUUID, authenticatedUser, traceId, spanId, stack },
    };

    console.log(logRecord);

    callback();
  }
}
