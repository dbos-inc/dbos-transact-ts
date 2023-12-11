import { transports, createLogger, format, Logger as IWinstonLogger } from "winston";
import TransportStream = require("winston-transport");
import { getApplicationVersion } from "../dbos-runtime/applicationVersion";
import { DBOSContext } from "../context";
import { context } from "@opentelemetry/api";
import { LogAttributes, LogRecord, SeverityNumber } from "@opentelemetry/api-logs";
import { Logger as OTelLogger, LogRecord as OTelLogRecord, LoggerProvider as OTelLoggerProvider } from "@opentelemetry/sdk-logs";
import { TelemetryCollector } from "./collector";

/*****************/
/* GLOBAL LOGGER */
/*****************/

export interface LoggerConfig {
  logLevel?: string;
  silent?: boolean;
  addContextMetadata?: boolean;
}

type ContextualMetadata = {
  includeContextMetadata?: boolean;
  workflowUUID: string;
  authenticatedUser: string;
  traceId: string;
  spanId: string;
};

interface StackTrace {
  stack?: string;
}

// Wrap around the winston logger to support configuration and access to our telemetry collector
export interface IGlobalLogger extends IWinstonLogger {
  readonly addContextMetadata: boolean;
  readonly logger: IWinstonLogger;
  readonly telemetryCollector: TelemetryCollector;
}

export class GlobalLogger {
  private readonly logger: IWinstonLogger;
  readonly addContextMetadata: boolean;

  constructor(
    private readonly telemetryCollector?: TelemetryCollector,
    config?: LoggerConfig
  ) {
    const winstonTransports: TransportStream[] = [];
    winstonTransports.push(
      new transports.Console({
        format: consoleFormat,
        level: config?.logLevel || "info",
        silent: config?.silent || false,
      })
    );
    // Only enable the OTLP transport if we have a telemetry collector and it has exporters
    if (this.telemetryCollector && this.telemetryCollector.exporters.length > 0) {
      winstonTransports.push(new OTLPLogQueueTransport(this.telemetryCollector));
    }
    this.logger = createLogger({ transports: winstonTransports });
    this.addContextMetadata = config?.addContextMetadata || false;
  }

  info(message: string, metadata?: ContextualMetadata): void {
    this.logger.info(message, metadata);
  }

  debug(message: string, metadata?: ContextualMetadata): void {
    this.logger.debug(message, metadata);
  }

  warn(message: string, metadata?: ContextualMetadata): void {
    this.logger.warn(message, metadata);
  }

  // metadata can have both ContextualMetadata and the error stack trace
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error(inputError: any, metadata?: ContextualMetadata & StackTrace): void {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    this.logger.error(inputError, metadata);
  }
}

/******************/
/* CONTEXT LOGGER */
/******************/

// Wrapper around our global logger. Expected to be instantiated by a new contexts so they can inject contextual metadata
export class Logger {
  readonly metadata: ContextualMetadata;
  constructor(
    private readonly globalLogger: GlobalLogger,
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
      this.globalLogger.error(inputError.message, { ...this.metadata, stack: inputError.stack });
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

/***********************/
/* FORMAT & TRANSPORTS */
/***********************/

const consoleFormat = format.combine(
  format.errors({ stack: true }),
  format.timestamp(),
  format.colorize(),
  format.printf((info) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const { timestamp, level, message, stack } = info;
    const applicationVersion = getApplicationVersion();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const ts = timestamp.slice(0, 19).replace("T", " ");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
    const formattedStack = stack?.split("\n").slice(1).join("\n");

    const contextualMetadata: ContextualMetadata = {
      workflowUUID: info.workflowUUID as string,
      authenticatedUser: info.authenticatedUser as string,
      traceId: info.traceId as string,
      spanId: info.spanId as string,
    };
    const messageString: string = typeof message === "string" ? message : JSON.stringify(message);
    const fullMessageString = `${messageString}${info.includeContextMetadata ? ` ${JSON.stringify(contextualMetadata)}` : ""}`;

    const versionString = applicationVersion ? ` [version ${applicationVersion}]` : "";
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    return `${ts}${versionString} [${level}]: ${fullMessageString} ${stack ? "\n" + formattedStack : ""}`;
  })
);

class OTLPLogQueueTransport extends TransportStream {
  readonly name = "OTLPLogQueueTransport";
  readonly otelLogger: OTelLogger;

  constructor(private readonly telemetryCollector: TelemetryCollector) {
    super();
    // not sure if we need a more explicit name here
    this.otelLogger = new OTelLoggerProvider().getLogger("default") as OTelLogger;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  log(info: any, callback: () => void): void {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const { level, message, stack } = info;

    const contextualMetadata: ContextualMetadata = {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      workflowUUID: info.workflowUUID as string,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      authenticatedUser: info.authenticatedUser as string,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      traceId: info.traceId as string,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      spanId: info.spanId as string,
    };

    const levelToSeverityNumber: { [key: string]: SeverityNumber } = {
      error: SeverityNumber.ERROR,
      warn: SeverityNumber.WARN,
      info: SeverityNumber.INFO,
      debug: SeverityNumber.DEBUG,
    };

    const log: LogRecord = {
      severityNumber: levelToSeverityNumber[level as string],
      severityText: level as string,
      body: message as string,
      timestamp: new Date().getTime(),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      attributes: { ...contextualMetadata, stack } as LogAttributes,
      // TODO as a nice-to-have, we could retrieve the operation current context, if we use a context manager, and inject the traceId/spanId in the LogRecord
      // See https://opentelemetry.io/docs/instrumentation/js/context/#active-context
      context: context.active(),
    };

    const logRecord: OTelLogRecord = new OTelLogRecord(this.otelLogger, log);

    this.telemetryCollector.push(logRecord);

    callback();
  }
}
