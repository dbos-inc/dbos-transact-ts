import { transports, createLogger, format, Logger as IWinstonLogger } from "winston";
import TransportStream = require("winston-transport");
import { getApplicationVersion } from "../dbos-runtime/applicationVersion";
import { DBOSContext } from "../context";
import { LogRecord, SeverityNumber } from "@opentelemetry/api-logs";
import { TelemetryCollector } from "./collector";
import { TelemetrySignal } from "./signals";

/*****************/
/* GLOBAL LOGGER */
/*****************/

export interface LoggerConfig {
  logLevel?: string;
  silent?: boolean;
  addContextMetadata?: boolean;
}

type ContextualMetadata = {
  includeContextMetadata: boolean;
  workflowUUID: string;
  authenticatedUser: string;
  traceId: string;
  spanId: string;
  [key: string]: string | boolean | undefined;
};

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
    // TODO Buidl transports array selectively
    // If config says logger should be silent, just don't add it
    // It config asks for OTLP, add it
    this.logger = createLogger({
      transports: [
        new transports.Console({
          format: consoleFormat,
          level: config?.logLevel || "info",
          silent: config?.silent || false,
        }),
        new OTLPLogQueueTransport(this.telemetryCollector),
      ],
    }) as IWinstonLogger;

    this.addContextMetadata = config?.addContextMetadata || false;
  }

  info(message: string, metadata?: { [key: string]: string | boolean | undefined }): void {
    this.logger.info(message, metadata);
  }

  debug(message: string, metadata?: { [key: string]: string | boolean | undefined }): void {
    this.logger.debug(message, metadata);
  }

  warn(message: string, metadata?: { [key: string]: string | boolean | undefined }): void {
    this.logger.warn(message, metadata);
  }

  error(inputError: any, metadata?: { [key: string]: string | boolean | undefined }): void {
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

  constructor(private readonly telemetryCollector?: TelemetryCollector) {
    super();
  }

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

    // TODO refactor TelemetrySignal types
    const temporarySignal: TelemetrySignal = {
      workflowUUID,
      operationName: "log",
      runAs: authenticatedUser,
      timestamp: new Date().getTime(),
    };
    this.telemetryCollector?.push(temporarySignal);

    callback();
  }
}
