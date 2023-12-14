import { transports, createLogger, format, Logger as IWinstonLogger } from "winston";
import TransportStream = require("winston-transport");
import { getApplicationVersion } from "../dbos-runtime/applicationVersion";
import { DBOSContextImpl } from "../context";
import { context } from "@opentelemetry/api";
import { Logger as OTelLogger, LogAttributes, SeverityNumber } from "@opentelemetry/api-logs";
import { LogRecord, LoggerProvider} from "@opentelemetry/sdk-logs";
import { Span } from "@opentelemetry/sdk-trace-base";
import { TelemetryCollector } from "./collector";

/*****************/
/* GLOBAL LOGGER */
/*****************/

export interface LoggerConfig {
  logLevel?: string;
  silent?: boolean;
  addContextMetadata?: boolean;
}

// This structure is mostly used to share contextual metadata with the console logger
// The span field is used by the OTLP transport for injection in the LogRecord. It allows us to tightly link logs and traces
type ContextualMetadata = {
  includeContextMetadata?: boolean;
  executorID?: string;
  operationUUID?: string;
  authenticatedUser?: string;
  authenticatedRoles?: string[];
  assumedRole?: string;
  span?: Span;
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
    // Only enable the OTLP transport if we have a telemetry collector and an exporter
    if (this.telemetryCollector?.exporter) {
      winstonTransports.push(new OTLPLogQueueTransport(this.telemetryCollector));
    }
    this.logger = createLogger({ transports: winstonTransports });
    this.addContextMetadata = config?.addContextMetadata || false;
  }

  // We use this form of winston logging methods: `(message: string, ...meta: any[])`. See node_modules/winston/index.d.ts
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  info(logEntry: any, metadata?: ContextualMetadata): void {
    if (typeof logEntry === "string") {
      this.logger.info(logEntry, metadata);
    } else {
      this.logger.info(JSON.stringify(logEntry), metadata);
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  debug(logEntry: any, metadata?: ContextualMetadata): void {
    if (typeof logEntry === "string") {
      this.logger.debug(logEntry, metadata);
    } else {
      this.logger.debug(JSON.stringify(logEntry), metadata);
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  warn(logEntry: any, metadata?: ContextualMetadata): void {
    if (typeof logEntry === "string") {
      this.logger.warn(logEntry, metadata);
    } else {
      this.logger.warn(JSON.stringify(logEntry), metadata);
    }
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
    private readonly ctx: DBOSContextImpl,
  ) {
    this.metadata = {
      span: ctx.span,
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  info(logEntry: any): void {
    if (typeof logEntry === "string") {
      this.globalLogger.info(logEntry, this.metadata);
    } else {
      this.globalLogger.info(JSON.stringify(logEntry), this.metadata);
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  debug(logEntry: any): void {
    if (typeof logEntry === "string") {
      this.globalLogger.debug(logEntry, this.metadata);
    } else {
      this.globalLogger.debug(JSON.stringify(logEntry), this.metadata);
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  warn(logEntry: any): void {
    if (typeof logEntry === "string") {
      this.globalLogger.warn(logEntry, this.metadata);
    } else {
      this.globalLogger.warn(JSON.stringify(logEntry), this.metadata);
    }
  }

  // We give users the same interface (message: string argument) but create an error to get a stack trace
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error(inputError: any): void {
    if (inputError instanceof Error) {
      this.globalLogger.error(inputError.message, { ...this.metadata, stack: inputError.stack });
    } else if (typeof inputError === "string") {
      this.globalLogger.error(inputError, { ...this.metadata, stack: new Error().stack });
    } else {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      this.globalLogger.error(JSON.stringify(inputError), { ...this.metadata, stack: new Error().stack });
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
      executorID: info.executorID as string,
      operationUUID: info.workflowUUID as string,
      authenticatedUser: info.authenticatedUser as string,
      authenticatedRoles: info.authenticatedRoles as string[],
      assumedRole: info.assumedRole as string,
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

  constructor(readonly telemetryCollector: TelemetryCollector) {
    super();
    // not sure if we need a more explicit name here
    const loggerProvider = new LoggerProvider();
    this.otelLogger = loggerProvider.getLogger("default");
    const logRecordProcessor = {
      forceFlush: async () => {
        // no-op
      },
      onEmit(logRecord: LogRecord) { // Use optionakl coàntext?
        telemetryCollector.push(logRecord);
      },
      shutdown: async () => {
        // no-op
      }
    };
    loggerProvider.addLogRecordProcessor(logRecordProcessor);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  log(info: any, callback: () => void): void {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const { level, message, stack, span } = info;

    const levelToSeverityNumber: { [key: string]: SeverityNumber } = {
      error: SeverityNumber.ERROR,
      warn: SeverityNumber.WARN,
      info: SeverityNumber.INFO,
      debug: SeverityNumber.DEBUG,
    };

    this.otelLogger.emit({
      severityNumber: levelToSeverityNumber[level as string],
      severityText: level as string,
      body: message as string,
      timestamp: new Date().getTime(), // So far I don't see a major difference between this and observedTimestamp
      observedTimestamp: new Date().getTime(),
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      // TODO: eventually we want to see span defined, always
      attributes: { ...span?.attributes, stack } as LogAttributes,
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
      context: span?.spanContext() || context.active(),
    });

    callback();
  }
}
