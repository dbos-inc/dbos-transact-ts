import { transports, createLogger, format, Logger as IWinstonLogger } from 'winston';
import TransportStream = require('winston-transport'); // eslint-disable-line @typescript-eslint/no-require-imports
import { Logger as OTelLogger, LogAttributes, SeverityNumber } from '@opentelemetry/api-logs';
import { LogRecord, LoggerProvider } from '@opentelemetry/sdk-logs';
import { TelemetryCollector } from './collector';
import { DBOSJSON, globalParams, interceptStreams } from '../utils';
import { LoggerConfig } from '../dbos-executor';
import { DBOSSpan } from './traces';

/*****************/
/* GLOBAL LOGGER */
/*****************/

type ContextualMetadata = {
  includeContextMetadata: boolean; // Should the console transport formatter include the context metadata?
  span?: DBOSSpan; // All context metadata should be attributes of the context's span
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
  private isLogging = false; // Prevent recursive logging
  private readonly otlpTransport?: OTLPLogQueueTransport;

  constructor(
    private readonly telemetryCollector?: TelemetryCollector,
    config?: LoggerConfig,
  ) {
    const winstonTransports: TransportStream[] = [];
    winstonTransports.push(
      new transports.Console({
        format: consoleFormat,
        level: config?.logLevel || 'info',
        silent: config?.silent || false,
        forceConsole: config?.forceConsole || false,
      }),
    );
    // Only enable the OTLP transport if we have a telemetry collector and an exporter
    if (this.telemetryCollector?.exporter) {
      this.otlpTransport = new OTLPLogQueueTransport(this.telemetryCollector, config?.logLevel || 'info');
      winstonTransports.push(this.otlpTransport);
    }
    this.logger = createLogger({ transports: winstonTransports });
    this.addContextMetadata = config?.addContextMetadata || false;

    if (process.env.DBOS__CAPTURE_STD !== 'false' && this.telemetryCollector?.exporter) {
      interceptStreams((msg, stream) => {
        if (stream === 'stdout') {
          if (!this.isLogging) {
            this.otlpTransport?.log({ level: 'info', message: msg.trim() }, () => {});
          }
        } else {
          if (!this.isLogging) {
            this.otlpTransport?.log({ level: 'error', message: msg.trim(), stack: new Error().stack }, () => {});
          }
        }
      });
    }
  }

  // We use this form of winston logging methods: `(message: string, ...meta: any[])`. See node_modules/winston/index.d.ts
  info(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.isLogging = true;
    if (typeof logEntry === 'string') {
      this.logger.info(logEntry, metadata);
    } else {
      this.logger.info(DBOSJSON.stringify(logEntry), metadata);
    }
    this.isLogging = false;
  }

  debug(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.isLogging = true;
    if (typeof logEntry === 'string') {
      this.logger.debug(logEntry, metadata);
    } else {
      this.logger.debug(DBOSJSON.stringify(logEntry), metadata);
    }
    this.isLogging = false;
  }

  warn(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.isLogging = true;
    if (typeof logEntry === 'string') {
      this.logger.warn(logEntry, metadata);
    } else {
      this.logger.warn(DBOSJSON.stringify(logEntry), metadata);
    }
    this.isLogging = false;
  }

  // metadata can have both ContextualMetadata and the error stack trace
  error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void {
    this.isLogging = true;
    if (inputError instanceof Error) {
      this.logger.error(inputError.message, { ...metadata, stack: inputError.stack });
    } else if (typeof inputError === 'string') {
      this.logger.error(inputError, { ...metadata, stack: new Error().stack });
    } else {
      this.logger.error(DBOSJSON.stringify(inputError), { ...metadata, stack: new Error().stack });
    }
    this.isLogging = false;
  }

  async destroy() {
    await this.telemetryCollector?.destroy();
  }
}

/******************/
/* CONTEXT LOGGER */
/******************/

export interface DLogger {
  info(logEntry: unknown, metadata?: ContextualMetadata): void;
  debug(logEntry: unknown, metadata?: ContextualMetadata): void;
  warn(logEntry: unknown, metadata?: ContextualMetadata): void;
  error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void;
}

export class DBOSContextualLogger implements DLogger {
  readonly includeContextMetadata: boolean;
  constructor(
    private readonly globalLogger: GlobalLogger,
    readonly ctx: () => DBOSSpan | undefined,
  ) {
    this.includeContextMetadata = this.globalLogger.addContextMetadata;
  }

  info(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.globalLogger.info(logEntry, {
      includeContextMetadata: this.includeContextMetadata,
      span: this.ctx(),
      ...metadata,
    });
  }

  debug(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.globalLogger.debug(logEntry, {
      includeContextMetadata: this.includeContextMetadata,
      span: this.ctx(),
      ...metadata,
    });
  }

  warn(logEntry: unknown, metadata?: ContextualMetadata): void {
    this.globalLogger.warn(logEntry, {
      includeContextMetadata: this.includeContextMetadata,
      span: this.ctx(),
      ...metadata,
    });
  }

  error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void {
    this.globalLogger.error(inputError, {
      includeContextMetadata: this.includeContextMetadata,
      span: this.ctx(),
      ...metadata,
    });
  }
}

/***********************/
/* FORMAT & TRANSPORTS */
/***********************/

export const consoleFormat = format.combine(
  format.errors({ stack: true }),
  format.timestamp(),
  format.colorize(),
  format.printf((info) => {
    const { timestamp, level, message, stack } = info;
    const applicationVersion = globalParams.appVersion;
    const ts = typeof timestamp === 'string' ? timestamp.slice(0, 19).replace('T', ' ') : undefined;
    const formattedStack = typeof stack === 'string' ? stack?.split('\n').slice(1).join('\n') : undefined;

    const messageString: string = typeof message === 'string' ? message : DBOSJSON.stringify(message);
    const fullMessageString = `${messageString}${info.includeContextMetadata ? ` ${DBOSJSON.stringify((info.span as DBOSSpan)?.attributes)}` : ''}`;

    const versionString = applicationVersion ? ` [version ${applicationVersion}]` : '';
    return `${ts}${versionString} [${level}]: ${fullMessageString} ${stack ? '\n' + formattedStack : ''}`;
  }),
);

class OTLPLogQueueTransport extends TransportStream {
  readonly name = 'OTLPLogQueueTransport';
  readonly otelLogger: OTelLogger;
  readonly applicationID: string;
  readonly executorID: string;

  constructor(
    readonly telemetryCollector: TelemetryCollector,
    logLevel: string,
  ) {
    super();
    this.level = logLevel;
    // not sure if we need a more explicit name here
    const loggerProvider = new LoggerProvider();
    this.otelLogger = loggerProvider.getLogger('default');
    this.applicationID = globalParams.appID;
    this.executorID = globalParams.executorID;
    const logRecordProcessor = {
      forceFlush: async () => {
        // no-op
      },
      onEmit(logRecord: LogRecord) {
        telemetryCollector.push(logRecord);
      },
      shutdown: async () => {
        // no-op
      },
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

    // Ideally we want to give the spanContext to the logRecord,
    // But there seems to some dependency bugs in opentelemetry-js
    // (span.getValue(SPAN_KEY) undefined when we pass the context, as commented bellow)
    // So for now we get the traceId and spanId directly from the context and pass them through the logRecord attributes
    this.otelLogger.emit({
      severityNumber: levelToSeverityNumber[level as string],
      severityText: level as string,
      body: message as string,
      timestamp: performance.now(), // So far I don't see a major difference between this and observedTimestamp
      observedTimestamp: performance.now(),
      attributes: {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
        ...span?.attributes,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
        traceId: span?.spanContext()?.traceId,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
        spanId: span?.spanContext()?.spanId,
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        stack,
        applicationID: this.applicationID,
        applicationVersion: globalParams.appVersion,
        executorID: this.executorID,
      } as LogAttributes,
    });

    callback();
  }
}
