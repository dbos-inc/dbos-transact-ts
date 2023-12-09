import { LoggerConfig } from "./logs";
import { OTLPExporterConfig } from "./exporters";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { LogRecord } from "@opentelemetry/api-logs";

// We could implement our own types and avoid having `any`, but this has likely little value
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isTraceSignal(signal: any): signal is ReadableSpan {
  // ReadableSpan is an interface that has a property 'kind'
  // eslint-disable-next-line @typescript-eslint/no-unsafe-return
  return signal && 'kind' in signal;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isLogSignal(signal: any): signal is LogRecord {
  // LogRecord is an interface that has a property 'severityText' and 'severityNumber'
  // eslint-disable-next-line @typescript-eslint/no-unsafe-return
  return signal && 'severityText' in signal && 'severityNumber' in signal;
}

export interface TelemetryConfig {
  logs?: LoggerConfig;
  OTLPExporters?: OTLPExporterConfig[];
}
