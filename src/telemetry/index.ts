import { LoggerConfig } from "./logs";
import { OTLPExporterConfig } from "./exporters";
import { Span } from "@opentelemetry/sdk-trace-base";
import { LogRecord } from "@opentelemetry/api-logs";
import { TelemetrySignal } from "./collector";

export function isTraceSignal(signal: TelemetrySignal): signal is Span {
  // Span is an interface that has a property 'kind'
  return 'kind' in signal;
}

export function isLogSignal(signal: TelemetrySignal): signal is LogRecord {
  // LogRecord is an interface that has a property 'severityText' and 'severityNumber'
  return 'severityText' in signal && 'severityNumber' in signal;
}

export interface TelemetryConfig {
  logs?: LoggerConfig;
  OTLPExporter?: OTLPExporterConfig;
}
