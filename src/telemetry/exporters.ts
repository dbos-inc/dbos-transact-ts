import { TelemetrySignal } from "./collector";
import { isLogSignal, isTraceSignal } from "./";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPLogExporter } from "@opentelemetry/exporter-logs-otlp-http";
import type { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import type { ReadableLogRecord } from '@opentelemetry/sdk-logs';
import { ExportResult, ExportResultCode } from "@opentelemetry/core";

export interface OTLPExporterConfig {
  endpoint: string;
  exportTraces: boolean;
  exportLogs: boolean;
}

export interface ITelemetryExporter<T, U> {
  export(signal: TelemetrySignal[]): Promise<T>;
  process?(signal: TelemetrySignal[]): U;
}

export class TelemetryExporter implements ITelemetryExporter<void, undefined> {
  private readonly tracesExporter?: OTLPTraceExporter;
  private readonly logsExporter?: OTLPLogExporter;
  constructor(config: OTLPExporterConfig) {
    if (config.exportTraces) {
      this.tracesExporter = new OTLPTraceExporter({
        url: config.endpoint,
      });
    }
    if (config.exportLogs) {
      this.logsExporter = new OTLPLogExporter({
        url: config.endpoint,
      });
    }
  }

  async export(signals: TelemetrySignal[]): Promise<void> {
    return await new Promise<void>((resolve) => {
      // Sort out traces and logs
      const exportSpans: ReadableSpan[] = [];
      const exportLogs: ReadableLogRecord[] = [];
      signals.forEach((signal) => {
        if (isTraceSignal(signal)) {
          exportSpans.push(signal as ReadableSpan);
        }
        if (isLogSignal(signal)) {
          exportLogs.push(signal as ReadableLogRecord);
        }
      });

      if (exportSpans.length > 0 && this.tracesExporter) {
        this.tracesExporter.export(exportSpans, (results: ExportResult) => {
          if (results.code !== ExportResultCode.SUCCESS) {
            // TODO log a proper error
            console.warn(`Trace export failed`);
          }
        });
      }

      if (exportLogs.length > 0 && this.logsExporter) {
        this.logsExporter.export(exportLogs, (results: ExportResult) => {
          if (results.code !== ExportResultCode.SUCCESS) {
            // TODO log a proper error
            console.warn(`Log export failed`);
          }
        });
      }

      resolve();
    });
  }
}

