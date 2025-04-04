import { TelemetrySignal } from './collector';
import { isLogSignal, isTraceSignal } from './';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-proto';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import type { ReadableLogRecord } from '@opentelemetry/sdk-logs';
import { ExportResult, ExportResultCode } from '@opentelemetry/core';

export interface OTLPExporterConfig {
  logsEndpoint?: string[];
  tracesEndpoint?: string[];
}

export interface ITelemetryExporter {
  export(signal: TelemetrySignal[]): Promise<void>;
  flush(): Promise<void>;
}

export class TelemetryExporter implements ITelemetryExporter {
  private readonly tracesExporters: OTLPTraceExporter[] = [];
  private readonly logsExporters: OTLPLogExporter[] = [];
  constructor(config: OTLPExporterConfig) {
    if (config.tracesEndpoint) {
      for (const endpoint of config.tracesEndpoint) {
        this.tracesExporters.push(
          new OTLPTraceExporter({
            url: endpoint,
          }),
        );
        console.log(`Traces will be exported to ${endpoint}`);
      }
    }
    if (config.logsEndpoint) {
      for (const endpoint of config.logsEndpoint) {
        this.logsExporters.push(
          new OTLPLogExporter({
            url: endpoint,
          }),
        );
        console.log(`Logs will be exported to ${endpoint}`);
      }
    }
  }

  async export(signals: TelemetrySignal[]): Promise<void> {
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
    const tasks: Promise<void>[] = [];
    // A short-lived app that exits before the callback of export() will lose its data.
    // We wrap these callbacks in promise objects to make sure we wait for them:
    if (exportSpans.length > 0 && this.tracesExporters) {
      const traceExportTask = new Promise<void>((resolve) => {
        const exportCallback = (results: ExportResult) => {
          if (results.code !== ExportResultCode.SUCCESS) {
            console.warn(`Trace export failed: ${results.code}`);
            console.warn(results);
          }
          resolve();
        };
        for (const exporter of this.tracesExporters) {
          exporter.export(exportSpans, exportCallback);
        }
      });
      tasks.push(traceExportTask);
    }
    if (exportLogs.length > 0 && this.logsExporters) {
      const logExportTask = new Promise<void>((resolve) => {
        const exportCallback = (results: ExportResult) => {
          if (results.code !== ExportResultCode.SUCCESS) {
            console.warn(`Log export failed: ${results.code}`);
            console.warn(results);
          }
          resolve();
        };
        for (const exporter of this.logsExporters) {
          exporter.export(exportLogs, exportCallback);
        }
      });
      tasks.push(logExportTask);
    }
    await Promise.all(tasks);
  }

  async flush() {
    for (const exporter of this.tracesExporters) {
      await exporter.forceFlush();
    }
    for (const exporter of this.logsExporters) {
      await exporter.forceFlush();
    }
  }
}
