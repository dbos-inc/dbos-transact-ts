/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-require-imports */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import type { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
import type { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-proto';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base';
import type { ReadableLogRecord } from '@opentelemetry/sdk-logs';
import type { ExportResult } from '@opentelemetry/core';
import type { Span } from '@opentelemetry/sdk-trace-base';
import type { LogRecord } from '@opentelemetry/api-logs';
import { OTLPExporterConfig } from '../dbos-executor';
import { globalParams } from '../utils';

// As DBOS OTLP is optional, OTLP objects must only be dynamically imported
// and only when OTLP is enabled. Importing OTLP types is fine as long
// as signatures using those types are not exported.

export interface ITelemetryExporter {
  export(signal: object[]): Promise<void>;
  flush(): Promise<void>;
}

function isTraceSignal(signal: object): signal is Span {
  // Span is an interface that has a property 'kind'
  return 'kind' in signal;
}

function isLogSignal(signal: object): signal is LogRecord {
  // LogRecord is an interface that has a property 'severityText' and 'severityNumber'
  return 'severityText' in signal && 'severityNumber' in signal;
}

export class TelemetryExporter implements ITelemetryExporter {
  private readonly tracesExporters: OTLPTraceExporter[] = [];
  private readonly logsExporters: OTLPLogExporter[] = [];

  constructor(config: OTLPExporterConfig) {
    if (!globalParams.enableOTLP) {
      return;
    }
    const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-proto');
    const { OTLPLogExporter } = require('@opentelemetry/exporter-logs-otlp-proto');

    const tracesSet = new Set<string>(config.tracesEndpoint);
    for (const endpoint of tracesSet) {
      this.tracesExporters.push(
        new OTLPTraceExporter({
          url: endpoint,
        }),
      );
      console.log(`Traces will be exported to ${endpoint}`);
    }

    const logsSet = new Set<string>(config.logsEndpoint);
    for (const endpoint of logsSet) {
      this.logsExporters.push(
        new OTLPLogExporter({
          url: endpoint,
        }),
      );
      console.log(`Logs will be exported to ${endpoint}`);
    }
  }

  async export(signals: object[]): Promise<void> {
    if (!globalParams.enableOTLP) {
      return;
    }
    const { ExportResultCode } = require('@opentelemetry/core');

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
    if (exportSpans.length > 0 && this.tracesExporters.length > 0) {
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
    if (exportLogs.length > 0 && this.logsExporters.length > 0) {
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
    if (!globalParams.enableOTLP) {
      return;
    }
    for (const exporter of this.tracesExporters) {
      await exporter.forceFlush();
    }
    for (const exporter of this.logsExporters) {
      await exporter.forceFlush();
    }
  }
}
