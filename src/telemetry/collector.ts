import { ITelemetryExporter } from './exporters';
import { Span } from '@opentelemetry/sdk-trace-base';
import { LogRecord } from '@opentelemetry/api-logs';

export type TelemetrySignal = LogRecord | Span;

/** This class collects log / span records and exports them in batches, maintaining order */
export class TelemetryCollector {
  // Signals buffer management
  private signals: TelemetrySignal[] = [];
  private readonly signalBufferID: NodeJS.Timeout;

  // We iterate on an interval and export whatever has accumulated so far
  private readonly processAndExportSignalsIntervalMs = 100;

  constructor(readonly exporter?: ITelemetryExporter) {
    this.signalBufferID = setInterval(() => {
      void this.processAndExportSignals();
    }, this.processAndExportSignalsIntervalMs);
  }

  async destroy() {
    clearInterval(this.signalBufferID);
    await this.processAndExportSignals();
    await this.exporter?.flush();
  }

  push(signal: TelemetrySignal) {
    this.signals.push(signal);
  }

  async processAndExportSignals(): Promise<void> {
    const batch: TelemetrySignal[] = [];
    let consumed = 0;
    while (this.signals.length > batch.length) {
      const signal = this.signals[consumed++];
      if (!signal) {
        break;
      }
      batch.push(signal);
    }
    this.signals = this.signals.splice(0, consumed);
    if (batch.length > 0) {
      if (this.exporter) {
        try {
          await this.exporter.export(batch);
        } catch (e) {
          console.error((e as Error).message);
        }
      }
    }
  }
}
