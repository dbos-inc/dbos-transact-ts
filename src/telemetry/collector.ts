import { ITelemetryExporter } from "./exporters";
import { Span } from "@opentelemetry/sdk-trace-base";
import { LogRecord } from "@opentelemetry/api-logs";

export type TelemetrySignal = LogRecord | Span;

class SignalsQueue {
  data: TelemetrySignal[] = [];

  push(signal: TelemetrySignal): void {
    this.data.push(signal);
  }

  pop(): TelemetrySignal | undefined {
    return this.data.shift();
  }

  size(): number {
    return this.data.length;
  }
}

// TODO: Handle temporary workflows properly.
export class TelemetryCollector {
  // Signals buffer management
  private readonly signals: SignalsQueue = new SignalsQueue();
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

  private pop(): TelemetrySignal | undefined {
    return this.signals.pop();
  }

  async processAndExportSignals(): Promise<void> {
    const batch: TelemetrySignal[] = [];
    while (this.signals.size() > 0) {
      const signal = this.pop();
      if (!signal) {
        break;
      }
      batch.push(signal);
    }
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
