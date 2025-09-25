import { ITelemetryExporter } from './exporters';

class SignalsQueue {
  data: object[] = [];

  push(signal: object): void {
    this.data.push(signal);
  }

  pop(): object | undefined {
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

  push(signal: object) {
    this.signals.push(signal);
  }

  private pop(): object | undefined {
    return this.signals.pop();
  }

  async processAndExportSignals(): Promise<void> {
    const batch: object[] = [];
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
