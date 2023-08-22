import { ITelemetryExporter } from "./exporters";
import { OperonSignal, ProvenanceSignal, TelemetrySignal } from "./signals";

class SignalsQueue {
  data: OperonSignal[] = [];

  push(signal: OperonSignal): void {
    this.data.push(signal);
  }

  pop(): OperonSignal | undefined {
    return this.data.shift();
  }

  size(): number {
    return this.data.length;
  }
}

export class TelemetryCollector {
  // Signals buffer management
  private readonly signals: SignalsQueue = new SignalsQueue();
  private readonly signalBufferID: NodeJS.Timeout;
  private readonly processAndExportSignalsIntervalMs = 1000;
  private readonly processAndExportSignalsMaxBatchSize = 10;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(readonly exporters: ITelemetryExporter<any, any>[]) {
    this.signalBufferID = setInterval(() => {
      void this.processAndExportSignals();
    }, this.processAndExportSignalsIntervalMs);
  }

  async init() {
    for (const exporter of this.exporters) {
      if (exporter.init) {
        await exporter.init();
      }
    }
  }

  async destroy() {
    clearInterval(this.signalBufferID);
    await this.processAndExportSignals();
    for (const exporter of this.exporters) {
      if (exporter.destroy) {
        await exporter.destroy();
      }
    }
  }

  push(signal: OperonSignal) {
    this.signals.push(signal);
  }

  private pop(): OperonSignal | undefined {
    return this.signals.pop();
  }

  async processAndExportSignals(): Promise<void> {
    const batch: OperonSignal[] = [];
    while (this.signals.size() > 0 && batch.length < this.processAndExportSignalsMaxBatchSize) {
      const signal = this.pop();
      if (!signal) {
        break;
      }
      batch.push(signal);
    }
    if (batch.length > 0) {
      const exports = [];
      for (const exporter of this.exporters) {
        exports.push(exporter.export(batch));
      }
      try {
        await Promise.all(exports);
      } catch (e) {
        console.error((e as Error).message);
      }
    }
  }
}
