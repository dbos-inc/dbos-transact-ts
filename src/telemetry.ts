export type TelemetryExporter = (signal: string) => Promise<void>;

export async function ConsoleExporter(signal: string) {
  return new Promise<void>((resolve) => {
    console.log(signal);
    resolve();
  });
}

export class TelemetryCollector {
  readonly signalBuffer: string[] = [];
  readonly signalBufferID: NodeJS.Timeout;
  readonly processAndExportSignalsIntervalMs = 1000;
  readonly processAndExportSignalsTimeoutMs = 500;

  constructor(readonly exporters: TelemetryExporter[]) {
    this.signalBufferID = setInterval(() => {
      void this.processAndExportSignals();
    }, this.processAndExportSignalsIntervalMs);
  }

  push(signal: string) {
    this.signalBuffer.push(signal);
  }

  private async processAndExportSignals() {
    let signal: string | undefined;
    /* eslint-disable-next-line no-cond-assign */
    while (signal = this.signalBuffer.shift()) { // Because we are single threaded, we don't have to worry about concurrent shift() and push() to the buffer
      if (!signal) {
        break;
      }
      for (const exporter of this.exporters) {
        await exporter(signal);
      }
    }
  }

  async destroy() {
    clearInterval(this.signalBufferID);
    await this.processAndExportSignals();
  }
}
