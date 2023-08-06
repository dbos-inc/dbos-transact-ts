import { Client, QueryConfig, QueryArrayResult } from "pg";
import postgresLogBackendSchema from "../schemas/postgresLogBackend";
import { Operon } from "./operon";

/*** EXPORTERS ***/

export interface ITelemetryExporter<T, U> {
  export(signal: string): Promise<T>;
  process?(signal: string): U;
  init?(): Promise<void>;
  destroy?(): Promise<void>;
}

export const CONSOLE_EXPORTER = "ConsoleExporter";
export class ConsoleExporter implements ITelemetryExporter<void, undefined> {
  async export(signal: string): Promise<void> {
    return new Promise<void>((resolve) => {
      console.log(signal);
      resolve();
    });
  }
}

export const POSTGRES_EXPORTER = "PostgresExporter";
export class PostgresExporter
implements ITelemetryExporter<QueryArrayResult, QueryConfig>
{
  readonly pgClient: Client;
  private readonly pgLogsDbName: string = "pglogsbackend"; // XXX we could make this DB name configurable for tests?

  constructor(private readonly operon: Operon) {
    const pgClientConfig = operon.pgSystemClientConfig;
    pgClientConfig.database = this.pgLogsDbName;
    this.pgClient = new Client(pgClientConfig);
  }

  async init() {
    // First check if the log database exists using operon pgSystemClient.
    // We assume this.operon.pgSystemClient is already connected.
    const dbExists = await this.operon.pgSystemClient.query(
      `SELECT FROM pg_database WHERE datname = '${this.pgLogsDbName}'`
    );
    if (dbExists.rows.length === 0) {
      // Create the logs backend database
      await this.operon.pgSystemClient.query(
        `CREATE DATABASE ${this.pgLogsDbName}`
      );
    }
    // Connect the exporter client and load the schema no matter what
    await this.pgClient.connect();
    await this.pgClient.query(postgresLogBackendSchema);
  }

  async destroy(): Promise<void> {
    await this.pgClient.end();
  }

  process(signal: string): QueryConfig {
    return {
      name: "insert-signal-log",
      text: "INSERT INTO log_signal (workflow_instance_id, function_id, log_signal_raw) VALUES ($1, $2, $3)",
      // TODO wire these values with the Signal data model
      values: [
        Math.floor(Math.random() * 1000),
        Math.floor(Math.random() * 1000),
        signal,
      ],
    };
  }

  async export(signal: string): Promise<QueryArrayResult> {
    const query = this.process(signal);
    return this.pgClient.query(query);
  }
}

/*** COLLECTOR ***/

// For now use strings. Eventually define a Signal class for the telemetry data model
class SignalsQueue {
  data: string[] = [];

  push(signal: string): void {
    this.data.push(signal);
  }

  pop(): string | undefined {
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

  push(signal: string) {
    this.signals.push(signal);
  }

  private pop(): string | undefined {
    return this.signals.pop();
  }

  private async processAndExportSignals(): Promise<void> {
    // eslint-disable-next-line no-cond-assign
    while (this.signals.size() > 0) {
      const signal = this.pop();
      // Because we are single threaded, we don't have to worry about concurrent shift() and push() to the buffer
      if (!signal) {
        break;
      }
      for (const exporter of this.exporters) {
        await exporter.export(signal);
      }
    }
  }
}
