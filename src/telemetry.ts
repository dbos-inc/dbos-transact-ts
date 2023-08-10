import { Client, QueryConfig, QueryArrayResult } from "pg";
import { observabilityDBSchema } from "../schemas/observability_db_schema";
import { Operon, registeredOperations } from "./operon";

/*** SIGNALS ***/

export interface TelemetrySignal {
  workflowName: string;
  workflowUUID: string;
  functionID: number;
  functionName: string;
  runAs: string;
  timestamp: number;
  severity: string;
  logMessage: string;
}

/*** EXPORTERS ***/

export interface ITelemetryExporter<T, U> {
  export(signal: TelemetrySignal): Promise<T>;
  process?(signal: TelemetrySignal): U;
  init?(): Promise<void>;
  destroy?(): Promise<void>;
}

export const CONSOLE_EXPORTER = "ConsoleExporter";
export class ConsoleExporter implements ITelemetryExporter<void, undefined> {
  async export(signal: TelemetrySignal): Promise<void> {
    return new Promise<void>((resolve) => {
      console.log(`[${signal.severity}] ${signal.logMessage}`);
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
    const pgClientConfig = { ...operon.config.poolConfig};
    pgClientConfig.database = this.pgLogsDbName;
    this.pgClient = new Client(pgClientConfig);
  }

  async init() {
    const pgSystemClient: Client = new Client(this.operon.config.poolConfig);
    await pgSystemClient.connect();
    // First check if the log database exists using operon pgSystemClient.
    const dbExists = await pgSystemClient.query(
      `SELECT FROM pg_database WHERE datname = '${this.pgLogsDbName}'`
    );
    if (dbExists.rows.length === 0) {
      // Create the logs backend database
      await pgSystemClient.query(`CREATE DATABASE ${this.pgLogsDbName}`);
    }
    await pgSystemClient.end();

    // Connect the exporter client
    await this.pgClient.connect();

    // Load the base schema
    await this.pgClient.query(observabilityDBSchema);

    // Now check for registered workflows
    for (const registeredOperation of registeredOperations) {
      const tableName = `signal_${registeredOperation.name}`;
      let createSignalTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (
   workflow_name TEXT NOT NULL,
   workflow_uuid TEXT NOT NULL,
   function_id INT NOT NULL,
   function_name TEXT NOT NULL,
   run_as TEXT NOT NULL,
   timestamp BIGINT NOT NULL,
   severity TEXT DEFAULT NULL,
   log_message TEXT DEFAULT NULL`;

      const parameterRows: string[] = [];
      for (let i = 0; i < registeredOperation.args.length; i++) {
        const arg = registeredOperation.args[i];
        let row = `${arg.name} ${arg.dataType.formatAsString()} DEFAULT NULL`;
        if (i < registeredOperation.args.length - 1) {
          row = row.concat(",");
        }
        row = row.concat("\n");
        parameterRows.push(row);
      }
      if (parameterRows.length > 0) {
        createSignalTableQuery = createSignalTableQuery.concat(",\n");
        createSignalTableQuery = createSignalTableQuery.concat(
          parameterRows.join("")
        );
      }
      createSignalTableQuery = createSignalTableQuery.concat("\n);");
      await this.pgClient.query(createSignalTableQuery);
    }
  }

  async destroy(): Promise<void> {
    await this.pgClient.end();
  }

  process(signal: TelemetrySignal): QueryConfig {
    const tableName: string = `signal_${signal.functionName}`;
    return {
      name: "insert-signal",
      text: `INSERT INTO ${tableName}
        (workflow_name, workflow_uuid, function_id, function_name, run_as, timestamp, severity, log_message)
        VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8)`,
      values: [
        signal.workflowName,
        signal.workflowUUID,
        signal.functionID,
        signal.functionName,
        signal.runAs,
        signal.timestamp,
        signal.severity,
        signal.logMessage,
      ],
    };
  }

  async export(signal: TelemetrySignal): Promise<QueryArrayResult> {
    const query = this.process(signal);
    return this.pgClient.query(query);
  }
}

/*** COLLECTOR ***/

// For now use strings. Eventually define a Signal class for the telemetry data model
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

  push(signal: TelemetrySignal) {
    this.signals.push(signal);
  }

  private pop(): TelemetrySignal | undefined {
    return this.signals.pop();
  }

  async processAndExportSignals(): Promise<void> {
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
