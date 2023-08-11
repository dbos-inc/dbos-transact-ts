import { Client, QueryConfig, QueryArrayResult } from "pg";
import { Operon } from "./operon";
import { groupBy } from "lodash";
import { forEachMethod } from "./decorators";

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
  export(signal: TelemetrySignal[]): Promise<T>;
  process?(signal: TelemetrySignal[]): U;
  init?(): Promise<void>;
  destroy?(): Promise<void>;
}

export const CONSOLE_EXPORTER = "ConsoleExporter";
export class ConsoleExporter implements ITelemetryExporter<void, undefined> {
  async export(signals: TelemetrySignal[]): Promise<void> {
    return new Promise<void>((resolve) => {
      for (const signal of signals) {
        console.log(`[${signal.severity}] ${signal.logMessage}`);
      }
      resolve();
    });
  }
}

export const POSTGRES_EXPORTER = "PostgresExporter";
export class PostgresExporter
  implements ITelemetryExporter<QueryArrayResult[], QueryConfig[]>
{
  readonly pgClient: Client;

  constructor(
    private readonly operon: Operon,
    readonly observabilityDBName: string = "operon_observability"
  ) {
    const pgClientConfig = { ...operon.config.poolConfig };
    pgClientConfig.database = this.observabilityDBName;
    this.pgClient = new Client(pgClientConfig);
  }

  async init() {
    const pgSystemClient: Client = new Client(this.operon.config.poolConfig);
    await pgSystemClient.connect();
    // First check if the log database exists using operon pgSystemClient.
    const dbExists = await pgSystemClient.query(
      `SELECT FROM pg_database WHERE datname = '${this.observabilityDBName}'`
    );
    if (dbExists.rows.length === 0) {
      // Create the logs backend database
      await pgSystemClient.query(`CREATE DATABASE ${this.observabilityDBName}`);
    }
    await pgSystemClient.end();

    // Connect the exporter client
    await this.pgClient.connect();

    // Configure tables for registered workflows
    forEachMethod(async (registeredOperation) => {
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
        const argName = arg.name.replace('(', ''); //XXX bug with parameter name parsing from toString()
        let row = `${argName} ${arg.dataType.formatAsString()} DEFAULT NULL`;
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
    });
  }

  async destroy(): Promise<void> {
    await this.pgClient.end();
  }

  process(signals: TelemetrySignal[]): QueryConfig[] {
    const groupByFunctionName: Map<string, TelemetrySignal[]> = new Map(
      Object.entries(groupBy(signals, ({ functionName }) => functionName))
    );
    const queries: QueryConfig[] = [];

    for (const [functionName, signals] of groupByFunctionName) {
      const tableName: string = `signal_${functionName}`;
      const query = `
        INSERT INTO ${tableName}
        SELECT * FROM jsonb_to_recordset($1::jsonb) AS tmp (workflow_name text, workflow_uuid text, function_id int, function_name text, run_as text, timestamp bigint, severity text, log_message text)
      `;

      const values: string = JSON.stringify(
        signals.map((signal) => {
          return {
            workflow_name: signal.workflowName,
            workflow_uuid: signal.workflowUUID,
            function_id: signal.functionID,
            function_name: signal.functionName,
            run_as: signal.runAs,
            timestamp: signal.timestamp,
            severity: signal.severity,
            log_message: signal.logMessage,
          };
        })
      );

      queries.push({
        name: "insert-signal",
        text: query,
        values: [values],
      });
    }
    return queries;
  }

  async export(signals: TelemetrySignal[]): Promise<QueryArrayResult[]> {
    const queries = this.process(signals);
    const results: Promise<QueryArrayResult>[] = [];
    for (const query of queries) {
      results.push(this.pgClient.query(query));
    }
    return Promise.all<QueryArrayResult>(results);
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

  push(signal: TelemetrySignal) {
    this.signals.push(signal);
  }

  private pop(): TelemetrySignal | undefined {
    return this.signals.pop();
  }

  async processAndExportSignals(): Promise<void> {
    const batch: TelemetrySignal[] = [];
    while (
      this.signals.size() > 0 &&
      batch.length < this.processAndExportSignalsMaxBatchSize
    ) {
      const signal = this.pop();
      if (!signal) {
        break;
      }
      batch.push(signal);
    }
    for (const exporter of this.exporters) {
      await exporter.export(batch);
    }
  }
}
