import { Client, QueryConfig, QueryArrayResult, PoolConfig } from "pg";
import { groupBy } from "lodash";
import { LogMasks, OperonDataType, OperonMethodRegistrationBase } from "./../decorators";
import { OperonPostgresExporterError } from "./../error";
import { OperonSignal, ProvenanceSignal, TelemetrySignal } from "./signals";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { ExportResult, ExportResultCode } from "@opentelemetry/core";
import { spanToString } from "./traces";

export interface ITelemetryExporter<T, U> {
  export(signal: OperonSignal[]): Promise<T>;
  process?(signal: OperonSignal[]): U;
  init?(registeredOperations?: ReadonlyArray<OperonMethodRegistrationBase>): Promise<void>;
  destroy?(): Promise<void>;
}

export const JAEGER_EXPORTER = "JaegerExporter";
export class JaegerExporter implements ITelemetryExporter<void, undefined> {
  private readonly exporter: OTLPTraceExporter;
  constructor(endpoint: string = "http://localhost:4318/v1/traces") {
    this.exporter = new OTLPTraceExporter({
      url: endpoint,
    });
  }

  async export(rawSignals: OperonSignal[]): Promise<void> {
    // Note: it is not compatible with provenance signal.
    const signals = rawSignals as TelemetrySignal[];
    return await new Promise<void>((resolve) => {
      const exportSpans: ReadableSpan[] = [];
      signals.forEach((signal) => {
        if (signal.traceSpan) {
          exportSpans.push(signal.traceSpan);
        }
      });
      this.exporter.export(exportSpans, (results: ExportResult) => {
        if (results.code !== ExportResultCode.SUCCESS) {
          console.warn(`Jaeger export failed`);
        }
      });
      resolve();
    });
  }
}

export const POSTGRES_EXPORTER = "PostgresExporter";
export class PostgresExporter implements ITelemetryExporter<QueryArrayResult[], QueryConfig[]> {
  readonly pgClient: Client;

  constructor(private readonly poolConfig: PoolConfig, readonly observabilityDBName: string = "operon_observability") {
    const pgClientConfig = { ...poolConfig };
    pgClientConfig.database = this.observabilityDBName;
    this.pgClient = new Client(pgClientConfig);
  }

  static getPGDataType(t: OperonDataType): string {
    if (t.dataType === "double") {
      return "double precision"; // aka "float8"
    }
    return t.formatAsString();
  }

  async init(registeredOperations: ReadonlyArray<OperonMethodRegistrationBase> = []) {
    const pgSystemClient: Client = new Client(this.poolConfig);
    await pgSystemClient.connect();
    // First check if the log database exists using operon pgSystemClient.
    const dbExists = await pgSystemClient.query(`SELECT FROM pg_database WHERE datname = '${this.observabilityDBName}'`);
    if (dbExists.rows.length === 0) {
      // Create the logs backend database
      await pgSystemClient.query(`CREATE DATABASE ${this.observabilityDBName}`);
    }
    await pgSystemClient.end();

    // Connect the exporter client
    await this.pgClient.connect();

    // Configure tables for registered workflows
    for (const registeredOperation of registeredOperations) {
      const tableName = `signal_${registeredOperation.name}`;
      let createSignalTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (
        workflow_uuid TEXT NOT NULL,
        function_name TEXT NOT NULL,
        run_as TEXT NOT NULL,
        timestamp BIGINT NOT NULL,
        transaction_id TEXT DEFAULT NULL,
        trace_id TEXT DEFAULT NULL,
        trace_span JSONB DEFAULT NULL,\n`;

      for (const arg of registeredOperation.args) {
        if (arg.logMask === LogMasks.SKIP) {
          continue;
        } else if (arg.logMask === LogMasks.HASH) {
          const row = `${arg.name} VARCHAR(64) DEFAULT NULL,\n`;
          createSignalTableQuery = createSignalTableQuery.concat(row);
        } else {
          const row = `${arg.name} ${PostgresExporter.getPGDataType(arg.dataType)} DEFAULT NULL,\n`;
          createSignalTableQuery = createSignalTableQuery.concat(row);
        }
      }
      // Trim last comma and line feed
      createSignalTableQuery = createSignalTableQuery.slice(0, -2).concat("\n);");
      await this.pgClient.query(createSignalTableQuery);

      // Create a table for provenance logs.
      // TODO: create a secondary index.
    }
    await this.pgClient.query(`CREATE TABLE IF NOT EXISTS provenance_logs (
      transaction_id TEXT NOT NULL,
      kind TEXT,
      schema_name TEXT,
      table_name TEXT,
      columnnames TEXT,
      columntypes TEXT,
      columnvalues TEXT
    );`);
  }

  async destroy(): Promise<void> {
    await this.pgClient.end();
  }

  process(signals: TelemetrySignal[]): QueryConfig[] {
    const groupByFunctionName: Map<string, TelemetrySignal[]> = new Map(Object.entries(groupBy(signals, ({ operationName }) => operationName)));
    const queries: QueryConfig[] = [];

    for (const [operationName, signals] of groupByFunctionName) {
      const tableName: string = `signal_${operationName}`;
      const query = `
        INSERT INTO ${tableName}
        SELECT * FROM jsonb_to_recordset($1::jsonb) AS tmp (workflow_uuid text, function_name text, run_as text, timestamp bigint, transaction_id text, trace_id text, trace_span json)
      `;

      const values: string = JSON.stringify(
        signals.map((signal) => {
          return {
            workflow_uuid: signal.workflowUUID,
            function_name: signal.operationName,
            run_as: signal.runAs,
            timestamp: signal.timestamp,
            transaction_id: signal.transactionID,
            trace_id: signal.traceID,
            trace_span: signal.traceSpan ? spanToString(signal.traceSpan) : null,
          };
        })
      );

      queries.push({
        name: `insert-${tableName}`,
        text: query,
        values: [values],
      });
    }
    return queries;
  }

  processProvenance(signals: ProvenanceSignal[]): QueryConfig {
    const query = `
      INSERT INTO provenance_logs
      SELECT * FROM jsonb_to_recordset($1::jsonb) AS tmp (
        transaction_id text, kind text, schema_name text, table_name text,
        columnnames text, columntypes text, columnvalues text);
    `;

    const values: string = JSON.stringify(
      signals.map((signal) => {
        return {
          transaction_id: signal.provTransactionID,
          kind: signal.kind,
          schema_name: signal.schema,
          table_name: signal.table,
          columnnames: signal.columnnames,
          columntypes: signal.columntypes,
          columnvalues: signal.columnvalues
        };
      })
    );
    return { name: `insert-provenance-log`, text: query, values: [values] };
  }

  async export(signals: OperonSignal[]): Promise<QueryArrayResult[]> {
    const results: Promise<QueryArrayResult>[] = [];
    // Find all telemetry signals and process.
    const telemetrySignals = signals.filter(obj => (obj as TelemetrySignal).workflowUUID !== undefined) as TelemetrySignal[];
    if (telemetrySignals.length > 0) {
      const queries = this.process(telemetrySignals);
      for (const query of queries) {
        results.push(this.pgClient.query(query));
      }
    }

    // Find all provenance signals and process.
    const provenanceSignals = signals.filter((obj) => (obj as ProvenanceSignal).provTransactionID !== undefined) as ProvenanceSignal[];
    if (provenanceSignals.length > 0) {
      const provQuery = this.processProvenance(provenanceSignals);
      results.push(this.pgClient.query(provQuery));
    }
    try {
      // We do await here so we can catch and format PostgresExporter specific errors
      return await Promise.all<QueryArrayResult>(results);
    } catch (err) {
      throw new OperonPostgresExporterError(err as Error);
    }
  }
}
