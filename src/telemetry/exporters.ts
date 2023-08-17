import { Client, QueryConfig, QueryArrayResult } from "pg";
import { Operon } from "./../operon";
import { groupBy } from "lodash";
import { forEachMethod, LogMasks, OperonDataType, OperonMethodRegistrationBase } from "./../decorators";
import { OperonPostgresExporterError, OperonJaegerExporterError } from "./../error";
import { TelemetrySignal } from "./signals";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import { ExportResult, ExportResultCode } from "@opentelemetry/core";
import { spanToString } from "./traces";

export interface ITelemetryExporter<T, U> {
  export(signal: TelemetrySignal[]): Promise<T>;
  process?(signal: TelemetrySignal[]): U;
  init?(): Promise<void>;
  destroy?(): Promise<void>;
}

export const JAEGER_EXPORTER = "JaegerExporter";
export class JaegerExporter implements ITelemetryExporter<void, undefined> {
  private readonly exporter: OTLPTraceExporter;
  constructor() {
    this.exporter = new OTLPTraceExporter({
      url: process.env.JAEGER_OTLP_ENDPOINT || "http://localhost:4318/v1/traces",
    });
  }

  async export(signals: TelemetrySignal[]): Promise<void> {
    return await new Promise<void>((resolve) => {
      const exportSpans: ReadableSpan[] = [];
      signals.forEach((signal) => {
        if (signal.traceSpan) {
          exportSpans.push(signal.traceSpan);
        }
      });
      this.exporter.export(exportSpans, (results: ExportResult) => {
        if (results.code !== ExportResultCode.SUCCESS) {
          throw new OperonJaegerExporterError(results);
        }
        console.log(results);
      });
      resolve();
    });
  }
}

export const CONSOLE_EXPORTER = "ConsoleExporter";
export class ConsoleExporter implements ITelemetryExporter<void, undefined> {
  async export(signals: TelemetrySignal[]): Promise<void> {
    return await new Promise<void>((resolve) => {
      for (const signal of signals) {
        if (signal.logMessage) {
          console.log(`[${signal.severity}] ${signal.logMessage}`);
        }
      }
      resolve();
    });
  }
}

export const POSTGRES_EXPORTER = "PostgresExporter";
export class PostgresExporter implements ITelemetryExporter<QueryArrayResult[], QueryConfig[]> {
  readonly pgClient: Client;

  constructor(private readonly operon: Operon, readonly observabilityDBName: string = "operon_observability") {
    const pgClientConfig = { ...operon.config.poolConfig };
    pgClientConfig.database = this.observabilityDBName;
    this.pgClient = new Client(pgClientConfig);
  }

  static getPGDataType(t: OperonDataType): string {
    if (t.dataType === "double") {
      return "double precision"; // aka "float8"
    }
    return t.formatAsString();
  }

  async init() {
    const pgSystemClient: Client = new Client(this.operon.config.poolConfig);
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
    const registeredOperations: OperonMethodRegistrationBase[] = [];
    forEachMethod((o) => {
      registeredOperations.push(o);
    });
    for (const registeredOperation of registeredOperations) {
      const tableName = `signal_${registeredOperation.name}`;
      let createSignalTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (
        workflow_uuid TEXT NOT NULL,
        function_id INT NOT NULL,
        function_name TEXT NOT NULL,
        run_as TEXT NOT NULL,
        timestamp BIGINT NOT NULL,
        severity TEXT DEFAULT NULL,
        log_message TEXT DEFAULT NULL,
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
    }
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
        SELECT * FROM jsonb_to_recordset($1::jsonb) AS tmp (workflow_uuid text, function_id int, function_name text, run_as text, timestamp bigint, severity text, log_message text, trace_id text, trace_span json)
      `;

      const values: string = JSON.stringify(
        signals.map((signal) => {
          return {
            workflow_uuid: signal.workflowUUID,
            function_id: signal.functionID,
            function_name: signal.operationName,
            run_as: signal.runAs,
            timestamp: signal.timestamp,
            severity: signal.severity,
            log_message: signal.logMessage,
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

  async export(signals: TelemetrySignal[]): Promise<QueryArrayResult[]> {
    const queries = this.process(signals);
    const results: Promise<QueryArrayResult>[] = [];
    for (const query of queries) {
      results.push(this.pgClient.query(query));
    }
    try {
      // We do await here so we can catch and format PostgresExporter specific errors
      return await Promise.all<QueryArrayResult>(results);
    } catch (err) {
      throw new OperonPostgresExporterError(err as Error);
    }
  }
}
