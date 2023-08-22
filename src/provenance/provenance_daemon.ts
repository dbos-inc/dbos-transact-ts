import { Client, DatabaseError } from "pg";
import { CONSOLE_EXPORTER, ConsoleExporter, POSTGRES_EXPORTER, PostgresExporter, TelemetryCollector } from "../telemetry";
import { OperonConfig } from "src/operon";
import { ProvenanceSignal } from "src/telemetry/signals";

interface wal2jsonRow {
  xid: string;
  data: string;
}

interface wal2jsonData {
  change: wal2jsonChange[];
}

interface wal2jsonChange {
  kind: string;
  schema: string;
  table: string;
  columnnames: string[];
  columntypes: string[];
  columnvalues: string[];
}

export class ProvenanceDaemon {
  readonly client;
  readonly daemonID;
  readonly recordProvenanceIntervalMs = 1000;
  readonly telemetryCollector: TelemetryCollector;
  initialized = false;

  constructor(operonConfig: OperonConfig, readonly slotName: string) {
    this.client = new Client(operonConfig.poolConfig);
    this.daemonID = setInterval(() => {
      void this.recordProvenance();
    }, this.recordProvenanceIntervalMs);
    const telemetryExporters = [];
    if (operonConfig.telemetryExporters) {
      for (const exporter of operonConfig.telemetryExporters) {
        if (exporter === CONSOLE_EXPORTER) {
          telemetryExporters.push(new ConsoleExporter());
        } else if (exporter === POSTGRES_EXPORTER) {
          telemetryExporters.push(new PostgresExporter(operonConfig.poolConfig, operonConfig?.observability_database));
        }
      }
    }
    
    this.telemetryCollector = new TelemetryCollector(telemetryExporters);
  }

  async start() {
    await this.client.connect();
    // Create a logical replication slot with the given name.
    try {
      await this.client.query("SELECT pg_create_logical_replication_slot($1, 'wal2json');", [this.slotName]);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === "42710") {
        // This means the slot has been created before.
      } else {
        console.error(err);
        throw err;
      }
    }
    await this.telemetryCollector.init();
    this.initialized = true;
  }

  async recordProvenance() {
    if (this.initialized) {
      const { rows } = await this.client.query<wal2jsonRow>("SELECT CAST(xid AS TEXT), data FROM pg_logical_slot_get_changes($1, NULL, NULL, 'filter-tables', 'operon.*')", [this.slotName]);
      for (const row of rows) {
        const data = (JSON.parse(row.data) as wal2jsonData).change;
        for (const change of data) {
          const signal: ProvenanceSignal = {
            transactionID: row.xid,
            kind: change.kind,
            schema: change.schema,
            table: change.table,
            columnnames: change.columnnames,
            columntypes: change.columntypes,
            columnvalues: change.columnvalues
          }
          this.telemetryCollector.push(signal);
        }
      }
    }
  }

  async stop() {
    clearInterval(this.daemonID);
    await this.client.end();
    await this.telemetryCollector.destroy();
  }
}
