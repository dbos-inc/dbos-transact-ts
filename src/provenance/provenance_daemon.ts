import { Client, ClientConfig, DatabaseError } from "pg";

interface wal2jsonRow {
  lsn: string;
  xid: string;
  data: string;
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
  initialized = false;

  constructor(clientConfig: ClientConfig, readonly slotName: string) {
    this.client = new Client(clientConfig);
    this.daemonID = setInterval(() => {
      void this.recordProvenance();
    }, this.recordProvenanceIntervalMs);
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
    this.initialized = true;
  }

  async recordProvenance() {
    if (this.initialized) {
      const { rows } = await this.client.query<wal2jsonRow>("SELECT * FROM pg_logical_slot_get_changes($1, NULL, NULL, 'filter-tables', 'operon.*')", [this.slotName]);
      for (let row of rows) {
        const data = JSON.parse(row.data).change as wal2jsonChange[];
        for (let change of data) {
          console.log(row.xid, change);
        }
      }
    }
  }

  async stop() {
    clearInterval(this.daemonID);
    await this.client.end();
  }
}
