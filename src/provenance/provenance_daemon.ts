import { Client, ClientConfig, DatabaseError } from "pg";

export class ProvenanceDaemon {
  readonly client;

  constructor(clientConfig: ClientConfig, readonly slotName: string) {
    this.client = new Client(clientConfig);
  }

  async start() {
    await this.client.connect();
    // Create a logical replication slot with the given name.
    try {
      await this.client.query("SELECT pg_create_logical_replication_slot($1, 'wal2json');", [this.slotName]);
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      if (err.code === '42710') {
        // This means the slot has been created before.
      } else {
        console.error(err);
        throw err;
      }
    }
  }

  async stop() {
    await this.client.end();
  }
}