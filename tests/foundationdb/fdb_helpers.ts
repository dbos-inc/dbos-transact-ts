import { FoundationDBSystemDatabase } from "../../src/foundationdb/fdb_system_database";

// Create a FDB system database and clean up existing tables.
export async function createInternalTestFDB(): Promise<FoundationDBSystemDatabase> {
  const systemDB: FoundationDBSystemDatabase = new FoundationDBSystemDatabase();
  await systemDB.init();
  return systemDB;
}
