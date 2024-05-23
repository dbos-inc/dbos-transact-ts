import { DBOSConfig } from "../src/dbos-executor";
import { Client } from "pg";
import { UserDatabaseName } from "../src/user_database";
import { setApplicationVersion } from "../src/dbos-runtime/applicationVersion";

/* DB management helpers */
export function generateDBOSTestConfig(dbClient?: UserDatabaseName, debugMode?: boolean, debugProxy?: string): DBOSConfig {
  const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
  if (!dbPassword) {
    throw new Error("DB_PASSWORD or PGPASSWORD environment variable not set");
  }

  const silenceLogs: boolean = process.env.SILENCE_LOGS === "true" ? true : false;

  setApplicationVersion("test");

  const dbosTestConfig: DBOSConfig = {
    poolConfig: {
      host: "localhost",
      port: 5432,
      user: "postgres",
      password: process.env.PGPASSWORD,
      // We can use another way of randomizing the DB name if needed
      database: "dbostest",
    },
    application: {
      counter: 3,
      shouldExist: 'exists',
    },
    telemetry: {
      logs: {
        silent: silenceLogs,
      },
    },
    system_database: "dbostest_dbos_sys",
    userDbclient: dbClient || UserDatabaseName.PGNODE,
    debugProxy: debugProxy,
    debugMode: debugMode,
  };

  return dbosTestConfig;
}

export async function setUpDBOSTestDb(config: DBOSConfig) {
  const pgSystemClient = new Client({
    user: config.poolConfig.user,
    port: config.poolConfig.port,
    host: config.poolConfig.host,
    password: config.poolConfig.password,
    database: "postgres",
  });
  await pgSystemClient.connect();
  await pgSystemClient.query(`DROP DATABASE IF EXISTS ${config.poolConfig.database};`);
  await pgSystemClient.query(`CREATE DATABASE ${config.poolConfig.database};`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS ${config.system_database};`);
  await pgSystemClient.end();
}

/* Common test types */
export interface TestKvTable {
  id?: number;
  value?: string;
}
