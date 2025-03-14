import { DBOSConfig } from '../src/dbos-executor';
import { Client } from 'pg';
import { UserDatabaseName } from '../src/user_database';

/* DB management helpers */
export function generateDBOSTestConfig(dbClient?: UserDatabaseName): DBOSConfig {
  const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
  if (!dbPassword) {
    throw new Error('DB_PASSWORD or PGPASSWORD environment variable not set');
  }

  const silenceLogs: boolean = process.env.SILENCE_LOGS === 'true' ? true : false;

  const dbosTestConfig: DBOSConfig = {
    poolConfig: {
      host: 'localhost',
      port: 5432,
      user: 'postgres',
      password: process.env.PGPASSWORD,
      // We can use another way of randomizing the DB name if needed
      database: 'dbostest',
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
    system_database: 'dbostest_dbos_sys',
    userDbclient: dbClient || UserDatabaseName.PGNODE,
  };

  return dbosTestConfig;
}

export async function setUpDBOSTestDb(config: DBOSConfig) {
  const pgSystemClient = new Client({
    user: config.poolConfig.user,
    port: config.poolConfig.port,
    host: config.poolConfig.host,
    password: config.poolConfig.password,
    database: 'postgres',
  });
  try {
    await pgSystemClient.connect();
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${config.poolConfig.database};`);
    await pgSystemClient.query(`CREATE DATABASE ${config.poolConfig.database};`);
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${config.system_database};`);
    // await pgSystemClient.end();
  } catch (e) {
    if (e instanceof AggregateError) {
      console.error(`Test database setup failed: AggregateError containing ${e.errors.length} errors:`);
      e.errors.forEach((err, index) => {
        console.error(`  Error ${index + 1}:`, err);
      });
    } else {
      console.error(`Test database setup failed:`, e);
    }
    throw e;
  } finally {
    await pgSystemClient.end();
  }
}

/* Common test types */
export interface TestKvTable {
  id?: number;
  value?: string;
}

// A helper class for testing concurrency. Behaves similarly to threading.Event in Python.
// The class contains a promise and a resolution.
// Await Event.wait() to await the promise.
// Call event.set() to resolve the promise.
export class Event {
  private _resolve: (() => void) | null = null;
  private _promise: Promise<void>;

  constructor() {
    this._promise = new Promise((resolve) => {
      this._resolve = resolve;
    });
  }

  set(): void {
    if (this._resolve) {
      this._resolve();
      this._resolve = null;
    }
  }

  wait(): Promise<void> {
    return this._promise;
  }

  clear(): void {
    this._promise = new Promise((resolve) => {
      this._resolve = resolve;
    });
  }
}
