import { DBOSConfig, DBOSConfigInternal, DBOSExecutor } from '../src/dbos-executor';
import { Client } from 'pg';
import { UserDatabaseName } from '../src/user_database';
import { DBOS } from '../src';
import { sleepms } from '../src/utils';
import { getSystemDatabaseUrl, translateDbosConfig } from '../src/dbos-runtime/config';

/* DB management helpers */
export function generateDBOSTestConfig(dbClient?: UserDatabaseName): DBOSConfigInternal {
  const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
  if (!dbPassword) {
    throw new Error('DB_PASSWORD or PGPASSWORD environment variable not set');
  }
  const silenceLogs = process.env.SILENCE_LOGS === 'true';

  const databaseUrl = `postgresql://postgres:${dbPassword}@localhost:5432/dbostest?sslmode=disable`;
  const systemDatabaseUrl = getSystemDatabaseUrl(databaseUrl);

  const dbosTestConfig: DBOSConfigInternal = {
    name: 'dbostest',
    databaseUrl,
    systemDatabaseUrl,
    telemetry: {
      logs: {
        silent: silenceLogs,
      },
    },
    userDbClient: dbClient || UserDatabaseName.PGNODE,
  };

  return dbosTestConfig;
}

export function generatePublicDBOSTestConfig(kwargs?: object): DBOSConfig {
  return {
    name: 'dbostest', // Passing a name is kind of required because otherwise, we'll take in the name of the framework package.json, which is not a valid DB name
    databaseUrl: `postgres://postgres:${process.env.PGPASSWORD}@localhost:5432/dbostest`,
    ...kwargs,
  };
}

export async function setUpDBOSTestDb(cfg: DBOSConfig) {
  if (!cfg.name) {
    cfg.name = 'dbostest';
  }
  const config = translateDbosConfig(cfg);
  const url = new URL(config.databaseUrl);
  const database = url.pathname.slice(1);
  url.pathname = '/postgres';
  const sysdburl = new URL(config.systemDatabaseUrl);
  const sysdb = sysdburl.pathname.slice(1);
  const pgSystemClient = new Client({
    connectionString: url.toString(),
  });
  try {
    await pgSystemClient.connect();
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE);`);
    await pgSystemClient.query(`CREATE DATABASE ${database};`);
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${sysdb} WITH (FORCE);`);
    await pgSystemClient.end();
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

export async function queueEntriesAreCleanedUp() {
  let maxTries = 10;
  let success = false;
  while (maxTries > 0) {
    const qtasks = await DBOS.listQueuedWorkflows({});
    if (qtasks.length === 0) {
      success = true;
      break;
    }
    await sleepms(1000);
    --maxTries;
  }
  return success;
}

// copied from https://github.com/uuidjs/uuid project
export function uuidValidate(uuid: string) {
  const regex =
    /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$/i;
  return regex.test(uuid);
}

export function recoverPendingWorkflows(executorIDs: string[] = ['local']) {
  expect(DBOSExecutor.globalInstance).toBeDefined();
  return DBOSExecutor.globalInstance!.recoverPendingWorkflows(executorIDs);
}

export function executeWorkflowById(workflowId: string) {
  expect(DBOSExecutor.globalInstance).toBeDefined();
  return DBOSExecutor.globalInstance!.executeWorkflowUUID(workflowId);
}

export async function dropDatabase(connectionString: string, database?: string) {
  const url = new URL(connectionString);
  database ||= url.pathname.slice(1);
  url.pathname = '/postgres';

  // Drop system database, for testing.
  const pgSystemClient = new Client({
    connectionString: url.toString(),
  });
  try {
    await pgSystemClient.connect();
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${database};`);
  } finally {
    await pgSystemClient.end();
  }
}
