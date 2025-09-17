import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { DBOS } from '../src';
import { sleepms } from '../src/utils';
import { getSysDatabaseUrlFromUserDb, translateDbosConfig } from '../src/dbos-runtime/config';
import { ensureSystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { dropPGDatabase, ensurePGDatabase, maskDatabaseUrl } from '../src/datasource';

/* DB management helpers */
export function generateDBOSTestConfig(): DBOSConfig {
  const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
  if (!dbPassword) {
    throw new Error('DB_PASSWORD or PGPASSWORD environment variable not set');
  }
  const _silenceLogs = process.env.SILENCE_LOGS === 'true';

  const databaseUrl = `postgresql://postgres:${dbPassword}@localhost:5432/dbostest?sslmode=disable`;
  const systemDatabaseUrl = getSysDatabaseUrlFromUserDb(databaseUrl);

  return {
    name: 'dbostest',
    databaseUrl,
    systemDatabaseUrl,
  };
}

export async function setUpDBOSTestDb(config: DBOSConfig) {
  config.name ??= 'dbostest';
  const internalConfig = translateDbosConfig(config);

  if (internalConfig.databaseUrl) {
    const r = await dropPGDatabase({ urlToDrop: internalConfig.databaseUrl, logger: () => {} });
    if (r.status !== 'did_not_exist' && r.status !== 'dropped') {
      throw new Error(`Unable to drop ${maskDatabaseUrl(internalConfig.databaseUrl)}`);
    }
    const rc = await ensurePGDatabase({ urlToEnsure: internalConfig.databaseUrl, logger: () => {} });
    if (rc.status !== 'already_exists' && rc.status !== 'created') {
      throw new Error(`Unable to create ${maskDatabaseUrl(internalConfig.databaseUrl)}`);
    }
  }
  const r = await dropPGDatabase({ urlToDrop: internalConfig.systemDatabaseUrl, logger: () => {} });
  if (r.status !== 'did_not_exist' && r.status !== 'dropped') {
    throw new Error(`Unable to drop ${maskDatabaseUrl(internalConfig.systemDatabaseUrl)}`);
  }
  await ensureSystemDatabase(internalConfig.systemDatabaseUrl, new GlobalLogger());
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
  const r = await dropPGDatabase({ urlToDrop: connectionString, dbToDrop: database });
  if (r.status !== 'did_not_exist' && r.status !== 'dropped') {
    throw new Error(`Unable to drop ${maskDatabaseUrl(connectionString)}`);
  }
}
