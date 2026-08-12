import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { DBOS, StatusString } from '../src';
import { getClientConfig, INTERNAL_QUEUE_NAME, sleepms } from '../src/utils';
import { isValidDatabaseName, translateDbosConfig } from '../src/config';
import { ensureSystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { deriveDatabaseUrl, dropPGDatabase, ensurePGDatabase, maskDatabaseUrl } from '../src/database_utils';
import { Client } from 'pg';

const silentDropLogger = { warn: () => {} };

/* DB management helpers */

/**
 * Provision a database for a test. `ensurePGDatabase` swallows every failure, so verify the
 * postcondition here and fail with the reason rather than several frames later.
 */
export async function ensureTestDatabase(databaseUrl: string) {
  const notes: string[] = [];
  const record = (msg: string) => {
    notes.push(msg);
  };
  await ensurePGDatabase(databaseUrl, { info: record, warn: record });
  const client = new Client(getClientConfig(databaseUrl));
  try {
    await client.connect();
    // CockroachDB connects to a nonexistent database, so hit a catalog that requires it to exist.
    await client.query('SELECT 1 FROM information_schema.schemata LIMIT 1');
  } catch (e) {
    const why = notes.length ? ` [${notes.join('; ')}]` : '';
    throw new Error(`Could not provision ${maskDatabaseUrl(databaseUrl)}: ${(e as Error).message}${why}`);
  } finally {
    await client.end().catch(() => {});
  }
}

function getSysDatabaseUrlFromUserDb(userDB: string) {
  const url = new URL(userDB);
  const dbName = url.pathname.slice(1);
  if (!isValidDatabaseName(dbName)) {
    throw new Error(`Database name in ${maskDatabaseUrl(userDB)} is invalid.`);
  }
  const sysDbName = `${dbName}_dbos_sys`;
  url.pathname = `/${sysDbName}`;
  return url.toString();
}

export function generateDBOSTestConfig(): DBOSConfig {
  const _silenceLogs = process.env.SILENCE_LOGS === 'true';

  let databaseUrl = process.env.DBOS_TEST_DB_URL;
  if (!databaseUrl) {
    const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
    if (!dbPassword) {
      throw new Error('DB_PASSWORD or PGPASSWORD environment variable not set');
    }
    databaseUrl = `postgresql://postgres:${dbPassword}@localhost:5432/dbostest?sslmode=disable`;
  }
  const systemDatabaseUrl = getSysDatabaseUrlFromUserDb(databaseUrl);

  const isCockroach = new URL(databaseUrl).port === '26257';

  return {
    name: 'dbostest',
    systemDatabaseUrl,
    ...(isCockroach ? { useListenNotify: false } : {}),
  };
}

export async function setUpDBOSTestSysDb(config: DBOSConfig) {
  config.name ??= 'dbostest';
  const internalConfig = translateDbosConfig(config);

  await dropPGDatabase(internalConfig.systemDatabaseUrl, silentDropLogger);
  await ensureSystemDatabase(
    internalConfig.systemDatabaseUrl,
    new GlobalLogger(),
    undefined,
    undefined,
    internalConfig.useListenNotify,
  );
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

// Poll `check` until it stops throwing, rethrowing its last failure if the deadline passes.
export async function retryUntilSuccess(check: () => void | Promise<void>, timeoutMs: number = 15000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      await check();
      return;
    } catch (e) {
      if (Date.now() >= deadline) throw e;
      await sleepms(100);
    }
  }
}

export function recoverPendingWorkflows(executorIDs: string[] = ['local']) {
  expect(DBOSExecutor.globalInstance).toBeDefined();
  return DBOSExecutor.globalInstance!.recoverPendingWorkflows(executorIDs);
}

// Recover and return the handle for `workflowID`; the handle array's order is unspecified, so indexing it can pick another test's workflow and block forever.
export async function recoverWorkflow(workflowID: string, executorIDs: string[] = ['local']) {
  const handles = (await recoverPendingWorkflows(executorIDs)).filter((h) => h.workflowID === workflowID);
  expect(handles).toHaveLength(1);
  return handles[0];
}

export async function setWfAndChildrenToPending(workflowId: string, resetRecoveryAttempts: boolean = true) {
  const wfl = await DBOS.listWorkflows({ workflow_id_prefix: workflowId });
  for (const wf of wfl) {
    await DBOSExecutor.globalInstance?.systemDatabase.setWorkflowStatus(
      wf.workflowID,
      StatusString.PENDING,
      resetRecoveryAttempts,
    );
  }
}

// Re-run a workflow the way recovery does: re-enqueue the row and let the queue dispatch it.
export async function reexecuteWorkflowById(
  workflowId: string,
  resetRecoveryAttempts: boolean = true,
  updateName?: string,
) {
  expect(DBOSExecutor.globalInstance).toBeDefined();
  const sysDB = DBOSExecutor.globalInstance!.systemDatabase;
  const status = await sysDB.getWorkflowStatus(workflowId);
  expect(status).not.toBeNull();
  await sysDB.setWorkflowStatus(workflowId, StatusString.ENQUEUED, resetRecoveryAttempts, {
    updateName,
    // Leave an already-queued workflow on its own queue, as reenqueueWorkflowsForRecovery does.
    queueName: status!.queueName ?? INTERNAL_QUEUE_NAME,
    resetStartedAtEpochMs: true,
  });
  return DBOS.retrieveWorkflow(workflowId);
}

export async function dropDatabase(connectionString: string, database?: string) {
  await dropPGDatabase(database ? deriveDatabaseUrl(connectionString, database) : connectionString, silentDropLogger);
}

export async function causeChaos(db: string): Promise<void> {
  const client = new Client({
    connectionString: db, // or your config object
  });
  // A concurrent causeChaos can terminate this backend too; without a listener that 'error' event crashes the process.
  client.on('error', () => {});

  try {
    await client.connect();

    await client.query(`
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE pid <> pg_backend_pid()
        AND datname = current_database();
    `);
  } catch (err) {
    //throw new Error(`Could not cause chaos, credentials insufficient? ${err as Error}`);
  } finally {
    try {
      await client.end();
    } catch (err) {}
  }
}
