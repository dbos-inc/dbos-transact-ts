import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { DBOS, StatusString, type WorkflowHandle } from '../src';
import { getClientConfig, INTERNAL_QUEUE_NAME, sleepms } from '../src/utils';
import { translateDbosConfig } from '../src/config';
import { ensureSystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { deriveDatabaseUrl, dropPGDatabase, ensurePGDatabase, maskDatabaseUrl } from '../src/database_utils';
import { Client } from 'pg';
import { randomUUID } from 'node:crypto';
import http from 'node:http';
import type { AddressInfo } from 'node:net';

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

/* Sharded test mode */

/** Whether the system database is a sharded one behind a router (requires @dbos-inc/dbos-enterprise). */
export function usingSharding(): boolean {
  return process.env.DBOS_DATABASE === 'SHARDED';
}

export function shardedSystemUrl(): string {
  return process.env.DBOS_SHARDED_SYSTEM_URL ?? 'postgresql://postgres:dbos@localhost:6432/dbos';
}

// Worker processes import this file outside jest, where `test` and `describe` are undefined.
const jestTest = typeof test === 'undefined' ? undefined : test;
const jestDescribe = typeof describe === 'undefined' ? undefined : describe;

/** For tests a sharded run can't serve: refused features, writes to unkeyed tables, own databases, batch timing. */
export const testUnlessSharded = (usingSharding() ? jestTest?.skip : jestTest) as jest.It;
export const describeUnlessSharded = (usingSharding() ? jestDescribe?.skip : jestDescribe) as jest.Describe;

/** The local test database: the system database of an unsharded run, and the user database of either. */
export function localTestDatabaseUrl(): string {
  const dbPassword = process.env.DB_PASSWORD || process.env.PGPASSWORD || 'dbos';
  const url = new URL(
    process.env.DBOS_TEST_DB_URL ?? `postgresql://postgres:${dbPassword}@localhost:5432/dbostest?sslmode=disable`,
  );
  // DBOS_TEST_DB_URL names the test database; the system database is named after it.
  url.pathname = `${url.pathname}_dbos_sys`;
  return url.toString();
}

/** Shard keys the sharded run's harness saw core lose on its own service paths. */
export function lostShardKeys(): string[] {
  const harness = (globalThis as { dbosShardedHarness?: { lostShardKeys: string[] } }).dbosShardedHarness;
  if (harness === undefined) {
    throw new Error(
      "A sharded run needs @dbos-inc/dbos-enterprise's harness: run jest with " +
        '-c ../dbos-enterprise-ts/tests/sharding/core.jest.config.js',
    );
  }
  return harness.lostShardKeys;
}

export function generateDBOSTestConfig(): DBOSConfig {
  if (usingSharding()) {
    // The harness supplies the keys core's tests don't pass.
    lostShardKeys();
    return { name: 'dbostest', systemDatabaseUrl: shardedSystemUrl(), sharding: true };
  }
  const url = localTestDatabaseUrl();
  return {
    name: 'dbostest',
    systemDatabaseUrl: url,
    ...(new URL(url).port === '26257' ? { useListenNotify: false } : {}),
  };
}

/** Drop the sharded schemas through the router, which runs DDL on every shard. */
async function resetShardedSystemDatabase(systemDatabaseUrl: string) {
  const client = new Client(getClientConfig(systemDatabaseUrl));
  await client.connect();
  try {
    // Only schemas DBOS created, including custom system schemas a test configured; a coordinator's own stay.
    const { rows } = await client.query<{ table_schema: string }>(
      `SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_name = 'dbos_migrations'
       UNION SELECT 'dbos'`,
    );
    for (const { table_schema } of rows) {
      await client.query(`DROP SCHEMA IF EXISTS "${table_schema.replaceAll('"', '""')}" CASCADE`);
    }
  } finally {
    await client.end();
  }
}

export async function setUpDBOSTestSysDb(config: DBOSConfig) {
  config.name ??= 'dbostest';
  const internalConfig = translateDbosConfig(config);

  if (internalConfig.sharding) {
    await resetShardedSystemDatabase(internalConfig.systemDatabaseUrl);
    // The user database stays plain local Postgres, reset as in any other run.
    await dropPGDatabase(localTestDatabaseUrl(), silentDropLogger);
    await ensureTestDatabase(localTestDatabaseUrl());
    await DBOS.migrate(internalConfig.systemDatabaseUrl, {
      schemaName: internalConfig.systemDatabaseSchemaName,
      sharding: true,
    });
    return;
  }

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

// Poll `check` until it stops throwing, rethrowing its last failure if the deadline passes.
/** Expect `call` to fail with an error of this class whose message matches. */
export async function expectError(
  call: Promise<unknown> | (() => unknown),
  errorClass: abstract new (...args: never[]) => Error,
  message: string | RegExp,
): Promise<void> {
  let error: unknown;
  try {
    await (typeof call === 'function' ? call() : call);
  } catch (e) {
    error = e;
  }
  expect(error).toBeInstanceOf(errorClass);
  expect((error as Error).message).toMatch(message);
}

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

/** Dispatch a workflow off its persisted row, marking it PENDING and taking ownership with a fresh token as a queue claim does. */
export async function redispatchWorkflowById(workflowId: string): Promise<WorkflowHandle<unknown>> {
  const exec = DBOSExecutor.globalInstance!;
  const ownerXid = randomUUID();
  await exec.systemDatabase.setWorkflowStatus(workflowId, StatusString.PENDING, false, { ownerXid });
  const status = await exec.systemDatabase.getWorkflowStatus(workflowId);
  expect(status).not.toBeNull();
  return await exec.executeDequeuedWorkflow(status!, ownerXid);
}

/**
 * Bound a polling handle's wait. The queue logs dispatch failures rather than throwing them, so an
 * unregistered function or an undeserializable input would otherwise leave the row PENDING and poll
 * until the test times out with no indication of the cause.
 */
function withDispatchDeadline<R>(handle: WorkflowHandle<R>, timeoutMs: number): WorkflowHandle<R> {
  return {
    get workflowID() {
      return handle.workflowID;
    },
    getStatus: () => handle.getStatus(),
    getResult: async (options?: Parameters<WorkflowHandle<R>['getResult']>[0]) => {
      const expired = Symbol('expired');
      const pending = handle.getResult(options);
      // The loser of the race keeps polling; swallow it so it cannot surface as an unhandled rejection.
      pending.catch(() => {});
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const raced = await Promise.race([
          pending,
          new Promise<typeof expired>((resolve) => {
            timer = setTimeout(() => resolve(expired), timeoutMs);
          }),
        ]);
        if (raced !== expired) return raced;
      } finally {
        clearTimeout(timer);
      }
      const status = await handle.getStatus();
      throw new Error(
        `Re-executed workflow ${handle.workflowID} produced no result within ${timeoutMs}ms; ` +
          `its status is ${status?.status ?? 'missing'}. A dispatch failure is logged, not thrown — check the queue's warnings.`,
      );
    },
  };
}

// Re-run a workflow the way recovery does: re-enqueue the row and let the queue dispatch it.
export async function reexecuteWorkflowById(
  workflowId: string,
  resetRecoveryAttempts: boolean = true,
  updateName?: string,
  timeoutMs: number = 30000,
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
  return withDispatchDeadline(DBOS.retrieveWorkflow(workflowId), timeoutMs);
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

/* Entitlements */

// Every entitlement Conductor reports today.
export const ALL_ENTITLED: Record<string, boolean> = { sharding: true, autoUpgrade: true, suspendWorkflows: true };

/**
 * A local stand-in for Conductor's entitlements route. Tests name it as their conductorURL,
 * and the SDK finds /v2/entitlements beside it.
 */
export class FakeConductor {
  status = 200;
  body: unknown = { entitlements: { ...ALL_ENTITLED } };
  headers: Record<string, string> = {};
  readonly requests: Array<{ path: string; authorization: string | undefined }> = [];
  readonly #server: http.Server;
  #url = '';

  private constructor() {
    this.#server = http.createServer((req, res) => {
      this.requests.push({ path: req.url ?? '', authorization: req.headers.authorization });
      const payload = typeof this.body === 'string' ? this.body : JSON.stringify(this.body);
      res.writeHead(this.status, { 'Content-Type': 'application/json', ...this.headers });
      res.end(payload);
    });
  }

  static async start(): Promise<FakeConductor> {
    const fake = new FakeConductor();
    await new Promise<void>((resolve) => fake.#server.listen(0, '127.0.0.1', resolve));
    const address = fake.#server.address() as AddressInfo;
    fake.#url = `ws://127.0.0.1:${address.port}`;
    return fake;
  }

  /** The Conductor URL to configure, which the SDK derives the entitlement route from. */
  get url(): string {
    return this.#url;
  }

  /** Answer every request with a problem detail. */
  reject(status: number, detail: string, headers: Record<string, string> = {}): void {
    this.status = status;
    this.body = { title: 'Rejected', status, detail };
    this.headers = headers;
  }

  /** Grant exactly these entitlements. */
  entitle(entitlements: Record<string, unknown>): void {
    this.status = 200;
    this.body = { entitlements };
    this.headers = {};
  }

  stop(): Promise<void> {
    return new Promise((resolve) => {
      this.#server.closeAllConnections();
      this.#server.close(() => resolve());
    });
  }
}
