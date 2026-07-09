import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { DBOS, DBOSClient, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import {
  ensureSQLiteSystemDatabase,
  isNativeSQLiteSupported,
  resetSQLiteSystemDatabase,
  SQLitePool,
} from '../src/sqlite_system_database';
import { allMigrations } from '../src/sysdb_migrations/internal/migrations';
import { GlobalLogger } from '../src/telemetry/logs';
import { globalParams, sleepms } from '../src/utils';

class SQLiteQueueWorkflows {
  @DBOS.workflow()
  static async echo(input: string): Promise<string> {
    await Promise.resolve();
    return `sqlite:${input}`;
  }
}

class SQLiteForkWorkflows {
  @DBOS.workflow()
  static async twoStep(input: string): Promise<string> {
    const first = await DBOS.runStep(() => Promise.resolve(`${input}:first`), { name: 'sqlite-first-step' });
    return DBOS.runStep(() => Promise.resolve(`${first}:second`), { name: 'sqlite-second-step' });
  }
}

function sqliteUrlFor(filePath: string): string {
  return `sqlite:////${filePath.replace(/^\/+/, '')}`;
}

async function waitForQueueDiscovery(queueName: string): Promise<void> {
  const deadline = Date.now() + 5000;
  for (;;) {
    const queue = await DBOS.retrieveQueue(queueName);
    if (queue) return;
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for queue ${queueName}`);
    await sleepms(50);
  }
}

function getConfiguredSystemDatabaseUrl(config: DBOSConfig): string {
  if (config.systemDatabaseUrl === undefined) {
    throw new Error('SQLite tests require a configured system database URL');
  }
  return config.systemDatabaseUrl;
}

async function readSQLiteMigrationVersion(databaseUrl: string): Promise<number> {
  const pool = new SQLitePool(databaseUrl);
  try {
    const result = await pool.query<{ version: number }>('SELECT version FROM dbos_migrations');
    return Number(result.rows[0].version);
  } finally {
    await pool.end();
  }
}

const describeNativeSQLite = isNativeSQLiteSupported() ? describe : describe.skip;

describeNativeSQLite('SQLite system database', () => {
  let tempDir: string;
  let dbPath: string;
  let config: DBOSConfig;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dbos-sqlite-'));
    dbPath = path.join(tempDir, 'system.sqlite');
    config = {
      name: 'dbos-sqlite-test',
      systemDatabaseUrl: sqliteUrlFor(dbPath),
      runAdminServer: false,
      useListenNotify: false,
    };
    DBOS.setConfig(config);
  });

  afterEach(async () => {
    await DBOS.shutdown();
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  test('serializes logical clients on one SQLite connection', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    try {
      await pool.query('CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)');
      const firstClient = await pool.connect();
      await firstClient.query('BEGIN');
      await firstClient.query('INSERT INTO t(value) VALUES ($1)', ['uncommitted']);

      let secondClientReady = false;
      const secondClientPromise = pool.connect().then((client) => {
        secondClientReady = true;
        return client;
      });
      await sleepms(25);
      expect(secondClientReady).toBe(false);

      await firstClient.query('ROLLBACK');
      firstClient.release();

      const secondClient = await secondClientPromise;
      try {
        expect(secondClientReady).toBe(true);
        const rows = await secondClient.query('SELECT * FROM t');
        expect(rows.rows).toEqual([]);
      } finally {
        secondClient.release();
      }
    } finally {
      await pool.end();
    }
  });

  test('rejects pending logical clients when the pool ends', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    const firstClient = await pool.connect();
    const pendingClient = pool.connect();
    const pendingAssertion = expect(pendingClient).rejects.toThrow('SQLite system database pool has ended');

    try {
      await pool.end();
      await pendingAssertion;
    } finally {
      firstClient.release();
    }
  });

  test('reports one effective native connection even when configured larger', async () => {
    const pool = new SQLitePool('sqlite:///:memory:', 'dbos', 8);
    try {
      expect(pool.options.max).toBe(1);
    } finally {
      await pool.end();
    }
  });

  test('uses serialized SQLite pool size for polling limits', async () => {
    const client = await DBOSClient.create({
      systemDatabaseUrl: getConfiguredSystemDatabaseUrl(config),
      systemDatabasePoolSize: 8,
      systemDatabasePollingConcurrency: 4,
    });

    try {
      const sysdb = client['systemDatabase'];
      expect(sysdb.pool.options.max).toBe(1);
      expect(sysdb.pollLimiter['available']).toBe(1);
    } finally {
      await client.destroy();
    }
  });

  test('reset removes SQLite database files and WAL sidecars', () => {
    const resetDbPath = path.join(tempDir, 'reset.sqlite');
    for (const filePath of [resetDbPath, `${resetDbPath}-wal`, `${resetDbPath}-shm`]) {
      fs.writeFileSync(filePath, 'sqlite-test');
    }

    resetSQLiteSystemDatabase(sqliteUrlFor(resetDbPath));

    expect(fs.existsSync(resetDbPath)).toBe(false);
    expect(fs.existsSync(`${resetDbPath}-wal`)).toBe(false);
    expect(fs.existsSync(`${resetDbPath}-shm`)).toBe(false);
  });

  test('reset removes SQLite WAL sidecars when the main database is absent', () => {
    const resetDbPath = path.join(tempDir, 'sidecar-only.sqlite');
    fs.writeFileSync(`${resetDbPath}-wal`, 'sqlite-test');
    fs.writeFileSync(`${resetDbPath}-shm`, 'sqlite-test');

    resetSQLiteSystemDatabase(sqliteUrlFor(resetDbPath));

    expect(fs.existsSync(`${resetDbPath}-wal`)).toBe(false);
    expect(fs.existsSync(`${resetDbPath}-shm`)).toBe(false);
  });

  test('reset treats in-memory SQLite databases as a no-op', () => {
    expect(() => resetSQLiteSystemDatabase('sqlite:///:memory:')).not.toThrow();
  });

  test('initializes fresh SQLite databases with the current migration version', async () => {
    const migrationDbUrl = sqliteUrlFor(path.join(tempDir, 'fresh-migrations.sqlite'));
    await ensureSQLiteSystemDatabase(migrationDbUrl, new GlobalLogger());

    const pool = new SQLitePool(migrationDbUrl);
    try {
      const tables = await pool.query<{ name: string }>(
        `SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ($1, $2, $3) ORDER BY name`,
        ['dbos_migrations', 'queues', 'workflow_status'],
      );
      expect(tables.rows.map((row) => row.name)).toEqual(['dbos_migrations', 'queues', 'workflow_status']);
      expect(await readSQLiteMigrationVersion(migrationDbUrl)).toBe(allMigrations().length);
    } finally {
      await pool.end();
    }
  });

  test('rerunning SQLite migrations preserves existing data and version', async () => {
    const migrationDbUrl = sqliteUrlFor(path.join(tempDir, 'idempotent-migrations.sqlite'));
    await ensureSQLiteSystemDatabase(migrationDbUrl, new GlobalLogger());

    const pool = new SQLitePool(migrationDbUrl);
    try {
      await pool.query(`INSERT INTO queues(name, polling_interval_sec) VALUES ($1, $2)`, ['preserved-queue', 1]);
    } finally {
      await pool.end();
    }

    await ensureSQLiteSystemDatabase(migrationDbUrl, new GlobalLogger());

    const verifyPool = new SQLitePool(migrationDbUrl);
    try {
      const rows = await verifyPool.query<{ name: string }>('SELECT name FROM queues WHERE name = $1', [
        'preserved-queue',
      ]);
      expect(rows.rows).toEqual([{ name: 'preserved-queue' }]);
      expect(await readSQLiteMigrationVersion(migrationDbUrl)).toBe(allMigrations().length);
    } finally {
      await verifyPool.end();
    }
  });

  test('reapplies the SQLite baseline when the recorded version is behind', async () => {
    const migrationDbUrl = sqliteUrlFor(path.join(tempDir, 'behind-migrations.sqlite'));
    await ensureSQLiteSystemDatabase(migrationDbUrl, new GlobalLogger());

    const pool = new SQLitePool(migrationDbUrl);
    try {
      await pool.query(`INSERT INTO queues(name, polling_interval_sec) VALUES ($1, $2)`, ['behind-queue', 1]);
      await pool.query(`UPDATE dbos_migrations SET version = $1`, [0]);
    } finally {
      await pool.end();
    }

    await ensureSQLiteSystemDatabase(migrationDbUrl, new GlobalLogger());

    const verifyPool = new SQLitePool(migrationDbUrl);
    try {
      const rows = await verifyPool.query<{ name: string }>('SELECT name FROM queues WHERE name = $1', [
        'behind-queue',
      ]);
      expect(rows.rows).toEqual([{ name: 'behind-queue' }]);
      expect(await readSQLiteMigrationVersion(migrationDbUrl)).toBe(allMigrations().length);
    } finally {
      await verifyPool.end();
    }
  });

  test('queue claim respects a per-poll dispatch budget on SQLite', async () => {
    DBOS.setConfig({ ...config, listenQueues: [] });
    await DBOS.launch();
    const queue = await DBOS.registerQueue('sqlite-budget-queue', { minPollingIntervalMs: 60000 });
    const handles = await Promise.all(
      Array.from({ length: 5 }, (_, index) =>
        DBOS.startWorkflow(SQLiteQueueWorkflows, {
          workflowID: `sqlite-budget-${index}`,
          queueName: queue.name,
        }).echo(`budget-${index}`),
      ),
    );

    const exec = DBOSExecutor.globalInstance!;
    const firstClaim = await exec.systemDatabase.findAndMarkStartableWorkflows(
      queue,
      exec.executorID,
      globalParams.appVersion,
      undefined,
      2,
    );
    expect(firstClaim).toHaveLength(2);

    const statusesAfterFirstClaim = await Promise.all(handles.map((handle) => handle.getStatus()));
    expect(statusesAfterFirstClaim.filter((status) => status?.status === StatusString.PENDING)).toHaveLength(2);
    expect(statusesAfterFirstClaim.filter((status) => status?.status === StatusString.ENQUEUED)).toHaveLength(3);

    const secondClaim = await exec.systemDatabase.findAndMarkStartableWorkflows(
      queue,
      exec.executorID,
      globalParams.appVersion,
      undefined,
      2,
    );
    expect(secondClaim).toHaveLength(2);

    const finalClaim = await exec.systemDatabase.findAndMarkStartableWorkflows(
      queue,
      exec.executorID,
      globalParams.appVersion,
      undefined,
      2,
    );
    expect(finalClaim).toHaveLength(1);

    for (const workflowID of [...firstClaim, ...secondClaim, ...finalClaim]) {
      await exec.executeWorkflowId(workflowID, { isQueueDispatch: true });
    }

    await expect(Promise.all(handles.map((handle) => handle.getResult({ pollingIntervalMs: 25 })))).resolves.toEqual([
      'sqlite:budget-0',
      'sqlite:budget-1',
      'sqlite:budget-2',
      'sqlite:budget-3',
      'sqlite:budget-4',
    ]);
  }, 10000);

  test('translates schema qualifiers, casts, locks, ANY arrays, and booleans', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    try {
      await pool.query('CREATE TABLE "dbos"."translation_items" (id INT4 PRIMARY KEY, value TEXT, flag INTEGER)');
      await pool.query('INSERT INTO dbos.translation_items (id, value, flag) VALUES ($1::int, $2::text, TRUE)', [
        1,
        'alpha',
      ]);
      await pool.query('INSERT INTO dbos.translation_items (id, value, flag) VALUES ($1::int, $2::text, FALSE)', [
        2,
        'beta',
      ]);

      const rows = await pool.query<{ id: number; value: string; flag: number }>(
        'SELECT id, value, flag FROM "dbos"."translation_items" WHERE value = ANY($1::text[]) ORDER BY id FOR UPDATE SKIP LOCKED',
        [['alpha', 'gamma']],
      );
      expect(rows.rows).toEqual([{ id: 1, value: 'alpha', flag: 1 }]);

      const emptyRows = await pool.query<{ id: number }>(
        'SELECT id FROM "dbos"."translation_items" WHERE value = ANY($1::text[]) FOR UPDATE NOWAIT',
        [[]],
      );
      expect(emptyRows.rows).toEqual([]);
    } finally {
      await pool.end();
    }
  });

  test('runs isolation-level BEGIN statements through SQLite transactions', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    const client = await pool.connect();
    try {
      await client.query('CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)');
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
      await client.query('INSERT INTO t(value) VALUES ($1::text)', ['rolled-back']);
      await client.query('ROLLBACK');
      const rows = await client.query<{ value: string }>('SELECT value FROM t');
      expect(rows.rows).toEqual([]);
    } finally {
      client.release();
      await pool.end();
    }
  });

  test('translates RETURNING table qualifiers', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    try {
      await pool.query('CREATE TABLE notifications (message_uuid TEXT PRIMARY KEY, message TEXT)');
      const result = await pool.query<{ message_uuid: string }>(
        'INSERT INTO notifications(message_uuid, message) VALUES ($1, $2) RETURNING notifications.message_uuid',
        ['message-id', 'payload'],
      );
      expect(result.rows).toEqual([{ message_uuid: 'message-id' }]);
    } finally {
      await pool.end();
    }
  });

  test('translates event dispatch GREATEST upsert expressions', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    try {
      await pool.query(
        'CREATE TABLE event_dispatch_kv (key TEXT PRIMARY KEY, update_time INTEGER, update_seq INTEGER)',
      );
      await pool.query('INSERT INTO event_dispatch_kv(key, update_time, update_seq) VALUES ($1, $2, $3)', [
        'event',
        10,
        2,
      ]);
      await pool.query(
        `INSERT INTO event_dispatch_kv(key, update_time, update_seq) VALUES ($1, $2, $3)
         ON CONFLICT(key) DO UPDATE SET
           update_time = GREATEST(EXCLUDED.update_time, event_dispatch_kv.update_time),
           update_seq = GREATEST(EXCLUDED.update_seq, event_dispatch_kv.update_seq)`,
        ['event', 7, 5],
      );

      const rows = await pool.query<{ update_time: number; update_seq: number }>(
        'SELECT update_time, update_seq FROM event_dispatch_kv WHERE key = $1',
        ['event'],
      );
      expect(rows.rows).toEqual([{ update_time: 10, update_seq: 5 }]);
    } finally {
      await pool.end();
    }
  });

  test('rejects SQLite attribute containment filters explicitly', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    try {
      await expect(
        pool.query('SELECT workflow_uuid FROM workflow_status WHERE attributes @> $1::jsonb', [
          JSON.stringify({ plan: 'sqlite' }),
        ]),
      ).rejects.toThrow('Filtering workflows by attributes is not supported on SQLite');
    } finally {
      await pool.end();
    }
  });

  test('runs WITH INSERT statements on SQLite', async () => {
    const pool = new SQLitePool('sqlite:///:memory:');
    try {
      await pool.query('CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT, count INTEGER)');
      await pool.query(
        'WITH mapping(value, count) AS (VALUES ($1::text, $2::int)) INSERT INTO t(value, count) SELECT value, count FROM mapping',
        ['from-cte', 7],
      );
      const rows = await pool.query<{ value: string; count: number }>('SELECT value, count FROM t');
      expect(rows.rows).toEqual([{ value: 'from-cte', count: 7 }]);
    } finally {
      await pool.end();
    }
  });

  test('runs database-backed queues on SQLite', async () => {
    await DBOS.launch();
    const queue = await DBOS.registerQueue('sqlite-test-queue', {
      onConflict: 'always_update',
      minPollingIntervalMs: 25,
    });
    await waitForQueueDiscovery(queue.name);

    const handle = await DBOS.startWorkflow(SQLiteQueueWorkflows, { queueName: queue.name }).echo('queue');

    await expect(handle.getResult({ pollingIntervalMs: 25 })).resolves.toBe('sqlite:queue');

    const client = await DBOSClient.create({ systemDatabaseUrl: getConfiguredSystemDatabaseUrl(config) });
    try {
      const persisted = await client.retrieveQueue(queue.name);
      if (persisted === null) {
        throw new Error(`Expected queue ${queue.name} to be persisted`);
      }
      expect(persisted.name).toBe(queue.name);
      expect(await persisted.getMinPollingIntervalMs()).toBe(25);

      const dbosQueues = await DBOS.listQueues();
      expect(dbosQueues.map((q) => q.name)).toContain(queue.name);
      const clientQueues = await client.listQueues();
      expect(clientQueues.map((q) => q.name)).toContain(queue.name);
    } finally {
      await client.destroy();
    }
  }, 10000);

  test('runs partitioned database-backed queues on SQLite', async () => {
    await DBOS.launch();
    const queue = await DBOS.registerQueue('sqlite-partitioned-queue', {
      onConflict: 'always_update',
      partitionQueue: true,
      workerConcurrency: 1,
      minPollingIntervalMs: 25,
    });
    await waitForQueueDiscovery(queue.name);

    const handle = await DBOS.startWorkflow(SQLiteQueueWorkflows, {
      queueName: queue.name,
      enqueueOptions: { queuePartitionKey: 'partition-a' },
    }).echo('partition');

    await expect(handle.getResult({ pollingIntervalMs: 25 })).resolves.toBe('sqlite:partition');
    const status = await handle.getStatus();
    expect(status?.queuePartitionKey).toBe('partition-a');
  }, 10000);

  test('returns boolean queue configuration values from SQLite', async () => {
    await DBOS.launch();
    const queue = await DBOS.registerQueue('sqlite-bool-queue', {
      onConflict: 'always_update',
      priorityEnabled: true,
      partitionQueue: true,
    });

    expect(await queue.getPriorityEnabled()).toBe(true);
    expect(await queue.getPartitionQueue()).toBe(true);

    const client = await DBOSClient.create({ systemDatabaseUrl: getConfiguredSystemDatabaseUrl(config) });
    try {
      const persisted = await client.retrieveQueue(queue.name);
      if (persisted === null) {
        throw new Error(`Expected queue ${queue.name} to be persisted`);
      }
      expect(await persisted.getPriorityEnabled()).toBe(true);
      expect(await persisted.getPartitionQueue()).toBe(true);
    } finally {
      await client.destroy();
    }
  }, 10000);

  test('forks workflows from copied step state on SQLite', async () => {
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(SQLiteForkWorkflows).twoStep('fork');
    await expect(handle.getResult()).resolves.toBe('fork:first:second');

    const forked = await DBOS.forkWorkflow(handle.workflowID, 1);
    await expect(forked.getResult()).resolves.toBe('fork:first:second');
  }, 10000);
});
