import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { DBOS, DBOSClient } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { isNativeSQLiteSupported, SQLitePool } from '../src/sqlite_system_database';
import { sleepms } from '../src/utils';

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
