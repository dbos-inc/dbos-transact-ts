import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { DBOS, DBOSClient } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { sleepms } from '../src/utils';

class SQLiteQueueWorkflows {
  @DBOS.workflow()
  static async echo(input: string): Promise<string> {
    await Promise.resolve();
    return `sqlite:${input}`;
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

describe('SQLite system database', () => {
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

  test('runs database-backed queues on SQLite', async () => {
    await DBOS.launch();
    const queue = await DBOS.registerQueue('sqlite-test-queue', {
      onConflict: 'always_update',
      minPollingIntervalMs: 25,
    });
    await waitForQueueDiscovery(queue.name);

    const handle = await DBOS.startWorkflow(SQLiteQueueWorkflows, { queueName: queue.name }).echo('queue');

    await expect(handle.getResult({ pollingIntervalMs: 25 })).resolves.toBe('sqlite:queue');

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const persisted = await client.retrieveQueue(queue.name);
      expect(persisted?.name).toBe(queue.name);
      expect(await persisted?.getMinPollingIntervalMs()).toBe(25);

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
});
