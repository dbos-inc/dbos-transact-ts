// recv and sleep with `suspend: true` are Transact Enterprise features: without suspend mode the calls are
// refused. Waking a suspended recv is core: every send does it, so a sender without suspend mode still
// wakes a workflow another process parked. The operations themselves are tested in
// @dbos-inc/dbos-enterprise.

import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { DBOS, DBOSClient, DBOSConfig } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, testUnlessSharded } from './helpers';

const waiter = DBOS.registerWorkflow(
  async () => {
    return String(await DBOS.recv<string>('topic', { timeoutSeconds: 1, suspend: true }));
  },
  { name: 'suspendRefusedWaiter' },
);

const sleeper = DBOS.registerWorkflow(
  async () => {
    await DBOS.sleepSeconds(1, { suspend: true });
  },
  { name: 'suspendRefusedSleeper' },
);

const parked = DBOS.registerWorkflow(async () => Promise.resolve(), { name: 'suspendParked' });

// A deadline far enough out that only a wake-up clears it.
const PARKED_DELAY_MS = 2 ** 52;

/** Park a finished workflow by hand, as @dbos-inc/dbos-enterprise's recv would. */
async function parkByHand(db: Client, topic: string): Promise<string> {
  const workflowID = randomUUID();
  await (await DBOS.startWorkflow(parked, { workflowID })()).getResult();
  await db.query(
    `UPDATE dbos.workflow_status SET status = 'SUSPENDED', suspend_key = $1, delay_until_epoch_ms = $2
     WHERE workflow_uuid = $3`,
    [topic, PARKED_DELAY_MS, workflowID],
  );
  return workflowID;
}

async function parkedDelay(db: Client, workflowID: string): Promise<number> {
  const { rows } = await db.query<{ delay_until_epoch_ms: string }>(
    'SELECT delay_until_epoch_ms FROM dbos.workflow_status WHERE workflow_uuid = $1',
    [workflowID],
  );
  return Number(rows[0].delay_until_epoch_ms);
}

describe('suspend-refused', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('recv with suspend is refused without suspend mode', async () => {
    expect(DBOSExecutor.globalInstance!.systemDatabase.suspend).toBeUndefined();
    const handle = await DBOS.startWorkflow(waiter, { workflowID: randomUUID() })();
    await expect(handle.getResult()).rejects.toThrow('requires suspend mode');
  });

  test('sleep with suspend is refused without suspend mode', async () => {
    expect(DBOSExecutor.globalInstance!.systemDatabase.suspend).toBeUndefined();
    const handle = await DBOS.startWorkflow(sleeper, { workflowID: randomUUID() })();
    await expect(handle.getResult()).rejects.toThrow('requires suspend mode');
  });

  // Starts and sends without shard keys, and parks rows with unrouted SQL; enterprise tests the sharded case.
  testUnlessSharded('a send wakes suspended receivers without suspend mode', async () => {
    expect(DBOSExecutor.globalInstance!.systemDatabase.suspend).toBeUndefined();
    const db = new Client({ connectionString: config.systemDatabaseUrl });
    await db.connect();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const byApp = await parkByHand(db, 'approve');
      const byClient = await parkByHand(db, 'approve');
      await DBOS.send(byApp, 'no', 'other');
      expect(await parkedDelay(db, byApp)).toBe(PARKED_DELAY_MS);
      await DBOS.send(byApp, 'yes', 'approve');
      expect(await parkedDelay(db, byApp)).toBe(0);
      await client.send(byClient, 'yes', 'approve');
      expect(await parkedDelay(db, byClient)).toBe(0);
    } finally {
      await client.destroy();
      await db.end();
    }
  });
});
