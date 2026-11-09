// recv and sleep with `suspend: true` are Transact Enterprise features: without suspend mode the calls are
// refused. Waking a suspended recv is core: every send does it, so a sender without suspend mode still
// wakes a workflow another process parked. The operations themselves are tested in
// @dbos-inc/dbos-enterprise.

import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { DBOS, DBOSClient, DBOSConfig, DBOSWorkflowSuspendedError } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, retryUntilSuccess, setUpDBOSTestSysDb, testUnlessSharded } from './helpers';

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
async function parkByHand(db: Client, topic: string, deadlineMs: number | null = null): Promise<string> {
  const workflowID = randomUUID();
  await (await DBOS.startWorkflow(parked, { workflowID })()).getResult();
  await db.query(
    `UPDATE dbos.workflow_status SET status = 'SUSPENDED', suspend_key = $1, delay_until_epoch_ms = $2,
       workflow_deadline_epoch_ms = $4
     WHERE workflow_uuid = $3`,
    [topic, PARKED_DELAY_MS, workflowID, deadlineMs],
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

test('the suspended error names what the workflow waits on', () => {
  expect(new DBOSWorkflowSuspendedError('w', 'approve').message).toBe('Workflow w suspended waiting on topic approve');
  expect(new DBOSWorkflowSuspendedError('w', '__null__topic__').message).toBe(
    'Workflow w suspended waiting on a message with no topic',
  );
  expect(new DBOSWorkflowSuspendedError('w', undefined).message).toBe('Workflow w suspended sleeping');
});

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

  // Parks a row with unrouted SQL; enterprise tests the sharded case.
  testUnlessSharded('a parked workflow is still active: waitFirst skips it and queue listings show it', async () => {
    const db = new Client({ connectionString: config.systemDatabaseUrl });
    await db.connect();
    try {
      const workflowID = await parkByHand(db, 'approve');
      await db.query('UPDATE dbos.workflow_status SET queue_name = $1 WHERE workflow_uuid = $2', [
        'suspend-q',
        workflowID,
      ]);
      expect(await DBOSExecutor.globalInstance!.systemDatabase.checkFirstWorkflowId([workflowID])).toBeUndefined();
      const queued = await DBOS.listQueuedWorkflows({ queueName: 'suspend-q' });
      expect(queued.map((w) => [w.workflowID, w.status])).toEqual([[workflowID, 'SUSPENDED']]);
    } finally {
      await db.end();
    }
  });

  // Parks rows with unrouted SQL; enterprise tests the sharded case.
  testUnlessSharded('cancel, resume and the timeout sweep clear the suspend key', async () => {
    const db = new Client({ connectionString: config.systemDatabaseUrl });
    await db.connect();
    const row = async (workflowID: string) => {
      const { rows } = await db.query<{ status: string; suspend_key: string | null }>(
        'SELECT status, suspend_key FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [workflowID],
      );
      return rows[0];
    };
    try {
      const cancelled = await parkByHand(db, 'approve');
      await DBOS.cancelWorkflow(cancelled);
      expect(await row(cancelled)).toEqual({ status: 'CANCELLED', suspend_key: null });

      const resumed = await parkByHand(db, 'approve');
      await (await DBOS.resumeWorkflow(resumed)).getResult();
      expect(await row(resumed)).toEqual({ status: 'SUCCESS', suspend_key: null });

      const timedOut = await parkByHand(db, 'approve', 1);
      await retryUntilSuccess(async () => {
        await DBOSExecutor.globalInstance!.systemDatabase.cancelTimedOutWorkflows(10);
        expect(await row(timedOut)).toEqual({ status: 'CANCELLED', suspend_key: null });
      });
    } finally {
      await db.end();
    }
  });

  // Every send runs the wake update, so it must probe the primary key: planned with statistics that
  // predate the parked rows, it must not scan idx_workflow_status_suspended.
  testUnlessSharded('the wake update probes the primary key', async () => {
    // A PostgreSQL planner check.
    if (new URL(config.systemDatabaseUrl!).port === '26257') return;
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase as unknown as {
      wakeReceivers(db: ClientLike, destinations: string[], topics: string[]): Promise<void>;
    };
    const wake = sysdb.wakeReceivers.bind(sysdb);
    const captured: { text: string; params: unknown[] }[] = [];
    const spy = jest.spyOn(sysdb, 'wakeReceivers').mockImplementation((client, destinations, topics) =>
      wake(
        new Proxy(client, {
          get(target, prop, receiver) {
            if (prop !== 'query') return Reflect.get(target, prop, receiver) as unknown;
            return (text: string, params: unknown[]) => {
              captured.push({ text, params });
              return target.query(text, params);
            };
          },
        }),
        destinations,
        topics,
      ),
    );
    const db = new Client({ connectionString: config.systemDatabaseUrl });
    await db.connect();
    try {
      // Not parked, so the statistics gathered below see no SUSPENDED rows.
      const target = randomUUID();
      await (await DBOS.startWorkflow(parked, { workflowID: target })()).getResult();
      await DBOS.send(target, 'm', 'approve');
      spy.mockRestore();
      expect(captured).toHaveLength(1);

      // Analyzed before the rows are parked, the planner takes the suspended index for empty.
      await db.query('ANALYZE dbos.workflow_status');
      await db.query(
        `INSERT INTO dbos.workflow_status (workflow_uuid, name, status, created_at, updated_at, suspend_key, delay_until_epoch_ms)
         SELECT 'wake-plan-' || i || '-' || $1::text, 'wake_plan_probe', 'SUSPENDED', 1, 1, 'approve', $2::bigint
         FROM generate_series(1, 2000) AS i`,
        [randomUUID(), PARKED_DELAY_MS],
      );
      const plan = await db.query<{ 'QUERY PLAN': string }>(`EXPLAIN ${captured[0].text}`, captured[0].params);
      const text = plan.rows.map((r) => r['QUERY PLAN']).join('\n');
      expect(text).not.toContain('idx_workflow_status_suspended');
      expect(text).toContain('workflow_status_pkey');
    } finally {
      spy.mockRestore();
      await db.query(`DELETE FROM dbos.workflow_status WHERE name = 'wake_plan_probe'`);
      await db.end();
    }
  });
});

type ClientLike = { query(text: string, params: unknown[]): Promise<unknown> };
