// recv, sleep, getResult and waitFirst with `suspend: true` are Transact Enterprise features: without
// suspend mode the calls are refused. Waking a suspended workflow is core: every send wakes its receivers
// and every completion its result waiters, so a process without suspend mode still wakes a workflow
// another process parked. The operations themselves are tested in @dbos-inc/dbos-enterprise.

import { randomUUID } from 'node:crypto';
import { Client, Pool } from 'pg';
import { DBOS, DBOSClient, DBOSConfig, DBOSWorkflowSuspendedError } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import * as enterprise from '../src/enterprise';
import { resultSuspendKey, WAIT_FIRST_SUSPEND_KEY } from '../src/system_database';
import { Event, generateDBOSTestConfig, retryUntilSuccess, setUpDBOSTestSysDb, testUnlessSharded } from './helpers';

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

const resultWaiter = DBOS.registerWorkflow(
  async (workflowID: string, how: string) => {
    if (how === 'handle') return String(await DBOS.retrieveWorkflow(workflowID).getResult({ suspend: true }));
    if (how === 'waitFirst') {
      return (await DBOS.waitFirst([DBOS.retrieveWorkflow(workflowID)], { suspend: true })).workflowID;
    }
    return String(await DBOS.getResult(workflowID, { suspend: true }));
  },
  { name: 'suspendRefusedResultWaiter' },
);

let release = new Event();
const blockedTarget = DBOS.registerWorkflow(
  async (fail: boolean) => {
    await release.wait();
    if (fail) throw new Error('target failed');
    return Promise.resolve('done');
  },
  { name: 'suspendBlockedTarget' },
);

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
  expect(new DBOSWorkflowSuspendedError('w', undefined, ['a']).message).toBe(
    'Workflow w suspended awaiting the result of workflow a',
  );
  expect(new DBOSWorkflowSuspendedError('w', undefined, ['a', 'b']).message).toBe(
    'Workflow w suspended awaiting the first of 2 workflows',
  );
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

  test('getResult and waitFirst with suspend are refused without suspend mode or outside a workflow', async () => {
    expect(DBOSExecutor.globalInstance!.systemDatabase.suspend).toBeUndefined();
    const target = randomUUID();
    await (await DBOS.startWorkflow(parked, { workflowID: target })()).getResult();
    for (const how of ['handle', 'id', 'waitFirst']) {
      const handle = await DBOS.startWorkflow(resultWaiter, { workflowID: randomUUID() })(target, how);
      await expect(handle.getResult()).rejects.toThrow('requires suspend mode');
    }
    const outside = 'must be called from within a workflow';
    await expect(DBOS.retrieveWorkflow(target).getResult({ suspend: true })).rejects.toThrow(outside);
    await expect(DBOS.getResult(target, { suspend: true })).rejects.toThrow(outside);
    await expect(DBOS.waitFirst([DBOS.retrieveWorkflow(target)], { suspend: true })).rejects.toThrow(outside);
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await expect(client.retrieveWorkflow(target).getResult({ suspend: true })).rejects.toThrow(
        'A DBOSClient handle cannot suspend',
      );
    } finally {
      await client.destroy();
    }
  });

  test('getResult and waitFirst with suspend are refused when sharded or on CockroachDB', async () => {
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const target = randomUUID();
    await (await DBOS.startWorkflow(parked, { workflowID: target })()).getResult();
    sysdb.suspend = {} as enterprise.Suspend;
    try {
      Object.defineProperty(sysdb, 'sharded', { get: () => true, configurable: true });
      try {
        const handle = await DBOS.startWorkflow(resultWaiter, { workflowID: randomUUID() })(target, 'id');
        await expect(handle.getResult()).rejects.toThrow('getResult with suspend: true is not supported on a sharded');
      } finally {
        delete (sysdb as { sharded?: boolean }).sharded;
      }
      const spy = jest.spyOn(sysdb, 'isCockroach').mockResolvedValue(true);
      try {
        const handle = await DBOS.startWorkflow(resultWaiter, { workflowID: randomUUID() })(target, 'waitFirst');
        await expect(handle.getResult()).rejects.toThrow(
          'waitFirst with suspend: true is not supported on CockroachDB',
        );
      } finally {
        spy.mockRestore();
      }
    } finally {
      sysdb.suspend = undefined;
    }
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

  // Parks waiters by hand, as @dbos-inc/dbos-enterprise's getResult and waitFirst would; enterprise tests the sharded case.
  testUnlessSharded('every completion wakes its result waiters and keeps its list', async () => {
    // CockroachDB completions don't wake result waiters: getResult and waitFirst with suspend are refused there.
    if (new URL(config.systemDatabaseUrl!).port === '26257') return;
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const db = new Client({ connectionString: config.systemDatabaseUrl });
    await db.connect();
    release = new Event();
    const row = async (workflowID: string) => {
      const { rows } = await db.query<{ delay: string | null; waiters: string[] | null }>(
        'SELECT delay_until_epoch_ms AS delay, result_waiter_ids AS waiters FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [workflowID],
      );
      return rows[0];
    };
    const parkOnResult = async (suspendKey: string) => {
      const workflowID = await parkByHand(db, suspendKey);
      await db.query('UPDATE dbos.workflow_status SET delay_until_epoch_ms = NULL WHERE workflow_uuid = $1', [
        workflowID,
      ]);
      return workflowID;
    };
    // A running target with two waiters parked on it, one in waitFirst, and a listed one parked elsewhere.
    const start = async (fail: boolean = false) => {
      const handle = await DBOS.startWorkflow(blockedTarget, { workflowID: randomUUID() })(fail);
      const key = resultSuspendKey(handle.workflowID);
      const waiters = [await parkOnResult(key), await parkOnResult(key), await parkOnResult(WAIT_FIRST_SUSPEND_KEY)];
      const other = await parkOnResult(resultSuspendKey(randomUUID()));
      await db.query('UPDATE dbos.workflow_status SET result_waiter_ids = $1 WHERE workflow_uuid = $2', [
        [...waiters, other],
        handle.workflowID,
      ]);
      return { handle, waiters, other };
    };
    const check = async ({ handle, waiters, other }: Awaited<ReturnType<typeof start>>) => {
      expect(await Promise.all(waiters.map(async (w) => (await row(w)).delay))).toEqual(['0', '0', '0']);
      expect((await row(other)).delay).toBeNull();
      expect((await row(handle.workflowID)).waiters).toEqual([...waiters, other]);
    };
    try {
      // Each completes while the target still runs, except success and error, released at the end.
      const succeeded = await start();
      const failed = await start(true);
      const cancelled = await start();
      await DBOS.cancelWorkflow(cancelled.handle.workflowID);
      await check(cancelled);
      const dead = await start();
      await sysdb.deadLetterWorkflows([dead.handle.workflowID], 0);
      await check(dead);
      const timedOut = await start();
      await db.query('UPDATE dbos.workflow_status SET workflow_deadline_epoch_ms = 1 WHERE workflow_uuid = $1', [
        timedOut.handle.workflowID,
      ]);
      // The background timeout sweep may cancel it first; either sweep wakes its waiters.
      await retryUntilSuccess(async () => {
        await sysdb.cancelTimedOutWorkflows(100);
        expect((await DBOS.getWorkflowStatus(timedOut.handle.workflowID))?.status).toBe('CANCELLED');
      });
      await check(timedOut);
      release.set();
      await expect(succeeded.handle.getResult()).resolves.toBe('done');
      await check(succeeded);
      await expect(failed.handle.getResult()).rejects.toThrow('target failed');
      await check(failed);
      await Promise.allSettled([cancelled, dead, timedOut].map((t) => t.handle.getResult()));
    } finally {
      release.set();
      await db.end();
    }
  });

  // Every completion runs the waiter wake, so it must probe the primary key: planned with statistics that
  // predate many parked rows, no part of it may scan the table or another index.
  testUnlessSharded('the outcome wake probes the primary key', async () => {
    // A PostgreSQL planner check.
    if (new URL(config.systemDatabaseUrl!).port === '26257') return;
    const pool = (DBOSExecutor.globalInstance!.systemDatabase as unknown as { pool: Pool }).pool;
    const query = pool.query.bind(pool) as (text: string, params: unknown[]) => Promise<unknown>;
    const captured: { text: string; params: unknown[] }[] = [];
    const spy = jest.spyOn(pool, 'query').mockImplementation(((text: string, params: unknown[]) => {
      if (typeof text === 'string' && text.includes('woken AS')) captured.push({ text, params });
      return query(text, params);
    }) as never);
    const db = new Client({ connectionString: config.systemDatabaseUrl });
    await db.connect();
    try {
      await (await DBOS.startWorkflow(parked, { workflowID: randomUUID() })()).getResult();
      spy.mockRestore();
      expect(captured.length).toBeGreaterThan(0);

      // Analyzed before the rows are parked, the planner takes the table for near-empty.
      await db.query('ANALYZE dbos.workflow_status');
      await db.query(
        `INSERT INTO dbos.workflow_status (workflow_uuid, name, status, created_at, updated_at, suspend_key, result_waiter_ids)
         SELECT 'wake-plan-' || i || '-' || $1::text, 'wake_plan_probe', 'SUSPENDED', 1, 1,
                CASE WHEN i % 2 = 0 THEN $2::text ELSE $3::text END, ARRAY['other']
         FROM generate_series(1, 5000) AS i`,
        [randomUUID(), resultSuspendKey('other'), WAIT_FIRST_SUSPEND_KEY],
      );
      const { rows } = await db.query<{ 'QUERY PLAN': PlanNode[] }>(
        `EXPLAIN (FORMAT JSON) ${captured[0].text}`,
        captured[0].params,
      );
      const find = (node: PlanNode): PlanNode | undefined =>
        node['Subplan Name'] === 'CTE woken' ? node : (node.Plans ?? []).map(find).find((n) => n !== undefined);
      const woken = find(rows[0]['QUERY PLAN'][0].Plan!);
      expect(woken).toBeDefined();
      // Any node naming an index, so a Bitmap Index Scan, which has no relation, is checked too.
      const scans: [string, string][] = [];
      const collect = (node: PlanNode) => {
        if (
          node['Node Type'] !== 'ModifyTable' &&
          (node['Relation Name'] === 'workflow_status' || node['Index Name'] !== undefined)
        ) {
          scans.push([node['Node Type'], node['Index Name'] ?? '']);
        }
        (node.Plans ?? []).forEach(collect);
      };
      collect(woken!);
      expect(scans.length).toBeGreaterThan(0);
      for (const [nodeType, index] of scans) {
        expect(nodeType).not.toBe('Seq Scan');
        expect(['', 'workflow_status_pkey']).toContain(index);
      }
    } finally {
      spy.mockRestore();
      await db.query(`DELETE FROM dbos.workflow_status WHERE name = 'wake_plan_probe'`);
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

type PlanNode = {
  'Node Type': string;
  'Subplan Name'?: string;
  'Relation Name'?: string;
  'Index Name'?: string;
  Plans?: PlanNode[];
  Plan?: PlanNode;
};
