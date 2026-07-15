import { randomUUID } from 'node:crypto';

import { DBOS, StatusString } from '../src';
import { runWithTopContext } from '../src/context';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { getOrCreateQueue, initWorkflows, prepareEnqueuedWorkflow, PreparedWorkflow } from '../src/eventreceiver';
import { DBOSNotRegisteredError } from '../src/error';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

// Batch enqueue, as used by event receivers (e.g. the Kafka consumers). These tests exercise
// prepareEnqueuedWorkflow + initWorkflows directly; nothing here needs a broker.

async function batchWorkflow(_value: string) {
  await Promise.resolve();
}
const batchWf = DBOS.registerWorkflow(batchWorkflow, { name: 'batchWorkflow' });

async function unregisteredWorkflow(_value: string) {
  await Promise.resolve();
}

/** An undeclared queue: rows stay ENQUEUED, so nothing executes them out from under the test. */
function unpolledQueue() {
  return `unpolled-${randomUUID()}`;
}

async function createdAtByWorkflowID(prefix: string): Promise<Map<string, { createdAt: number; key: string | null }>> {
  const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
  const { rows } = await sysdb.pool.query<{
    workflow_uuid: string;
    created_at: string;
    queue_partition_key: string | null;
  }>(
    `SELECT workflow_uuid, created_at, queue_partition_key
     FROM "${sysdb.schemaName}".workflow_status WHERE workflow_uuid LIKE $1`,
    [`${prefix}-%`],
  );
  const byID = new Map<string, { createdAt: number; key: string | null }>();
  for (const row of rows) {
    byID.set(row.workflow_uuid, { createdAt: Number(row.created_at), key: row.queue_partition_key });
  }
  return byID;
}

describe('batch-enqueue', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('prepareEnqueuedWorkflow builds an ENQUEUED row, ignoring the ambient context', async () => {
    const partitioned = getOrCreateQueue(`ctx-part-${randomUUID()}`, { partitionQueue: true });
    // Build under an authenticated context. The normal enqueue path copies these fields off the
    // context store; this one documents that it does not, so the context must be present for the
    // assertions below to mean anything.
    const status = await runWithTopContext(
      {
        authenticatedUser: 'alice',
        authenticatedRoles: ['admin'],
        assumedRole: 'admin',
        request: { url: '/orders' },
      },
      async () =>
        await prepareEnqueuedWorkflow(batchWf, ['hi'], {
          queueName: partitioned.name,
          workflowID: `w-${randomUUID()}`,
          queuePartitionKey: 'pk',
        }),
    );
    expect(status.status).toBe(StatusString.ENQUEUED);
    expect(status.queueName).toBe(partitioned.name);
    expect(status.queuePartitionKey).toBe('pk');
    expect(status.workflowName).toBe('batchWorkflow');
    // Nothing leaked from the surrounding context.
    expect(status.authenticatedUser).toBe('');
    expect(status.assumedRole).toBe('');
    expect(status.authenticatedRoles).toEqual([]);
    expect(status.request).toEqual({});
    expect(status.parentWorkflowID).toBeUndefined();
    expect(status.attributes).toBeUndefined();
  });

  test('prepareEnqueuedWorkflow rejects an unregistered function', async () => {
    await expect(
      prepareEnqueuedWorkflow(unregisteredWorkflow, ['a'], { queueName: 'q', workflowID: 'w-unreg' }),
    ).rejects.toThrow(DBOSNotRegisteredError);
  });

  test('initWorkflows is idempotent under redelivery', async () => {
    const queueName = unpolledQueue();
    const prefix = `batch-${randomUUID()}`;
    const build = async (): Promise<PreparedWorkflow[]> =>
      await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          prepareEnqueuedWorkflow(batchWf, [`value-${i}`], { queueName, workflowID: `${prefix}-${i}` }),
        ),
      );

    const inserted = await initWorkflows(await build());
    expect(inserted).toEqual(new Set(Array.from({ length: 5 }, (_, i) => `${prefix}-${i}`)));

    // Redelivering the same batch inserts nothing.
    expect(await initWorkflows(await build())).toEqual(new Set());

    // A partially-redelivered batch inserts only the new row.
    const partial = await build();
    partial[0].workflowUUID = `${prefix}-new`;
    expect(await initWorkflows(partial)).toEqual(new Set([`${prefix}-new`]));
  });

  test('initWorkflows rejects rows it cannot durably enqueue', async () => {
    expect(await initWorkflows([])).toEqual(new Set());

    const queueName = unpolledQueue();
    const notEnqueued = await prepareEnqueuedWorkflow(batchWf, ['v'], {
      queueName,
      workflowID: `bad-${randomUUID()}`,
    });
    notEnqueued.status = StatusString.PENDING;
    await expect(initWorkflows([notEnqueued])).rejects.toThrow(/only accepts ENQUEUED/);

    const deduped = await prepareEnqueuedWorkflow(batchWf, ['v'], { queueName, workflowID: `dd-${randomUUID()}` });
    deduped.deduplicationID = 'some-dedup-id';
    await expect(initWorkflows([deduped])).rejects.toThrow(/does not support deduplication IDs/);
  });

  test('unordered rows get wall-clock created_at and never touch the per-key cursors', async () => {
    const queueName = unpolledQueue();
    const prefix = `none-${randomUUID()}`;
    const before = Date.now();
    const rows = await Promise.all(
      Array.from({ length: 3 }, (_, i) =>
        prepareEnqueuedWorkflow(batchWf, [String(i)], { queueName, workflowID: `${prefix}-${i}` }),
      ),
    );
    expect(await initWorkflows(rows)).toEqual(new Set(Array.from({ length: 3 }, (_, i) => `${prefix}-${i}`)));

    const byID = await createdAtByWorkflowID(prefix);
    expect(byID.size).toBe(3);
    for (const { createdAt, key } of byID.values()) {
      expect(key).toBeNull();
      expect(createdAt).toBeGreaterThanOrEqual(before);
      expect(createdAt).toBeLessThanOrEqual(Date.now());
    }
  });

  test('a large batch stays created_at-monotonic per key across the insert chunk boundary', async () => {
    // num_keys * per_key = 1200 rows, past the 500-row chunk boundary.
    const queueName = unpolledQueue();
    const prefix = `bigbatch-${randomUUID()}`;
    const numKeys = 3;
    const perKey = 400;

    const statuses: PreparedWorkflow[] = [];
    for (let i = 0; i < perKey; i++) {
      for (let k = 0; k < numKeys; k++) {
        // Interleave keys so each key's rows span the whole batch.
        statuses.push(
          await prepareEnqueuedWorkflow(batchWf, [`${k}-${i}`], {
            queueName,
            workflowID: `${prefix}-${k}-${i}`,
            queuePartitionKey: `${prefix}-key-${k}`,
          }),
        );
      }
    }
    expect((await initWorkflows(statuses)).size).toBe(numKeys * perKey);

    const byID = await createdAtByWorkflowID(prefix);
    expect(byID.size).toBe(numKeys * perKey);

    const allCreated: number[] = [];
    for (let k = 0; k < numKeys; k++) {
      const pairs: { createdAt: number; offset: number }[] = [];
      for (let i = 0; i < perKey; i++) {
        const row = byID.get(`${prefix}-${k}-${i}`)!;
        expect(row.key).toBe(`${prefix}-key-${k}`);
        pairs.push({ createdAt: row.createdAt, offset: i });
        allCreated.push(row.createdAt);
      }
      pairs.sort((a, b) => a.createdAt - b.createdAt);
      // created_at order equals enqueue order, strictly increasing with no ties.
      expect(pairs.map((p) => p.offset)).toEqual(Array.from({ length: perKey }, (_, i) => i));
      expect(new Set(pairs.map((p) => p.createdAt)).size).toBe(perKey);
    }
    // Per-key cursors keep keys decoupled: all rows fit one perKey-wide window
    // (a process-wide cursor would span numKeys * perKey).
    expect(Math.max(...allCreated) - Math.min(...allCreated)).toBe(perKey - 1);
  });

  test('a fresh owner re-seeds its created_at cursor from the database', async () => {
    // Regression: a restart/rebalance must re-seed from the DB high-water mark. The backlog is
    // forced an hour into the future, so a fresh owner that fell back to wall-clock would sort
    // its newer rows *below* the backlog and invert per-key order.
    const queueName = unpolledQueue();
    const prefix = `restart-${randomUUID()}`;
    const key = `${prefix}-key`;
    const build = async (from: number, to: number): Promise<PreparedWorkflow[]> =>
      await Promise.all(
        Array.from({ length: to - from }, (_, n) =>
          prepareEnqueuedWorkflow(batchWf, [`value-${from + n}`], {
            queueName,
            workflowID: `${prefix}-${from + n}`,
            queuePartitionKey: key,
          }),
        ),
      );

    expect((await initWorkflows(await build(0, 5))).size).toBe(5);

    // Drift the backlog an hour ahead: wall-clock alone can no longer order past it.
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const future = Date.now() + 3_600_000;
    await sysdb.pool.query(
      `UPDATE "${sysdb.schemaName}".workflow_status
       SET created_at = $1 + CAST(SUBSTRING(workflow_uuid FROM '[0-9]+$') AS BIGINT)
       WHERE workflow_uuid LIKE $2`,
      [future, `${prefix}-%`],
    );

    // Simulate a fresh owner: shutting down and relaunching drops the in-memory cursors along with
    // the old SystemDatabase instance, so the next batch can only order correctly by re-seeding.
    await DBOS.shutdown();
    await DBOS.launch();

    expect((await initWorkflows(await build(5, 10))).size).toBe(5);

    const byID = await createdAtByWorkflowID(prefix);
    expect(byID.size).toBe(10);
    const ordered = Array.from({ length: 10 }, (_, i) => byID.get(`${prefix}-${i}`)!.createdAt);
    // created_at strictly increases in enqueue order: no inversion across the restart.
    expect([...ordered].sort((a, b) => a - b)).toEqual(ordered);
    expect(new Set(ordered).size).toBe(10);
    // Every post-restart row sorts after the entire pre-restart backlog.
    expect(Math.min(...ordered.slice(5))).toBeGreaterThan(Math.max(...ordered.slice(0, 5)));
  });

  test('getOrCreateQueue resolves an existing queue rather than throwing', () => {
    const name = `goc-${randomUUID()}`;
    const first = getOrCreateQueue(name, { concurrency: 3 });
    const second = getOrCreateQueue(name);
    expect(second).toBe(first);
    expect(second.concurrency).toBe(3);
  });
});
