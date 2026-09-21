import { DBOS, DBOSClient, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event } from './helpers';
import { Client } from 'pg';
import { randomUUID } from 'node:crypto';
import { DBOSError, DBOSNonExistentWorkflowError } from '../src/error';
import { INTERNAL_QUEUE_NAME, globalParams, sleepms } from '../src/utils';
import { deserializeValue } from '../src/serialization';

// Everything DBOS runs has to be registered before launch, so the workflows under
// test live at module scope and the per-test state they read lives in `runs` and the
// counters below, reset in beforeEach.

const runs: Record<string, number> = {};

function runCount(name: string): number {
  runs[name] = (runs[name] ?? 0) + 1;
  return runs[name];
}

/** Latches for the paused-queue blockers and the deliberately-stuck workflow, by name. */
const gates: Record<string, { started: Event; release: Event }> = {};

function gate(name: string): { started: Event; release: Event } {
  gates[name] ??= { started: new Event(), release: new Event() };
  return gates[name];
}

const blocker = DBOS.registerWorkflow(
  async (name: string) => {
    const g = gate(name);
    g.started.set();
    await g.release.wait();
    return 'held';
  },
  { name: 'rewind_blocker' },
);

/**
 * A rewind re-enqueues, so without a gate every assertion about the state a rewind
 * leaves behind races the queue manager picking the workflow back up and overwriting
 * it. `pausedQueue` registers a single-worker queue and holds its only worker, so
 * anything enqueued onto it stays ENQUEUED until the callback returns.
 */
async function pausedQueue<R>(name: string, callback: () => Promise<R>): Promise<R> {
  const g = gate(name);
  await DBOS.registerQueue(name, { workerConcurrency: 1, onConflict: 'always_update' });
  const handle = await DBOS.startWorkflow(blocker, { queueName: name })(name);
  await g.started.wait();
  try {
    return await callback();
  } finally {
    g.release.set();
    await handle.getResult();
  }
}

const counter = (label: string) =>
  DBOS.registerWorkflow(() => Promise.resolve(runCount(label)), { name: `${label}_counter` });

const replayCounter = counter('replay');
const partitionCounter = counter('partition');
const versionCounter = counter('version');
const validationCounter = counter('validation');
const clientCounter = counter('client');

const stepCounts = { first: 0, second: 0 };
const firstStep = DBOS.registerStep(
  () => {
    stepCounts.first += 1;
    return Promise.resolve('first');
  },
  { name: 'firstStep' },
);
const secondStep = DBOS.registerStep(
  () => {
    stepCounts.second += 1;
    return Promise.resolve('second');
  },
  { name: 'secondStep' },
);
const cutWorkflow = DBOS.registerWorkflow(async () => `${await firstStep()}-${await secondStep()}-${runCount('cut')}`, {
  name: 'cut_workflow',
});

const receiver = DBOS.registerWorkflow(
  async () => {
    const run = runCount('recv');
    const a = await DBOS.recv<string>('cmd', 10);
    const b = await DBOS.recv<string>('cmd', 10);
    return `${a}${b}:${run}`;
  },
  { name: 'recv_workflow' },
);

const publisher = DBOS.registerWorkflow(
  async () => {
    const run = runCount('events');
    await DBOS.setEvent('below', 'kept');
    await DBOS.setEvent('both', 'old');
    if (run === 1) {
      await DBOS.setEvent('both', 'new');
      await DBOS.setEvent('above', 'doomed');
      return 'first';
    }
    await DBOS.setEvent('both', 'republished');
    return 'second';
  },
  { name: 'events_workflow' },
);

const streamWriter = DBOS.registerWorkflow(
  async () => {
    const run = runCount('stream');
    await DBOS.writeStream('out', `v${run}`);
    await DBOS.closeStream('out');
    return `run${run}`;
  },
  { name: 'stream_workflow' },
);

const dbStateWorkflow = DBOS.registerWorkflow(
  async () => {
    const run = runCount('dbstate');
    await DBOS.setEvent('phase', `run${run}`);
    await DBOS.recv<string>('cmd', 10);
    return `run${run}`;
  },
  { name: 'dbstate_workflow' },
);

describe('rewind', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;
  let schema: string;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    for (const key of Object.keys(runs)) delete runs[key];
    for (const key of Object.keys(gates)) delete gates[key];
    stepCounts.first = 0;
    stepCounts.second = 0;
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
    schema = config.systemDatabaseSchemaName ?? 'dbos';
    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
    process.env.DBOS__APPVERSION = undefined;
  });

  /** The function_id of the nth step with this name, as the workflow recorded it. */
  async function stepIDOf(workflowID: string, name: string, occurrence = 0): Promise<number> {
    const steps = (await DBOS.listWorkflowSteps(workflowID)) ?? [];
    const matches = steps.filter((s) => s.name === name || s.name.endsWith(`.${name}`)).map((s) => s.functionID);
    expect(matches.length).toBeGreaterThan(occurrence);
    return matches[occurrence];
  }

  async function statusRow(workflowID: string) {
    const { rows } = await systemDBClient.query<{
      status: string;
      application_version: string;
      queue_name: string | null;
      queue_partition_key: string | null;
      recovery_attempts: string;
      started_at_epoch_ms: string | null;
      completed_at: string | null;
      workflow_deadline_epoch_ms: string | null;
      deduplication_id: string | null;
      name: string;
      created_at: string;
    }>(`SELECT * FROM "${schema}".workflow_status WHERE workflow_uuid = $1`, [workflowID]);
    return rows[0];
  }

  async function stepIDs(workflowID: string): Promise<number[]> {
    const { rows } = await systemDBClient.query<{ function_id: number }>(
      `SELECT function_id FROM "${schema}".operation_outputs WHERE workflow_uuid = $1 ORDER BY function_id`,
      [workflowID],
    );
    return rows.map((r) => r.function_id);
  }

  async function eventHistoryIDs(workflowID: string): Promise<number[]> {
    const { rows } = await systemDBClient.query<{ function_id: number }>(
      `SELECT function_id FROM "${schema}".workflow_events_history WHERE workflow_uuid = $1 ORDER BY function_id`,
      [workflowID],
    );
    return rows.map((r) => r.function_id);
  }

  /** (message, consumed, consumed_by_function_id) for a workflow's mailbox, oldest first. */
  async function mailbox(workflowID: string): Promise<[unknown, boolean, number | null][]> {
    const { rows } = await systemDBClient.query<{
      message: string;
      serialization: string | null;
      consumed: boolean;
      consumed_by_function_id: number | null;
    }>(
      `SELECT message, serialization, consumed, consumed_by_function_id FROM "${schema}".notifications
       WHERE destination_uuid = $1 ORDER BY created_at_epoch_ms`,
      [workflowID],
    );
    return Promise.all(
      rows.map(
        async (r) =>
          [
            await deserializeValue(r.message, r.serialization, sysdb().serializer),
            r.consumed,
            r.consumed_by_function_id,
          ] as [unknown, boolean, number | null],
      ),
    );
  }

  /** The values in a stream, including the close sentinel a reader stops at. */
  async function streamValues(workflowID: string, key: string): Promise<unknown[]> {
    const { rows } = await systemDBClient.query<{ value: string; serialization: string | null }>(
      `SELECT value, serialization FROM "${schema}".streams WHERE workflow_uuid = $1 AND key = $2 ORDER BY "offset"`,
      [workflowID, key],
    );
    return Promise.all(rows.map((r) => deserializeValue(r.value, r.serialization, sysdb().serializer)));
  }

  async function collect<T>(gen: AsyncGenerator<T>): Promise<T[]> {
    const out: T[] = [];
    for await (const v of gen) {
      out.push(v);
    }
    return out;
  }

  const sysdb = () => DBOSExecutor.globalInstance!.systemDatabase;

  //////////////////////////////////////////
  // Replay
  //////////////////////////////////////////

  test('replays-the-workflow-under-the-same-id', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(replayCounter()).resolves.toBe(1);
    });

    const handle = await DBOS.rewindWorkflow<number>(workflowID);
    // The handle addresses the original ID: a rewind writes no new workflow.
    expect(handle.workflowID).toBe(workflowID);
    await expect(handle.getResult()).resolves.toBe(2);
  });

  test('keeps-the-steps-below-the-cut', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(cutWorkflow()).resolves.toBe('first-second-1');
    });
    expect(stepCounts).toEqual({ first: 1, second: 1 });

    // Cut at the second step, so the first one's checkpoint survives and is replayed
    // from the log rather than re-executed.
    const cut = await stepIDOf(workflowID, 'secondStep');
    const handle = await DBOS.rewindWorkflow<string>(workflowID, { startStep: cut });
    await expect(handle.getResult()).resolves.toBe('first-second-2');
    expect(stepCounts).toEqual({ first: 1, second: 2 });
  });

  //////////////////////////////////////////
  // Notifications
  //////////////////////////////////////////

  test('deletes-the-notifications-the-discarded-run-consumed', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(receiver, { workflowID })();
    await DBOS.send(workflowID, 'a', 'cmd');
    await DBOS.send(workflowID, 'b', 'cmd');
    await expect(handle.getResult()).resolves.toBe('ab:1');

    // Each recv stamped the row it took with its own step, which is what lets the
    // rewind delete exactly the messages the discarded steps consumed.
    const firstRecv = await stepIDOf(workflowID, 'DBOS.recv');
    const secondRecv = await stepIDOf(workflowID, 'DBOS.recv', 1);
    expect(firstRecv).not.toBe(secondRecv);
    expect(await mailbox(workflowID)).toEqual([
      ['a', true, firstRecv],
      ['b', true, secondRecv],
    ]);

    await pausedQueue('rewind_delete_gate', async () => {
      await sysdb().rewindWorkflow(workflowID, secondRecv, { queueName: 'rewind_delete_gate' });
      // Only the message the discarded step took is gone. The first recv's message
      // stays consumed: its step survived the cut.
      expect(await mailbox(workflowID)).toEqual([['a', true, firstRecv]]);
      // A message that arrives after the cut is what the replayed recv gets.
      await DBOS.send(workflowID, 'c', 'cmd');
    });

    await expect(DBOS.retrieveWorkflow<string>(workflowID).getResult()).resolves.toBe('ac:2');
    expect(await mailbox(workflowID)).toEqual([
      ['a', true, firstRecv],
      ['c', true, secondRecv],
    ]);
  });

  //////////////////////////////////////////
  // Events
  //////////////////////////////////////////

  test('unpublishes-events-past-the-cut-and-restores-those-below', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(publisher()).resolves.toBe('first');
    });
    await expect(DBOS.getEvent(workflowID, 'both', 1)).resolves.toBe('new');
    await expect(DBOS.getEvent(workflowID, 'above', 1)).resolves.toBe('doomed');

    // Cut at the third setEvent, so "below" and the first "both" survive.
    const cut = await stepIDOf(workflowID, 'DBOS.setEvent', 2);

    await pausedQueue('rewind_events_gate', async () => {
      await sysdb().rewindWorkflow(workflowID, cut, { queueName: 'rewind_events_gate' });

      // Never touched past the cut, so left exactly as it was.
      await expect(DBOS.getEvent(workflowID, 'below', 1)).resolves.toBe('kept');
      // Reverted to its last value from below the cut, not deleted.
      await expect(DBOS.getEvent(workflowID, 'both', 1)).resolves.toBe('old');
      // Only ever published past the cut, so it is gone entirely.
      await expect(DBOS.getEvent(workflowID, 'above', 1)).resolves.toBeNull();
      expect(await eventHistoryIDs(workflowID)).toEqual([0, 1]);
    });

    await expect(DBOS.retrieveWorkflow<string>(workflowID).getResult()).resolves.toBe('second');

    // The replay republishes over the reverted state. "above" stays gone: nothing
    // sets it again.
    await expect(DBOS.getEvent(workflowID, 'both', 2)).resolves.toBe('republished');
    await expect(DBOS.getEvent(workflowID, 'below', 2)).resolves.toBe('kept');
    await expect(DBOS.getEvent(workflowID, 'above', 1)).resolves.toBeNull();
  });

  //////////////////////////////////////////
  // Streams
  //////////////////////////////////////////

  test('keeps-stream-entries-and-reopens-a-closed-stream', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(streamWriter()).resolves.toBe('run1');
    });
    expect(await collect(DBOS.readStream<string>(workflowID, 'out'))).toEqual(['v1']);

    // The sentinel terminates every reader that reaches it, so one left over from the
    // discarded run would hide the replay's entry with no error anywhere.
    await expect(DBOS.rewindWorkflow<string>(workflowID).then((h) => h.getResult())).resolves.toBe('run2');
    // Offsets are addresses peers read by, so the discarded run's entry keeps its own
    // and the replay appends after it.
    expect(await collect(DBOS.readStream<string>(workflowID, 'out'))).toEqual(['v1', 'v2']);

    // Cut above the close and the sentinel is not the discarded run's to undo: its
    // step survives, so nothing replays it and it has to stay.
    const closeStep = await stepIDOf(workflowID, 'DBOS.closeStream');
    await expect(
      DBOS.rewindWorkflow<string>(workflowID, { startStep: closeStep + 1 }).then((h) => h.getResult()),
    ).resolves.toBe('run3');
    expect(await streamValues(workflowID, 'out')).toEqual(['v1', 'v2', '__DBOS_STREAM_CLOSED__']);
  });

  //////////////////////////////////////////
  // Database state between rewind and replay
  //////////////////////////////////////////

  test('leaves-the-workflow-enqueued-with-its-history-dropped', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(dbStateWorkflow, { workflowID })();
    await DBOS.send(workflowID, 'go', 'cmd');
    await expect(handle.getResult()).resolves.toBe('run1');

    const before = await statusRow(workflowID);
    expect(before.status).toBe(StatusString.SUCCESS);
    expect(before.completed_at).not.toBeNull();

    await pausedQueue('rewind_state_gate', async () => {
      await sysdb().rewindWorkflow(workflowID, 0, {
        queueName: 'rewind_state_gate',
        queuePartitionKey: 'pk',
      });

      const after = await statusRow(workflowID);
      expect(after.status).toBe(StatusString.ENQUEUED);
      expect(after.queue_name).toBe('rewind_state_gate');
      expect(after.queue_partition_key).toBe('pk');
      expect(Number(after.recovery_attempts)).toBe(0);
      expect(after.started_at_epoch_ms).toBeNull();
      expect(after.completed_at).toBeNull();
      expect(after.workflow_deadline_epoch_ms).toBeNull();
      expect(after.deduplication_id).toBeNull();
      // The identity of the workflow is untouched.
      expect(after.name).toBe(before.name);
      expect(after.created_at).toBe(before.created_at);

      expect(await stepIDs(workflowID)).toEqual([]);
      expect(await eventHistoryIDs(workflowID)).toEqual([]);
      expect(await mailbox(workflowID)).toEqual([]);
      await DBOS.send(workflowID, 'go', 'cmd');
    });

    await expect(DBOS.retrieveWorkflow<string>(workflowID).getResult()).resolves.toBe('run2');
    await expect(DBOS.getEvent(workflowID, 'phase', 2)).resolves.toBe('run2');
  });

  //////////////////////////////////////////
  // Queues and application versions
  //////////////////////////////////////////

  test('re-enqueues-onto-a-queue-with-a-partition-key', async () => {
    await DBOS.registerQueue('rewind_partitioned', { partitionConcurrency: 1, onConflict: 'always_update' });

    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(partitionCounter, {
      workflowID,
      queueName: 'rewind_partitioned',
      enqueueOptions: { queuePartitionKey: 'original' },
    })();
    await expect(handle.getResult()).resolves.toBe(1);

    await sysdb().rewindWorkflow(workflowID, 0, {
      queueName: 'rewind_partitioned',
      queuePartitionKey: 'repaired',
    });
    await expect(DBOS.retrieveWorkflow<number>(workflowID).getResult()).resolves.toBe(2);
    let status = await statusRow(workflowID);
    expect(status.queue_name).toBe('rewind_partitioned');
    expect(status.queue_partition_key).toBe('repaired');

    // Omitting the key clears it, omitting the queue falls back to the internal queue.
    await sysdb().rewindWorkflow(workflowID, 0);
    await expect(DBOS.retrieveWorkflow<number>(workflowID).getResult()).resolves.toBe(3);
    status = await statusRow(workflowID);
    expect(status.queue_name).toBe(INTERNAL_QUEUE_NAME);
    expect(status.queue_partition_key).toBeNull();
  });

  test('restamps-the-application-version-and-refuses-the-enqueued-workflow', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(versionCounter()).resolves.toBe(1);
    });
    const runningVersion = globalParams.appVersion;

    // Dequeueing matches on application version, so a workflow restamped with a
    // version nothing is running on stays enqueued instead of replaying.
    await sysdb().rewindWorkflow(workflowID, 0, { applicationVersion: 'not-this-deployment' });
    expect((await statusRow(workflowID)).application_version).toBe('not-this-deployment');
    await sleepms(2500); // several queue polls, any of which would pick it up
    expect((await statusRow(workflowID)).status).toBe(StatusString.ENQUEUED);
    expect(runs['version']).toBe(1);

    // Getting out of that takes a cancel first: the workflow is ENQUEUED now, and only
    // a terminal workflow can be rewound.
    await expect(sysdb().rewindWorkflow(workflowID, 0, { applicationVersion: runningVersion })).rejects.toThrow(
      /only a workflow in a terminal state/,
    );
    await DBOS.cancelWorkflow(workflowID);

    // Restamped with the version this executor is running, it replays.
    await sysdb().rewindWorkflow(workflowID, 0, { applicationVersion: runningVersion });
    await expect(DBOS.retrieveWorkflow<number>(workflowID).getResult()).resolves.toBe(2);

    // Omitted, the workflow keeps the version it already had.
    await sysdb().rewindWorkflow(workflowID, 0);
    await expect(DBOS.retrieveWorkflow<number>(workflowID).getResult()).resolves.toBe(3);
    expect((await statusRow(workflowID)).application_version).toBe(runningVersion);
  });

  //////////////////////////////////////////
  // Refusals and validation
  //////////////////////////////////////////

  test('refuses-a-missing-workflow', async () => {
    await expect(sysdb().rewindWorkflow(randomUUID(), 0)).rejects.toThrow(DBOSNonExistentWorkflowError);
    await expect(DBOS.rewindWorkflow(randomUUID())).rejects.toThrow(DBOSNonExistentWorkflowError);
  });

  test('refuses-a-running-workflow', async () => {
    const workflowID = randomUUID();
    const g = gate('stuck');
    const handle = await DBOS.startWorkflow(blocker, { workflowID })('stuck');
    await g.started.wait();
    try {
      await expect(sysdb().rewindWorkflow(workflowID, 0)).rejects.toThrow(/only a workflow in a terminal state/);
    } finally {
      g.release.set();
      await handle.getResult();
    }

    // Terminal now, so the same call goes through.
    await expect(DBOS.rewindWorkflow<string>(workflowID).then((h) => h.getResult())).resolves.toBe('held');
  });

  test('rejects-a-negative-start-step', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(validationCounter()).resolves.toBe(1);
    });

    await expect(sysdb().rewindWorkflow(workflowID, -1)).rejects.toThrow(DBOSError);
    await expect(sysdb().rewindWorkflow(workflowID, -1)).rejects.toThrow(/must be >= 0/);

    // That wrote nothing.
    expect((await statusRow(workflowID)).status).toBe(StatusString.SUCCESS);
    expect(runs['validation']).toBe(1);
  });

  //////////////////////////////////////////
  // Client
  //////////////////////////////////////////

  test('client-rewinds-without-a-running-application', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(clientCounter()).resolves.toBe(1);
    });

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const handle = await client.rewindWorkflow<number>(workflowID);
      expect(handle.workflowID).toBe(workflowID);
      await expect(handle.getResult()).resolves.toBe(2);
    } finally {
      await client.destroy();
    }
  });
});
