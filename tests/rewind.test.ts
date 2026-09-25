import { DBOS, DBOSClient, StatusString } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event, retryUntilSuccess, recoverWorkflow } from './helpers';
import { Client, Pool, PoolClient } from 'pg';
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { DBOSError, DBOSNonExistentWorkflowError, DBOSWorkflowCancelledError } from '../src/error';
import { INTERNAL_QUEUE_NAME, globalParams, sleepms } from '../src/utils';
import { deserializeValue } from '../src/serialization';
import {
  createTransactionCompletionSchemaPG,
  createTransactionCompletionTablePG,
  registerDataSource,
  registerTransaction,
  replayRecordedStep,
  type DataSourceTransactionHandler,
} from '../src/datasource';
import { SuperJSON } from 'superjson';

// Everything DBOS runs has to be registered before launch, so the workflows under
// test live at module scope and the per-test state they read lives in `runs` and the
// counters below, reset in beforeEach.

const config = generateDBOSTestConfig();

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

/** The transaction client and the schema to reach it through, for a transaction body. */
const dsContext = new AsyncLocalStorage<{ client: PoolClient; schema: string }>();

/**
 * The smallest data source that keeps checkpoints of its own: a
 * `<schema>.transaction_completion` row per step, replayed in place of the body. Those
 * rows have to go with the steps a rewind drops, or the replay reads one back as the
 * transaction's result without running it. Each instance owns a schema, so two of them
 * in one database stand in for two independent stores.
 */
class CheckpointDataSource implements DataSourceTransactionHandler {
  #poolField: Pool | undefined;

  constructor(
    readonly name: string,
    readonly schema: string,
  ) {}

  async initialize(): Promise<void> {
    this.#poolField = new Pool({ connectionString: config.systemDatabaseUrl });
    await this.pool.query(createTransactionCompletionSchemaPG(this.schema));
    await this.pool.query(createTransactionCompletionTablePG(this.schema));
    await this.pool.query(`CREATE TABLE IF NOT EXISTS "${this.schema}".rows (v TEXT)`);
  }

  async destroy(): Promise<void> {
    const pool = this.#poolField;
    this.#poolField = undefined;
    await pool?.end();
  }

  get pool(): Pool {
    if (!this.#poolField) {
      throw new Error(`${this.name} is not initialized`);
    }
    return this.#poolField;
  }

  async deleteCheckpoints(workflowID: string, startStep: number, beforeCommit?: () => Promise<void>): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `DELETE FROM "${this.schema}".transaction_completion WHERE workflow_id = $1 AND function_num >= $2`,
        [workflowID, startStep],
      );
      await beforeCommit?.();
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /** The step IDs this data source holds a checkpoint for. */
  async checkpoints(workflowID: string): Promise<number[]> {
    const { rows } = await this.pool.query<{ function_num: number }>(
      `SELECT function_num FROM "${this.schema}".transaction_completion WHERE workflow_id = $1 ORDER BY function_num`,
      [workflowID],
    );
    return rows.map((r) => r.function_num);
  }

  /** What the transactions actually wrote, which a replayed checkpoint does not. */
  async rows(): Promise<string[]> {
    const { rows } = await this.pool.query<{ v: string }>(`SELECT v FROM "${this.schema}".rows ORDER BY v`);
    return rows.map((r) => r.v);
  }

  async invokeTransactionFunction<This, Args extends unknown[], Return>(
    _config: unknown,
    target: This,
    func: (this: This, ...args: Args) => Promise<Return>,
    ...args: Args
  ): Promise<Return> {
    const workflowID = DBOS.workflowID!;
    const stepID = DBOS.stepID!;

    const { rows } = await this.pool.query<{ output: string | null; error: string | null }>(
      `SELECT output, error FROM "${this.schema}".transaction_completion WHERE workflow_id = $1 AND function_num = $2`,
      [workflowID, stepID],
    );
    if (rows.length > 0) {
      return replayRecordedStep<Return>(rows[0]);
    }

    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await dsContext.run({ client, schema: this.schema }, () => func.call(target, ...args));
      await client.query(
        `INSERT INTO "${this.schema}".transaction_completion (workflow_id, function_num, output) VALUES ($1, $2, $3)`,
        [workflowID, stepID, SuperJSON.stringify(result)],
      );
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}

const firstDS = new CheckpointDataSource('rewind_ds_first', 'rewind_ds1');
const secondDS = new CheckpointDataSource('rewind_ds_second', 'rewind_ds2');
registerDataSource(firstDS);
registerDataSource(secondDS);

async function insertRow(v: string): Promise<string> {
  const ctx = dsContext.getStore()!;
  await ctx.client.query(`INSERT INTO "${ctx.schema}".rows (v) VALUES ($1)`, [v]);
  return v;
}

const insertFirst = registerTransaction(firstDS.name, insertRow, { name: 'insertFirst' });
const insertSecond = registerTransaction(secondDS.name, insertRow, { name: 'insertSecond' });

// Interleaved, so each data source holds every other step: a cut has to land inside
// both of them rather than truncating one.
const dsWorkflow = DBOS.registerWorkflow(
  async () => {
    const run = runCount('ds');
    await insertFirst('a');
    await insertSecond('b');
    await insertFirst('c');
    await insertSecond('d');
    return run;
  },
  { name: 'ds_workflow' },
);

// Holds after its transactions, so a test can look at the checkpoints of a running workflow.
const gatedDSWorkflow = DBOS.registerWorkflow(
  async () => {
    await insertFirst('a');
    await insertSecond('b');
    const g = gate('ds_gated');
    g.started.set();
    await g.release.wait();
    return 'done';
  },
  { name: 'gated_ds_workflow' },
);

const afterTransaction = DBOS.registerStep(() => Promise.resolve(), { name: 'after_transaction' });

// Takes a step after the gate, so a cancel issued while it waits is observed there.
const cancellableDSWorkflow = DBOS.registerWorkflow(
  async () => {
    await insertFirst('a');
    const g = gate('ds_cancellable');
    g.started.set();
    await g.release.wait();
    await afterTransaction();
    return 'done';
  },
  { name: 'cancellable_ds_workflow' },
);

const failingDSWorkflow = DBOS.registerWorkflow(
  async (databaseError: boolean) => {
    await insertFirst('a');
    if (databaseError) {
      // A real driver error, raised outside any step.
      await firstDS.pool.query('SELECT 1/0');
    }
    throw new Error('workflow failed');
  },
  { name: 'failing_ds_workflow' },
);

/** Give the workflow to another execution, as a recovery or resume would. */
async function handOff(workflowID: string) {
  const client = new Client({ connectionString: config.systemDatabaseUrl });
  try {
    await client.connect();
    await client.query(
      `UPDATE "${config.systemDatabaseSchemaName ?? 'dbos'}".workflow_status SET owner_xid = 'another-execution' WHERE workflow_uuid = $1`,
      [workflowID],
    );
  } finally {
    await client.end();
  }
}

// Loses the workflow after its transactions, so its completion is a stale execution's.
const handedOffDSWorkflow = DBOS.registerWorkflow(
  async () => {
    await insertFirst('a');
    await insertSecond('b');
    await handOff(DBOS.workflowID!);
    return 'done';
  },
  { name: 'handed_off_ds_workflow' },
);

const secondOnlyDSWorkflow = DBOS.registerWorkflow(
  async () => {
    await insertSecond('b');
    return 'done';
  },
  { name: 'second_only_ds_workflow' },
);

const idleWorkflow = DBOS.registerWorkflow(() => Promise.resolve('idle'), { name: 'idle_workflow' });

const rewindChild = DBOS.registerWorkflow(
  (value: number) => {
    runCount('child');
    return Promise.resolve(value * 2);
  },
  { name: 'rewind_child' },
);

const rewindParent = DBOS.registerWorkflow(
  async (name: string) => {
    const run = runCount(name);
    const handle = await DBOS.startWorkflow(rewindChild)(21);
    return (await handle.getResult()) + run;
  },
  { name: 'rewind_parent' },
);

describe('rewind', () => {
  let systemDBClient: Client;
  let schema: string;

  beforeAll(() => {
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
    jest.restoreAllMocks();
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
      output: string | null;
      error: string | null;
      name: string;
      created_at: string;
    }>(`SELECT * FROM "${schema}".workflow_status WHERE workflow_uuid = $1`, [workflowID]);
    return rows[0];
  }

  async function outputRow(workflowID: string) {
    const { rows } = await systemDBClient.query<{ output: string | null; error: string | null }>(
      `SELECT output, error FROM "${schema}".workflow_output WHERE workflow_uuid = $1`,
      [workflowID],
    );
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

  /** Skip the cleanup at completion, leaving checkpoints as a workflow finished before it existed did. */
  function keepCompletionCheckpoints() {
    return jest.spyOn(DBOSExecutor.prototype, 'deleteCompletedDataSourceCheckpoints').mockResolvedValue(undefined);
  }

  /** Record whether each of `ds`'s checkpoint deletes committed or rolled back. */
  function recordDeletes(ds: CheckpointDataSource): string[] {
    const outcomes: string[] = [];
    const deleteCheckpoints = ds.deleteCheckpoints.bind(ds);
    jest.spyOn(ds, 'deleteCheckpoints').mockImplementation(async (...args) => {
      try {
        await deleteCheckpoints(...args);
        outcomes.push('committed');
      } catch (error) {
        outcomes.push('rolled back');
        throw error;
      }
    });
    return outcomes;
  }

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

  test('deletes-the-notifications-consumed-or-received-past-the-cut', async () => {
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
    // A message that arrives once the workflow is done sits unconsumed.
    await DBOS.send(workflowID, 'stray', 'cmd');
    expect(await mailbox(workflowID)).toEqual([
      ['a', true, firstRecv],
      ['b', true, secondRecv],
      ['stray', false, null],
    ]);

    await pausedQueue('rewind_delete_gate', async () => {
      await sysdb().rewindWorkflow(workflowID, secondRecv, { queueName: 'rewind_delete_gate' });
      // The message the discarded step took is gone, and so is the one still waiting.
      // The first recv's message stays consumed: its step survived the cut.
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
      expect(await outputRow(workflowID)).toBeUndefined();
      expect(after.output).toBeNull();
      expect(after.error).toBeNull();
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

  /**
   * The validation has to happen before the data sources are touched: `DBOS.rewindWorkflow`
   * drops their checkpoints first and only then calls into the system database, so a
   * `startStep` the system database would reject must never get that far.
   */
  test('rejects-a-negative-start-step-before-dropping-checkpoints', async () => {
    keepCompletionCheckpoints();
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(dsWorkflow()).resolves.toBe(1);
    });
    expect(await firstDS.checkpoints(workflowID)).toEqual([0, 2]);
    expect(await secondDS.checkpoints(workflowID)).toEqual([1, 3]);

    await expect(DBOS.rewindWorkflow(workflowID, { startStep: -1 })).rejects.toThrow(/must be >= 0/);

    // Every checkpoint is still there, so the steps keep their crash-window protection.
    expect(await firstDS.checkpoints(workflowID)).toEqual([0, 2]);
    expect(await secondDS.checkpoints(workflowID)).toEqual([1, 3]);
    expect((await statusRow(workflowID)).status).toBe(StatusString.SUCCESS);
    expect(runs['ds']).toBe(1);
  });

  //////////////////////////////////////////
  // Data sources
  //////////////////////////////////////////

  test('drops-every-registered-data-sources-checkpoints-past-the-cut', async () => {
    keepCompletionCheckpoints();
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(dsWorkflow()).resolves.toBe(1);
    });
    expect(await firstDS.checkpoints(workflowID)).toEqual([0, 2]);
    expect(await secondDS.checkpoints(workflowID)).toEqual([1, 3]);

    // Cut at the third step: each data source keeps one checkpoint and loses one.
    await pausedQueue('rewind_datasource_gate', async () => {
      await DBOS.rewindWorkflow<number>(workflowID, { startStep: 2, queueName: 'rewind_datasource_gate' });
      expect(await firstDS.checkpoints(workflowID)).toEqual([0]);
      expect(await secondDS.checkpoints(workflowID)).toEqual([1]);
    });

    await expect(DBOS.retrieveWorkflow<number>(workflowID).getResult()).resolves.toBe(2);
    expect(await firstDS.checkpoints(workflowID)).toEqual([0, 2]);
    expect(await secondDS.checkpoints(workflowID)).toEqual([1, 3]);
    // The first transaction on each data source replayed, the second ran again.
    expect(await firstDS.rows()).toEqual(['a', 'c', 'c']);
    expect(await secondDS.rows()).toEqual(['b', 'd', 'd']);
  });

  //////////////////////////////////////////
  // Child workflows
  //////////////////////////////////////////

  /**
   * A child's ID derives from the parent's step (`${callerID}-${callerFunctionID}`), so a
   * rewound parent re-derives the same ID and adopts the child it already has rather than
   * starting a second one.
   */
  test('rewound-parent-adopts-its-existing-child', async () => {
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(rewindParent('adopt')).resolves.toBe(43);
    });
    expect(runs['child']).toBe(1);
    const childID = `${workflowID}-0`;
    await expect(DBOS.retrieveWorkflow<number>(childID).getResult()).resolves.toBe(42);

    await expect(DBOS.rewindWorkflow<number>(workflowID).then((h) => h.getResult())).resolves.toBe(44);

    // The child is the same workflow, still SUCCESS, and it did not run a second time.
    expect(runs['child']).toBe(1);
    await expect(DBOS.retrieveWorkflow<number>(childID).getResult()).resolves.toBe(42);
    expect((await statusRow(childID)).status).toBe(StatusString.SUCCESS);
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

  /**
   * The documented limitation: a client rewinds the system database and nothing else.
   * It reaches the data sources through no registry — the application process owns
   * those — so their checkpoints stay behind and are replayed as results. Rewinding a
   * workflow with transactions is a job for `DBOS.rewindWorkflow`, from inside the
   * application.
   */
  test('client-rewind-leaves-the-data-sources-checkpoints-behind', async () => {
    keepCompletionCheckpoints();
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(dsWorkflow()).resolves.toBe(1);
    });

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const handle = await client.rewindWorkflow<number>(workflowID, { startStep: 2 });
      // The workflow body itself ran again, so the run counter moves.
      await expect(handle.getResult()).resolves.toBe(2);
    } finally {
      await client.destroy();
    }

    // Nothing was dropped, so the two steps past the cut re-entered their transactions,
    // found the checkpoints still there and replayed them: no INSERT ran a second time.
    expect(await firstDS.checkpoints(workflowID)).toEqual([0, 2]);
    expect(await secondDS.checkpoints(workflowID)).toEqual([1, 3]);
    expect(await firstDS.rows()).toEqual(['a', 'c']);
    expect(await secondDS.rows()).toEqual(['b', 'd']);
  });

  //////////////////////////////////////////
  // Data source checkpoints at completion
  //////////////////////////////////////////

  /** Checkpoints hold while the workflow runs and are cleared from every data source once it succeeds. */
  test('completion-drops-every-data-sources-checkpoints', async () => {
    const g = gate('ds_gated');
    const handle = await DBOS.startWorkflow(gatedDSWorkflow)();
    await g.started.wait();
    expect(await firstDS.checkpoints(handle.workflowID)).toEqual([0]);
    expect(await secondDS.checkpoints(handle.workflowID)).toEqual([1]);

    g.release.set();
    await expect(handle.getResult()).resolves.toBe('done');
    expect(await firstDS.checkpoints(handle.workflowID)).toEqual([]);
    expect(await secondDS.checkpoints(handle.workflowID)).toEqual([]);
    expect(await firstDS.rows()).toEqual(['a']);
    expect(await secondDS.rows()).toEqual(['b']);
  });

  test('an-error-drops-the-checkpoints', async () => {
    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => failingDSWorkflow(false))).rejects.toThrow(
      'workflow failed',
    );
    expect((await statusRow(workflowID)).status).toBe(StatusString.ERROR);
    expect(await firstDS.checkpoints(workflowID)).toEqual([]);
  });

  /** A database error may have cost a transaction its step checkpoint, leaving its data source checkpoint the only record. */
  test('a-database-error-keeps-the-checkpoints', async () => {
    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => failingDSWorkflow(true))).rejects.toThrow(
      'division by zero',
    );
    expect((await statusRow(workflowID)).status).toBe(StatusString.ERROR);
    expect(await firstDS.checkpoints(workflowID)).toEqual([0]);
  });

  /** A cancelled workflow can be resumed, and a transaction whose step checkpoint was lost then replays off its data source checkpoint. */
  test('a-cancelled-workflow-keeps-the-checkpoints-for-its-resume', async () => {
    const g = gate('ds_cancellable');
    const handle = await DBOS.startWorkflow(cancellableDSWorkflow)();
    const workflowID = handle.workflowID;
    await g.started.wait();
    await DBOS.cancelWorkflow(workflowID);
    g.release.set();
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    await retryUntilSuccess(() => expect(sysdb().checkForRunningWorkflow(workflowID)).toBe(false));
    expect(await firstDS.checkpoints(workflowID)).toEqual([0]);

    // Lose the transaction's step checkpoint, as a crash just after its commit would.
    await systemDBClient.query(`DELETE FROM "${schema}".operation_outputs WHERE workflow_uuid = $1`, [workflowID]);
    const resumed = await DBOS.resumeWorkflow<string>(workflowID);
    await expect(resumed.getResult()).resolves.toBe('done');
    expect(await firstDS.rows()).toEqual(['a']);
    expect(await firstDS.checkpoints(workflowID)).toEqual([]);
  });

  /** A leftover checkpoint is harmless, so a data source that cannot be cleared neither fails the workflow nor stops the others from being cleared. */
  test('a-failed-cleanup-still-records-the-outcome', async () => {
    jest.spyOn(firstDS, 'deleteCheckpoints').mockRejectedValue(new Error('datasource down'));
    const workflowID = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await expect(dsWorkflow()).resolves.toBe(1);
    });
    expect((await statusRow(workflowID)).status).toBe(StatusString.SUCCESS);
    expect(await firstDS.checkpoints(workflowID)).toEqual([0, 2]);
    expect(await secondDS.checkpoints(workflowID)).toEqual([]);
  });

  /** A stale execution rolls its delete back, since the checkpoints may be the new owner's only record of a transaction. */
  test('a-stale-execution-keeps-the-checkpoints', async () => {
    const firstDeletes = recordDeletes(firstDS);
    const secondDeletes = recordDeletes(secondDS);
    const handle = await DBOS.startWorkflow(handedOffDSWorkflow)();
    const workflowID = handle.workflowID;

    await retryUntilSuccess(() => expect(firstDeletes).toHaveLength(1));
    // Rolled back on the first data source, which stops the cleanup there.
    expect(firstDeletes).toEqual(['rolled back']);
    expect(secondDeletes).toEqual([]);
    expect(await firstDS.checkpoints(workflowID)).toEqual([0]);
    expect(await secondDS.checkpoints(workflowID)).toEqual([1]);

    // Release the parked execution.
    await DBOS.cancelWorkflow(workflowID);
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    await retryUntilSuccess(() => expect(sysdb().checkForRunningWorkflow(workflowID)).toBe(false));
  });

  test('cleanup-skips-the-data-sources-the-workflow-never-called', async () => {
    const firstDeletes = recordDeletes(firstDS);
    const secondDeletes = recordDeletes(secondDS);
    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => secondOnlyDSWorkflow())).resolves.toBe('done');
    await expect(idleWorkflow()).resolves.toBe('idle');
    expect(firstDeletes).toEqual([]);
    expect(secondDeletes).toEqual(['committed']);
    expect(await secondDS.checkpoints(workflowID)).toEqual([]);
  });

  /** A recovered execution replays every transaction from its step checkpoint, yet still clears what the earlier execution left. */
  test('recovery-clears-an-earlier-executions-checkpoints', async () => {
    const keep = keepCompletionCheckpoints();
    const workflowID = randomUUID();
    await expect(DBOS.withNextWorkflowID(workflowID, () => secondOnlyDSWorkflow())).resolves.toBe('done');
    expect(await secondDS.checkpoints(workflowID)).toEqual([0]);
    keep.mockRestore();

    await systemDBClient.query(`UPDATE "${schema}".workflow_status SET status = $2 WHERE workflow_uuid = $1`, [
      workflowID,
      StatusString.PENDING,
    ]);
    const handle = await recoverWorkflow(workflowID);
    await expect(handle.getResult()).resolves.toBe('done');
    expect(await secondDS.rows()).toEqual(['b']);
    expect(await secondDS.checkpoints(workflowID)).toEqual([]);
  });
});
