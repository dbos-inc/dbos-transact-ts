import { randomUUID } from 'node:crypto';
import { Client } from 'pg';

import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { context, SpanStatusCode, trace } from '@opentelemetry/api';

import { DBOS, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import {
  DBOSAwaitedWorkflowCancelledError,
  DBOSMaxRecoveryAttemptsExceededError,
  DBOSNonExistentWorkflowError,
  DBOSWorkflowCancelledError,
} from '../src/error';
import { serializeResError, serializeValue } from '../src/serialization';
import { sleepms } from '../src/utils';
import { Event, generateDBOSTestConfig, retryUntilSuccess, setUpDBOSTestSysDb } from './helpers';
import { NodeTracerProvider } from './nodetraceprovider';

// Each run blocks until the test has rewritten its row, then returns a
// result the test can tell apart from anything recorded out-of-band.
// `workflowID`/`stepID` are published by the run itself, for the cases where
// the test has to address a workflow (or a step checkpoint) whose identity it
// does not choose.
type Control = { started: Event; release: Event; workflowID?: string; stepID?: number };
const controls = new Map<string, Control>();

function control(id: string): Control {
  const ctrl = controls.get(id);
  if (!ctrl) {
    throw new Error(`no control registered for workflow ${id}`);
  }
  return ctrl;
}

class OutcomeOwnership {
  @DBOS.workflow()
  static async blockedWorkflow(id: string): Promise<string> {
    const ctrl = control(id);
    ctrl.workflowID = DBOS.workflowID;
    ctrl.started.set();
    await ctrl.release.wait();
    return 'own-result';
  }

  // Stands in for a run that observes its own cancellation mid-flight: the
  // cancellation is thrown only after the test has rewritten the row.
  @DBOS.workflow()
  static async selfCancellingWorkflow(id: string): Promise<string> {
    const ctrl = control(id);
    ctrl.workflowID = DBOS.workflowID;
    ctrl.started.set();
    await ctrl.release.wait();
    throw new DBOSWorkflowCancelledError(id);
  }

  // A run that fails on its own terms: it computes an error, not a result.
  @DBOS.workflow()
  static async throwingWorkflow(id: string): Promise<string> {
    const ctrl = control(id);
    ctrl.workflowID = DBOS.workflowID;
    ctrl.started.set();
    await ctrl.release.wait();
    throw new Error('own failure');
  }

  // Same as throwingWorkflow, but portable-serialized: reviving a portable
  // error throws the revived PortableWorkflowError rather than returning it, so
  // this run's error must not be revived before the ownership decision.
  @DBOS.workflow({ serialization: 'portable' })
  static async throwingPortableWorkflow(id: string): Promise<string> {
    const ctrl = control(id);
    ctrl.workflowID = DBOS.workflowID;
    ctrl.started.set();
    await ctrl.release.wait();
    throw new Error('portable own failure');
  }

  // Blocks inside a step, after the step's function ID is knowable but before
  // its checkpoint is written, so the test can plant a conflicting checkpoint.
  @DBOS.workflow()
  static async blockedStepWorkflow(id: string): Promise<string> {
    const ctrl = control(id);
    ctrl.workflowID = DBOS.workflowID;
    return await DBOS.runStep(
      async () => {
        ctrl.stepID = DBOS.stepID;
        ctrl.started.set();
        await ctrl.release.wait();
        return 'own-result';
      },
      { name: 'blockedStep' },
    );
  }

  // Awaits a child workflow directly, so the child's own cancellation error
  // propagates into this run unwrapped.
  @DBOS.workflow()
  static async parentOfBlockedWorkflow(id: string): Promise<string> {
    return await OutcomeOwnership.blockedWorkflow(id);
  }
}

type BlockingWorkflow =
  | 'blockedWorkflow'
  | 'selfCancellingWorkflow'
  | 'throwingWorkflow'
  | 'throwingPortableWorkflow'
  | 'blockedStepWorkflow'
  | 'parentOfBlockedWorkflow';

// Start a run and return once it is blocked inside the workflow function,
// with its row PENDING.
async function startBlockedRun(workflow: BlockingWorkflow = 'blockedWorkflow') {
  const id = `outcome-ownership-${randomUUID()}`;
  const ctrl: Control = { started: new Event(), release: new Event() };
  controls.set(id, ctrl);
  const handle = await DBOS.startWorkflow(OutcomeOwnership, { workflowID: id })[workflow](id);
  await ctrl.started.wait();
  return { handle, ctrl };
}

// Encode a value/error the way the workflow's outcome would be recorded.
async function encodeOutput(value: string) {
  return await serializeValue(value, DBOSExecutor.globalInstance!.serializer, undefined);
}
async function encodeError(message: string) {
  return await serializeResError(new Error(message), DBOSExecutor.globalInstance!.serializer, undefined);
}

// Take the row away from the blocked run, standing in for the concurrent
// resume/recovery/cancel that would do it in production.
async function rewriteRowWith(
  client: Client,
  workflowID: string,
  status: (typeof StatusString)[keyof typeof StatusString],
  fields: { output?: string | null; error?: string | null; serialization?: string | null } = {},
) {
  await client.query(
    `UPDATE dbos.workflow_status
     SET status=$1, output=$2, error=$3, serialization=COALESCE($4, serialization)
     WHERE workflow_uuid=$5`,
    [status, fields.output ?? null, fields.error ?? null, fields.serialization ?? null, workflowID],
  );
  // Both destinations, as the execution that really recorded this outcome would have.
  await client.query(
    `INSERT INTO dbos.workflow_output (workflow_uuid, output, error) VALUES ($1, $2, $3)
     ON CONFLICT (workflow_uuid) DO UPDATE SET output = EXCLUDED.output, error = EXCLUDED.error`,
    [workflowID, fields.output ?? null, fields.error ?? null],
  );
}

/** The outcome a reader resolves: the payload table, falling back to the legacy columns. */
async function readOutcome(
  client: Client,
  workflowID: string,
): Promise<{ status: string; output: string | null; error: string | null }> {
  const { rows } = await client.query<{ status: string; output: string | null; error: string | null }>(
    `SELECT ws.status, COALESCE(wo.output, ws.output) AS output, COALESCE(wo.error, ws.error) AS error
     FROM dbos.workflow_status ws
     LEFT JOIN dbos.workflow_output wo ON wo.workflow_uuid = ws.workflow_uuid
     WHERE ws.workflow_uuid=$1`,
    [workflowID],
  );
  return rows[0];
}

// Plant the checkpoint the blocked step is about to write, with a different
// completion time so the step's own write is refused as a conflict. Stands in
// for the concurrent execution that would have written it in production.
async function plantConflictingCheckpoint(client: Client, workflowID: string, stepID: number | undefined) {
  expect(stepID).toBeDefined();
  await client.query(
    `INSERT INTO dbos.operation_outputs
       (workflow_uuid, function_id, function_name, started_at_epoch_ms, completed_at_epoch_ms)
     VALUES ($1, $2, 'blockedStep', 1, 1)`,
    [workflowID, stepID],
  );
}

// A run may record its outcome only while its workflow_status row is still
// PENDING: that row is what says "this run is what the workflow is doing".
// Every other status means the run lost ownership (a concurrent resume
// re-enqueued it, a recovery raced it, it was cancelled or dead-lettered) and
// the recorded outcome, not the one the run computed, is the workflow's
// outcome.
describe('workflow-outcome-ownership', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
  });

  function rewriteRow(
    workflowID: string,
    status: (typeof StatusString)[keyof typeof StatusString],
    fields: { output?: string | null; error?: string | null; serialization?: string | null } = {},
  ) {
    return rewriteRowWith(systemDBClient, workflowID, status, fields);
  }

  test('recorded-success-supersedes-the-run-result', async () => {
    const { handle, ctrl } = await startBlockedRun();
    const recorded = await encodeOutput('recorded-elsewhere');
    await rewriteRow(handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();

    // The run must report the recorded output, not its own.
    await expect(handle.getResult()).resolves.toBe('recorded-elsewhere');

    // The recorded output must not be overwritten.
    const outcome = await readOutcome(systemDBClient, handle.workflowID);
    expect(outcome.status).toBe(StatusString.SUCCESS);
    expect(outcome.output).toBe(recorded.serializedValue);
  });

  test('recorded-error-supersedes-the-run-result', async () => {
    const { handle, ctrl } = await startBlockedRun();
    const recorded = await encodeError('recorded failure');
    await rewriteRow(handle.workflowID, StatusString.ERROR, {
      error: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();

    // The recorded error must be adopted.
    await expect(handle.getResult()).rejects.toThrow('recorded failure');
    await expect(DBOS.getWorkflowStatus(handle.workflowID)).resolves.toMatchObject({ status: StatusString.ERROR });
  });

  test('non-terminal-row-parks-the-run-until-an-outcome-is-recorded', async () => {
    const { handle, ctrl } = await startBlockedRun();
    // ENQUEUED with no queue name: nothing dequeues it, so the run stays
    // parked until this test records the outcome itself.
    await rewriteRow(handle.workflowID, StatusString.ENQUEUED);
    ctrl.release.set();

    const resultPromise: Promise<{ result?: string; error?: Error }> = handle.getResult().then(
      (result) => ({ result }),
      (error: Error) => ({ error }),
    );

    // The run releases its running-workflow entry immediately before it tries
    // to record its outcome. Waiting for that makes the check below assert
    // that the run parked, rather than merely that it had not gotten around to
    // the write yet.
    await retryUntilSuccess(() => {
      expect(DBOSExecutor.globalInstance!.systemDatabase.checkForRunningWorkflow(handle.workflowID)).toBe(false);
    }, 30000);

    // The run must wait for the owning execution.
    const parked = await Promise.race([resultPromise.then(() => false), sleepms(2000).then(() => true)]);
    expect(parked).toBe(true);

    const recorded = await encodeOutput('recorded-by-owner');
    await rewriteRow(handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });

    // The parked run must adopt the recorded outcome, not its own.
    await expect(resultPromise).resolves.toEqual({ result: 'recorded-by-owner' });
  }, 60000);

  test('dead-lettered-row-fails-the-run', async () => {
    const { handle, ctrl } = await startBlockedRun();
    await rewriteRow(handle.workflowID, StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
    // A dead-lettered row carries an exhausted retry budget.
    await systemDBClient.query(`UPDATE dbos.workflow_status SET recovery_attempts=$1 WHERE workflow_uuid=$2`, [
      100,
      handle.workflowID,
    ]);
    ctrl.release.set();

    // A dead-lettered workflow must not report a completion.
    await expect(handle.getResult()).rejects.toThrow(DBOSMaxRecoveryAttemptsExceededError);

    // The refused outcome must not have recorded an output.
    const outcome = await readOutcome(systemDBClient, handle.workflowID);
    expect(outcome.status).toBe(StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
    expect(outcome.output).toBeNull();
  });

  test('deleted-row-fails-the-run-with-non-existent-workflow', async () => {
    const { handle, ctrl } = await startBlockedRun();
    await systemDBClient.query(`DELETE FROM dbos.workflow_status WHERE workflow_uuid=$1`, [handle.workflowID]);
    ctrl.release.set();

    // A run whose row vanished must not report a completion.
    await expect(handle.getResult()).rejects.toThrow(DBOSNonExistentWorkflowError);
  });

  test('cancelled-run-adopts-a-recorded-outcome', async () => {
    // A run that observes its own cancellation adopts the recorded outcome
    // rather than trusting its local view: here a concurrent "resume" already
    // rewrote the row to SUCCESS, so the handle reports that outcome instead
    // of a cancellation that is no longer the workflow's state.
    const { handle, ctrl } = await startBlockedRun('selfCancellingWorkflow');
    const recorded = await encodeOutput('recorded-after-cancel');
    await rewriteRow(handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();

    await expect(handle.getResult()).resolves.toBe('recorded-after-cancel');
  });

  test('cancelled-run-still-reports-cancellation-for-a-cancelled-row', async () => {
    const { handle, ctrl } = await startBlockedRun('selfCancellingWorkflow');
    await rewriteRow(handle.workflowID, StatusString.CANCELLED);
    ctrl.release.set();

    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('failed-run-adopts-the-recorded-outcome-instead-of-its-own-error', async () => {
    // The ownership rule applies to a run that fails as much as to one that
    // succeeds: a run whose error write is refused must deliver the recorded
    // outcome, not the error it computed locally.
    const { handle, ctrl } = await startBlockedRun('throwingWorkflow');
    const recorded = await encodeOutput('recorded-elsewhere');
    await rewriteRow(handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();

    // The run's own failure must not surface, and must not be recorded.
    await expect(handle.getResult()).resolves.toBe('recorded-elsewhere');
    const outcome = await readOutcome(systemDBClient, handle.workflowID);
    expect(outcome.status).toBe(StatusString.SUCCESS);
    expect(outcome.output).toBe(recorded.serializedValue);
    expect(outcome.error).toBeNull();
  });

  test('duplicate-execution-releases-the-running-entry-before-parking', async () => {
    // A conflicting step checkpoint means another execution owns this
    // workflow's progress. The run must release its running-workflow entry
    // before parking, so a resume re-dispatched to this executor is not
    // blocked by the parked run, and then adopt the recorded outcome.
    const { handle, ctrl } = await startBlockedRun('blockedStepWorkflow');
    await plantConflictingCheckpoint(systemDBClient, handle.workflowID, ctrl.stepID);
    // ENQUEUED with no queue name: nothing dequeues it, so the run stays
    // parked until this test records the outcome itself.
    await rewriteRow(handle.workflowID, StatusString.ENQUEUED);
    ctrl.release.set();

    const resultPromise: Promise<{ result?: string; error?: Error }> = handle.getResult().then(
      (result) => ({ result }),
      (error: Error) => ({ error }),
    );

    // The parked run must not hold the running-workflow entry.
    await retryUntilSuccess(() => {
      expect(DBOSExecutor.globalInstance!.systemDatabase.checkForRunningWorkflow(handle.workflowID)).toBe(false);
    }, 30000);
    const parked = await Promise.race([resultPromise.then(() => false), sleepms(2000).then(() => true)]);
    expect(parked).toBe(true);

    const recorded = await encodeOutput('recorded-by-owner');
    await rewriteRow(handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    await expect(resultPromise).resolves.toEqual({ result: 'recorded-by-owner' });
  }, 60000);

  test('awaited-cancellation-adopts-the-recorded-outcome', async () => {
    // The parent fails because its child was cancelled, but its row was taken
    // away while it waited: the recorded outcome wins over the awaited-
    // cancellation error the parent computed.
    const { handle, ctrl } = await startBlockedRun('parentOfBlockedWorkflow');
    const childID = ctrl.workflowID!;
    expect(childID).not.toBe(handle.workflowID);

    const recorded = await encodeOutput('recorded-elsewhere');
    await rewriteRow(handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    // Cancel only the child: the parent's outcome comes from its rewritten row.
    await DBOS.cancelWorkflow(childID);
    ctrl.release.set();

    await expect(handle.getResult()).resolves.toBe('recorded-elsewhere');
    const outcome = await readOutcome(systemDBClient, handle.workflowID);
    expect(outcome.status).toBe(StatusString.SUCCESS);
    expect(outcome.error).toBeNull();
  });

  test('portable-failed-run-adopts-the-recorded-outcome', async () => {
    // A portable-serialized run reaches the same ownership decision as any
    // other. Its error is revived only where the revived error is used, because
    // reviving a portable error throws it: reviving before the decision would
    // deliver this run's own failure while the row says the workflow succeeded.
    const { handle, ctrl } = await startBlockedRun('throwingPortableWorkflow');
    const recorded = await encodeOutput('recorded-elsewhere');
    await rewriteRow(handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();

    await expect(handle.getResult()).resolves.toBe('recorded-elsewhere');
    const outcome = await readOutcome(systemDBClient, handle.workflowID);
    expect(outcome.status).toBe(StatusString.SUCCESS);
    expect(outcome.output).toBe(recorded.serializedValue);
    expect(outcome.error).toBeNull();
  });

  test('failed-run-fails-fast-when-its-row-is-deleted', async () => {
    // Same fail-fast as the success path, reached through the error path: the
    // refused error write leaves the run parking on a row it knows existed, so
    // a missing row must not be read as "not inserted yet" and polled forever.
    const { handle, ctrl } = await startBlockedRun('throwingWorkflow');
    await systemDBClient.query(`DELETE FROM dbos.workflow_status WHERE workflow_uuid=$1`, [handle.workflowID]);
    ctrl.release.set();

    await expect(handle.getResult()).rejects.toThrow(DBOSNonExistentWorkflowError);
  });

  test('duplicate-execution-reports-the-workflows-own-cancellation', async () => {
    // The duplicate-execution outcome is delivered to the workflow's own
    // handle, so a CANCELLED row must surface as the workflow's cancellation —
    // not as DBOSAwaitedWorkflowCancelledError, which is what an awaiter of
    // some other workflow would see.
    const { handle, ctrl } = await startBlockedRun('blockedStepWorkflow');
    await plantConflictingCheckpoint(systemDBClient, handle.workflowID, ctrl.stepID);
    await rewriteRow(handle.workflowID, StatusString.CANCELLED);
    ctrl.release.set();

    const error = await handle.getResult().then(
      () => undefined,
      (e: Error) => e,
    );
    expect(error).toBeInstanceOf(DBOSWorkflowCancelledError);
    expect(error).not.toBeInstanceOf(DBOSAwaitedWorkflowCancelledError);
  });

  test('duplicate-execution-reports-the-dead-letter-error', async () => {
    // Same perspective for a dead-lettered row: the duplicate execution must
    // report the dead-letter error rather than a completion.
    const { handle, ctrl } = await startBlockedRun('blockedStepWorkflow');
    await plantConflictingCheckpoint(systemDBClient, handle.workflowID, ctrl.stepID);
    await rewriteRow(handle.workflowID, StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
    ctrl.release.set();

    await expect(handle.getResult()).rejects.toThrow(DBOSMaxRecoveryAttemptsExceededError);
  });
});

// The workflow span reflects the adopted outcome: `cached` marks a result
// served from the store rather than computed by this run, and the final span
// status follows the adopted result, overriding anything stamped before
// parking.
describe('workflow-outcome-ownership-spans', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;
  const memoryExporter = new InMemorySpanExporter();

  beforeAll(() => {
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
    });
    provider.register();
    // Same system database as the suite above, already migrated by its
    // beforeAll. Re-running setUpDBOSTestSysDb here would drop it, which races
    // with connections that suite has not finished closing.
    config = { ...generateDBOSTestConfig(), tracingEnabled: true };
    DBOS.setConfig(config);
  });

  afterAll(() => {
    trace.disable();
    context.disable();
  });

  beforeEach(async () => {
    memoryExporter.reset();
    await DBOS.launch();
    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
  });

  function workflowSpan(workflowID: string) {
    return memoryExporter.getFinishedSpans().find((s) => s.attributes['operationUUID'] === workflowID);
  }

  test('adopted-success-sets-span-ok-and-cached', async () => {
    const { handle, ctrl } = await startBlockedRun();
    const recorded = await encodeOutput('recorded-elsewhere');
    await rewriteRowWith(systemDBClient, handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();
    await expect(handle.getResult()).resolves.toBe('recorded-elsewhere');

    const span = workflowSpan(handle.workflowID);
    expect(span).toBeDefined();
    expect(span!.attributes['cached']).toBe(true);
    expect(span!.status.code).toBe(SpanStatusCode.OK);
  });

  test('adopted-cancellation-sets-span-error-and-cached', async () => {
    const { handle, ctrl } = await startBlockedRun('selfCancellingWorkflow');
    await rewriteRowWith(systemDBClient, handle.workflowID, StatusString.CANCELLED);
    ctrl.release.set();
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);

    const span = workflowSpan(handle.workflowID);
    expect(span).toBeDefined();
    expect(span!.attributes['cached']).toBe(true);
    expect(span!.status.code).toBe(SpanStatusCode.ERROR);
    expect(span!.status.message).toContain('cancelled');
  });

  test('cancelled-run-adopting-a-recorded-success-sets-span-ok', async () => {
    // The cancellation branch stamps no ERROR up front: when the adopted
    // outcome is a success (a resume raced the cancellation), the span must
    // report OK.
    const { handle, ctrl } = await startBlockedRun('selfCancellingWorkflow');
    const recorded = await encodeOutput('recorded-after-cancel');
    await rewriteRowWith(systemDBClient, handle.workflowID, StatusString.SUCCESS, {
      output: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();
    await expect(handle.getResult()).resolves.toBe('recorded-after-cancel');

    const span = workflowSpan(handle.workflowID);
    expect(span).toBeDefined();
    expect(span!.attributes['cached']).toBe(true);
    expect(span!.status.code).toBe(SpanStatusCode.OK);
  });

  test('adopted-error-on-the-duplicate-execution-path-sets-span-error-and-cached', async () => {
    // The duplicate-execution path stamps no status up front, so the adopted
    // error is what must set it: an adopted failure here previously left the
    // span status unset.
    const { handle, ctrl } = await startBlockedRun('blockedStepWorkflow');
    await plantConflictingCheckpoint(systemDBClient, handle.workflowID, ctrl.stepID);
    const recorded = await encodeError('recorded failure');
    await rewriteRowWith(systemDBClient, handle.workflowID, StatusString.ERROR, {
      error: recorded.serializedValue,
      serialization: recorded.serialization,
    });
    ctrl.release.set();
    await expect(handle.getResult()).rejects.toThrow('recorded failure');

    const span = workflowSpan(handle.workflowID);
    expect(span).toBeDefined();
    expect(span!.attributes['cached']).toBe(true);
    expect(span!.status.code).toBe(SpanStatusCode.ERROR);
    expect(span!.status.message).toContain('recorded failure');
  });
});
