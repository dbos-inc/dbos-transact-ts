import { randomUUID } from 'node:crypto';
import { Client } from 'pg';

import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { context, SpanStatusCode, trace } from '@opentelemetry/api';

import { DBOS, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import {
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
const controls = new Map<string, { started: Event; release: Event }>();

class OutcomeOwnership {
  @DBOS.workflow()
  static async blockedWorkflow(id: string): Promise<string> {
    const ctrl = controls.get(id);
    if (!ctrl) {
      throw new Error(`no control registered for workflow ${id}`);
    }
    ctrl.started.set();
    await ctrl.release.wait();
    return 'own-result';
  }

  // Stands in for a run that observes its own cancellation mid-flight: the
  // cancellation is thrown only after the test has rewritten the row.
  @DBOS.workflow()
  static async selfCancellingWorkflow(id: string): Promise<string> {
    const ctrl = controls.get(id);
    if (!ctrl) {
      throw new Error(`no control registered for workflow ${id}`);
    }
    ctrl.started.set();
    await ctrl.release.wait();
    throw new DBOSWorkflowCancelledError(id);
  }
}

// Start a run and return once it is blocked inside the workflow function,
// with its row PENDING.
async function startBlockedRun(workflow: 'blockedWorkflow' | 'selfCancellingWorkflow' = 'blockedWorkflow') {
  const id = `outcome-ownership-${randomUUID()}`;
  const ctrl = { started: new Event(), release: new Event() };
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
    const { rows } = await systemDBClient.query<{ status: string; output: string }>(
      `SELECT status, output FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    expect(rows[0].status).toBe(StatusString.SUCCESS);
    expect(rows[0].output).toBe(recorded.serializedValue);
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
    const { rows } = await systemDBClient.query<{ status: string; output: string | null }>(
      `SELECT status, output FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    expect(rows[0].status).toBe(StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
    expect(rows[0].output).toBeNull();
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
});

// The workflow span reflects the adopted outcome: `cached` marks a result
// served from the store rather than computed by this run, and the final span
// status follows the adopted result, overriding anything stamped before
// parking.
describe('workflow-outcome-ownership-spans', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;
  const memoryExporter = new InMemorySpanExporter();

  beforeAll(async () => {
    const provider = new NodeTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(memoryExporter)],
    });
    provider.register();
    config = { ...generateDBOSTestConfig(), tracingEnabled: true };
    await setUpDBOSTestSysDb(config);
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
});
