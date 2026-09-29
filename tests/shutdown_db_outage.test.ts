import { DBOS, StatusString } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { SystemDatabase } from '../src/system_database';
import { runWithAbortableDbRetries } from '../src/context';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event } from './helpers';
import { dbRetryConfig, sleepms } from '../src/utils';

// Every wait in this file is bounded, so a regression fails the test instead of hanging jest.
const WAIT_TIMEOUT_MS = 10000;

async function within<T>(promise: Promise<T>, what: string, ms: number = WAIT_TIMEOUT_MS): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out waiting for ${what}`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function connectionRefused(): Error {
  return Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:5432'), { code: 'ECONNREFUSED' });
}

/** Make status reads of matching workflows fail as if the database refused connections, beneath dbRetry. */
function failStatusReads(matches: (workflowID: string) => boolean) {
  const failing = new Event();
  // eslint-disable-next-line @typescript-eslint/unbound-method -- always called with the instance as `this`
  const original = SystemDatabase.prototype.listWorkflows;
  const spy = jest.spyOn(SystemDatabase.prototype, 'listWorkflows').mockImplementation(async function (
    this: SystemDatabase,
    input,
  ) {
    if (input.workflowIDs?.some(matches)) {
      failing.set();
      throw connectionRefused();
    }
    return original.call(this, input);
  });
  return { failing, spy };
}

/** Make step checkpoint writes fail as if the database refused connections, while `state.failing` is set. */
function failStepCheckpoints() {
  const state = { failing: true, attempts: 0 };
  const failed = new Event();
  type Internal = { recordOperationResultInternal: (...args: unknown[]) => Promise<void> };
  const proto = SystemDatabase.prototype as unknown as Internal;
  const original = proto.recordOperationResultInternal;
  const spy = jest.spyOn(proto, 'recordOperationResultInternal').mockImplementation(async function (
    this: unknown,
    ...args: unknown[]
  ) {
    if (state.failing) {
      state.attempts++;
      failed.set();
      throw connectionRefused();
    }
    return original.apply(this, args);
  });
  return { state, failed, spy };
}

/** Make the SUCCESS write of `workflowID`'s outcome fail as if the database refused connections, while `state.failing` is set. */
function failOutputWrites(workflowID: string) {
  const state = { failing: true, attempts: 0 };
  const failed = new Event();
  type Internal = {
    updateWorkflowStatus: (client: unknown, id: string, status: string, ...rest: unknown[]) => Promise<number>;
  };
  const proto = SystemDatabase.prototype as unknown as Internal;
  const original = proto.updateWorkflowStatus;
  const spy = jest.spyOn(proto, 'updateWorkflowStatus').mockImplementation(async function (
    this: unknown,
    client: unknown,
    id: string,
    status: string,
    ...rest: unknown[]
  ) {
    if (state.failing && id === workflowID && status === StatusString.SUCCESS) {
      state.attempts++;
      failed.set();
      throw connectionRefused();
    }
    return original.call(this, client, id, status, ...rest);
  });
  return { state, failed, spy };
}

async function scheduledWorkflow(_scheduledDate: Date, _context: unknown) {}
const checkpointQueueName = 'shutdown_db_outage_checkpoint_queue';
const dispatchQueueName = 'shutdown_db_outage_dispatch_queue';

const regScheduledWorkflow = DBOS.registerWorkflow(scheduledWorkflow, { name: 'shutdownDbOutageScheduled' });

const dispatchedWorkflow = DBOS.registerWorkflow(() => Promise.resolve('dispatched'), {
  name: 'shutdownDbOutageDispatched',
});

const checkpointedWorkflow = DBOS.registerWorkflow(
  async () => {
    return await DBOS.runStep(() => Promise.resolve('step'), { name: 'shutdownDbOutageStep' });
  },
  { name: 'shutdownDbOutageCheckpointed' },
);

describe('shutdown-during-db-outage', () => {
  let config: DBOSConfig;
  const savedBackoff = { ...dbRetryConfig };

  beforeAll(async () => {
    // Short enough to retry many times per test, long enough that the retries span a shutdown.
    dbRetryConfig.initialBackoffSec = 0.1;
    dbRetryConfig.maxBackoffSec = 0.3;
    config = generateDBOSTestConfig();
    config.schedulerPollingIntervalMs = 1000;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  afterAll(() => {
    Object.assign(dbRetryConfig, savedBackoff);
  });

  beforeEach(async () => {
    await DBOS.launch();
    for (const name of [checkpointQueueName, dispatchQueueName]) {
      await DBOS.registerQueue(name, { onConflict: 'always_update', minPollingIntervalMs: 100 });
    }
  });

  afterEach(async () => {
    jest.restoreAllMocks();
    await DBOS.shutdown();
  }, 30000);

  test('scheduler-stuck-in-db-retry-does-not-hang-shutdown', async () => {
    const { failing } = failStatusReads((id) => id.startsWith('sched-shutdown-db-outage-'));
    await DBOS.createSchedule({
      scheduleName: 'shutdown-db-outage',
      workflowFn: regScheduledWorkflow,
      schedule: '* * * * * *',
    });

    // A tick fires and its idempotency read retries against the "unreachable" database.
    await within(failing.wait(), 'a scheduler tick to hit the outage');
    await within(DBOS.shutdown(), 'shutdown to return');
    jest.restoreAllMocks();

    await DBOS.launch();
    await DBOS.deleteSchedule('shutdown-db-outage');
  }, 30000);

  test('queue-dispatch-stuck-in-db-retry-does-not-hang-shutdown', async () => {
    const workflowID = `shutdown-db-outage-dispatch-${Date.now()}`;
    const { failing } = failStatusReads((id) => id === workflowID);
    await DBOS.startWorkflow(dispatchedWorkflow, { workflowID, queueName: dispatchQueueName })();

    // The runner claims the workflow, then retries reading its status to dispatch it.
    await within(failing.wait(), 'the queue runner to hit the outage');
    await within(DBOS.shutdown(), 'shutdown to return');
    jest.restoreAllMocks();

    // The abandoned claim leaves the workflow PENDING, and recovery runs it on the next launch.
    await DBOS.launch();
    const handle = DBOS.retrieveWorkflow<string>(workflowID);
    await expect(within(handle.getResult(), 'the recovered workflow')).resolves.toBe('dispatched');
  }, 30000);

  test('queued-workflow-checkpoint-keeps-retrying-through-shutdown', async () => {
    const workflowID = `shutdown-db-outage-checkpoint-${Date.now()}`;
    const { state, failed } = failStepCheckpoints();
    await DBOS.startWorkflow(checkpointedWorkflow, { workflowID, queueName: checkpointQueueName })();

    // The workflow was launched by the queue runner, whose retries shutdown abandons.
    await within(failed.wait(), 'the step checkpoint to hit the outage');
    const shutdown = DBOS.shutdown({ workflowCompletionTimeoutMS: WAIT_TIMEOUT_MS });
    // Let shutdown stop the queue runner while the checkpoint write is still failing.
    await sleepms(1000);
    const attemptsDuringShutdown = state.attempts;
    state.failing = false;
    await within(shutdown, 'shutdown to drain the workflow');

    // The write kept retrying after the runner stopped, so the workflow succeeded instead of recording the outage.
    expect(attemptsDuringShutdown).toBeGreaterThan(1);
    await DBOS.launch();
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.SUCCESS);
    await expect(DBOS.retrieveWorkflow<string>(workflowID).getResult()).resolves.toBe('step');
  }, 30000);

  test('queued-workflow-output-keeps-retrying-through-shutdown', async () => {
    const workflowID = `shutdown-db-outage-output-${Date.now()}`;
    const { state, failed } = failOutputWrites(workflowID);
    await DBOS.startWorkflow(dispatchedWorkflow, { workflowID, queueName: dispatchQueueName })();

    // The outcome write runs outside the workflow's context, so only the run's own retry scope protects it.
    await within(failed.wait(), 'the output write to hit the outage');
    const attemptsAtShutdown = state.attempts;
    const shutdown = DBOS.shutdown({ workflowCompletionTimeoutMS: WAIT_TIMEOUT_MS });
    await sleepms(1000);
    const attemptsBeforeRecovery = state.attempts;
    state.failing = false;
    await within(shutdown, 'shutdown to drain the workflow');

    // Had the write been abandoned, the connection error would have been recorded as the workflow's outcome.
    expect(attemptsBeforeRecovery).toBeGreaterThan(attemptsAtShutdown);
    await DBOS.launch();
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.SUCCESS);
    await expect(DBOS.retrieveWorkflow<string>(workflowID).getResult()).resolves.toBe('dispatched');
  }, 30000);

  test('workflow-code-ignores-an-aborted-retry-scope', async () => {
    const { state, failed } = failStepCheckpoints();
    const aborted = new AbortController();
    aborted.abort();

    // Even run directly inside an aborted scope, a workflow's checkpoint writes retry until they succeed.
    const result = runWithAbortableDbRetries(aborted.signal, () => checkpointedWorkflow());
    await within(failed.wait(), 'the step checkpoint to hit the outage');
    await sleepms(500);
    state.failing = false;
    await expect(within(result, 'the workflow')).resolves.toBe('step');
    expect(state.attempts).toBeGreaterThan(1);
  }, 30000);
});
