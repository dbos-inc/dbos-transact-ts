import { randomUUID } from 'node:crypto';
import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { DBOSMaxStepRetriesError, DBOSStepTimeoutError, DBOSWorkflowCancelledError } from '../src/error';
import { StatusString } from '../src/workflow';

// Sleep that resolves after `ms`, or rejects with the signal's reason when it aborts.
// Models a step operation that cooperatively cancels on timeout.
function sleepUnlessAborted(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason as Error);
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal!.reason as Error);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

class StepTimeoutTestClass {
  static attempts = 0;
  static abortsObserved = 0;
  static hangAttempts = 0; // The first `hangAttempts` attempts hang until aborted

  static reset() {
    StepTimeoutTestClass.attempts = 0;
    StepTimeoutTestClass.abortsObserved = 0;
    StepTimeoutTestClass.hangAttempts = 0;
  }

  static async slowStepGuts() {
    StepTimeoutTestClass.attempts++;
    const signal = DBOS.stepStatus?.timeoutSignal;
    expect(signal).toBeDefined();
    if (StepTimeoutTestClass.attempts <= StepTimeoutTestClass.hangAttempts) {
      try {
        await sleepUnlessAborted(5000, signal);
      } catch (e) {
        StepTimeoutTestClass.abortsObserved++;
        throw e;
      }
    }
    return StepTimeoutTestClass.attempts;
  }

  @DBOS.step({ retriesAllowed: true, maxAttempts: 3, intervalSeconds: 0, timeoutMS: 200 })
  static async slowStep() {
    return await StepTimeoutTestClass.slowStepGuts();
  }

  @DBOS.step({
    retriesAllowed: true,
    maxAttempts: 3,
    intervalSeconds: 0,
    timeoutMS: 200,
    shouldRetry: (e) => !(e instanceof DBOSStepTimeoutError),
  })
  static async noRetryOnTimeoutStep() {
    return await StepTimeoutTestClass.slowStepGuts();
  }

  @DBOS.step({ retriesAllowed: false, timeoutMS: 200 })
  static async noRetriesStep() {
    return await StepTimeoutTestClass.slowStepGuts();
  }

  @DBOS.step()
  static async plainStep() {
    expect(DBOS.stepStatus?.timeoutSignal).toBeUndefined();
    return Promise.resolve('plain');
  }

  @DBOS.workflow()
  static async slowStepWorkflow() {
    return await StepTimeoutTestClass.slowStep();
  }

  @DBOS.workflow()
  static async catchTimeoutWorkflow() {
    try {
      return await StepTimeoutTestClass.noRetryOnTimeoutStep();
    } catch (e) {
      return `caught: ${(e as Error).message}`;
    }
  }

  @DBOS.workflow()
  static async runStepTimeoutWorkflow() {
    return await DBOS.runStep(
      async () => {
        return await StepTimeoutTestClass.slowStepGuts();
      },
      { name: 'run-step-with-timeout', retriesAllowed: true, maxAttempts: 2, intervalSeconds: 0, timeoutMS: 200 },
    );
  }
}

describe('step-timeout-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    StepTimeoutTestClass.reset();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('timeout-attempt-is-retried-then-succeeds', async () => {
    const wfid = randomUUID();
    StepTimeoutTestClass.hangAttempts = 1;

    await DBOS.withNextWorkflowID(wfid, async () => {
      await expect(StepTimeoutTestClass.slowStepWorkflow()).resolves.toBe(2);
    });
    expect(StepTimeoutTestClass.attempts).toBe(2);
    expect(StepTimeoutTestClass.abortsObserved).toBe(1);

    // The step timeout did not cancel the workflow
    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status?.status).toBe(StatusString.SUCCESS);
  });

  test('timeouts-exhaust-retries', async () => {
    StepTimeoutTestClass.hangAttempts = 3;

    try {
      await StepTimeoutTestClass.slowStepWorkflow();
      expect(true).toBe(false); // An exception should be thrown first
    } catch (error) {
      const e = error as DBOSMaxStepRetriesError;
      expect(e).toBeInstanceOf(DBOSMaxStepRetriesError);
      expect(e.errors.length).toBe(3);
      expect(e.errors.every((err) => err instanceof DBOSStepTimeoutError)).toBe(true);
      expect(e.errors[0].message).toContain('timed out after 200ms');
    }
    expect(StepTimeoutTestClass.attempts).toBe(3);
    expect(StepTimeoutTestClass.abortsObserved).toBe(3);
  });

  test('shouldRetry-can-opt-out-of-timeouts', async () => {
    const wfid = randomUUID();
    StepTimeoutTestClass.hangAttempts = 3;

    await DBOS.withNextWorkflowID(wfid, async () => {
      await expect(StepTimeoutTestClass.catchTimeoutWorkflow()).resolves.toBe(
        'caught: Step noRetryOnTimeoutStep timed out after 200ms',
      );
    });
    expect(StepTimeoutTestClass.attempts).toBe(1);

    // The timeout error was durably recorded for the step
    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].error).not.toBeNull();
    expect(steps[0].error!.message).toContain('timed out after 200ms');
  });

  test('timeout-applies-without-retries', async () => {
    StepTimeoutTestClass.hangAttempts = 1;

    await expect(StepTimeoutTestClass.noRetriesStep()).rejects.toThrow(new DBOSStepTimeoutError('noRetriesStep', 200));
    expect(StepTimeoutTestClass.attempts).toBe(1);
    expect(StepTimeoutTestClass.abortsObserved).toBe(1);
  });

  test('timeout-applies-to-runStep', async () => {
    StepTimeoutTestClass.hangAttempts = 1;

    await expect(StepTimeoutTestClass.runStepTimeoutWorkflow()).resolves.toBe(2);
    expect(StepTimeoutTestClass.attempts).toBe(2);
    expect(StepTimeoutTestClass.abortsObserved).toBe(1);
  });

  test('fast-step-is-unaffected', async () => {
    await expect(StepTimeoutTestClass.slowStepWorkflow()).resolves.toBe(1);
    expect(StepTimeoutTestClass.attempts).toBe(1);
    expect(StepTimeoutTestClass.abortsObserved).toBe(0);
  });

  test('no-timeout-no-signal', async () => {
    await expect(StepTimeoutTestClass.plainStep()).resolves.toBe('plain');
  });

  test('workflow-deadline-wins-over-step-timeout', async () => {
    const workflowID = randomUUID();
    StepTimeoutTestClass.hangAttempts = 3;

    const handle = await DBOS.startWorkflow(StepTimeoutTestClass, { workflowID, timeoutMS: 100 }).slowStepWorkflow();
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
  });
});
