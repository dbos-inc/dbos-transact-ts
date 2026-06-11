import { randomUUID } from 'node:crypto';
import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { DBOSMaxStepRetriesError, DBOSStepTimeoutError, DBOSWorkflowCancelledError } from '../src/error';
import { StatusString } from '../src/workflow';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
  static zombieDone = false;

  static reset() {
    StepTimeoutTestClass.attempts = 0;
    StepTimeoutTestClass.abortsObserved = 0;
    StepTimeoutTestClass.hangAttempts = 0;
    StepTimeoutTestClass.zombieDone = false;
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

  // Ignores the timeout signal: the first attempt keeps running long past the timeout, then resolves
  @DBOS.step({ retriesAllowed: true, maxAttempts: 2, intervalSeconds: 0, timeoutMS: 100 })
  static async uncooperativeStep() {
    StepTimeoutTestClass.attempts++;
    if (StepTimeoutTestClass.attempts === 1) {
      await sleep(1000);
      StepTimeoutTestClass.zombieDone = true;
      return 'zombie';
    }
    return 'fresh';
  }

  // Ignores the timeout signal: the first attempt keeps running long past the timeout, then rejects
  @DBOS.step({ retriesAllowed: true, maxAttempts: 2, intervalSeconds: 0, timeoutMS: 100 })
  static async uncooperativeThrowingStep() {
    StepTimeoutTestClass.attempts++;
    if (StepTimeoutTestClass.attempts === 1) {
      await sleep(1000);
      StepTimeoutTestClass.zombieDone = true;
      throw new Error('zombie rejection');
    }
    return 'fresh';
  }

  @DBOS.workflow()
  static async uncooperativeStepWorkflow() {
    return await StepTimeoutTestClass.uncooperativeStep();
  }

  @DBOS.workflow()
  static async uncooperativeThrowingStepWorkflow() {
    return await StepTimeoutTestClass.uncooperativeThrowingStep();
  }

  @DBOS.workflow()
  static async badRunStepWorkflow() {
    return await DBOS.runStep(async () => Promise.resolve(1), { name: 'bad-run-step', timeoutMS: -5 });
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

  test('uncooperative-zombie-resolution-is-discarded', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      await expect(StepTimeoutTestClass.uncooperativeStepWorkflow()).resolves.toBe('fresh');
    });
    expect(StepTimeoutTestClass.attempts).toBe(2);
    // The retry won while the abandoned first attempt was still running
    expect(StepTimeoutTestClass.zombieDone).toBe(false);

    // Let the abandoned attempt run to completion, then confirm its result went nowhere
    while (!StepTimeoutTestClass.zombieDone) {
      await sleep(50);
    }
    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].output).toBe('fresh');

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status?.status).toBe(StatusString.SUCCESS);
  });

  test('uncooperative-zombie-rejection-is-swallowed', async () => {
    const zombieRejections: unknown[] = [];
    const onUnhandled = (reason: unknown) => {
      if (reason instanceof Error && reason.message === 'zombie rejection') {
        zombieRejections.push(reason);
      }
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      await expect(StepTimeoutTestClass.uncooperativeThrowingStepWorkflow()).resolves.toBe('fresh');
      expect(StepTimeoutTestClass.attempts).toBe(2);

      // Let the abandoned attempt reject, then give the event loop a beat to surface any unhandled rejection
      while (!StepTimeoutTestClass.zombieDone) {
        await sleep(50);
      }
      await sleep(100);
      expect(zombieRejections.length).toBe(0);
    } finally {
      process.removeListener('unhandledRejection', onUnhandled);
    }
  });

  test('invalid-timeoutMS-is-rejected', async () => {
    // At registration, via DBOS.registerStep
    for (const timeoutMS of [0, -100, NaN, Infinity]) {
      expect(() =>
        DBOS.registerStep(async () => Promise.resolve(), { name: `bad-step-${timeoutMS}`, timeoutMS }),
      ).toThrow(`Invalid timeoutMS (${timeoutMS}) in configuration of step bad-step-${timeoutMS}`);
    }

    // At class definition, via the @DBOS.step decorator (registration requires DBOS to not be launched)
    await DBOS.shutdown();
    expect(() => {
      class BadStepClass {
        @DBOS.step({ name: 'bad-decorated-step', timeoutMS: -1 })
        static async badStep() {
          return Promise.resolve();
        }
      }
      void BadStepClass;
    }).toThrow('Invalid timeoutMS (-1)');
    await DBOS.launch();

    // At call time, via DBOS.runStep
    await expect(StepTimeoutTestClass.badRunStepWorkflow()).rejects.toThrow(
      'Invalid timeoutMS (-5) in configuration of step bad-run-step',
    );
  });

  test('workflow-deadline-wins-over-step-timeout', async () => {
    const workflowID = randomUUID();
    StepTimeoutTestClass.hangAttempts = 3;

    const handle = await DBOS.startWorkflow(StepTimeoutTestClass, { workflowID, timeoutMS: 100 }).slowStepWorkflow();
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);

    // The cancellation stopped the retry loop after the first attempt instead of consuming all three,
    // and was not recorded as the step's durable result
    expect(StepTimeoutTestClass.attempts).toBe(1);
    const steps = (await DBOS.listWorkflowSteps(workflowID))!;
    expect(steps.length).toBe(0);
  });
});
