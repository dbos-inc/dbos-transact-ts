import { DBOS, ConfiguredInstance } from '../src/';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';
import { randomUUID } from 'node:crypto';
import { StatusString } from '../src/workflow';
import { DBOSError, DBOSMaxStepRetriesError, DBOSNotRegisteredError, DBOSUnexpectedStepError } from '../src/error';
import { DBOSConfig } from '../src/dbos-executor';

describe('failures-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    FailureTestClass.cnt = 0;
    FailureTestClass.success = '';
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('dbos-error', async () => {
    const wfUUID1 = randomUUID();
    await DBOS.withNextWorkflowID(
      wfUUID1,
      async () =>
        await expect(FailureTestClass.testStep(11)).rejects.toThrow(new DBOSError('test dbos error with code.', 11)),
    );

    const retrievedHandle = DBOS.retrieveWorkflow<string>(wfUUID1);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ERROR,
    });
    await expect(retrievedHandle.getResult()).rejects.toThrow(new DBOSError('test dbos error with code.', 11));

    // Test without code.
    await expect(FailureTestClass.testStep()).rejects.toThrow(new DBOSError('test dbos error without code.'));
  });

  test('failing-step', async () => {
    let startTime = Date.now();
    await expect(FailureTestClass.testFailStep()).resolves.toBe(2);
    expect(Date.now() - startTime).toBeGreaterThanOrEqual(1000);

    startTime = Date.now();
    try {
      await FailureTestClass.testFailStep();
      expect(true).toBe(false); // An exception should be thrown first
    } catch (error) {
      const e = error as DBOSMaxStepRetriesError;
      expect(e.message).toContain('Step testFailStep has exceeded its maximum of 2 retries.');
      expect(e.errors.length).toBe(2);
      expect(e.errors[0].message).toBe('bad number');
      expect(e.errors[1].message).toBe('bad number');
    }
    expect(Date.now() - startTime).toBeGreaterThanOrEqual(1000);
  });

  test('nonretry-step', async () => {
    const workflowUUID = randomUUID();

    // Should throw an error.
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(FailureTestClass.testNoRetry()).rejects.toThrow(new Error('failed no retry'));
    });
    expect(FailureTestClass.cnt).toBe(1);

    // If we retry again, we should get the same error, but numRun should still be 1 (OAOO).
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(FailureTestClass.testNoRetry()).rejects.toThrow(new Error('failed no retry'));
    });
    expect(FailureTestClass.cnt).toBe(1);
  });

  test('no-registration-startwf', async () => {
    // Invoke an unregistered workflow.
    expect(() => DBOS.startWorkflow(FailureTestClass).noRegWorkflow2(10)).toThrow(DBOSNotRegisteredError);

    // Invoke an unregistered transaction.
    expect(() => DBOS.startWorkflow(new FailureTestClass()).noRegFunction(10)).toThrow(DBOSNotRegisteredError);

    return Promise.resolve();
  });

  class TestStepStatus {
    static count = 0;
    static max_attempts = 5;

    @DBOS.step({ retriesAllowed: true, maxAttempts: TestStepStatus.max_attempts, intervalSeconds: 0 })
    static async stepOne() {
      expect(DBOS.stepID).toBe(0);
      expect(DBOS.stepStatus?.stepID).toBe(0);
      expect(DBOS.stepStatus?.currentAttempt).toBe(TestStepStatus.count + 1);
      expect(DBOS.stepStatus?.maxAttempts).toBe(TestStepStatus.max_attempts);
      TestStepStatus.count += 1;
      if (TestStepStatus.count < TestStepStatus.max_attempts) {
        throw new Error('fail');
      }
      return Promise.resolve();
    }

    @DBOS.step()
    static async stepTwo() {
      expect(DBOS.stepID).toBe(1);
      expect(DBOS.stepStatus?.stepID).toBe(1);
      expect(DBOS.stepStatus?.currentAttempt).toBeUndefined();
      expect(DBOS.stepStatus?.maxAttempts).toBeUndefined();
      return Promise.resolve();
    }

    @DBOS.step()
    static async stepThree() {
      expect(DBOS.stepID).toBe(2);
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async workflow() {
      await TestStepStatus.stepOne();
      await TestStepStatus.stepTwo();
      await TestStepStatus.stepThree();
      return DBOS.workflowID;
    }
  }

  test('test-step-status', async () => {
    await expect(TestStepStatus.workflow()).resolves.toBeTruthy();
  });

  test('non-deterministic-workflow-step', async () => {
    const wfidnds = 'NonDetWFStep';

    await DBOS.withNextWorkflowID(wfidnds, async () => {
      await NDWFS.nondetWorkflow();
    });
    NDWFS.flag = false;

    const ndh = await reexecuteWorkflowById(wfidnds);
    await expect(ndh!.getResult()).rejects.toThrow(DBOSUnexpectedStepError);
  });

  test('non-deterministic-workflow-tx', async () => {
    const wfidndt = 'NonDetWFTx';

    await DBOS.withNextWorkflowID(wfidndt, async () => {
      await NDWFT.nondetWorkflow();
    });
    NDWFT.flag = false;

    const ndh = await reexecuteWorkflowById(wfidndt);
    await expect(ndh!.getResult()).rejects.toThrow(DBOSUnexpectedStepError);
  });

  test('not launched', async () => {
    await DBOS.shutdown();
    const df = DBOS.registerWorkflow(
      async () => {
        return Promise.resolve('Unreached');
      },
      { name: 'unregistered' },
    );

    await expect(df()).rejects.toThrow('`DBOS.launch()` must be called before running workflows');

    await DBOS.launch();
  });
});

class FailureTestClass extends ConfiguredInstance {
  initialize(): Promise<void> {
    return Promise.resolve();
  }

  constructor() {
    super('name');
  }

  static cnt = 0;
  static success: string = '';

  @DBOS.step({ retriesAllowed: false })
  static async testStep(code?: number) {
    const err = code
      ? new DBOSError('test dbos error with code.', code)
      : new DBOSError('test dbos error without code.');
    return Promise.reject(err);
  }

  @DBOS.step({ retriesAllowed: true, intervalSeconds: 1, maxAttempts: 2 })
  static async testFailStep() {
    FailureTestClass.cnt++;
    if (FailureTestClass.cnt !== DBOS.stepStatus!.maxAttempts) {
      throw new Error('bad number');
    }
    return Promise.resolve(FailureTestClass.cnt);
  }

  @DBOS.step({ retriesAllowed: false })
  static async testNoRetry() {
    FailureTestClass.cnt++;
    return Promise.reject(new Error('failed no retry'));
  }

  static async noRegWorkflow2(code: number) {
    return Promise.resolve(code + 1);
  }

  async noRegFunction(code: number) {
    return Promise.resolve(code + 1);
  }
}

class NDWFS {
  static flag = true;

  @DBOS.step()
  static async stepOne() {
    return Promise.resolve();
  }

  @DBOS.step()
  static async stepTwo() {
    return Promise.resolve();
  }

  @DBOS.workflow()
  static async nondetWorkflow() {
    if (NDWFS.flag) return NDWFS.stepOne();
    return NDWFS.stepTwo();
  }
}

class NDWFT {
  static flag = true;

  @DBOS.step()
  static async stepOne() {
    return Promise.resolve();
  }

  @DBOS.step()
  static async stepTwo() {
    return Promise.resolve();
  }

  @DBOS.workflow()
  static async nondetWorkflow() {
    if (NDWFT.flag) return NDWFT.stepOne();
    return NDWFT.stepTwo();
  }
}
