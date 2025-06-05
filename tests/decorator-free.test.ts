/* eslint-disable @typescript-eslint/unbound-method */
import { ConfiguredInstance, DBOS, DBOSClient, WorkflowQueue } from '../src/';
import { DBOSConflictingRegistrationError } from '../src/error';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { randomUUID } from 'node:crypto';

const queue = new WorkflowQueue('example_queue');

function stepTest(value: number): Promise<number> {
  expect(DBOS.stepStatus).toBeDefined();
  return Promise.resolve(value * 100);
}

const retryTestAttempts = Array<boolean>(5).fill(false);
function retryTest(value: number): Promise<number> {
  expect(DBOS.stepStatus!.currentAttempt).toBeDefined();
  retryTestAttempts[DBOS.stepStatus!.currentAttempt!] = true;
  if (DBOS.stepStatus!.currentAttempt! < 3) {
    throw new Error('retry-test-error');
  }
  return Promise.resolve(value * 100);
}

const regStepTest = DBOS.registerStep(stepTest);
const regRetryTest = DBOS.registerStep(retryTest, { retriesAllowed: true });

function wfRegStep(value: number) {
  return regStepTest(value);
}

function wfRunStep(value: number) {
  return DBOS.runStep(() => stepTest(value), { name: 'stepTest-runStep' });
}

function wfRegRetry(value: number) {
  return regRetryTest(value);
}

const regWFRegStep = DBOS.registerWorkflow(wfRegStep, 'wfRegStep');
const regWFRunStep = DBOS.registerWorkflow(wfRunStep, 'wfRunStep');
const regWFRunRetry = DBOS.registerWorkflow(wfRegRetry, 'wfRegRetry');

class TestClass extends ConfiguredInstance {
  static stepTest(value: number): Promise<number> {
    expect(DBOS.stepStatus).toBeDefined();
    return Promise.resolve(value * 100);
  }

  static readonly retryTestAttempts = Array<boolean>(5).fill(false);
  static retryTest(value: number): Promise<number> {
    expect(DBOS.stepStatus!.currentAttempt).toBeDefined();
    TestClass.retryTestAttempts[DBOS.stepStatus!.currentAttempt!] = true;
    if (DBOS.stepStatus!.currentAttempt! < 3) {
      throw new Error('retry-test-error');
    }
    return Promise.resolve(value * 100);
  }

  static wfRegStep(value: number) {
    return TestClass.stepTest(value);
  }

  static wfRunStep(value: number) {
    return DBOS.runStep(() => stepTest(value), { name: 'stepTest-runStep' });
  }

  static wfRegRetry(value: number) {
    return TestClass.retryTest(value);
  }

  stepTest(value: number): Promise<number> {
    expect(DBOS.stepStatus).toBeDefined();
    return Promise.resolve(value * 100);
  }

  readonly retryTestAttempts = Array<boolean>(5).fill(false);
  retryTest(value: number): Promise<number> {
    expect(DBOS.stepStatus!.currentAttempt).toBeDefined();
    this.retryTestAttempts[DBOS.stepStatus!.currentAttempt!] = true;
    if (DBOS.stepStatus!.currentAttempt! < 3) {
      throw new Error('retry-test-error');
    }
    return Promise.resolve(value * 100);
  }

  wfRegStep(value: number) {
    return this.stepTest(value);
  }

  wfRunStep(value: number) {
    return DBOS.runStep(() => stepTest(value), { name: 'stepTest-runStep' });
  }

  wfRegRetry(value: number) {
    return this.retryTest(value);
  }
}

TestClass.stepTest = DBOS.registerStep(TestClass.stepTest);
TestClass.retryTest = DBOS.registerStep(TestClass.retryTest, { retriesAllowed: true });
TestClass.wfRegStep = DBOS.registerWorkflow(TestClass.wfRegStep, 'TestClass.wfRegStep');
TestClass.wfRunStep = DBOS.registerWorkflow(TestClass.wfRunStep, 'TestClass.wfRunStep');
TestClass.wfRegRetry = DBOS.registerWorkflow(TestClass.wfRegRetry, 'TestClass.wfRegRetry');

TestClass.prototype.stepTest = DBOS.registerStep(TestClass.prototype.stepTest);
TestClass.prototype.retryTest = DBOS.registerStep(TestClass.prototype.retryTest, { retriesAllowed: true });
TestClass.prototype.wfRegStep = DBOS.registerWorkflow(TestClass.prototype.wfRegStep, 'TestClass.prototype.wfRegStep');
TestClass.prototype.wfRunStep = DBOS.registerWorkflow(TestClass.prototype.wfRunStep, 'TestClass.prototype.wfRunStep');
TestClass.prototype.wfRegRetry = DBOS.registerWorkflow(
  TestClass.prototype.wfRegRetry,
  'TestClass.prototype.wfRegRetry',
);

describe('decorator-free-tests', () => {
  const config = generateDBOSTestConfig();
  const inst = new TestClass('TestClassInstance');

  beforeAll(async () => {
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('wf-free-step-reg', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await regWFRegStep(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('wfRegStep');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-free-step-reg-queue', async () => {
    const client = await DBOSClient.create(config.databaseUrl!);
    const handle = await client.enqueue<typeof regWFRegStep>(
      {
        queueName: queue.name,
        workflowClassName: '',
        workflowName: 'wfRegStep',
      },
      10,
    );
    await expect(handle.getResult()).resolves.toBe(1000);

    const status = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('wfRegStep');

    const steps = (await DBOS.listWorkflowSteps(handle.workflowID))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();

    await client.destroy();
  });

  test('wf-free-step-run', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await regWFRunStep(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('wfRunStep');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest-runStep');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-free-step-retry', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await regWFRunRetry(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('wfRegRetry');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('retryTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();

    expect(retryTestAttempts).toEqual([false, true, true, true, false]);
  });

  test('wf-static-step-reg', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await TestClass.wfRegStep(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRegStep');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-static-step-run', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await TestClass.wfRunStep(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRunStep');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest-runStep');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-static-step-retry', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await TestClass.wfRegRetry(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRegRetry');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('retryTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();

    expect(TestClass.retryTestAttempts).toEqual([false, true, true, true, false]);
  });

  test('wf-inst-step-reg', async () => {
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await inst.wfRegStep(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.prototype.wfRegStep');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-inst-step-run', async () => {
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await inst.wfRunStep(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.prototype.wfRunStep');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest-runStep');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-inst-step-retry', async () => {
    inst.retryTestAttempts.fill(false); // reset for test

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await inst.wfRegRetry(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.prototype.wfRegRetry');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('retryTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();

    expect(inst.retryTestAttempts).toEqual([false, true, true, true, false]);
  });
});

describe('registerWorkflow-tests', () => {
  test('dont-allow-duplicate-workflow-registration', () => {
    function workflow1(value: number) {
      return DBOS.runStep(() => stepTest(value), { name: 'stepTest-runStep' });
    }

    function workflow2(value: number) {
      return DBOS.runStep(() => stepTest(value), { name: 'stepTest-runStep' });
    }

    DBOS.registerWorkflow(workflow1, 'workflow1');
    expect(() => DBOS.registerWorkflow(workflow2, 'workflow1')).toThrow(DBOSConflictingRegistrationError);
  });
});
