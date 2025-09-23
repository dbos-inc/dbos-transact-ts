import { Client } from 'pg';
import { ConfiguredInstance, DBOS, DBOSClient, WorkflowHandle, WorkflowQueue } from '../src/';
import { DBOSConflictingRegistrationError } from '../src/error';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { randomUUID } from 'node:crypto';
import { promises as fsp } from 'node:fs';
import { DBOSExecutor } from '../src/dbos-executor';
import axios from 'axios';

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

const regWFRegStep = DBOS.registerWorkflow(wfRegStep, { name: 'wfRegStep' });
const regWFRunStep = DBOS.registerWorkflow(wfRunStep, { name: 'wfRunStep' });
const regWFRunRetry = DBOS.registerWorkflow(wfRegRetry);

const wfReturningString = DBOS.registerWorkflow(async (x: string) => Promise.resolve(`${x}${x}`), {
  name: 'wfReturningString',
});
const wfReturningHandle = DBOS.registerWorkflow(async (x: string) => await DBOS.startWorkflow(wfReturningString)(x), {
  name: 'wfReturningHandle',
});

class TestClass extends ConfiguredInstance {
  @DBOS.workflow()
  static decoratedWorkflow(value: number): Promise<number> {
    return TestClass.stepTestStatic(value);
  }

  static stepTestStatic(value: number): Promise<number> {
    expect(DBOS.stepStatus).toBeDefined();
    return Promise.resolve(value * 100);
  }

  static readonly retryTestAttempts = Array<boolean>(5).fill(false);
  static retryTestStatic(value: number): Promise<number> {
    expect(DBOS.stepStatus!.currentAttempt).toBeDefined();
    TestClass.retryTestAttempts[DBOS.stepStatus!.currentAttempt!] = true;
    if (DBOS.stepStatus!.currentAttempt! < 3) {
      throw new Error('retry-test-error');
    }
    return Promise.resolve(value * 100);
  }

  static wfRegStepStatic(value: number) {
    return TestClass.stepTestStatic(value);
  }

  static wfRunStepStatic(value: number) {
    return DBOS.runStep(() => stepTest(value), { name: 'stepTest-runStep' });
  }

  static wfRegRetryStatic(value: number) {
    return TestClass.retryTestStatic(value);
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

const inst = new TestClass('TestClassInstance');

TestClass.stepTestStatic = DBOS.registerStep(TestClass.stepTestStatic);
TestClass.retryTestStatic = DBOS.registerStep(TestClass.retryTestStatic, { retriesAllowed: true });
TestClass.wfRegStepStatic = DBOS.registerWorkflow(TestClass.wfRegStepStatic, { name: 'TestClass.wfRegStepStatic' });
TestClass.wfRunStepStatic = DBOS.registerWorkflow(TestClass.wfRunStepStatic, { name: 'TestClass.wfRunStepStatic' });
TestClass.wfRegRetryStatic = DBOS.registerWorkflow(TestClass.wfRegRetryStatic, { name: 'TestClass.wfRegRetryStatic' });

/* eslint-disable @typescript-eslint/unbound-method */
TestClass.prototype.stepTest = DBOS.registerStep(TestClass.prototype.stepTest);
TestClass.prototype.retryTest = DBOS.registerStep(TestClass.prototype.retryTest, {
  retriesAllowed: true,
});
TestClass.prototype.wfRegStep = DBOS.registerWorkflow(TestClass.prototype.wfRegStep, {
  name: 'TestClass.prototype.wfRegStep',
  ctorOrProto: TestClass,
});
TestClass.prototype.wfRunStep = DBOS.registerWorkflow(TestClass.prototype.wfRunStep, {
  name: 'TestClass.prototype.wfRunStep',
  ctorOrProto: TestClass,
});
TestClass.prototype.wfRegRetry = DBOS.registerWorkflow(TestClass.prototype.wfRegRetry, {
  name: 'TestClass.prototype.wfRegRetry',
  ctorOrProto: TestClass,
});
/* eslint-enable @typescript-eslint/unbound-method */

describe('decorator-free-tests', () => {
  const config = generateDBOSTestConfig();

  beforeAll(async () => {
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('static-registered-wf-startWorkflow', async () => {
    const handle = await DBOS.startWorkflow(TestClass, { queueName: queue.name }).wfRegStepStatic(10);
    await expect(handle.getResult()).resolves.toBe(1000);

    const wfid = handle.workflowID;
    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRegStepStatic');
    expect(status!.queueName).toBe(queue.name);

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTestStatic');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('static-registered-wf-startWorkflow-2', async () => {
    const handle = await DBOS.startWorkflow(TestClass.wfRegStepStatic, { queueName: queue.name })(10);
    await expect(handle.getResult()).resolves.toBe(1000);

    const wfid = handle.workflowID;
    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRegStepStatic');
    expect(status!.queueName).toBe(queue.name);

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTestStatic');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('instance-registered-wf-startWorkflow', async () => {
    const handle = await DBOS.startWorkflow(inst, { queueName: queue.name }).wfRegStep(10);
    await expect(handle.getResult()).resolves.toBe(1000);

    const wfid = handle.workflowID;
    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.prototype.wfRegStep');
    expect(status!.queueName).toBe(queue.name);

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('decorated-wf-startWorkflow', async () => {
    const handle = await DBOS.startWorkflow(TestClass, { queueName: queue.name }).decoratedWorkflow(10);
    await expect(handle.getResult()).resolves.toBe(1000);

    const wfid = handle.workflowID;
    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('decoratedWorkflow');
    expect(status!.queueName).toBe(queue.name);

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTestStatic');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
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

  test('wf-free-step-reg-startWorkflow', async () => {
    const handle = await DBOS.startWorkflow(regWFRegStep, { queueName: queue.name })(10);
    await expect(handle.getResult()).resolves.toBe(1000);

    const status = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('wfRegStep');
    expect(status!.queueName).toBe(queue.name);

    const steps = (await DBOS.listWorkflowSteps(handle.workflowID))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-free-step-reg-client', async () => {
    expect(config.systemDatabaseUrl).toBeDefined();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const handle = await client.enqueue<typeof regWFRegStep>(
        {
          queueName: queue.name,
          workflowName: 'wfRegStep',
        },
        10,
      );
      await expect(handle.getResult()).resolves.toBe(1000);

      const status = await DBOS.getWorkflowStatus(handle.workflowID);
      expect(status).not.toBeNull();
      expect(status!.workflowName).toBe('wfRegStep');
      expect(status!.queueName).toBe(queue.name);

      const steps = (await DBOS.listWorkflowSteps(handle.workflowID))!;
      expect(steps.length).toBe(1);
      expect(steps[0].functionID).toBe(0);
      expect(steps[0].name).toBe('stepTest');
      expect(steps[0].output).toEqual(1000);
      expect(steps[0].error).toBeNull();
      expect(steps[0].childWorkflowID).toBeNull();
    } finally {
      await client.destroy();
    }
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
      const res = await TestClass.wfRegStepStatic(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRegStepStatic');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTestStatic');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-static-step-reg-startWorkflow', async () => {
    const handle = await DBOS.startWorkflow(TestClass, { queueName: queue.name }).wfRegStepStatic(10);
    await expect(handle.getResult()).resolves.toBe(1000);

    const status = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRegStepStatic');
    expect(status!.queueName).toBe(queue.name);

    const steps = (await DBOS.listWorkflowSteps(handle.workflowID))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTestStatic');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-static-step-reg-client', async () => {
    expect(config.systemDatabaseUrl).toBeDefined();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const handle = await client.enqueue<typeof TestClass.wfRunStepStatic>(
        {
          queueName: queue.name,
          workflowName: 'TestClass.wfRegStepStatic',
        },
        10,
      );
      await expect(handle.getResult()).resolves.toBe(1000);

      const status = await DBOS.getWorkflowStatus(handle.workflowID);
      expect(status).not.toBeNull();
      expect(status!.workflowName).toBe('TestClass.wfRegStepStatic');
      expect(status!.queueName).toBe(queue.name);

      const steps = (await DBOS.listWorkflowSteps(handle.workflowID))!;
      expect(steps.length).toBe(1);
      expect(steps[0].functionID).toBe(0);
      expect(steps[0].name).toBe('stepTestStatic');
      expect(steps[0].output).toEqual(1000);
      expect(steps[0].error).toBeNull();
      expect(steps[0].childWorkflowID).toBeNull();
    } finally {
      await client.destroy();
    }
  });

  test('wf-static-step-run', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await TestClass.wfRunStepStatic(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRunStepStatic');

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
      const res = await TestClass.wfRegRetryStatic(10);
      expect(res).toBe(1000);
    });

    const status = await DBOS.getWorkflowStatus(wfid);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.wfRegRetryStatic');

    const steps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('retryTestStatic');
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

  test('wf-inst-step-reg-startWorkflow', async () => {
    const handle = await DBOS.startWorkflow(inst, { queueName: queue.name }).wfRegStep(10);
    await expect(handle.getResult()).resolves.toBe(1000);

    const status = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(status).not.toBeNull();
    expect(status!.workflowName).toBe('TestClass.prototype.wfRegStep');
    expect(status!.queueName).toBe(queue.name);

    const steps = (await DBOS.listWorkflowSteps(handle.workflowID))!;
    expect(steps.length).toBe(1);
    expect(steps[0].functionID).toBe(0);
    expect(steps[0].name).toBe('stepTest');
    expect(steps[0].output).toEqual(1000);
    expect(steps[0].error).toBeNull();
    expect(steps[0].childWorkflowID).toBeNull();
  });

  test('wf-inst-step-reg-client', async () => {
    expect(config.systemDatabaseUrl).toBeDefined();
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const handle = await client.enqueue<typeof TestClass.prototype.wfRegStep>(
        {
          queueName: queue.name,
          workflowClassName: 'TestClass',
          workflowConfigName: 'TestClassInstance',
          workflowName: 'TestClass.prototype.wfRegStep',
        },
        10,
      );
      await expect(handle.getResult()).resolves.toBe(1000);
      const status = await DBOS.getWorkflowStatus(handle.workflowID);
      expect(status).not.toBeNull();
      expect(status!.workflowName).toBe('TestClass.prototype.wfRegStep');
      expect(status!.queueName).toBe(queue.name);

      const steps = (await DBOS.listWorkflowSteps(handle.workflowID))!;
      expect(steps.length).toBe(1);
      expect(steps[0].functionID).toBe(0);
      expect(steps[0].name).toBe('stepTest');
      expect(steps[0].output).toEqual(1000);
      expect(steps[0].error).toBeNull();
      expect(steps[0].childWorkflowID).toBeNull();
    } finally {
      await client.destroy();
    }
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

    DBOS.registerWorkflow(workflow1);
    expect(() => DBOS.registerWorkflow(workflow2, { name: 'workflow1' })).toThrow(DBOSConflictingRegistrationError);
  });
});

const wfDoesSomethingNasty1 = DBOS.registerWorkflow(
  async () => {
    return (
      await DBOS.runStep(
        async () => {
          return Promise.resolve({
            getResult: () => 'Hello',
          });
        },
        { name: 'step1' },
      )
    ).getResult();
  },
  { name: 'wfDoesSomethingNasty1' },
);

const wfDoesSomethingNasty2 = DBOS.registerWorkflow(
  async () => {
    const sr = await DBOS.runStep(
      async () => {
        return Promise.resolve('Hello');
      },
      { name: 'step1' },
    );
    return {
      getResult: () => sr,
    };
  },
  { name: 'wfDoesSomethingNasty2' },
);

const wfDoesSomethingNasty3 = DBOS.registerWorkflow(
  async (input: { getResult: () => string }) => {
    const sr = await DBOS.runStep(
      async () => {
        return Promise.resolve(input.getResult());
      },
      { name: 'step1' },
    );
    return sr;
  },
  { name: 'wfDoesSomethingNasty3' },
);

const wfReturnsAPromise = DBOS.registerWorkflow(
  async (input: string) => {
    // eslint-disable-next-line @typescript-eslint/require-await
    const stepPromise = await DBOS.runStep(async () => {
      const p = Promise.resolve(input);
      return { p };
    });
    return await stepPromise.p;
  },
  { name: 'wfReturnsAPromise' },
);

const wfReturnsAFileHandle = DBOS.registerWorkflow(
  async () => {
    const path = __filename; // current file
    const flags = 'r';

    const fh = await fsp.open(path, flags);
    try {
      const retFH = await DBOS.runStep(async () => Promise.resolve(fh), { name: 'returnFH' });
      await retFH.readFile();
    } finally {
      await fh.close();
    }
  },
  { name: 'wfReturnsAFileHandle' },
);

const wfReturnsAPGClient = DBOS.registerWorkflow(
  async () => {
    const systemDBClient = new Client({
      connectionString: DBOSExecutor.globalInstance!.config.systemDatabaseUrl,
    });
    await systemDBClient.connect();
    try {
      const retConn = await DBOS.runStep(async () => Promise.resolve(systemDBClient), { name: 'returnClient' });
      await retConn.query('SELECT 1');
    } finally {
      await systemDBClient.end();
    }
  },
  { name: 'wfReturnsAPGClient' },
);

const returnsAFetchResponse = DBOS.registerWorkflow(
  async () => {
    const fetchRes = await DBOS.runStep(async () => await fetch('https://example.com'));
    return fetchRes.status;
  },
  { name: 'wfReturnsAFetchResponse' },
);

const returnsAnAxiosResponse = DBOS.registerWorkflow(
  async () => {
    const fetchRes = await DBOS.runStep(async () => await axios.get('https://example.com'));
    return fetchRes.status;
  },
  { name: 'wfReturnsAnAxiosResponse' },
);

class Frobnicator {
  constructor(
    readonly frobni: string,
    readonly cator: string,
  ) {}
  frobnicate() {
    return this.frobni + this.cator;
  }
}

const frobnicateWorkflow = DBOS.registerWorkflow(
  async (f: string, c: string) => Promise.resolve(new Frobnicator(f, c)),
  { name: 'frobnicate' },
);

describe('unserializable-negative-tests', () => {
  test('nonserializable-step-return', async () => {
    await DBOS.launch();
    try {
      // We want a clear error from this case.  We don't want it to only fail in recovery.
      const wfid = randomUUID();
      await expect(
        DBOS.withNextWorkflowID(wfid, async () => {
          return await wfDoesSomethingNasty1();
        }),
      ).rejects.toThrow(
        `Attempted to call 'getResult' at path step1.<result> on an object that is a serialized function input our output value. Functions are not preserved through serialization; see 'DBOS.registerSerialization'.`,
      );
    } finally {
      await DBOS.shutdown();
    }
  });

  test('nonserializable-wf-return', async () => {
    await DBOS.launch();
    try {
      // We want a clear error from this case, because the code will not work if
      //  queued or restarted in recovery
      const wfid = randomUUID();
      await expect(
        DBOS.withNextWorkflowID(wfid, async () => {
          (await wfDoesSomethingNasty2()).getResult();
        }),
      ).rejects.toThrow(
        `Attempted to call 'getResult' at path wfDoesSomethingNasty2.<result> on an object that is a serialized function input our output value. Functions are not preserved through serialization; see 'DBOS.registerSerialization'.`,
      );
    } finally {
      await DBOS.shutdown();
    }
  });

  test('nonserializable-wf-input', async () => {
    await DBOS.launch();
    try {
      // We want a clear error from this case, so that the code works in recovery
      const wfid = randomUUID();
      await expect(
        DBOS.withNextWorkflowID(wfid, async () => {
          await wfDoesSomethingNasty3({ getResult: () => 'TakeThis' });
        }),
      ).rejects.toThrow(
        `Attempted to call 'getResult' at path wfDoesSomethingNasty3.<arguments>["0"] on an object that is a serialized function input our output value. Functions are not preserved through serialization; see 'DBOS.registerSerialization'.`,
      );
    } finally {
      await DBOS.shutdown();
    }
  });

  test('nonserializable-randomstuff', async () => {
    await DBOS.launch();
    try {
      await expect(wfReturnsAPromise('hello')).rejects.toThrow(
        `Attempted to call 'then' at path [""].<result>.p on an object that is a serialized function input our output value. Functions are not preserved through serialization; see 'DBOS.registerSerialization'.`,
      );
      await expect(wfReturnsAFileHandle()).rejects.toThrow(
        `Attempted to call 'readFile' at path returnFH.<result> on an object that is a serialized function input our output value. Functions are not preserved through serialization; see 'DBOS.registerSerialization'.`,
      );
      await expect(wfReturnsAPGClient()).rejects.toThrow(
        `Attempted to call 'query' at path returnClient.<result> on an object that is a serialized function input our output value. Functions are not preserved through serialization; see 'DBOS.registerSerialization'.`,
      );
      await expect(returnsAFetchResponse()).resolves.toBe(undefined);
      await expect(returnsAnAxiosResponse()).rejects.toThrow(`Converting circular structure to JSON`);
    } finally {
      await DBOS.shutdown();
    }
  });

  test('wf-returns-wfh', async () => {
    await DBOS.launch();
    try {
      const wfh: WorkflowHandle<string> = await wfReturningHandle('hello');
      await expect(wfh.getResult()).resolves.toBe('hellohello');
    } finally {
      await DBOS.shutdown();
    }
  });

  test('custom-serialize', async () => {
    DBOS.registerSerialization<Frobnicator, { f: string; c: string }>({
      name: 'mycompany.Frobnicator',
      serialize: (f: Frobnicator) => {
        return { f: f.frobni, c: f.cator };
      },
      deserialize: (fc: { f: string; c: string }) => new Frobnicator(fc.f, fc.c),
      isApplicable: (v: unknown): v is Frobnicator => v instanceof Frobnicator,
    });
    await DBOS.launch();
    try {
      const f = await frobnicateWorkflow('a', 'b');
      expect(f.frobnicate()).toBe('ab');
    } finally {
      await DBOS.shutdown();
    }
  });
});
