import { DBOS } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { randomUUID } from 'node:crypto';

class DecoratorFreeTest {
  @DBOS.workflow()
  static async testFreeStep(value: number) {
    return await regFreeStep(value);
  }

  @DBOS.workflow()
  static async testStaticStep(value: number) {
    return await DecoratorFreeTest.staticStep(value);
  }

  @DBOS.workflow()
  static async testFreeStepRetries(value: number) {
    return await regFreeStepRetries(value);
  }

  @DBOS.workflow()
  static async testRunAsStep(value: number) {
    return DBOS.runAsStep(
      async () => {
        return value * 10;
      },
      { name: 'testRunStep' },
    );
  }

  static runAsStepRetriesAttempts = Array<boolean>(5).fill(false);

  @DBOS.workflow()
  static async testRunAsStepRetries(value: number) {
    return DBOS.runAsStep(
      async () => {
        expect(DBOS.stepStatus).toBeDefined();
        expect(DBOS.stepStatus!.currentAttempt).toBeDefined();
        const currentAttempt = DBOS.stepStatus!.currentAttempt!;
        DecoratorFreeTest.runAsStepRetriesAttempts[currentAttempt - 1] = true;
        if (currentAttempt < 3) {
          throw new Error('Test error');
        }

        return value * 10;
      },
      { name: 'testRunStep', retriesAllowed: true },
    );
  }

  static async staticStep(value: number): Promise<number> {
    return value * 1000;
  }
}

DecoratorFreeTest.staticStep = DBOS.registerStep(DecoratorFreeTest.staticStep);

async function freeStep(value: number): Promise<number> {
  return value * 100;
}

const regFreeStep = DBOS.registerStep(freeStep);

const freeStepRetriesAttempts = Array<boolean>(5).fill(false);
async function freeStepRetries(value: number): Promise<number> {
  expect(DBOS.stepStatus).toBeDefined();
  expect(DBOS.stepStatus!.currentAttempt).toBeDefined();
  const currentAttempt = DBOS.stepStatus!.currentAttempt!;
  freeStepRetriesAttempts[currentAttempt - 1] = true;
  if (currentAttempt < 3) {
    throw new Error('Test error');
  }
  return value * 100;
}
const regFreeStepRetries = DBOS.registerStep(freeStepRetries, { retriesAllowed: true });

describe('decorator-free-tests', () => {
  const config = generateDBOSTestConfig();

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

  test('wf-free-step', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DecoratorFreeTest.testFreeStep(10);
      expect(res).toBe(1000);
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('freeStep');
    expect(wfsteps[0].output).toEqual(1000);
    expect(wfsteps[0].error).toBeNull();
    expect(wfsteps[0].childWorkflowID).toBeNull();
  });

  test('wf-static-step', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DecoratorFreeTest.testStaticStep(10);
      expect(res).toBe(10000);
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('staticStep');
    expect(wfsteps[0].output).toEqual(10000);
    expect(wfsteps[0].error).toBeNull();
    expect(wfsteps[0].childWorkflowID).toBeNull();
  });

  test('wf-free-step-retries', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DecoratorFreeTest.testFreeStepRetries(10);
      expect(res).toBe(1000);
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('freeStepRetries');
    expect(wfsteps[0].output).toEqual(1000);
    expect(wfsteps[0].error).toBeNull();
    expect(wfsteps[0].childWorkflowID).toBeNull();

    expect(freeStepRetriesAttempts).toEqual([true, true, true, false, false]);
  });

  test('wf-run-as-step', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DecoratorFreeTest.testRunAsStep(10);
      expect(res).toBe(100);
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('testRunStep');
    expect(wfsteps[0].output).toEqual(100);
    expect(wfsteps[0].error).toBeNull();
    expect(wfsteps[0].childWorkflowID).toBeNull();
  });

  test('wf-run-as-step-retries', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DecoratorFreeTest.testRunAsStepRetries(10);
      expect(res).toBe(100);
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('testRunStep');
    expect(wfsteps[0].output).toEqual(100);
    expect(wfsteps[0].error).toBeNull();
    expect(wfsteps[0].childWorkflowID).toBeNull();

    expect(DecoratorFreeTest.runAsStepRetriesAttempts).toEqual([true, true, true, false, false]);
  });
});
