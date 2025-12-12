import { DBOS } from '../src';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';

const step1 = DBOS.registerStep(async () => Promise.resolve(1), { name: 'step1' });
const step2 = DBOS.registerStep(async () => Promise.resolve(2), { name: 'step2' });
const step3 = DBOS.registerStep(async () => Promise.resolve(3), { name: 'step3' });

const origWF = DBOS.registerWorkflow(
  async () => {
    const a = await step1();
    const b = await step2();
    return a + b;
  },
  { name: 'origWF' },
);

const patchedWF1 = DBOS.registerWorkflow(
  async () => {
    const a = await step1();
    let b = 0;
    if (await DBOS.patch('patch1')) {
      b = await step3();
    } else {
      b = await step2();
    }
    return a + b;
  },
  { name: 'patchedWF1' },
);

const patchedWF2 = DBOS.registerWorkflow(
  async () => {
    let a = 0;
    if (await DBOS.patch('patch2')) {
      a = await step3();
    } else {
      a = await step1();
    }
    const b = await step2();
    return a + b;
  },
  { name: 'patchedWF2' },
);

describe('patching-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    config.enablePatching = true;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('patch-last-step', async () => {
    const origWfh = await DBOS.startWorkflow(origWF)();
    await expect(origWfh.getResult()).resolves.toBe(3);
    await expect((await reexecuteWorkflowById(origWfh.workflowID))?.getResult()).resolves.toBe(3);
    // Patch does not affect reexecution
    await expect((await reexecuteWorkflowById(origWfh.workflowID, true, 'patchedWF1'))!.getResult()).resolves.toBe(3);

    const patchedWfh = await DBOS.startWorkflow(patchedWF1)();
    await expect(patchedWfh.getResult()).resolves.toBe(4);
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF1'))!.getResult()).resolves.toBe(
      4,
    );

    // Deprecation does not affect reexecution
    //
  });

  test('patch-first-step', async () => {
    const origWfh = await DBOS.startWorkflow(origWF)();
    await expect(origWfh.getResult()).resolves.toBe(3);
    await expect((await reexecuteWorkflowById(origWfh.workflowID))?.getResult()).resolves.toBe(3);
    // Patch does not affect reexecution
    await expect((await reexecuteWorkflowById(origWfh.workflowID, true, 'patchedWF2'))!.getResult()).resolves.toBe(3);

    const patchedWfh = await DBOS.startWorkflow(patchedWF2)();
    await expect(patchedWfh.getResult()).resolves.toBe(5);
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF2'))!.getResult()).resolves.toBe(
      5,
    );

    // Deprecation does not affect reexecution
    //
  });
});
