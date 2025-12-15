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

const depatchedWF1 = DBOS.registerWorkflow(
  async () => {
    const a = await step1();
    let b = 0;
    if (await DBOS.deprecatePatch('patch1')) {
      b = await step3();
    } else {
      expect(true).toBeFalsy();
    }
    return a + b;
  },
  { name: 'depatchedWF1' },
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

const patchedWF3a = DBOS.registerWorkflow(
  async () => {
    let a = 0;
    if (await DBOS.patch('patch2')) {
      if (await DBOS.patch('patch3')) {
        a = await step2();
      } else {
        a = await step3();
      }
    } else {
      a = await step1();
    }
    const b = await step2();
    return a + b;
  },
  { name: 'patchedWF3a' },
);

const patchedWF3b = DBOS.registerWorkflow(
  async () => {
    let a = 0;
    if (await DBOS.patch('patch3')) {
      a = await step2();
    } else {
      if (await DBOS.patch('patch2')) {
        a = await step3();
      } else {
        a = await step1();
      }
    }
    const b = await step2();
    return a + b;
  },
  { name: 'patchedWF3b' },
);

const depatchedWF2 = DBOS.registerWorkflow(
  async () => {
    let a = 0;
    if (await DBOS.deprecatePatch('patch2')) {
      a = await step3();
    } else {
      expect(true).toBeFalsy();
    }
    const b = await step2();
    return a + b;
  },
  { name: 'depatchedWF2' },
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
    expect(DBOS.applicationVersion).toBe('PATCHING_ENABLED');
    const origWfh = await DBOS.startWorkflow(origWF)();
    await expect(origWfh.getResult()).resolves.toBe(3);
    await expect((await reexecuteWorkflowById(origWfh.workflowID))?.getResult()).resolves.toBe(3);
    const ls = await DBOS.listWorkflowSteps(origWfh.workflowID);
    expect(ls?.length).toBe(2);
    expect(ls?.filter((s) => s.name.startsWith('DBOS.patch')).length).toBe(0);
    // Patch does not affect reexecution
    await expect((await reexecuteWorkflowById(origWfh.workflowID, true, 'patchedWF1'))!.getResult()).resolves.toBe(3);

    const patchedWfh = await DBOS.startWorkflow(patchedWF1)();
    await expect(patchedWfh.getResult()).resolves.toBe(4);
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF1'))!.getResult()).resolves.toBe(
      4,
    );
    const lsp = await DBOS.listWorkflowSteps(patchedWfh.workflowID);
    expect(lsp?.length).toBe(3);
    expect(lsp?.filter((s) => s.name.startsWith('DBOS.patch')).length).toBe(1);
    expect(lsp?.[1].name).toBe('DBOS.patch-patch1');

    // Deprecation does not affect reexecution
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'depatchedWF1'))!.getResult()).resolves.toBe(
      4,
    );

    const depatchedWfh = await DBOS.startWorkflow(depatchedWF1)();
    await expect(depatchedWfh.getResult()).resolves.toBe(4);
    const lsdep = await DBOS.listWorkflowSteps(depatchedWfh.workflowID);
    expect(lsdep?.length).toBe(2);
    expect(lsdep?.filter((s) => s.name.startsWith('DBOS.patch')).length).toBe(0);
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
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'depatchedWF2'))!.getResult()).resolves.toBe(
      5,
    );

    const depatchedWfh = await DBOS.startWorkflow(depatchedWF2)();
    await expect(depatchedWfh.getResult()).resolves.toBe(5);

    // Check 2 patches same place
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF3a'))!.getResult()).resolves.toBe(
      5,
    );
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF3b'))!.getResult()).resolves.toBe(
      5,
    );
    await expect(patchedWF3a()).resolves.toBe(4);
    await expect(patchedWF3b()).resolves.toBe(4);
  });

  // TODO Check fork
  // TODO Negative testing / mismanaged patches
});
