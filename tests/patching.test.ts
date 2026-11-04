import { Client } from 'pg';
import { DBOS } from '../src';
import { Event, generateDBOSTestConfig, reexecuteWorkflowById, retryUntilSuccess, setUpDBOSTestSysDb } from './helpers';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { DBOSWorkflowCancelledError, getDBOSErrorCode, UnexpectedStep } from '../src/error';
import { GlobalLogger } from '../src/telemetry/logs';

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

// Workflow IDs that took a patched branch, so a test can tell which executions did.
const patchedRuns: string[] = [];

const waitStep = DBOS.registerStep(async () => Promise.resolve(1), { name: 'waitStep' });

// v1: waitStep, then a recv whose timeout sleep is recorded at the position after its own.
const recvWFv1 = DBOS.registerWorkflow(
  async () => {
    await waitStep();
    return String(await DBOS.recv<string>('approval', 60));
  },
  { name: 'recvWFv1' },
);

// v2 adds a patch between the step and the recv.
const recvWFv2 = DBOS.registerWorkflow(
  async () => {
    await waitStep();
    if (await DBOS.patch('before-wait')) {
      patchedRuns.push(DBOS.workflowID!);
    }
    return String(await DBOS.recv<string>('approval', 60));
  },
  { name: 'recvWFv2' },
);

const concStepOneStarted = new Event();
const concStepOneRelease = new Event();
const concStepOneAborted = new Event();

// Blocks until released, or fails when its workflow is cancelled, which leaves it unrecorded.
const concStepOne = DBOS.registerStep(
  async () => {
    concStepOneStarted.set();
    const signal = DBOS.stepStatus!.cancelSignal;
    await new Promise<void>((resolve, reject) => {
      const abort = () => {
        concStepOneAborted.set();
        reject(signal.reason as Error);
      };
      if (signal.aborted) return abort();
      signal.addEventListener('abort', abort, { once: true });
      void concStepOneRelease.wait().then(resolve);
    });
    return 1;
  },
  { name: 'concStepOne' },
);

const concStepTwo = DBOS.registerStep(async () => Promise.resolve(2), { name: 'concStepTwo' });

// v1: the steps take their positions up front and checkpoint in any order.
const concWFv1 = DBOS.registerWorkflow(
  async () => {
    const [a, b] = await Promise.all([concStepOne(), concStepTwo()]);
    return a + b;
  },
  { name: 'concWFv1' },
);

// v2 adds a patch before the concurrent steps.
const concWFv2 = DBOS.registerWorkflow(
  async () => {
    if (await DBOS.patch('before-steps')) {
      patchedRuns.push(DBOS.workflowID!);
    }
    const [a, b] = await Promise.all([concStepOne(), concStepTwo()]);
    return a + b;
  },
  { name: 'concWFv2' },
);

describe('patching-tests', () => {
  let config: DBOSConfig;
  let warnSpy: jest.SpyInstance;
  let systemDBClient: Client;
  let schema: string;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    // The deprecated flag still pins the application version, and says it is deprecated.
    config.enablePatching = true;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    schema = config.systemDatabaseSchemaName ?? 'dbos';
    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();
  });

  afterAll(async () => {
    await systemDBClient.end();
  });

  beforeEach(async () => {
    warnSpy = jest.spyOn(GlobalLogger.prototype, 'warn');
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
    warnSpy.mockRestore();
  });

  test('patch-last-step', async () => {
    expect(DBOS.applicationVersion).toBe('PATCHING_ENABLED');
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('enablePatching is deprecated'));
    const origWfh = await DBOS.startWorkflow(origWF)();
    await expect(origWfh.getResult()).resolves.toBe(3);
    await expect((await reexecuteWorkflowById(origWfh.workflowID))?.getResult()).resolves.toBe(3);
    const ls = await DBOS.listWorkflowSteps(origWfh.workflowID);
    expect(ls?.length).toBe(2);
    expect(ls?.filter((s) => s.name.startsWith('DBOS.patch')).length).toBe(0);
    // Patch does not affect reexecution
    await expect((await reexecuteWorkflowById(origWfh.workflowID, true, 'patchedWF1')).getResult()).resolves.toBe(3);

    // Test fork before and after patch (will affect reexecution)
    const fwfhpost = await DBOS.forkWorkflow(origWfh.workflowID, 2);
    await expect(fwfhpost.getResult()).resolves.toBe(3);

    const fwfhpre = await DBOS.forkWorkflow(origWfh.workflowID, 1);
    await expect(fwfhpre.getResult()).resolves.toBe(4);

    const patchedWfh = await DBOS.startWorkflow(patchedWF1)();
    await expect(patchedWfh.getResult()).resolves.toBe(4);
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF1')).getResult()).resolves.toBe(4);
    const lsp = await DBOS.listWorkflowSteps(patchedWfh.workflowID);
    expect(lsp?.length).toBe(3);
    expect(lsp?.filter((s) => s.name.startsWith('DBOS.patch')).length).toBe(1);
    expect(lsp?.[1].name).toBe('DBOS.patch-patch1');

    // Deprecation does not affect reexecution
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'depatchedWF1')).getResult()).resolves.toBe(
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
    await expect((await reexecuteWorkflowById(origWfh.workflowID, true, 'patchedWF2')).getResult()).resolves.toBe(3);

    const patchedWfh = await DBOS.startWorkflow(patchedWF2)();
    await expect(patchedWfh.getResult()).resolves.toBe(5);
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF2')).getResult()).resolves.toBe(5);

    // Deprecation does not affect reexecution
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'depatchedWF2')).getResult()).resolves.toBe(
      5,
    );

    // Reexecution with the old code will fail, because it does not expect the patch marker.
    // Instanceof is not preserved by serialization, so we must assert the error code.
    const staleRerun = await (await reexecuteWorkflowById(patchedWfh.workflowID, true, 'origWF')).getResult().then(
      () => undefined,
      (e: Error) => e,
    );
    expect(getDBOSErrorCode(staleRerun!)).toBe(UnexpectedStep);

    const depatchedWfh = await DBOS.startWorkflow(depatchedWF2)();
    await expect(depatchedWfh.getResult()).resolves.toBe(5);

    // Check 2 patches same place
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF3a')).getResult()).resolves.toBe(
      5,
    );
    await expect((await reexecuteWorkflowById(patchedWfh.workflowID, true, 'patchedWF3b')).getResult()).resolves.toBe(
      5,
    );
    await expect(patchedWF3a()).resolves.toBe(4);
    await expect(patchedWF3b()).resolves.toBe(4);
  });

  test('patch-before-waiting-recv', async () => {
    // A recv checkpoints its timeout sleep, at the position after its own, while it waits.
    // A patch added before that recv finds the recv's position empty on recovery; it must read the sleep past it
    // as "already passed", not insert a marker that shifts the recv onto the sleep's position.
    const v1 = await DBOS.startWorkflow(recvWFv1)();
    await DBOS.send(v1.workflowID, 'first', 'approval');
    await expect(v1.getResult()).resolves.toBe('first');
    const v1Steps = (await DBOS.listWorkflowSteps(v1.workflowID))!;
    expect(v1Steps.map((s) => s.name)).toEqual(['waitStep', 'DBOS.recv', 'DBOS.sleep']);

    // The process died while the recv waited: the step and the sleep are recorded, the recv is not.
    await systemDBClient.query(
      `DELETE FROM "${schema}".operation_outputs WHERE workflow_uuid = $1 AND function_id = $2`,
      [v1.workflowID, v1Steps[1].functionID],
    );

    // Recovered on v2, the v1 execution resumes on the old path.
    const recovered = await reexecuteWorkflowById(v1.workflowID, true, 'recvWFv2');
    await DBOS.send(v1.workflowID, 'second', 'approval');
    await expect(recovered.getResult()).resolves.toBe('second');
    expect(patchedRuns).not.toContain(v1.workflowID);
    const recoveredSteps = (await DBOS.listWorkflowSteps(v1.workflowID))!;
    expect(recoveredSteps.map((s) => [s.functionID, s.name])).toEqual(v1Steps.map((s) => [s.functionID, s.name]));

    // A new execution takes the patch and records its marker before the recv.
    const v2 = await DBOS.startWorkflow(recvWFv2)();
    await DBOS.send(v2.workflowID, 'third', 'approval');
    await expect(v2.getResult()).resolves.toBe('third');
    expect(patchedRuns).toContain(v2.workflowID);
    const v2Steps = (await DBOS.listWorkflowSteps(v2.workflowID))!;
    expect(v2Steps.map((s) => s.name)).toEqual(['waitStep', 'DBOS.patch-before-wait', 'DBOS.recv', 'DBOS.sleep']);
  });

  test('patch-before-concurrent-steps', async () => {
    // Concurrent steps take their positions up front and checkpoint in any order, so an execution can stop with a
    // later position recorded and an earlier one empty. A patch added before them must treat the later checkpoint
    // as proof the execution passed that point on the old code.
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const v1 = await DBOS.startWorkflow(concWFv1)();
    await concStepOneStarted.wait();
    await retryUntilSuccess(async () => {
      const steps = await DBOS.listWorkflowSteps(v1.workflowID);
      expect(steps!.map((s) => s.name)).toEqual(['concStepTwo']);
    });

    // Cancelling the workflow aborts concStepOne, so its earlier position stays empty.
    await DBOS.cancelWorkflow(v1.workflowID);
    await concStepOneAborted.wait();
    await expect(v1.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    while (sysdb.checkForRunningWorkflow(v1.workflowID)) {
      await new Promise((r) => setTimeout(r, 20));
    }
    const v1Steps = (await DBOS.listWorkflowSteps(v1.workflowID))!;
    expect(v1Steps.map((s) => s.name)).toEqual(['concStepTwo']);
    const stepTwoID = v1Steps[0].functionID;

    // Resumed on v2, the v1 execution stays on the old path: concStepOne takes the position before concStepTwo,
    // and concStepTwo replays at its own.
    concStepOneRelease.set();
    const resumed = await reexecuteWorkflowById(v1.workflowID, true, 'concWFv2');
    await expect(resumed.getResult()).resolves.toBe(3);
    expect(patchedRuns).not.toContain(v1.workflowID);
    const resumedSteps = (await DBOS.listWorkflowSteps(v1.workflowID))!;
    expect(resumedSteps.map((s) => [s.functionID, s.name])).toEqual([
      [stepTwoID - 1, 'concStepOne'],
      [stepTwoID, 'concStepTwo'],
    ]);

    // A new execution takes the patch and records its marker first.
    const v2 = await DBOS.startWorkflow(concWFv2)();
    await expect(v2.getResult()).resolves.toBe(3);
    expect(patchedRuns).toContain(v2.workflowID);
    const v2Steps = (await DBOS.listWorkflowSteps(v2.workflowID))!;
    expect(v2Steps.map((s) => s.name)).toEqual(['DBOS.patch-before-steps', 'concStepOne', 'concStepTwo']);
  });
});
