/* eslint-disable @typescript-eslint/require-await */
import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { DEBUG_TRIGGER_STEP_COMMIT, setDebugTrigger } from '../src/debugpoint';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { randomUUID } from 'node:crypto';

describe('run-workflow-once-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  ///
  /// Check against concurrent execution
  ///
  class TryConcExec {
    static concExec = 0;
    static maxConc = 0;

    @DBOS.step()
    static async testConcStep() {
      ++TryConcExec.concExec;
      TryConcExec.maxConc = Math.max(TryConcExec.concExec, TryConcExec.maxConc);
      await sleepms(1000);
      --TryConcExec.concExec;
    }

    @DBOS.workflow()
    static async testConcWorkflow() {
      await TryConcExec.testConcStep();
    }
  }

  test('step-conc', async () => {
    const workflowUUID: string = randomUUID();

    const wfh1 = await DBOS.startWorkflow(TryConcExec, { workflowID: workflowUUID }).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(TryConcExec, { workflowID: workflowUUID }).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();
    expect(TryConcExec.maxConc).toBe(1);
  });

  class CatchPlainException1 {
    static execNum = 0;
    static started = false;
    static completed = false;
    static aborted = false;
    static trouble = false;

    @DBOS.step()
    static async testStartAction() {
      await sleepms(1000);
      CatchPlainException1.started = true;
    }

    @DBOS.step()
    static async testCompleteAction() {
      expect(CatchPlainException1.started).toBeTruthy();
      await sleepms(1000);
      CatchPlainException1.completed = true;
    }

    @DBOS.step()
    static async testCancelAction() {
      CatchPlainException1.aborted = true;
      CatchPlainException1.started = false;
    }

    static async reportTrouble() {
      CatchPlainException1.trouble = true;
      expect('Trouble?').toBe('None!');
    }

    @DBOS.workflow()
    static async testConcWorkflow() {
      try {
        // Step 1, tell external system to start processing
        await CatchPlainException1.testStartAction();
      } catch (e) {
        // If we fail for any reason, try to abort
        // (We don't know if the external system even heard us)
        // I have been careful, my undo action in the other system
        //  is idempotent, and will be fine if it never heard the start
        try {
          await CatchPlainException1.testCancelAction();
        } catch (e2) {
          // We have no idea if we managed to get to the external system at any point above
          // We may be leaving system in inconsistent state
          // Take some other notification action (sysadmin!)
          await CatchPlainException1.reportTrouble();
        }
      }
      // Step 2, finish the process
      await CatchPlainException1.testCompleteAction();
    }
  }

  test('step-undoredo', async () => {
    const workflowUUID: string = randomUUID();

    const wfh1 = await DBOS.startWorkflow(CatchPlainException1, { workflowID: workflowUUID }).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(CatchPlainException1, { workflowID: workflowUUID }).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();

    // In our invocations above, there are no errors
    console.log(
      `Started: ${CatchPlainException1.started}; Completed: ${CatchPlainException1.completed}; Aborted: ${CatchPlainException1.aborted}; Trouble: ${CatchPlainException1.trouble}`,
    );
    expect(CatchPlainException1.started).toBeTruthy();
    expect(CatchPlainException1.completed).toBeTruthy();
    expect(CatchPlainException1.trouble).toBeFalsy();
  });

  class UsingFinallyClause {
    static execNum = 0;
    static started = false;
    static completed = false;
    static aborted = false;
    static trouble = false;

    @DBOS.step()
    static async testStartAction() {
      await sleepms(1000);
      UsingFinallyClause.started = true;
    }

    @DBOS.step()
    static async testCompleteAction() {
      expect(UsingFinallyClause.started).toBeTruthy();
      await sleepms(1000);
      UsingFinallyClause.completed = true;
    }

    @DBOS.step()
    static async testCancelAction() {
      UsingFinallyClause.aborted = true;
      UsingFinallyClause.started = false;
    }

    static async reportTrouble() {
      UsingFinallyClause.trouble = true;
      expect('Trouble?').toBe('None!');
    }

    @DBOS.workflow()
    static async testConcWorkflow() {
      let finished = false;
      try {
        // Step 1, tell external system to start processing
        await UsingFinallyClause.testStartAction();

        // Step 2, finish the process
        await UsingFinallyClause.testCompleteAction();

        finished = true;
      } finally {
        if (!finished) {
          // If we fail for any reason, try to abort
          // (We don't know if the external system even heard us)
          // I have been careful, my undo action in the other system
          try {
            await UsingFinallyClause.testCancelAction();
          } catch (e2) {
            // We have no idea if we managed to get to the external system at any point above
            // We may be leaving system in inconsistent state
            // Take some other notification action (sysadmin!)
            await UsingFinallyClause.reportTrouble();
          }
        }
      }
    }
  }

  test('step-undoredo2', async () => {
    const workflowUUID: string = randomUUID();

    const wfh1 = await DBOS.startWorkflow(UsingFinallyClause, { workflowID: workflowUUID }).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(UsingFinallyClause, { workflowID: workflowUUID }).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();

    // In our invocations above, there are no errors
    console.log(
      `Started: ${UsingFinallyClause.started}; Completed: ${UsingFinallyClause.completed}; Aborted: ${UsingFinallyClause.aborted}; Trouble: ${UsingFinallyClause.trouble}`,
    );
    expect(UsingFinallyClause.started).toBeTruthy();
    expect(UsingFinallyClause.completed).toBeTruthy();
    expect(UsingFinallyClause.trouble).toBeFalsy();
  });

  class TryConcExec2 {
    static curExec = 0;
    static curStep = 0;

    @DBOS.step()
    static async step1() {
      // This makes the step take a while ... sometimes.
      if (TryConcExec2.curExec++ % 2 === 0) {
        await sleepms(1000);
      }
      TryConcExec2.curStep = 1;
    }

    @DBOS.step()
    static async step2() {
      TryConcExec2.curStep = 2;
    }

    @DBOS.workflow()
    static async testConcWorkflow() {
      await TryConcExec2.step1();
      await TryConcExec2.step2();
    }
  }

  test('step-sequence', async () => {
    const workflowUUID: string = randomUUID();

    const wfh1 = await DBOS.startWorkflow(TryConcExec2, { workflowID: workflowUUID }).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(TryConcExec2, { workflowID: workflowUUID }).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();
    expect(TryConcExec2.curStep).toBe(2);
  });

  // Self-abort test, for another round of testing...
  class TryDbGlitch {
    @DBOS.step()
    static async step1() {
      await sleepms(1000);
      return 'Yay!';
    }

    @DBOS.workflow()
    static async testWorkflow() {
      return await TryDbGlitch.step1();
    }
  }

  test('step-commit-hiccup', async () => {
    expect(await TryDbGlitch.testWorkflow()).toBe('Yay!');
    let forceRetries = 1;
    setDebugTrigger(DEBUG_TRIGGER_STEP_COMMIT, {
      asyncCallback: async () => {
        if (forceRetries-- > 0) throw new Error('ECONNRESET');
      },
    });
    expect(await TryDbGlitch.testWorkflow()).toBe('Yay!');
  });
});
