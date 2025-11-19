/* eslint-disable @typescript-eslint/require-await */
import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { randomUUID } from 'node:crypto';

describe('oaoo-tests', () => {
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
  /// That the behavior is not transparent, that's the first red flag.
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

    // I am a user who expects the workflow deduplication by ID to protect me!
    //  You said OAOO was a feature.  Also queue deduplication possibly has the same issue?
    // And these sleep calls are expensive!
    const wfh1 = await DBOS.startWorkflow(TryConcExec, { workflowID: workflowUUID }).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(TryConcExec, { workflowID: workflowUUID }).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();
    expect(TryConcExec.maxConc).toBe(1);
  });

  /*
  //
  // That you have to be so careful with exception handling, that's the second red flag.
  //   (But will anyone actually see it?)
  //
  class BadErrorHandling1 {
    static execNum  = 0;
    static started = false;
    static completed = false;
    static aborted = false;
    static trouble = false;

    @DBOS.step()
    static async testStartAction() {
      await sleepms(1000);
      BadErrorHandling1.started = true;
    }

    @DBOS.step()
    static async testCompleteAction() {
      expect(BadErrorHandling1.started).toBeTruthy();
      await sleepms(1000);
      BadErrorHandling1.completed = true;
    }

    @DBOS.step()
    static async testCancelAction() {
      BadErrorHandling1.aborted = true;
      BadErrorHandling1.started = false;
    }

    static async reportTrouble() {
      BadErrorHandling1.trouble = true;
      expect('Trouble?').toBe('None!');
    }

    @DBOS.workflow()
    static async testConcWorkflow() {
      try {
        // Step 1, tell external system to start processing
        await BadErrorHandling1.testStartAction();
      }
      catch (e) {
        // If we fail for any reason, try to abort
        // (We don't know if the external system even heard us)
        // I have been careful, my undo action in the other system
        //  is idempotent, and will be fine if it never heard the start
        try {
          await BadErrorHandling1.testCancelAction();
        }
        catch (e2) {
          // We have no idea if we managed to get to the external system at any point above
          // We may be leaving system in inconsistent state
          // Take some other notification action (sysadmin!)
          await BadErrorHandling1.reportTrouble();
        }
      }
      // Step 2, finish the process
      await BadErrorHandling1.testCompleteAction();
    }
  }

  test('step-undoredo', async () => {
    const workflowUUID: string = randomUUID();

    const wfh1 = await DBOS.startWorkflow(BadErrorHandling1, {workflowID: workflowUUID}).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(BadErrorHandling1, {workflowID: workflowUUID}).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();

    // In our invocations above, there are no errors
    console.log(`Started: ${BadErrorHandling1.started}; Completed: ${BadErrorHandling1.completed}; Aborted: ${BadErrorHandling1.aborted}; Trouble: ${BadErrorHandling1.trouble}`);
    expect(BadErrorHandling1.started).toBeTruthy();
    expect(BadErrorHandling1.completed).toBeTruthy();
    expect(BadErrorHandling1.trouble).toBeFalsy();
  });

  //
  // That you have to be so careful with exception handling, that's the second red flag.
  //   (But will anyone actually see it?)
  //
  class BadErrorHandling2 {
    static execNum  = 0;
    static started = false;
    static completed = false;
    static aborted = false;
    static trouble = false;

    @DBOS.step()
    static async testStartAction() {
      await sleepms(1000);
      BadErrorHandling2.started = true;
    }

    @DBOS.step()
    static async testCompleteAction() {
      expect(BadErrorHandling2.started).toBeTruthy();
      await sleepms(1000);
      BadErrorHandling2.completed = true;
    }

    @DBOS.step()
    static async testCancelAction() {
      BadErrorHandling2.aborted = true;
      BadErrorHandling2.started = false;
    }

    static async reportTrouble() {
      BadErrorHandling2.trouble = true;
      expect('Trouble?').toBe('None!');
    }

    @DBOS.workflow()
    static async testConcWorkflow() {
      let finished = false;
      try {
        // Step 1, tell external system to start processing
        await BadErrorHandling2.testStartAction();

        // Step 2, finish the process
        await BadErrorHandling2.testCompleteAction();

        finished = true;
      }
      finally {
        if (!finished) {
          // If we fail for any reason, try to abort
          // (We don't know if the external system even heard us)
          // I have been careful, my undo action in the other system
          try {
            await BadErrorHandling2.testCancelAction();
          }
          catch (e2) {
            // We have no idea if we managed to get to the external system at any point above
            // We may be leaving system in inconsistent state
            // Take some other notification action (sysadmin!)
            await BadErrorHandling2.reportTrouble();
          }
        }
      }
    }
  }

  test('step-undoredo2', async () => {
    const workflowUUID: string = randomUUID();

    const wfh1 = await DBOS.startWorkflow(BadErrorHandling2, {workflowID: workflowUUID}).testConcWorkflow();
    //const wfh2 = await DBOS.startWorkflow(BadErrorHandling2, {workflowID: workflowUUID}).testConcWorkflow();

    await wfh1.getResult();
    //await wfh2.getResult();

    // In our invocations above, there are no errors
    console.log(`Started: ${BadErrorHandling2.started}; Completed: ${BadErrorHandling2.completed}; Aborted: ${BadErrorHandling2.aborted}; Trouble: ${BadErrorHandling2.trouble}`);
    expect(BadErrorHandling2.started).toBeTruthy();
    expect(BadErrorHandling2.completed).toBeTruthy();
    expect(BadErrorHandling2.trouble).toBeFalsy();
  });

  ///
  /// It is worse than just exception handling model
  ///
  class TryConcExec2 {
    static curExec = 0;
    static curStep = 0;

    @DBOS.step()
    static async step1() {
      // Ignore the foreshadowing!
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

    const wfh1 = await DBOS.startWorkflow(TryConcExec2, {workflowID: workflowUUID}).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(TryConcExec2, {workflowID: workflowUUID}).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();
    expect(TryConcExec2.curStep).toBe(2);
  });

  ///
  /// Self-abort
  /// This is a bit tricky... but will be simulated with a simulated database glitch
  ///
  class TryDbGlitch {
    @DBOS.step()
    static async step1() {
      await sleepms(1000);
      return "Yay!";
    }

    @DBOS.workflow()
    static async testWorkflow() {
      return await TryDbGlitch.step1();
    }
  }

  test('step-sequence', async () => {
    expect(await TryDbGlitch.testWorkflow()).toBe('Yay!');
    process.env.COMMIT_GLITCH='Hahaha'; // Targeted...
    expect(await TryDbGlitch.testWorkflow()).toBe('Yay!');
  });
  */
});
