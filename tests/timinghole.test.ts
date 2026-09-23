/* eslint-disable @typescript-eslint/require-await */
import { DBOS, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { DEBUG_TRIGGER_STEP_COMMIT, DEBUG_TRIGGER_INITWF_COMMIT, setDebugTrigger } from '../src/debugpoint';
import { globalParams, INTERNAL_QUEUE_NAME, sleepms } from '../src/utils';
import {
  Event,
  generateDBOSTestConfig,
  redispatchWorkflowById,
  reexecuteWorkflowById,
  retryUntilSuccess,
  setUpDBOSTestSysDb,
  setWfAndChildrenToPending,
} from './helpers';
import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { DBOSStepNondeterminismError, DBOSWorkflowCancelledError } from '../src/error';

class Handoff {
  static release = new Event();
  static started = new Event();
  static blockedCalls = 0;
  static afterCalls = 0;
  static recvCalls = 0;
  static childCalls = 0;
  static blockedStepID: number | undefined;

  static reset() {
    Handoff.release = new Event();
    Handoff.started = new Event();
    Handoff.blockedCalls = 0;
    Handoff.afterCalls = 0;
    Handoff.recvCalls = 0;
    Handoff.childCalls = 0;
    Handoff.blockedStepID = undefined;
  }

  @DBOS.step()
  static async blockedStep(): Promise<string> {
    Handoff.blockedCalls++;
    Handoff.blockedStepID = DBOS.stepID;
    await Handoff.release.wait();
    return 'blocked';
  }

  @DBOS.step()
  static async afterStep(): Promise<string> {
    Handoff.afterCalls++;
    return 'after';
  }

  @DBOS.workflow()
  static async handedOffWorkflow(): Promise<string> {
    return (await Handoff.blockedStep()) + (await Handoff.afterStep());
  }

  @DBOS.workflow()
  static async cancelledWorkflow(): Promise<string> {
    return await Handoff.blockedStep();
  }

  @DBOS.workflow()
  static async recvWorkflow(): Promise<string> {
    Handoff.recvCalls++;
    return String(await DBOS.recv<string>('topic', 30));
  }

  @DBOS.workflow()
  static async outcomeWorkflow(): Promise<string> {
    Handoff.started.set();
    await Handoff.release.wait();
    return 'done';
  }

  @DBOS.workflow()
  static async sleepingWorkflow(): Promise<string> {
    Handoff.started.set();
    await Handoff.release.wait();
    await DBOS.sleep(20_000);
    return 'slept';
  }

  @DBOS.workflow()
  static async childWorkflow(): Promise<string> {
    Handoff.childCalls++;
    return 'child';
  }

  @DBOS.workflow()
  static async parentWorkflow(childID: string): Promise<string> {
    Handoff.started.set();
    await Handoff.release.wait();
    return await DBOS.withNextWorkflowID(childID, () => Handoff.childWorkflow());
  }
}

/** Hand the workflow to another execution without changing its status, as a resume's claim does. */
async function stealOwnership(systemDatabaseUrl: string | undefined, workflowID: string) {
  const client = new Client({ connectionString: systemDatabaseUrl });
  await client.connect();
  try {
    await client.query(`UPDATE dbos.workflow_status SET execution_xid = 'another-execution' WHERE workflow_uuid = $1`, [
      workflowID,
    ]);
  } finally {
    await client.end();
  }
}

/** Plant a checkpoint for the step this execution is about to record, with a different completion time. */
async function plantForeignCheckpoint(
  systemDatabaseUrl: string | undefined,
  workflowID: string,
  stepID: number,
  name: string,
) {
  const client = new Client({ connectionString: systemDatabaseUrl });
  await client.connect();
  try {
    await client.query(
      `INSERT INTO dbos.operation_outputs (workflow_uuid, function_id, function_name, started_at_epoch_ms, completed_at_epoch_ms)
       VALUES ($1, $2, $3, 1, 1)`,
      [workflowID, stepID, name],
    );
  } finally {
    await client.end();
  }
}

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

    static concWf = 0;
    static maxWf = 0;
    static stepRuns = 0;

    @DBOS.step()
    static async testConcStep() {
      ++TryConcExec.stepRuns;
      ++TryConcExec.concExec;
      TryConcExec.maxConc = Math.max(TryConcExec.concExec, TryConcExec.maxConc);
      await sleepms(1000);
      --TryConcExec.concExec;
    }

    @DBOS.workflow()
    static async testConcWorkflow() {
      ++TryConcExec.concWf;
      TryConcExec.maxWf = Math.max(TryConcExec.concWf, TryConcExec.maxWf);
      await sleepms(500);
      await TryConcExec.testConcStep();
      await sleepms(500);
      --TryConcExec.concWf;
    }
  }

  test('step-conc', async () => {
    const workflowUUID: string = randomUUID();

    const wfh1 = await DBOS.startWorkflow(TryConcExec, { workflowID: workflowUUID }).testConcWorkflow();
    const wfh2 = await DBOS.startWorkflow(TryConcExec, { workflowID: workflowUUID }).testConcWorkflow();

    await wfh1.getResult();
    await wfh2.getResult();
    expect(TryConcExec.maxConc).toBe(1);
    expect(TryConcExec.maxWf).toBe(1);

    // Re-enqueued once: a second re-enqueue would just race the first dispatch's claim, not the fence.
    const wfhr = await reexecuteWorkflowById(workflowUUID);
    await wfhr.getResult();
    expect(TryConcExec.maxConc).toBe(1);
    expect(TryConcExec.maxWf).toBe(1);

    // Two dispatches of one ID each take ownership in turn, so the first stops at its next
    // write and adopts the second's outcome. Their bodies may overlap.
    await setWfAndChildrenToPending(workflowUUID);
    const stepRunsBefore = TryConcExec.stepRuns;
    const wfh1r = await redispatchWorkflowById(workflowUUID);
    const wfh2r = await redispatchWorkflowById(workflowUUID);
    await expect(wfh1r.getResult()).resolves.toBeUndefined();
    await expect(wfh2r.getResult()).resolves.toBeUndefined();
    // The step was already checkpointed, so neither dispatch runs its body again.
    expect(TryConcExec.stepRuns).toBe(stepRunsBefore);
    expect((await DBOS.getWorkflowStatus(workflowUUID))?.status).toBe(StatusString.SUCCESS);
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

    forceRetries = 1;
    setDebugTrigger(DEBUG_TRIGGER_INITWF_COMMIT, {
      asyncCallback: async () => {
        if (forceRetries-- > 0) throw new Error('ECONNRESET');
      },
    });
    expect(await TryDbGlitch.testWorkflow()).toBe('Yay!');
  });

  test('handoff-by-resume-parks-the-live-execution', async () => {
    await expectHandoffParksLiveExecution('resume');
  });

  test('handoff-by-recovery-parks-the-live-execution', async () => {
    await expectHandoffParksLiveExecution('recovery');
  });

  // A running execution whose workflow is handed to another stops at its next checkpoint,
  // and the new owner runs alongside it in the same process and finishes.
  async function expectHandoffParksLiveExecution(handoff: 'resume' | 'recovery') {
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(Handoff, { workflowID }).handedOffWorkflow();
    try {
      await retryUntilSuccess(() => expect(Handoff.blockedCalls).toBe(1));
      const firstOwner = await sysdb.getWorkflowOwner(workflowID);
      expect(firstOwner).not.toBeNull();

      if (handoff === 'resume') {
        await DBOS.resumeWorkflow(workflowID);
      } else {
        const recovered = await sysdb.reenqueueWorkflowsForRecovery(
          globalParams.executorID,
          globalParams.appVersion,
          INTERNAL_QUEUE_NAME,
        );
        expect(recovered).toContain(workflowID);
      }

      // The new owner starts without waiting for the stale execution to let go.
      await retryUntilSuccess(async () => {
        const owner = await sysdb.getWorkflowOwner(workflowID);
        expect(owner).not.toBeNull();
        expect(owner).not.toBe(firstOwner);
        expect(Handoff.blockedCalls).toBe(2);
      });
      Handoff.release.set();

      await expect(handle.getResult()).resolves.toBe('blockedafter');
      await expect(DBOS.retrieveWorkflow(workflowID).getResult()).resolves.toBe('blockedafter');
      // The stale execution's step result was refused, and it never ran on past it.
      expect(Handoff.blockedCalls).toBe(2);
      expect(Handoff.afterCalls).toBe(1);
      expect(await DBOS.listWorkflowSteps(workflowID)).toHaveLength(2);
    } finally {
      // A failed assertion must not leave the step blocked, which wedges shutdown.
      Handoff.release.set();
    }
  }

  test('handoff-while-waiting-in-recv', async () => {
    // A stale execution blocked in recv wakes with its replacement; the message goes to the owner.
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(Handoff, { workflowID }).recvWorkflow();
    await retryUntilSuccess(() => expect(Handoff.recvCalls).toBe(1));
    const firstOwner = await sysdb.getWorkflowOwner(workflowID);
    await DBOS.resumeWorkflow(workflowID);

    await retryUntilSuccess(async () => {
      const owner = await sysdb.getWorkflowOwner(workflowID);
      expect(owner).not.toBeNull();
      expect(owner).not.toBe(firstOwner);
      expect(Handoff.recvCalls).toBe(2);
    });
    await DBOS.send(workflowID, 'hello', 'topic');
    await DBOS.send(workflowID, 'second', 'topic');

    await expect(handle.getResult()).resolves.toBe('hello');
    await expect(DBOS.retrieveWorkflow(workflowID).getResult()).resolves.toBe('hello');
    // The stale execution's consume rolled back with its refused checkpoint, so the
    // second message is still waiting; a leaked consume would have taken it.
    const client = new Client({ connectionString: config.systemDatabaseUrl });
    await client.connect();
    try {
      const { rows } = await client.query(
        `SELECT message FROM dbos.notifications WHERE destination_uuid = $1 AND consumed = false`,
        [workflowID],
      );
      expect(rows).toHaveLength(1);
    } finally {
      await client.end();
    }
  });

  test('cancel-refuses-a-running-step-result', async () => {
    // A step that finishes after its workflow is cancelled records nothing, so a resume re-runs it.
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(Handoff, { workflowID }).cancelledWorkflow();
    try {
      await retryUntilSuccess(() => expect(Handoff.blockedCalls).toBe(1));

      await DBOS.cancelWorkflow(workflowID);
      expect(await sysdb.getWorkflowOwner(workflowID)).toBeNull();
      Handoff.release.set();
      await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
      expect(await DBOS.listWorkflowSteps(workflowID)).toHaveLength(0);

      await DBOS.resumeWorkflow(workflowID);
      await expect(DBOS.retrieveWorkflow(workflowID).getResult()).resolves.toBe('blocked');
      expect(Handoff.blockedCalls).toBe(2);
    } finally {
      Handoff.release.set();
    }
  });

  test('stale-owner-cannot-write-the-outcome', async () => {
    // An execution that lost ownership after its last step cannot record the outcome.
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const writes: boolean[] = [];
    const realRecordOutput = sysdb.recordWorkflowOutput.bind(sysdb);
    const spy = jest.spyOn(sysdb, 'recordWorkflowOutput').mockImplementation(async (...args) => {
      const landed = await realRecordOutput(...args);
      writes.push(landed);
      return landed;
    });
    try {
      const workflowID = randomUUID();
      const handle = await DBOS.startWorkflow(Handoff, { workflowID }).outcomeWorkflow();
      await Handoff.started.wait();
      const client = new Client({ connectionString: config.systemDatabaseUrl });
      await client.connect();
      try {
        await client.query(
          `UPDATE dbos.workflow_status SET execution_xid = 'another-execution' WHERE workflow_uuid = $1`,
          [workflowID],
        );
      } finally {
        await client.end();
      }
      Handoff.release.set();

      await retryUntilSuccess(() => expect(writes).toEqual([false]));
      expect((await DBOS.getWorkflowStatus(workflowID))?.status).toBe(StatusString.PENDING);

      // Release the parked execution, which waits on an outcome nobody else will write.
      await DBOS.cancelWorkflow(workflowID);
      await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    } finally {
      Handoff.release.set();
      spy.mockRestore();
    }
  });
  test('lost-ownership-parks-at-a-sleep', async () => {
    // A stale execution reaching DBOS.sleep parks there instead of sleeping the full duration.
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(Handoff, { workflowID }).sleepingWorkflow();
    try {
      await Handoff.started.wait();
      await stealOwnership(config.systemDatabaseUrl, workflowID);
      Handoff.release.set();
      // Released before parking, well inside the 20s it would otherwise sleep.
      await retryUntilSuccess(() => expect(sysdb.checkForRunningWorkflow(workflowID)).toBe(false));
      expect(await DBOS.listWorkflowSteps(workflowID)).toHaveLength(0);
    } finally {
      Handoff.release.set();
      // Nobody else will write an outcome: cancel so the parked execution returns.
      await DBOS.cancelWorkflow(workflowID);
    }
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('stale-parent-cannot-start-an-inline-child', async () => {
    // A parent that lost ownership can neither insert nor run a directly invoked child.
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const workflowID = randomUUID();
    const childID = randomUUID();
    const handle = await DBOS.startWorkflow(Handoff, { workflowID }).parentWorkflow(childID);
    try {
      await Handoff.started.wait();
      await stealOwnership(config.systemDatabaseUrl, workflowID);
      Handoff.release.set();
      await retryUntilSuccess(() => expect(sysdb.checkForRunningWorkflow(workflowID)).toBe(false));
      // Refused at the child's insert: no child row, no child run, no parent step.
      expect(Handoff.childCalls).toBe(0);
      expect(await DBOS.getWorkflowStatus(childID)).toBeNull();
      expect(await DBOS.listWorkflowSteps(workflowID)).toHaveLength(0);
    } finally {
      Handoff.release.set();
      await DBOS.cancelWorkflow(workflowID);
    }
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('step-recorded-twice-by-the-owner-is-nondeterminism', async () => {
    // A checkpoint that finds its step already recorded with another completion time,
    // while this execution still owns the workflow, is the workflow's own nondeterminism.
    Handoff.reset();
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(Handoff, { workflowID }).cancelledWorkflow();
    try {
      await retryUntilSuccess(() => expect(Handoff.blockedCalls).toBe(1));
      await plantForeignCheckpoint(config.systemDatabaseUrl, workflowID, Handoff.blockedStepID!, 'blockedStep');
      Handoff.release.set();
      await expect(handle.getResult()).rejects.toThrow(DBOSStepNondeterminismError);
      // Still the owner, so the error is the workflow's recorded outcome.
      expect((await DBOS.getWorkflowStatus(workflowID))?.status).toBe(StatusString.ERROR);
    } finally {
      Handoff.release.set();
    }
  });

  test('step-recorded-over-a-child-row-is-nondeterminism', async () => {
    // A child-start row and a step row at the same function ID can only come from the same
    // execution recording different things, so the second write is nondeterminism.
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const handle = await DBOS.startWorkflow(Handoff).childWorkflow();
    await expect(handle.getResult()).resolves.toBe('child');
    const workflowID = handle.workflowID;
    const now = Date.now();
    await sysdb.recordOperationResult(workflowID, 10, 'child.wf', true, now, now, { childWorkflowID: randomUUID() });
    // A far-future completion time, so it cannot equal the child row's.
    await expect(
      sysdb.recordOperationResult(workflowID, 10, 'a.step', true, now, now + 3_600_000, { output: '1' }),
    ).rejects.toThrow(DBOSStepNondeterminismError);
  });

  test('running-entries-release-their-own-bucket', () => {
    // A resumed workflow's executions can sit in different queues; each release removes only its own.
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const workflowID = randomUUID();
    const first = sysdb.registerRunningWorkflow(workflowID, 'bucket-a');
    const second = sysdb.registerRunningWorkflow(workflowID, 'bucket-b');
    // The older entry first: removing the most recent entry instead would drop bucket-b.
    first.release();
    first.release(); // idempotent
    expect(sysdb.countRunningWorkflowsForQueue('bucket-a')).toBe(0);
    expect(sysdb.countRunningWorkflowsForQueue('bucket-b')).toBe(1);
    expect(sysdb.checkForRunningWorkflow(workflowID)).toBe(true);
    second.release();
    expect(sysdb.checkForRunningWorkflow(workflowID)).toBe(false);
  });

  test('dispatch-without-a-token-is-refused', async () => {
    // A dequeued dispatch must carry the claim's token; without one nothing runs or registers.
    Handoff.reset();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(Handoff, { workflowID }).outcomeWorkflow();
    try {
      await Handoff.started.wait();
      const status = (await sysdb.getWorkflowStatus(workflowID))!;
      await expect(
        DBOSExecutor.globalInstance!.executeDequeuedWorkflow(status, undefined as unknown as string),
      ).rejects.toThrow('missing its execution token');
    } finally {
      Handoff.release.set();
    }
    await expect(handle.getResult()).resolves.toBe('done');
  });
});
