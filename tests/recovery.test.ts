import { DBOS } from '../src/';
import {
  generateDBOSTestConfig,
  setUpDBOSTestSysDb,
  Event,
  recoverPendingWorkflows,
  retryUntilSuccess,
  setWfAndChildrenToPending,
} from './helpers';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { Client } from 'pg';
import { StatusString } from '../dist/src';
import { DBOSAwaitedWorkflowExceededMaxRecoveryAttempts, DBOSMaxRecoveryAttemptsExceededError } from '../src/error';
import { INTERNAL_QUEUE_NAME } from '../src/utils';
import { runWithTopContext } from '../src/context';
import assert from 'assert';

import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import { writeFile, rm } from 'node:fs/promises';
import { globalParams } from '../src/utils';

describe('recovery-tests', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;
  const queue = { name: 'DLQQ' };

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.registerQueue(queue.name, { onConflict: 'always_update', concurrency: 1 });
    process.env.DBOS__VMID = '';
    systemDBClient = new Client({
      connectionString: config.systemDatabaseUrl,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
  });

  /**
   * Test for the default local workflow recovery.
   */
  class LocalRecovery {
    static cnt = 0;

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      LocalRecovery.resolve2 = resolve;
    });

    static startEvent = new Event();

    @DBOS.workflow()
    static async testRecoveryWorkflow(input: number) {
      if (DBOS.authenticatedUser === 'test_recovery_user' && DBOS.request.url === 'test-recovery-url') {
        LocalRecovery.cnt += input;
      }

      // Signal the workflow has been executed more than once.
      if (LocalRecovery.cnt > input) {
        LocalRecovery.resolve2();
      }

      return Promise.resolve(DBOS.authenticatedUser);
    }

    static resolve3: () => void;
    static promise3 = new Promise<void>((resolve) => {
      LocalRecovery.resolve3 = resolve;
    });

    static resolve4: () => void;
    static promise4 = new Promise<void>((resolve) => {
      LocalRecovery.resolve4 = resolve;
    });

    static recoveryCount = 0;
    static readonly maxRecoveryAttempts = 5;

    @DBOS.workflow({ maxRecoveryAttempts: LocalRecovery.maxRecoveryAttempts })
    static async deadLetterWorkflow() {
      LocalRecovery.recoveryCount += 1;
      return Promise.resolve();
    }

    @DBOS.workflow({ maxRecoveryAttempts: LocalRecovery.maxRecoveryAttempts })
    static async fencedDeadLetterWorkflow() {
      LocalRecovery.startEvent.set();
      LocalRecovery.recoveryCount += 1;
      return Promise.resolve();
    }
  }

  class BlockedRecovery {
    static startCount = 0;
    static blocker = new Event();

    @DBOS.workflow()
    static async blockedWorkflow(input: string): Promise<string> {
      BlockedRecovery.startCount += 1;
      await BlockedRecovery.blocker.wait();
      return input;
    }
  }

  test('dead-letter-queue', async () => {
    LocalRecovery.cnt = 0;

    const handle = await DBOS.startWorkflow(LocalRecovery).deadLetterWorkflow();
    await handle.getResult();

    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts; i++) {
      await setWfAndChildrenToPending(handle.workflowID, false); // Simulate not finishing
      await (await recoverPendingWorkflows())[0].getResult();
      expect(LocalRecovery.recoveryCount).toBe(i + 2);
    }

    // Send to DLQ and verify it enters the DLQ status.
    await setWfAndChildrenToPending(handle.workflowID, false); // Simulate not finishing
    await recoverPendingWorkflows();
    // Recovery re-enqueues, so the DLQ transition happens when the queue dequeues the workflow.
    await retryUntilSuccess(async () => {
      expect((await handle.getStatus())?.status).toBe(StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
    });
    let status = await handle.getStatus();
    expect(status?.recoveryAttempts).toBe(LocalRecovery.maxRecoveryAttempts + 2);

    // Verify a direct invocation errors
    await expect(
      DBOS.startWorkflow(LocalRecovery, { workflowID: handle.workflowID }).deadLetterWorkflow(),
    ).rejects.toThrow(DBOSMaxRecoveryAttemptsExceededError);

    // Verify retrieving the status throws an exception
    const retrievedHandle = DBOS.retrieveWorkflow(handle.workflowID);
    await expect(retrievedHandle.getResult()).rejects.toThrow(DBOSAwaitedWorkflowExceededMaxRecoveryAttempts);

    // Resume the workflow. Verify it returns to PENDING status without error and attempts are reset.
    const resumedHandle = await DBOS.resumeWorkflow(handle.workflowID);
    status = await resumedHandle.getStatus();
    expect(status?.recoveryAttempts).toBe(0);
    expect(status?.status).toBe(StatusString.ENQUEUED);

    // Complete the resumed workflow. Verify it succeeds.
    await resumedHandle.getResult();
    status = await resumedHandle.getStatus();
    expect(status?.status).toBe(StatusString.SUCCESS);

    // Verify a direct invocation no longer errors
    await expect(
      DBOS.startWorkflow(LocalRecovery, { workflowID: handle.workflowID }).deadLetterWorkflow(),
    ).resolves.toBeDefined();
  });

  test('enqueued-dead-letter-queue', async () => {
    LocalRecovery.recoveryCount = 0;

    const handle = await DBOS.startWorkflow(LocalRecovery, { queueName: queue.name }).fencedDeadLetterWorkflow();
    await handle.getResult();

    // Enqueue the workflow repeatedly, verify recovery attempts is not increased
    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts; i++) {
      await DBOS.startWorkflow(LocalRecovery, {
        queueName: queue.name,
        workflowID: handle.workflowID,
      }).fencedDeadLetterWorkflow();
    }
    let status = await handle.getStatus();
    expect(status?.recoveryAttempts).toBeLessThanOrEqual(1);

    // Wait for the workflow to start
    await LocalRecovery.startEvent.wait();
    expect(LocalRecovery.recoveryCount).toBe(1);

    // Attempt to recover the workflow the maximum number of times
    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts; i++) {
      LocalRecovery.startEvent.clear();
      await setWfAndChildrenToPending(handle.workflowID, false);
      await (await recoverPendingWorkflows())[0].getResult();
      expect(LocalRecovery.recoveryCount).toBe(i + 2);
    }

    // One more recovery attempt should move the workflow to the dead-letter queue.
    await setWfAndChildrenToPending(handle.workflowID, false);
    await recoverPendingWorkflows();
    // Can't wait() because the workflow will land in the DLQ instead of starting.
    await retryUntilSuccess(async () => {
      expect((await handle.getStatus())?.status).toBe(StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
    });
    status = await handle.getStatus();
    expect(status?.recoveryAttempts).toBe(LocalRecovery.maxRecoveryAttempts + 2);
  });

  test('local-recovery', async () => {
    LocalRecovery.cnt = 0;
    // Run a workflow until pending and start recovery.

    const handle = await runWithTopContext(
      {
        authenticatedUser: 'test_recovery_user',
        request: { url: 'test-recovery-url' },
      },
      async () => await DBOS.startWorkflow(LocalRecovery).testRecoveryWorkflow(5),
    );

    await handle.getResult();
    await setWfAndChildrenToPending(handle.workflowID);
    const recoverHandles = await recoverPendingWorkflows();
    await LocalRecovery.promise2; // Wait for the recovery to be done.

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe('test_recovery_user');
    await expect(handle.getResult()).resolves.toBe('test_recovery_user');
    expect(LocalRecovery.cnt).toBe(10); // Should run twice.
  });

  test('recovery-reenqueue-is-ownership-conditional', async () => {
    // Recovery only re-enqueues workflows still owned by an executor it declared dead, so a duplicate or delayed sweep for that executor cannot yank back a workflow a live executor has since claimed.
    const sysDB = DBOSExecutor.globalInstance!.systemDatabase;
    BlockedRecovery.startCount = 0;
    BlockedRecovery.blocker.clear();
    const handle = await DBOS.startWorkflow(BlockedRecovery).blockedWorkflow('bob');

    // Orphan the workflow: PENDING, owned by an executor that is now dead.
    await systemDBClient.query(`UPDATE dbos.workflow_status SET status=$1, executor_id=$2 WHERE workflow_uuid=$3`, [
      StatusString.PENDING,
      'deadexecutor',
      handle.workflowID,
    ]);

    // Release the workflow even on failure: an assertion that escapes while it is blocked wedges shutdown in teardown and hangs the suite.
    try {
      // A sweep naming a different executor must not touch the row.
      await expect(
        sysDB.reenqueueWorkflowsForRecovery('someotherexecutor', globalParams.appVersion, INTERNAL_QUEUE_NAME),
      ).resolves.toEqual([]);
      expect((await handle.getStatus())?.status).toBe(StatusString.PENDING);

      // A sweep naming the dead executor re-enqueues it onto the internal queue.
      await expect(
        sysDB.reenqueueWorkflowsForRecovery('deadexecutor', globalParams.appVersion, INTERNAL_QUEUE_NAME),
      ).resolves.toEqual([handle.workflowID]);

      // The queue's dequeue returns the row to PENDING, now stamped with this live executor's ID -- the state the claim rests on.
      await retryUntilSuccess(async () => {
        const status = await handle.getStatus();
        expect(status?.status).toBe(StatusString.PENDING);
        expect(status?.executorId).toBe(DBOSExecutor.globalInstance!.executorID);
      });

      // The row is PENDING again, so only the executor predicate can reject this duplicate sweep.
      await expect(
        sysDB.reenqueueWorkflowsForRecovery('deadexecutor', globalParams.appVersion, INTERNAL_QUEUE_NAME),
      ).resolves.toEqual([]);
      expect((await handle.getStatus())?.status).toBe(StatusString.PENDING);
    } finally {
      BlockedRecovery.blocker.set();
    }

    await expect(handle.getResult()).resolves.toBe('bob');
  });

  test('duplicate-recovery-does-not-rerun-running-workflow', async () => {
    // Recovery hands a running workflow back to the queue, and each sweep yields exactly one dequeue.
    BlockedRecovery.startCount = 0;
    BlockedRecovery.blocker.clear();
    const handle = await DBOS.startWorkflow(BlockedRecovery).blockedWorkflow('bob');
    await retryUntilSuccess(() => expect(BlockedRecovery.startCount).toBe(1));

    // Started directly, so it carries no queue at all.
    expect((await handle.getStatus())?.queueName).toBeUndefined();

    // Release the workflow even on failure, or teardown hangs waiting on it.
    try {
      // Each sweep re-enqueues the running workflow (recovering a live executor is not prevented).
      for (const expectedAttempts of [2, 3]) {
        await recoverPendingWorkflows([DBOSExecutor.globalInstance!.executorID]);

        // A workflow that was never enqueued acquires the internal queue's name, and it is never cleared afterwards, so reading it here is not a race.
        expect((await handle.getStatus())?.queueName).toBe(INTERNAL_QUEUE_NAME);

        // The ENQUEUED->PENDING dequeue admits a single runner, so recoveryAttempts advances by exactly one per sweep.
        await retryUntilSuccess(async () => {
          const status = await handle.getStatus();
          expect(status?.recoveryAttempts).toBe(expectedAttempts);
          expect(status?.status).toBe(StatusString.PENDING);
        });
        // The workflow is already running in this process, so it is not started a second time.
        expect(BlockedRecovery.startCount).toBe(1);
      }
    } finally {
      BlockedRecovery.blocker.set();
    }

    await expect(handle.getResult()).resolves.toBe('bob');
    expect(BlockedRecovery.startCount).toBe(1);
  });

  async function stepOne(): Promise<number | undefined> {
    return Promise.resolve(DBOS.stepID);
  }
  async function stepTwo(): Promise<number | undefined> {
    return Promise.resolve(DBOS.stepID);
  }

  const evt = new Event();

  const childWorkflow = DBOS.registerWorkflow(
    async () => {
      for (let i = 0; i < 10; i++) {
        let id = await DBOS.runStep(stepOne);
        assert.equal(id, i * 2);
        id = await DBOS.runStep(stepTwo);
        assert.equal(id, i * 2 + 1);
        await evt.wait();
      }
      return DBOS.workflowID;
    },
    { name: 'childWorkflow' },
  );
  const parentWorkflow = DBOS.registerWorkflow(
    async () => {
      await DBOS.runStep(stepOne);
      await DBOS.runStep(stepTwo);
      const handle = await DBOS.startWorkflow(childWorkflow)();
      return handle.workflowID;
    },
    { name: 'parentWorkflow' },
  );

  test('child-workflow-recovery', async () => {
    const childID = await parentWorkflow();
    const originalChildHandle = DBOS.retrieveWorkflow(childID);
    const recoveredChildHandle = await DBOS.startWorkflow(childWorkflow, { workflowID: childID })();
    evt.set();

    await expect(originalChildHandle.getResult()).resolves.toEqual(originalChildHandle.workflowID);
    await expect(recoveredChildHandle.getResult()).resolves.toEqual(recoveredChildHandle.workflowID);
  });

  test('recv-recovery-with-two-processes-on-local', async () => {
    const workflowID = randomUUID();
    const topic = `recovery-topic-${randomUUID()}`;
    const timeoutSeconds = 30;
    const barrierPath = path.join(os.tmpdir(), `dbos-recv-recovery-${randomUUID()}`);

    // The workers run under their own application version: recovery re-enqueues onto the internal
    // queue, and this process, which has no registration for the worker's workflow, must not dequeue it.
    const workerAppVersion = `recv-recovery-${randomUUID()}`;

    const startWorker = spawnRecvWorker(['start', workflowID, topic, `${timeoutSeconds}`], {
      ...process.env,
      DBOS__VMID: 'local',
      DBOS__APPVERSION: workerAppVersion,
    });
    await startWorker.waitFor('STARTED');
    const startResult = await startWorker.done;
    expect(startResult.code).toBe(0);

    const recoveryWorker1 = spawnRecvWorker(['recover', workflowID, topic, `${timeoutSeconds}`, barrierPath], {
      ...process.env,
      DBOS__VMID: 'test-recv-worker-1',
      DBOS__APPVERSION: workerAppVersion,
    });
    const recoveryWorker2 = spawnRecvWorker(['recover', workflowID, topic, `${timeoutSeconds}`, barrierPath], {
      ...process.env,
      DBOS__VMID: 'test-recv-worker-2',
      DBOS__APPVERSION: workerAppVersion,
    });

    try {
      // Two separate processes both attempt to recover the same workflow from the
      // "local" executor. This is the recovery scenario we want to stress.
      await Promise.all([recoveryWorker1.waitFor('PREPARED'), recoveryWorker2.waitFor('PREPARED')]);
      await writeFile(barrierPath, 'go');
      await Promise.all([recoveryWorker1.waitFor('RECOVERED:'), recoveryWorker2.waitFor('RECOVERED:')]);

      await DBOS.send(workflowID, 'testmsg', topic);

      const [worker1Result, worker2Result] = await Promise.all([recoveryWorker1.done, recoveryWorker2.done]);
      console.log('worker1 stdout\n', worker1Result.stdout);
      console.log('worker1 stderr\n', worker1Result.stderr);
      console.log('worker2 stdout\n', worker2Result.stdout);
      console.log('worker2 stderr\n', worker2Result.stderr);
      expect(worker1Result.code).toBe(0);
      expect(worker2Result.code).toBe(0);

      const recoveredByWorker1 = /RECOVERED:(.*)/.exec(worker1Result.stdout)?.[1] ?? '';
      const recoveredByWorker2 = /RECOVERED:(.*)/.exec(worker2Result.stdout)?.[1] ?? '';
      expect([recoveredByWorker1, recoveredByWorker2].some((r) => r.includes(workflowID))).toBe(true);

      const result1 = /RESULT:(.*)/.exec(worker1Result.stdout)?.[1] ?? '';
      const result2 = /RESULT:(.*)/.exec(worker2Result.stdout)?.[1] ?? '';
      expect([result1, result2]).toContain('testmsg');

      const handle = DBOS.retrieveWorkflow<string>(workflowID);
      // Repro output seen while developing this test:
      // - one recovery worker reported RESULT:testmsg
      // - the other recovery worker reported RESULT:NULL
      // - DBOS.retrieveWorkflow(workflowID).getResult() also resolved to "NULL"
      // - but dbos.operation_outputs still checkpointed function_id 0 (DBOS.recv) as "testmsg"
      await expect(handle.getResult()).resolves.toBe('testmsg');
      await expect(handle.getStatus()).resolves.toMatchObject({ status: StatusString.SUCCESS });

      const steps = (await DBOS.listWorkflowSteps(workflowID)) ?? [];
      const recvSteps = steps.filter((s) => s.name === 'DBOS.recv');
      expect(recvSteps).toHaveLength(1);
      expect(recvSteps[0].output).toBe('testmsg');
      expect(recvSteps[0].error).toBeNull();
    } finally {
      await rm(barrierPath, { force: true });
    }
  });
});

function spawnRecvWorker(args: string[], env: NodeJS.ProcessEnv) {
  const child = spawn('npx', ['ts-node', './tests/recoveryRecvWorker.ts', ...args], {
    cwd: process.cwd(),
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdout = '';
  let stderr = '';

  child.stdout.on('data', (chunk: Buffer) => {
    stdout += chunk.toString();
  });
  child.stderr.on('data', (chunk: Buffer) => {
    stderr += chunk.toString();
  });

  const waitFor = (needle: string, timeoutMs: number = 10000) =>
    new Promise<void>((resolve, reject) => {
      const start = Date.now();
      const timer = setInterval(() => {
        if (stdout.includes(needle) || stderr.includes(needle)) {
          clearInterval(timer);
          resolve();
          return;
        }
        if (Date.now() - start > timeoutMs) {
          clearInterval(timer);
          reject(new Error(`Timed out waiting for "${needle}". stdout=${stdout} stderr=${stderr}`));
        }
      }, 25);
    });

  const done = new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve) => {
    child.on('close', (code) => {
      resolve({ code, stdout, stderr });
    });
  });

  return { waitFor, done };
}
