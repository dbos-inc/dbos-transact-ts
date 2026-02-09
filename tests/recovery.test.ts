import { WorkflowQueue, DBOS } from '../src/';
import {
  generateDBOSTestConfig,
  setUpDBOSTestSysDb,
  Event,
  recoverPendingWorkflows,
  setWfAndChildrenToPending,
} from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import { Client } from 'pg';
import { StatusString } from '../dist/src';
import { DBOSAwaitedWorkflowExceededMaxRecoveryAttempts, DBOSMaxRecoveryAttemptsExceededError } from '../src/error';
import { sleepms } from '../src/utils';
import { runWithTopContext } from '../src/context';
import assert from 'assert';

describe('recovery-tests', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;
  const queue = new WorkflowQueue('DLQQ', { concurrency: 1 });

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
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
    let status = await handle.getStatus();
    expect(status?.recoveryAttempts).toBe(LocalRecovery.maxRecoveryAttempts + 2);
    expect(status?.status).toBe(StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);

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
    await sleepms(2000); // Can't wait() because the workflow will land in the DLQ
    status = await handle.getStatus();
    expect(status?.recoveryAttempts).toBe(LocalRecovery.maxRecoveryAttempts + 2);
    expect(status?.status).toBe(StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
  }, 20000);

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
});
