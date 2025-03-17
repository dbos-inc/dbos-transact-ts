import { WorkflowContext, Workflow, WorkflowQueue, TestingRuntime } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import { PostgresSystemDatabase } from '../src/system_database';
import { TestingRuntimeImpl, createInternalTestRuntime } from '../src/testing/testing_runtime';
import { Client } from 'pg';
import { StatusString } from '../dist/src';
import { DBOSDeadLetterQueueError } from '../src/error';
import { sleepms } from '../src/utils';

describe('recovery-tests', () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    process.env.DBOS__VMID = '';
    testRuntime = await createInternalTestRuntime(undefined, config);
    systemDBClient = new Client({
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: config.system_database,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await testRuntime.destroy();
  });

  /**
   * Test for the default local workflow recovery.
   */
  class LocalRecovery {
    static cnt = 0;

    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      LocalRecovery.resolve1 = resolve;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      LocalRecovery.resolve2 = resolve;
    });

    static startEvent = new Event();
    static endEvent = new Event();

    @Workflow()
    static async testRecoveryWorkflow(ctxt: WorkflowContext, input: number) {
      if (ctxt.authenticatedUser === 'test_recovery_user' && ctxt.request.url === 'test-recovery-url') {
        LocalRecovery.cnt += input;
      }

      // Signal the workflow has been executed more than once.
      if (LocalRecovery.cnt > input) {
        LocalRecovery.resolve2();
      }

      await LocalRecovery.promise1;
      return ctxt.authenticatedUser;
    }

    static recoveryCount = 0;
    static readonly maxRecoveryAttempts = 5;
    static deadLetterResolve: () => void;
    static deadLetterPromise = new Promise<void>((resolve) => {
      LocalRecovery.deadLetterResolve = resolve;
    });

    @Workflow({ maxRecoveryAttempts: LocalRecovery.maxRecoveryAttempts })
    static async deadLetterWorkflow(_ctxt: WorkflowContext) {
      LocalRecovery.recoveryCount += 1;
      await LocalRecovery.deadLetterPromise;
    }

    @Workflow({ maxRecoveryAttempts: LocalRecovery.maxRecoveryAttempts })
    static async fencedDeadLetterWorkflow(_ctxt: WorkflowContext) {
      LocalRecovery.startEvent.set();
      LocalRecovery.recoveryCount += 1;
      await LocalRecovery.endEvent.wait();
    }
  }

  test('dead-letter-queue', async () => {
    LocalRecovery.cnt = 0;
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const handle = await testRuntime.startWorkflow(LocalRecovery).deadLetterWorkflow();

    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts; i++) {
      await dbosExec.recoverPendingWorkflows();
      expect(LocalRecovery.recoveryCount).toBe(i + 2);
    }

    // Send to DLQ and verify it enters the DLQ status.
    await dbosExec.recoverPendingWorkflows();
    let result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    // recovery_attempts is set before checking the number of attempts/retry
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 2));
    expect(result.rows[0].status).toBe(StatusString.RETRIES_EXCEEDED);

    // Verify a direct invocation errors
    await expect(testRuntime.startWorkflow(LocalRecovery, handle.workflowID).deadLetterWorkflow()).rejects.toThrow(
      DBOSDeadLetterQueueError,
    );

    // Resume the workflow. Verify it returns to PENDING status without error and attempts are reset.
    const resumedHandle = await dbosExec.resumeWorkflow(handle.workflowID);
    result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.PENDING);

    // Verify a direct invocation no longer errors
    await expect(
      testRuntime.startWorkflow(LocalRecovery, handle.workflowID).deadLetterWorkflow(),
    ).resolves.toBeDefined();

    // Complete the blocked workflow. Verify it succeeds with two attempts (the resumption and the direct invocation).
    LocalRecovery.deadLetterResolve();
    await handle.getResult();
    await resumedHandle.getResult();
    await dbosExec.flushWorkflowBuffers();
    result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(2));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
  });

  test('enqueued-dead-letter-queue', async () => {
    LocalRecovery.recoveryCount = 0;
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const queue = new WorkflowQueue('DLQQ', { concurrency: 1 });

    const handle = await testRuntime
      .startWorkflow(LocalRecovery, undefined, undefined, queue)
      .fencedDeadLetterWorkflow();
    await LocalRecovery.startEvent.wait();
    expect(LocalRecovery.recoveryCount).toBe(1);

    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts; i++) {
      LocalRecovery.startEvent.clear();
      await dbosExec.recoverPendingWorkflows();
      await LocalRecovery.startEvent.wait();
      expect(LocalRecovery.recoveryCount).toBe(i + 2);
    }

    // One more recovery attempt should move the workflow to the dead-letter queue.
    await dbosExec.recoverPendingWorkflows();
    await sleepms(2000); // Can't wait() because the workflow will land in the DLQ
    let result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    // recovery_attempts is set before checking the number of attempts/retry
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 2));
    expect(result.rows[0].status).toBe(StatusString.RETRIES_EXCEEDED);

    LocalRecovery.endEvent.set();
    await handle.getResult();

    await dbosExec.flushWorkflowBuffers();
    result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 2));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
  }, 20000);

  test('local-recovery', async () => {
    LocalRecovery.cnt = 0;
    // Run a workflow until pending and start recovery.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const handle = await testRuntime
      .startWorkflow(LocalRecovery, undefined, {
        authenticatedUser: 'test_recovery_user',
        request: { url: 'test-recovery-url' },
      })
      .testRecoveryWorkflow(5);

    const recoverHandles = await dbosExec.recoverPendingWorkflows();
    await LocalRecovery.promise2; // Wait for the recovery to be done.
    LocalRecovery.resolve1(); // Both can finish now.

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe('test_recovery_user');
    await expect(handle.getResult()).resolves.toBe('test_recovery_user');
    expect(LocalRecovery.cnt).toBe(10); // Should run twice.
  });

  test('test-resuming-already-completed-queue-workflow', async () => {
    LocalRecovery.startEvent.clear();
    LocalRecovery.endEvent.clear();
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    // Disable buffer flush
    clearInterval(dbosExec.flushBufferID);

    const queue = new WorkflowQueue('test-queue');
    const handle = await testRuntime
      .startWorkflow(LocalRecovery, undefined, undefined, queue)
      .fencedDeadLetterWorkflow();
    await LocalRecovery.startEvent.wait();
    LocalRecovery.startEvent.clear();
    LocalRecovery.endEvent.set();
    await sleepms(dbosExec.flushBufferIntervalMs);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
    }); // Result is not flushed

    // Clear workflow outputs buffer (simulates a process restart)
    (dbosExec.systemDatabase as PostgresSystemDatabase).workflowStatusBuffer.clear();

    // Recovery will pick up on the workflow
    const recoveredHandles = await dbosExec.recoverPendingWorkflows();
    expect(recoveredHandles.length).toBe(1);
    expect(recoveredHandles[0].getWorkflowUUID()).toBe(handle.getWorkflowUUID());
    await LocalRecovery.startEvent.wait();
    LocalRecovery.endEvent.set();
    // Manually flush
    await sleepms(2000); // Wait until our wrapper actually inserts the workflow status in the buffer
    await dbosExec.flushWorkflowBuffers();
    await expect(recoveredHandles[0].getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
      executorId: 'local',
    });
  });
});
