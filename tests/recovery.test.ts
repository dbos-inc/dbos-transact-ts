import { WorkflowContext, Workflow, WorkflowQueue, TestingRuntime } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import { TestingRuntimeImpl, createInternalTestRuntime } from '../src/testing/testing_runtime';
import { WorkflowRecoveryUrl } from '../src/httpServer/server';
import request from 'supertest';
import { Client } from 'pg';
import { StatusString } from '../dist/src';
import { DBOSDeadLetterQueueError } from '../src/error';

describe('recovery-tests', () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime(undefined, config);
    process.env.DBOS__VMID = '';
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
    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      LocalRecovery.resolve1 = resolve;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      LocalRecovery.resolve2 = resolve;
    });

    static resolve3: () => void;
    static promise3 = new Promise<void>((resolve) => {
      LocalRecovery.resolve3 = resolve;
    });

    static cnt = 0;

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
      LocalRecovery.recoveryCount += 1;
      LocalRecovery.resolve3();
      await LocalRecovery.deadLetterPromise;
    }
  }

  test('dead-letter-queue', async () => {
    LocalRecovery.cnt = 0;
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const handle = await testRuntime.startWorkflow(LocalRecovery).deadLetterWorkflow();

    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts * 2; i++) {
      await dbosExec.recoverPendingWorkflows();
      expect(LocalRecovery.recoveryCount).toBeGreaterThanOrEqual(Math.min(i, LocalRecovery.maxRecoveryAttempts));
      expect(LocalRecovery.recoveryCount).toBeLessThanOrEqual(LocalRecovery.maxRecoveryAttempts);
    }

    // Attempt to recover the blocked workflow in excess of the maximum number of times. Verify it enters the DLQ status.
    let result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 1));
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
    LocalRecovery.cnt = 0;
    LocalRecovery.recoveryCount = 0;
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const queue = new WorkflowQueue('DLQQ', { concurrency: 1 });

    const handle = await testRuntime
      .startWorkflow(LocalRecovery, undefined, undefined, queue)
      .fencedDeadLetterWorkflow();
    await LocalRecovery.promise3;

    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts * 2; i++) {
      await dbosExec.recoverPendingWorkflows();
      expect(LocalRecovery.recoveryCount).toBeGreaterThanOrEqual(Math.min(i, LocalRecovery.maxRecoveryAttempts));
      expect(LocalRecovery.recoveryCount).toBeLessThanOrEqual(LocalRecovery.maxRecoveryAttempts);
    }

    let result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 1));
    expect(result.rows[0].status).toBe(StatusString.RETRIES_EXCEEDED);

    LocalRecovery.deadLetterResolve();
    await handle.getResult();

    await dbosExec.flushWorkflowBuffers();
    result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
  });

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

  test('skip-local-recovery', async () => {
    process.env.DBOS__VMID = 'testskip';
    LocalRecovery.cnt = 0;

    // Run a workflow until pending and start recovery. We should skip the recovery since the DBOS__VMID is not empty.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const handle = await testRuntime
      .startWorkflow(LocalRecovery, undefined, {
        authenticatedUser: 'test_recovery_user',
        request: { url: 'test-recovery-url' },
      })
      .testRecoveryWorkflow(5);

    const recoverHandles = await dbosExec.recoverPendingWorkflows();

    expect(recoverHandles.length).toBe(0);
    LocalRecovery.resolve1(); // Both can finish now.
    await expect(handle.getResult()).resolves.toBe('test_recovery_user');
    expect(LocalRecovery.cnt).toBe(5); // Should run once because we skipped the local recovery
  });

  /**
   * Test for selectively recovering workflows run by an executor.
   */
  class ExecutorRecovery {
    static localResolve: () => void;
    static localPromise = new Promise<void>((resolve) => {
      ExecutorRecovery.localResolve = resolve;
    });

    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve1 = resolve;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve2 = resolve;
    });

    static localCnt = 0;
    static executorCnt = 0;

    @Workflow()
    static async localWorkflow(ctxt: WorkflowContext, input: number) {
      ExecutorRecovery.localCnt += input;
      await ExecutorRecovery.localPromise;
      return ctxt.authenticatedUser;
    }

    @Workflow()
    static async executorWorkflow(ctxt: WorkflowContext, input: number) {
      ExecutorRecovery.executorCnt += input;

      // Signal the workflow has been executed more than once.
      if (ExecutorRecovery.executorCnt > input) {
        ExecutorRecovery.resolve2();
      }

      await ExecutorRecovery.promise1;
      return ctxt.authenticatedUser;
    }
  }

  test('selective-recovery', async () => {
    // Invoke a workflow multiple times with different executor IDs, but only recover workflows for a specific executor.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const localHandle = await testRuntime
      .startWorkflow(ExecutorRecovery, undefined, { authenticatedUser: 'local_user' })
      .localWorkflow(3);

    process.env.DBOS__VMID = 'fcvm123';
    const execHandle = await testRuntime
      .invoke(ExecutorRecovery, undefined, { authenticatedUser: 'cloud_user' })
      .executorWorkflow(5);

    const recoverHandles = await dbosExec.recoverPendingWorkflows(['fcvm123']);
    await ExecutorRecovery.promise2; // Wait for the recovery to be done.
    ExecutorRecovery.resolve1();
    ExecutorRecovery.localResolve();

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe('cloud_user');
    await expect(localHandle.getResult()).resolves.toBe('local_user');
    await expect(execHandle.getResult()).resolves.toBe('cloud_user');

    expect(ExecutorRecovery.localCnt).toBe(3); // Should run only once.
    expect(ExecutorRecovery.executorCnt).toBe(10); // Should run twice.
  });

  test('http-recovery', async () => {
    // Invoke a workflow and invoke a recovery through HTTP endpoint.
    // Reset variables.
    ExecutorRecovery.executorCnt = 0;
    ExecutorRecovery.promise1 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve1 = resolve;
    });
    ExecutorRecovery.promise2 = new Promise<void>((resolve) => {
      ExecutorRecovery.resolve2 = resolve;
    });

    process.env.DBOS__VMID = 'fcvm123';
    const execHandle = await testRuntime
      .startWorkflow(ExecutorRecovery, undefined, { authenticatedUser: 'cloud_user' })
      .executorWorkflow(5);

    const response = await request(testRuntime.getAdminCallback()).post(WorkflowRecoveryUrl).send(['fcvm123']);
    expect(response.statusCode).toBe(200);
    expect(response.body).toStrictEqual([execHandle.getWorkflowUUID()]);

    await ExecutorRecovery.promise2; // Wait for the recovery to be done.
    ExecutorRecovery.resolve1();

    // Check output.
    await expect(execHandle.getResult()).resolves.toBe('cloud_user');
    expect(ExecutorRecovery.executorCnt).toBe(10); // Should run twice.
  });
});
