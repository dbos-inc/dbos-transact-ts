import { WorkflowQueue, DBOS } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event, recoverPendingWorkflows } from './helpers';
import { DBOSConfigInternal } from '../src/dbos-executor';
import { Client } from 'pg';
import { StatusString } from '../dist/src';
import { DBOSDeadLetterQueueError } from '../src/error';
import { sleepms } from '../src/utils';
import { runWithTopContext } from '../src/context';

describe('recovery-tests', () => {
  let config: DBOSConfigInternal;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
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
    await DBOS.shutdown();
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

    @DBOS.workflow()
    static async testRecoveryWorkflow(input: number) {
      if (DBOS.authenticatedUser === 'test_recovery_user' && DBOS.request.url === 'test-recovery-url') {
        LocalRecovery.cnt += input;
      }

      // Signal the workflow has been executed more than once.
      if (LocalRecovery.cnt > input) {
        LocalRecovery.resolve2();
      }

      await LocalRecovery.promise1;
      return DBOS.authenticatedUser;
    }

    static resolve3: () => void;
    static promise3 = new Promise<void>((resolve) => {
      LocalRecovery.resolve3 = resolve;
    });

    static resolve4: () => void;
    static promise4 = new Promise<void>((resolve) => {
      LocalRecovery.resolve4 = resolve;
    });

    @DBOS.workflow()
    static async testTxErrorWorkflow(input: number) {
      const message = `Error in transaction with input: ${input}`;
      let errorMessage: string | undefined = undefined;
      try {
        await LocalRecovery.errorTransaction(message);
      } catch (e) {
        errorMessage = (e as Error).message;
      }

      LocalRecovery.cnt += input;
      if (LocalRecovery.cnt > input) {
        LocalRecovery.resolve4();
      }

      await LocalRecovery.promise3;
      return { errorMessage };
    }

    @DBOS.transaction()
    static async errorTransaction(message: string) {
      // simulate async work to make linter happy
      await Promise.resolve();
      throw new Error(message);
    }

    static recoveryCount = 0;
    static readonly maxRecoveryAttempts = 5;
    static deadLetterResolve: () => void;
    static deadLetterPromise = new Promise<void>((resolve) => {
      LocalRecovery.deadLetterResolve = resolve;
    });

    @DBOS.workflow({ maxRecoveryAttempts: LocalRecovery.maxRecoveryAttempts })
    static async deadLetterWorkflow() {
      LocalRecovery.recoveryCount += 1;
      await LocalRecovery.deadLetterPromise;
    }

    @DBOS.workflow({ maxRecoveryAttempts: LocalRecovery.maxRecoveryAttempts })
    static async fencedDeadLetterWorkflow() {
      LocalRecovery.startEvent.set();
      LocalRecovery.recoveryCount += 1;
      await LocalRecovery.endEvent.wait();
    }
  }

  test('dead-letter-queue', async () => {
    LocalRecovery.cnt = 0;

    const handle = await DBOS.startWorkflow(LocalRecovery).deadLetterWorkflow();

    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts; i++) {
      await recoverPendingWorkflows();
      expect(LocalRecovery.recoveryCount).toBe(i + 2);
    }

    // Send to DLQ and verify it enters the DLQ status.
    await recoverPendingWorkflows();
    let result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    // recovery_attempts is set before checking the number of attempts/retry
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 2));
    expect(result.rows[0].status).toBe(StatusString.RETRIES_EXCEEDED);

    // Verify a direct invocation errors
    await expect(
      DBOS.startWorkflow(LocalRecovery, { workflowID: handle.workflowID }).deadLetterWorkflow(),
    ).rejects.toThrow(DBOSDeadLetterQueueError);

    // Resume the workflow. Verify it returns to PENDING status without error and attempts are reset.
    const resumedHandle = await DBOS.resumeWorkflow(handle.workflowID);
    result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(0));
    expect(result.rows[0].status).toBe(StatusString.ENQUEUED);

    // Verify a direct invocation no longer errors
    await expect(
      DBOS.startWorkflow(LocalRecovery, { workflowID: handle.workflowID }).deadLetterWorkflow(),
    ).resolves.toBeDefined();

    // Complete the blocked workflow. Verify it succeeds with two attempts (the resumption and the direct invocation).
    LocalRecovery.deadLetterResolve();
    await handle.getResult();
    await resumedHandle.getResult();
    result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
  });

  test('enqueued-dead-letter-queue', async () => {
    LocalRecovery.recoveryCount = 0;

    const queue = new WorkflowQueue('DLQQ', { concurrency: 1 });

    const handle = await DBOS.startWorkflow(LocalRecovery, { queueName: queue.name }).fencedDeadLetterWorkflow();
    await LocalRecovery.startEvent.wait();
    expect(LocalRecovery.recoveryCount).toBe(1);

    for (let i = 0; i < LocalRecovery.maxRecoveryAttempts; i++) {
      LocalRecovery.startEvent.clear();
      await recoverPendingWorkflows();
      await LocalRecovery.startEvent.wait();
      expect(LocalRecovery.recoveryCount).toBe(i + 2);
    }

    // One more recovery attempt should move the workflow to the dead-letter queue.
    await recoverPendingWorkflows();
    await sleepms(2000); // Can't wait() because the workflow will land in the DLQ
    let result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    // recovery_attempts is set before checking the number of attempts/retry
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 2));
    expect(result.rows[0].status).toBe(StatusString.RETRIES_EXCEEDED);

    LocalRecovery.endEvent.set();
    await handle.getResult();

    result = await systemDBClient.query<{ status: string; recovery_attempts: number }>(
      `SELECT status, recovery_attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    expect(result.rows[0].recovery_attempts).toBe(String(LocalRecovery.maxRecoveryAttempts + 2));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
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

    const recoverHandles = await recoverPendingWorkflows();
    await LocalRecovery.promise2; // Wait for the recovery to be done.
    LocalRecovery.resolve1(); // Both can finish now.

    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toBe('test_recovery_user');
    await expect(handle.getResult()).resolves.toBe('test_recovery_user');
    expect(LocalRecovery.cnt).toBe(10); // Should run twice.
  });

  test('failing-tx-correct-exception-on-recovery', async () => {
    LocalRecovery.cnt = 0;
    // Run a workflow until pending and start recovery.

    const handle = await DBOS.startWorkflow(LocalRecovery).testTxErrorWorkflow(5);

    const recoverHandles = await recoverPendingWorkflows();
    await LocalRecovery.promise4; // Wait for the recovery to be done.
    LocalRecovery.resolve3(); // Both can finish now.

    const expected = {
      errorMessage: 'Error in transaction with input: 5',
    };
    expect(recoverHandles.length).toBe(1);
    await expect(recoverHandles[0].getResult()).resolves.toEqual(expected);
    await expect(handle.getResult()).resolves.toEqual(expected);
    expect(LocalRecovery.cnt).toBe(10); // Should run twice.
  });
});
