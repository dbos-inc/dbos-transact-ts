import { DBOS, DBOSRuntimeConfig, StatusString } from '../../src';
import { DBOSConfig, DBOSConfigInternal } from '../../src/dbos-executor';
import { WorkflowQueue } from '../../src';
import {
  generateDBOSTestConfig,
  generatePublicDBOSTestConfig,
  queueEntriesAreCleanedUp,
  setUpDBOSTestDb,
} from '../helpers';
import { QueueMetadataResponse } from '../../src/httpServer/server';
import { HealthUrl, WorkflowQueuesMetadataUrl, WorkflowRecoveryUrl } from '../../src/httpServer/server';
import { globalParams, sleepms } from '../../src/utils';
import { Client } from 'pg';
import { step_info } from '../../schemas/system_db_schema';
import http from 'http';
import { DBOSWorkflowCancelledError } from '../../src/error';

// Add type definitions for admin server API responses
interface WorkflowResponse {
  workflow_id: string;
  status: string;
  workflow_name: string;
  workflow_class_name: string;
  workflow_config_name?: string;
  queue_name?: string;
  authenticated_user?: string;
  assumed_role?: string;
  authenticated_roles?: string[];
  output?: unknown;
  error?: unknown;
  input?: unknown[];
  executor_id?: string;
  app_version?: string;
  application_id?: string;
  recovery_attempts?: number;
  created_at?: number;
  updated_at?: number;
  timeout_ms?: number;
  deadline_epoch_ms?: number;
}

interface ErrorResponse {
  error: string;
}

describe('not-running-admin-server', () => {
  let config: DBOSConfig;
  beforeEach(async () => {
    await DBOS.shutdown();
  });

  test('test-admin-server-not-running', async () => {
    config = generatePublicDBOSTestConfig({ runAdminServer: false });
    DBOS.setConfig(config);
    await setUpDBOSTestDb(config);
    await DBOS.launch();

    await expect(async () => {
      await fetch(`http://localhost:3001${HealthUrl}`, {
        method: 'GET',
      });
    }).rejects.toThrow();

    await DBOS.shutdown();
  });

  test('admin-port-already-in-use', async () => {
    // Start a dummy server on the admin port
    const server = http.createServer().listen(3001, '127.0.0.1');
    try {
      config = generatePublicDBOSTestConfig({ runAdminServer: true });
      DBOS.setConfig(config);
      await setUpDBOSTestDb(config);
      await DBOS.launch();
      await DBOS.shutdown();
    } finally {
      server.close();
    }
  });
});

describe('running-admin-server-tests', () => {
  let config: DBOSConfigInternal;
  let systemDBClient: Client;

  beforeEach(async () => {
    process.env.DBOS__VMID = 'test-executor';
    await DBOS.shutdown();
    config = generateDBOSTestConfig();
    const runtimeConfig: DBOSRuntimeConfig = {
      entrypoints: [],
      port: 3000,
      admin_port: 3001,
      runAdminServer: true,
      start: [],
      setup: [],
    };
    DBOS.setConfig(config, runtimeConfig);
    await setUpDBOSTestDb(config);
    await DBOS.launch();
    await DBOS.launchAppHTTPServer();
    systemDBClient = new Client({
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: config.system_database,
    });
    await systemDBClient.connect();
    TestAdminWorkflow.counter = 0;
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
  }, 10000);

  const testQueueOne = new WorkflowQueue('test-queue-1');
  const testQueueTwo = new WorkflowQueue('test-queue-2', { concurrency: 1 });
  const testQueueThree = new WorkflowQueue('test-queue-3', { concurrency: 1, workerConcurrency: 1 });
  const testQueueFour = new WorkflowQueue('test-queue-4', {
    concurrency: 1,
    workerConcurrency: 1,
    rateLimit: { limitPerPeriod: 0, periodSec: 0 },
  });

  class TestAdminWorkflow {
    static counter = 0;

    @DBOS.workflow()
    static async simpleWorkflow(value: number) {
      TestAdminWorkflow.counter++;
      const msg = await DBOS.recv<string>();
      return `${value}-${msg}`;
    }

    @DBOS.step()
    static async stepOne() {
      return Promise.resolve();
    }

    @DBOS.step()
    static async stepTwo() {
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async workflowWithSteps() {
      await TestAdminWorkflow.stepOne();
      await DBOS.sleepSeconds(1);
      await TestAdminWorkflow.stepTwo();
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async exampleWorkflow(input: number) {
      return Promise.resolve(input);
    }

    @DBOS.workflow()
    static async blockedWorkflow() {
      while (true) {
        await DBOS.sleep(100);
      }
    }
  }

  test('test-admin-workflow-management', async () => {
    // Run the workflow. Verify it succeeds.
    const handle = await DBOS.startWorkflow(TestAdminWorkflow).simpleWorkflow(42);

    // Cancel the workflow. Verify it was cancelled.
    let response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/cancel`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(204);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });

    // Resume the workflow. Verify it succeeds again.
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/resume`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(204);

    // Resume a non-existent workflow should fail
    response = await fetch(`http://localhost:3001/workflows/invalid-workflow-id/resume`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(500);

    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });

    await DBOS.send(handle.workflowID, 'message');
    const newHandle = DBOS.retrieveWorkflow(handle.workflowID);
    await expect(newHandle.getResult()).resolves.toEqual('42-message');
    await expect(newHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Restart the workflow. Verify it runs
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/restart`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(200);

    const { workflow_id: restartWorkflowID } = (await response.json()) as { workflow_id: string };
    const restartHandle = DBOS.retrieveWorkflow(restartWorkflowID);
    await expect(restartHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });

    await DBOS.send(restartWorkflowID, 'restart-message');
    await expect(restartHandle.getResult()).resolves.toEqual('42-restart-message');

    // test fork
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/fork`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ start_step: 0 }),
    });
    expect(response.status).toBe(200);

    const { workflow_id: forkWorkflowID } = (await response.json()) as { workflow_id: string };
    const forkStatus = await DBOS.getWorkflowStatus(forkWorkflowID);
    expect(forkStatus?.status).toBe(StatusString.ENQUEUED);

    // test fork with new workflow ID, version
    const applicationVersion = 'newVersion';
    globalParams.appVersion = applicationVersion;
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/fork`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ start_step: 0, new_workflow_id: '123456', application_version: applicationVersion }),
    });
    expect(response.status).toBe(200);

    const { workflow_id: forkWorkflowID2 } = (await response.json()) as { workflow_id: string };
    expect(forkWorkflowID2).toBe('123456');
    const forkHandle2 = DBOS.retrieveWorkflow(forkWorkflowID2);
    const forkStatus2 = await DBOS.getWorkflowStatus(forkWorkflowID2);
    expect(forkStatus2?.status).toBe(StatusString.ENQUEUED);
    expect(forkStatus2?.applicationVersion).toBe(applicationVersion);

    await DBOS.send(forkWorkflowID2, 'fork-message');
    await expect(forkHandle2.getResult()).resolves.toEqual('42-fork-message');
  });

  test('test-admin-list-workflow-steps', async () => {
    const handle = await DBOS.startWorkflow(TestAdminWorkflow).workflowWithSteps();
    await handle.getResult();
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    const response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/steps`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(200);
    //eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const steps: step_info[] = await response.json();
    expect(steps.length).toBe(3);
    expect(steps[0].function_name).toBe('stepOne');
    expect(steps[1].function_name).toBe('DBOS.sleep');
    expect(steps[2].function_name).toBe('stepTwo');
  });

  test('test-admin-workflow-recovery', async () => {
    // Verify the executor ID is set.
    expect(globalParams.executorID).toBe('test-executor');

    // Run the workflow. Verify it succeeds.
    const handle = await DBOS.startWorkflow(TestAdminWorkflow).simpleWorkflow(42);
    await DBOS.send(handle.workflowID, 'message');
    await expect(handle.getResult()).resolves.toEqual('42-message');
    expect(TestAdminWorkflow.counter).toBe(1);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Set the workflow back to pending and change the executor ID.
    await systemDBClient.query(
      `UPDATE dbos.workflow_status SET status='PENDING', executor_id=$1 WHERE workflow_uuid=$2`,
      ['other-executor', handle.workflowID],
    );
    const wfStatus = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(wfStatus).not.toBeNull();
    expect(wfStatus?.executorId).toBe('other-executor');
    expect(wfStatus?.status).toBe(StatusString.PENDING);

    // Recover the workflow, and make sure it finishes with the correct executor ID.
    const data = ['other-executor'];
    const recoveryResponse = await fetch(`http://localhost:3001${WorkflowRecoveryUrl}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    expect(recoveryResponse.status).toBe(200);
    expect(await recoveryResponse.json()).toEqual([handle.workflowID]);

    // Wait until it succeeds.
    let succeeded = false;
    for (let i = 0; i < 10; i++) {
      const status = await DBOS.getWorkflowStatus(handle.workflowID);
      if (status?.status === StatusString.SUCCESS) {
        expect(status.executorId).toBe('test-executor');
        expect(status.status).toBe(StatusString.SUCCESS);
        succeeded = true;
        break;
      }
      await sleepms(1000);
    }
    expect(succeeded).toBe(true);
  });

  test('test-admin-endpoints', async () => {
    // Test GET /dbos-healthz
    const healthzResponse = await fetch(`http://localhost:3001${HealthUrl}`, {
      method: 'GET',
    });
    expect(healthzResponse.status).toBe(200);
    expect(await healthzResponse.text()).toBe('healthy');

    // Test POST /dbos-workflow-recovery
    const data = ['executor1', 'executor2'];
    const recoveryResponse = await fetch(`http://localhost:3001${WorkflowRecoveryUrl}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    expect(recoveryResponse.status).toBe(200);
    expect(await recoveryResponse.json()).toEqual([]);

    // Test WorkflowQueuesMetadataUrl
    const metadataResponse = await fetch(`http://localhost:3001${WorkflowQueuesMetadataUrl}`, {
      method: 'GET',
    });
    expect(metadataResponse.status).toBe(200);
    const queueMetadata: QueueMetadataResponse[] = (await metadataResponse.json()) as QueueMetadataResponse[];
    expect(queueMetadata.length).toBe(6);
    for (const q of queueMetadata) {
      if (q.name === testQueueOne.name) {
        expect(q.concurrency).toBeUndefined();
        expect(q.workerConcurrency).toBeUndefined();
        expect(q.rateLimit).toBeUndefined();
      } else if (q.name === testQueueTwo.name) {
        expect(q.concurrency).toBe(1);
        expect(q.workerConcurrency).toBeUndefined();
        expect(q.rateLimit).toBeUndefined();
      } else if (q.name === testQueueThree.name) {
        expect(q.concurrency).toBe(1);
        expect(q.workerConcurrency).toBe(1);
        expect(q.rateLimit).toBeUndefined();
      } else if (q.name === testQueueFour.name) {
        expect(q.concurrency).toBe(1);
        expect(q.workerConcurrency).toBe(1);
        expect(q.rateLimit).toEqual({ limitPerPeriod: 0, periodSec: 0 });
      }
    }

    // Test GET not found
    const getNotFoundResponse = await fetch('http://localhost:3001/stuff', {
      method: 'GET',
    });
    expect(getNotFoundResponse.status).toBe(404);

    // Test POST not found
    const postNotFoundResponse = await fetch('http://localhost:3001/stuff', {
      method: 'POST',
    });
    expect(postNotFoundResponse.status).toBe(404);
  });

  const queue = new WorkflowQueue('test-admin-deactivate', {});

  test('test-admin-deactivate', async () => {
    const value = 5;
    let handle = await DBOS.startWorkflow(TestAdminWorkflow, { queueName: queue.name }).exampleWorkflow(value);
    await expect(handle.getResult()).resolves.toBe(value);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    const response = await fetch(`http://localhost:3001/deactivate`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(200);

    // Verify queues still work after deactivation
    handle = await DBOS.startWorkflow(TestAdminWorkflow, { queueName: queue.name }).exampleWorkflow(value);
    await expect(handle.getResult()).resolves.toBe(value);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  test('test-admin-garbage-collect', async () => {
    const value = 5;
    await expect(TestAdminWorkflow.exampleWorkflow(value)).resolves.toBe(value);
    expect((await DBOS.listWorkflows({})).length).toBe(1);

    const response = await fetch(`http://localhost:3001/dbos-garbage-collect`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ cutoff_epoch_timestamp_ms: Date.now() }),
    });
    expect(response.status).toBe(204);

    expect((await DBOS.listWorkflows({})).length).toBe(0);
  });

  test('test-admin-global-timeout', async () => {
    const handle = await DBOS.startWorkflow(TestAdminWorkflow).blockedWorkflow();
    await sleepms(1000);

    const response = await fetch(`http://localhost:3001/dbos-global-timeout`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ cutoff_epoch_timestamp_ms: Date.now() - 1000 }),
    });
    expect(response.status).toBe(204);

    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-admin-get-workflow', async () => {
    // Run a workflow to have something to get
    const handle = await DBOS.startWorkflow(TestAdminWorkflow).exampleWorkflow(123);
    await handle.getResult();
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Test GET /workflows/:workflow_id - existing workflow
    let response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(200);
    const workflow = (await response.json()) as WorkflowResponse;
    expect(workflow.workflow_id).toBe(handle.workflowID);
    expect(workflow.status).toBe(StatusString.SUCCESS);
    expect(workflow.workflow_name).toBe('exampleWorkflow');

    // Test GET /workflows/:workflow_id - non-existing workflow
    response = await fetch(`http://localhost:3001/workflows/non-existing-workflow-id`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(404);
    const errorResponse = (await response.json()) as ErrorResponse;
    expect(errorResponse.error).toBe('Workflow non-existing-workflow-id not found');
  });

  test('test-admin-list-workflows', async () => {
    // Run first workflow
    const handle1 = await DBOS.startWorkflow(TestAdminWorkflow).exampleWorkflow(456);
    await handle1.getResult();
    await expect(handle1.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Sleep 1 second
    await sleepms(1000);

    // Record time between workflows (this will be used for filtering)
    const firstWorkflowTime = new Date().toISOString();

    // Run second workflow
    const handle2 = await DBOS.startWorkflow(TestAdminWorkflow).exampleWorkflow(789);
    await handle2.getResult();
    await expect(handle2.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Test POST /workflows - list all workflows
    let response = await fetch(`http://localhost:3001/workflows`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });
    expect(response.status).toBe(200);
    let workflows = (await response.json()) as WorkflowResponse[];

    // Both workflows should appear in the results
    const workflowIds = workflows.map((w) => w.workflow_id);
    expect(workflows.length).toBe(2);
    expect(workflowIds).toContain(handle1.workflowID);
    expect(workflowIds).toContain(handle2.workflowID);

    // Test POST /workflows - list with filtering by start time and workflow IDs
    // This should only return the second workflow since we filter by time after the first workflow
    // and pass both IDs to make sure the correct filters are applied
    response = await fetch(`http://localhost:3001/workflows`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        start_time: firstWorkflowTime,
        workflow_ids: [handle1.workflowID, handle2.workflowID],
      }),
    });
    expect(response.status).toBe(200);
    workflows = (await response.json()) as WorkflowResponse[];

    // Only the second workflow should be returned since it was created after firstWorkflowTime
    expect(workflows.length).toBe(1);
    expect(workflows[0].workflow_id).toBe(handle2.workflowID);
    expect(workflows[0].status).toBe(StatusString.SUCCESS);
    expect(workflows[0].workflow_name).toBe('exampleWorkflow');
  });

  test('test-admin-list-queued-workflows', async () => {
    // Create a queue for testing
    const testQueue = new WorkflowQueue('test-admin-list-queue', { concurrency: 1 });

    // Enqueue some workflows that will be blocked
    const handle1 = await DBOS.startWorkflow(TestAdminWorkflow, { queueName: testQueue.name }).blockedWorkflow();
    const handle2 = await DBOS.startWorkflow(TestAdminWorkflow, { queueName: testQueue.name }).blockedWorkflow();
    const handle3 = await DBOS.startWorkflow(TestAdminWorkflow, { queueName: testQueue.name }).blockedWorkflow();

    // Also enqueue a workflow in a different queue
    const handle4 = await DBOS.startWorkflow(TestAdminWorkflow, { queueName: testQueueOne.name }).blockedWorkflow();

    // Test POST /queues - list all queued workflows
    let response = await fetch(`http://localhost:3001/queues`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });
    expect(response.status).toBe(200);
    let queuedWorkflows = (await response.json()) as WorkflowResponse[];

    // Should have at least 4 workflows (the ones we just enqueued)
    expect(queuedWorkflows.length).toBeGreaterThanOrEqual(4);

    // Test filtering by queue name
    response = await fetch(`http://localhost:3001/queues`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        queue_name: testQueue.name,
      }),
    });
    expect(response.status).toBe(200);
    queuedWorkflows = (await response.json()) as WorkflowResponse[];

    // Should have exactly 3 workflows for this specific queue
    expect(queuedWorkflows.length).toBe(3);
    queuedWorkflows.forEach((wf) => {
      expect(wf.queue_name).toBe(testQueue.name);
      expect(wf.workflow_name).toBe('blockedWorkflow');
    });

    // Test with limit
    response = await fetch(`http://localhost:3001/queues`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        limit: 2,
      }),
    });
    expect(response.status).toBe(200);
    queuedWorkflows = (await response.json()) as WorkflowResponse[];

    // Should have exactly 2 workflows
    expect(queuedWorkflows.length).toBe(2);

    // Cancel all the workflows to clean up
    await DBOS.cancelWorkflow(handle1.workflowID);
    await DBOS.cancelWorkflow(handle2.workflowID);
    await DBOS.cancelWorkflow(handle3.workflowID);
    await DBOS.cancelWorkflow(handle4.workflowID);
    await queueEntriesAreCleanedUp();
  });
});
