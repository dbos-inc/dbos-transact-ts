import { DBOS, DBOSRuntimeConfig, StatusString } from '../../src';
import { DBOSConfig, DBOSExecutor } from '../../src/dbos-executor';
import { WorkflowQueue } from '../../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import { QueueMetadataResponse } from '../../src/httpServer/server';
import { HealthUrl, WorkflowQueuesMetadataUrl, WorkflowRecoveryUrl } from '../../src/httpServer/server';

describe('admin-server-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    const runtimeConfig: DBOSRuntimeConfig = {
      entrypoints: [],
      port: 3000,
      admin_port: 3001,
      start: [],
      setup: [],
    };
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config, runtimeConfig);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.launchAppHTTPServer();
  });

  afterEach(async () => {
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

  class testAdminWorkflow {
    static counter = 0;

    @DBOS.workflow()
    static async simpleWorkflow() {
      testAdminWorkflow.counter++;
      return Promise.resolve();
    }
  }

  test('test-admin-workflow-management', async () => {
    // Run the workflow. Verify it succeeds.
    const handle = await DBOS.startWorkflow(testAdminWorkflow).simpleWorkflow();
    await handle.getResult();
    expect(testAdminWorkflow.counter).toBe(1);
    await DBOSExecutor.globalInstance?.flushWorkflowBuffers();
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

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
    await DBOSExecutor.globalInstance?.flushWorkflowBuffers();
    expect(testAdminWorkflow.counter).toBe(2);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Resume the workflow. Verify it does not run and status remains SUCCESS
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/resume`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(204);
    await DBOSExecutor.globalInstance?.flushWorkflowBuffers();
    expect(testAdminWorkflow.counter).toBe(2);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Restart the workflow. Verify it runs
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/restart`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(204);
    await DBOSExecutor.globalInstance?.flushWorkflowBuffers();
    expect(testAdminWorkflow.counter).toBe(3);
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
    expect(queueMetadata.length).toBe(4);
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
});
