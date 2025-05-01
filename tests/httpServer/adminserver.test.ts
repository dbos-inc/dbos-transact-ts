import { DBOS, DBOSRuntimeConfig, StatusString } from '../../src';
import { DBOSConfig, DBOSConfigInternal } from '../../src/dbos-executor';
import { WorkflowQueue } from '../../src';
import { generateDBOSTestConfig, generatePublicDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import { QueueMetadataResponse } from '../../src/httpServer/server';
import { HealthUrl, WorkflowQueuesMetadataUrl, WorkflowRecoveryUrl } from '../../src/httpServer/server';
import { globalParams, sleepms } from '../../src/utils';
import { Client } from 'pg';
import { step_info } from '../../schemas/system_db_schema';
import http from 'http';

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

  beforeAll(async () => {
    // Reset the executor ID
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
  });

  beforeEach(async () => {
    systemDBClient = new Client({
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: config.system_database,
    });
    await systemDBClient.connect();
    testAdminWorkflow.counter = 0;
  });

  afterEach(async () => {
    await systemDBClient.end();
  }, 10000);

  afterAll(async () => {
    await DBOS.shutdown();
  });

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
      await testAdminWorkflow.stepOne();
      await DBOS.sleepSeconds(1);
      await testAdminWorkflow.stepTwo();
      return Promise.resolve();
    }
  }

  test('test-admin-workflow-management', async () => {
    // Run the workflow. Verify it succeeds.
    const handle = await DBOS.startWorkflow(testAdminWorkflow).simpleWorkflow();
    await handle.getResult();
    expect(testAdminWorkflow.counter).toBe(1);
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
    await handle.getResult();

    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.ENQUEUED,
    });

    // Restart the workflow. Verify it runs
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/restart`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(200);

    let new_workflowID_json = (await response.json()) as { workflow_id: string };
    let new_workflowID = new_workflowID_json.workflow_id;

    let succeeded = false;
    for (let i = 0; i < 5; i++) {
      const status = await DBOS.getWorkflowStatus(new_workflowID);
      if (status !== null && status.status === StatusString.SUCCESS) {
        succeeded = true;
        break;
      }
      await sleepms(1000);
    }

    expect(succeeded).toBe(true);

    // test fork
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/fork`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ start_step: 0 }),
    });
    expect(response.status).toBe(200);

    new_workflowID_json = (await response.json()) as { workflow_id: string };
    new_workflowID = new_workflowID_json.workflow_id;

    succeeded = false;
    for (let i = 0; i < 5; i++) {
      const status = await DBOS.getWorkflowStatus(new_workflowID);
      if (status !== null && status.status === StatusString.SUCCESS) {
        succeeded = true;
        break;
      }
      await sleepms(1000);
    }
  });

  test('test-admin-list-workflow-steps', async () => {
    const handle = await DBOS.startWorkflow(testAdminWorkflow).workflowWithSteps();
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
    const handle = await DBOS.startWorkflow(testAdminWorkflow).simpleWorkflow();
    await handle.getResult();
    expect(testAdminWorkflow.counter).toBe(1);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Set the workflow back to pending and change the executor ID.
    await systemDBClient.query(
      `UPDATE dbos.workflow_status SET status='PENDING', executor_id=$1 WHERE workflow_uuid=$2`,
      ['other-executor', handle.getWorkflowUUID()],
    );
    const wfStatus = await DBOS.getWorkflowStatus(handle.getWorkflowUUID());
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
    expect(await recoveryResponse.json()).toEqual([handle.getWorkflowUUID()]);

    // Wait until it succeeds.
    let succeeded = false;
    for (let i = 0; i < 10; i++) {
      const status = await DBOS.getWorkflowStatus(handle.getWorkflowUUID());
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
    expect(queueMetadata.length).toBe(5);
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
