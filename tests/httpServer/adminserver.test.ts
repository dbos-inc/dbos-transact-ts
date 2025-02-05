import { DBOS, DBOSRuntimeConfig, StatusString } from '../../src';
import { DBOSConfig, DBOSExecutor } from '../../src/dbos-executor';
import { sleepms } from '../../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';

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

  test('test-admin-endpoints', async () => {
    // Test GET /dbos-healthz
    const healthzResponse = await fetch('http://localhost:3001/dbos-healthz', {
      method: 'GET',
    });
    expect(healthzResponse.status).toBe(200);
    expect(await healthzResponse.text()).toBe('healthy');

    // Test POST /dbos-workflow-recovery
    const data = ['executor1', 'executor2'];
    const recoveryResponse = await fetch('http://localhost:3001/dbos-workflow-recovery', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(data),
    });
    expect(recoveryResponse.status).toBe(200);
    expect(await recoveryResponse.json()).toEqual([]);

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

  class TestAdminResume {
    static counter = 0;

    @DBOS.workflow()
    static async simpleWorkflow() {
      TestAdminResume.counter++;
      return Promise.resolve();
    }
  }

  test('test-admin-resume', async () => {
    // Run the workflow. Verify it succeeds.
    const handle = await DBOS.startWorkflow(TestAdminResume).simpleWorkflow();
    await handle.getResult();
    expect(TestAdminResume.counter).toBe(1);
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
    expect(TestAdminResume.counter).toBe(2);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    // Resume the workflow. Verify it does not run and statuc remains SUCCESS
    response = await fetch(`http://localhost:3001/workflows/${handle.workflowID}/resume`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });
    expect(response.status).toBe(204);
    await DBOSExecutor.globalInstance?.flushWorkflowBuffers();
    expect(TestAdminResume.counter).toBe(2);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });
});
