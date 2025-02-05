import { DBOS, DBOSRuntimeConfig } from '../../src';
import { DBOSConfig } from '../../src/dbos-executor';
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
      signal: AbortSignal.timeout(5000),
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
      signal: AbortSignal.timeout(5000),
    });
    expect(recoveryResponse.status).toBe(200);
    expect(await recoveryResponse.json()).toEqual([]);

    // Test GET not found
    const getNotFoundResponse = await fetch('http://localhost:3001/stuff', {
      method: 'GET',
      signal: AbortSignal.timeout(5000),
    });
    expect(getNotFoundResponse.status).toBe(404);

    // Test POST not found
    const postNotFoundResponse = await fetch('http://localhost:3001/stuff', {
      method: 'POST',
      signal: AbortSignal.timeout(5000),
    });
    expect(postNotFoundResponse.status).toBe(404);
  });
});
