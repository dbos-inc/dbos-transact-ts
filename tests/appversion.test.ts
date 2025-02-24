import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

describe('test-app-version', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  class TestAppVersion {
    @DBOS.workflow()
    static async testWorkflow() {
      return Promise.resolve('5');
    }
  }

  test('test-app-version-stability', async () => {
    await expect(TestAppVersion.testWorkflow()).resolves.toEqual('5');
  });
});
