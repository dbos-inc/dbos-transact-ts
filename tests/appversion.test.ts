import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { globalAppVersion } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

describe('test-app-version', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  test('test-app-version-stability', async () => {
    function isHex(s: string): boolean {
      const hexChars = '0123456789abcdefABCDEF';
      return [...s].every((c) => hexChars.includes(c));
    }

    class TestAppVersion {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    // Verify the app version is correctly set to a hex string
    await DBOS.launch();
    await expect(TestAppVersion.testWorkflow()).resolves.toEqual(0);
    const appVersion = globalAppVersion.version;
    expect(appVersion.length).toBeGreaterThan(0);
    expect(isHex(appVersion)).toBe(true);
    await DBOS.shutdown();

    // Verify stability -- the same source produces the same app version
    expect(globalAppVersion.version.length).toBe(0);
    await DBOS.launch();
    expect(globalAppVersion.version).toEqual(appVersion);
    await expect(TestAppVersion.testWorkflow()).resolves.toEqual(0);

    // Verify that changing the workflow source changes the app version
    await DBOS.shutdown();

    class AnotherWorkflow {
      @DBOS.workflow()
      static async anotherWorkflow() {
        return Promise.resolve(1);
      }
    }

    expect(globalAppVersion.version.length).toBe(0);
    await DBOS.launch();
    expect(globalAppVersion.version.length).toBeGreaterThan(0);
    expect(globalAppVersion.version).not.toEqual(appVersion);
    await expect(AnotherWorkflow.anotherWorkflow()).resolves.toEqual(1);
  });
});
