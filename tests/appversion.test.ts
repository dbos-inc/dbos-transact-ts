import { DBOS, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, recoverPendingWorkflows, setUpDBOSTestDb } from './helpers';

describe('test-app-version', () => {
  let config: DBOSConfig;

  beforeEach(async () => {
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

    class TestAppVersionStability {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    // Verify the app version is correctly set to a hex string
    await DBOS.launch();
    await expect(TestAppVersionStability.testWorkflow()).resolves.toEqual(0);
    const appVersion = DBOS.applicationVersion;
    expect(appVersion.length).toBeGreaterThan(0);
    expect(isHex(appVersion)).toBe(true);
    await DBOS.shutdown();

    // Verify stability -- the same source produces the same app version
    expect(DBOS.applicationVersion.length).toBe(0);
    await DBOS.launch();
    expect(DBOS.applicationVersion).toEqual(appVersion);
    await expect(TestAppVersionStability.testWorkflow()).resolves.toEqual(0);

    // Verify that changing the workflow source changes the app version
    await DBOS.shutdown();

    class AnotherWorkflow {
      @DBOS.workflow()
      static async anotherWorkflow() {
        return Promise.resolve(1);
      }
    }

    expect(DBOS.applicationVersion.length).toBe(0);
    await DBOS.launch();
    expect(DBOS.applicationVersion.length).toBeGreaterThan(0);
    expect(DBOS.applicationVersion).not.toEqual(appVersion);
    await expect(AnotherWorkflow.anotherWorkflow()).resolves.toEqual(1);

    // Verify that app version can be set
    await DBOS.shutdown();
    config = generateDBOSTestConfig();
    const test_version = 'test_version';
    config.applicationVersion = test_version;
    DBOS.setConfig(config);
    expect(DBOS.applicationVersion.length).toBe(0);
    await DBOS.launch();
    expect(DBOS.applicationVersion).toBe(test_version);
  });

  test('test-app-version-recovery', async () => {
    class TestAppVersionRecovery {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    // Complete the workflow, then set its status to PENDING
    await DBOS.shutdown();
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(TestAppVersionRecovery).testWorkflow();
    await expect(handle.getResult()).resolves.toEqual(0);
    await DBOSExecutor.globalInstance?.systemDatabase.setWorkflowStatus(handle.workflowID, StatusString.PENDING, true);

    // Shutdown and restart with the same source code, verify it recovers correctly. Set status to PENDING again
    process.env.DBOS__VMID = 'test-app-version-recovery';
    await DBOS.shutdown();
    await DBOS.launch();
    let handles = await recoverPendingWorkflows();
    expect(handles.length).toBe(1);
    expect(handles[0].workflowID).toBe(handle.workflowID);
    await expect(handles[0].getResult()).resolves.toEqual(0);
    await DBOSExecutor.globalInstance?.systemDatabase.setWorkflowStatus(handle.workflowID, StatusString.PENDING, true);

    // Shutdown and restart with different source code. Verify it does not recover.
    await DBOS.shutdown();
    class YetAnotherWorkflow {
      @DBOS.workflow()
      static async anotherWorkflow() {
        return Promise.resolve(1);
      }
    }
    await DBOS.launch();
    handles = await recoverPendingWorkflows();
    expect(handles.length).toBe(0);
    await expect(YetAnotherWorkflow.anotherWorkflow()).resolves.toEqual(1);
  });
});
