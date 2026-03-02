import { randomUUID } from 'crypto';
import { DBOS, DBOSClient, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, recoverPendingWorkflows, setUpDBOSTestSysDb } from './helpers';

describe('test-app-version', () => {
  let config: DBOSConfig;

  beforeEach(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
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

  test('test-setting-app-version', async () => {
    class TestSettingAppVersion {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    // Reset DBOS with a set version and executor ID
    const testVersion = randomUUID();
    const testExecutorID = randomUUID();
    config.applicationVersion = testVersion;
    config.executorID = testExecutorID;
    await DBOS.shutdown();
    DBOS.setConfig(config);
    await DBOS.launch();

    // Run a workflow
    const handle = await DBOS.startWorkflow(TestSettingAppVersion).testWorkflow();
    await expect(handle.getResult()).resolves.toEqual(0);
    const status = await handle.getStatus();

    // Verify those values are set on workflows
    expect(status?.applicationVersion).toBe(testVersion);
    expect(status?.executorId).toBe(testExecutorID);
  });

  test('test-version-registration-on-launch', async () => {
    class _TestVersionReg {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    // Launch with a specific version
    const v1 = 'version-reg-v1';
    config.applicationVersion = v1;
    await DBOS.shutdown();
    DBOS.setConfig(config);
    await DBOS.launch();

    // The version should be registered and be the latest
    const latest = await DBOS.getLatestApplicationVersion();
    expect(latest.versionName).toBe(v1);

    const versions = await DBOS.listApplicationVersions();
    expect(versions.length).toBeGreaterThanOrEqual(1);
    expect(versions[0].versionName).toBe(v1);
    expect(versions[0].versionId).toBeDefined();
    expect(versions[0].versionTimestamp).toBeGreaterThan(0);
    expect(versions[0].createdAt).toBeGreaterThan(0);
  });

  test('test-list-and-set-latest-version', async () => {
    class _TestListVersions {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    // Launch with version v1
    const v1 = 'list-versions-v1';
    config.applicationVersion = v1;
    await DBOS.shutdown();
    DBOS.setConfig(config);
    await DBOS.launch();

    // Shutdown and launch with version v2
    const v2 = 'list-versions-v2';
    await DBOS.shutdown();
    config = generateDBOSTestConfig();
    config.applicationVersion = v2;
    DBOS.setConfig(config);
    await DBOS.launch();

    // v2 should be the latest (launched most recently)
    // Both should appear in the list
    const versions = await DBOS.listApplicationVersions();
    const versionNames = versions.map((v) => v.versionName);
    expect(versionNames).toContain(v1);
    expect(versionNames).toContain(v2);
    expect(versions[0].versionName).toBe(v2);

    const latest = await DBOS.getLatestApplicationVersion();
    expect(latest.versionName).toBe(v2);

    // Set v1 back as latest
    await DBOS.setLatestApplicationVersion(v1);
    const latestAgain = await DBOS.getLatestApplicationVersion();
    expect(latestAgain.versionName).toBe(v1);
  });

  test('test-idempotent-version-creation', async () => {
    class _TestIdempotent {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    const v1 = 'idempotent-v1';
    config.applicationVersion = v1;
    await DBOS.shutdown();
    DBOS.setConfig(config);
    await DBOS.launch();

    // Creating the same version again should be a no-op (relaunch with same version)
    const versionsBefore = await DBOS.listApplicationVersions();
    const v1Count = versionsBefore.filter((v) => v.versionName === v1).length;
    expect(v1Count).toBe(1);

    await DBOS.shutdown();
    config = generateDBOSTestConfig();
    config.applicationVersion = v1;
    DBOS.setConfig(config);
    await DBOS.launch();

    const versionsAfter = await DBOS.listApplicationVersions();
    const v1CountAfter = versionsAfter.filter((v) => v.versionName === v1).length;
    expect(v1CountAfter).toBe(1);
  });

  test('test-client-version-api', async () => {
    class _TestClientVersions {
      @DBOS.workflow()
      static async testWorkflow() {
        return Promise.resolve(0);
      }
    }

    // Launch with a version
    const v1 = 'client-version-v1';
    config.applicationVersion = v1;
    await DBOS.shutdown();
    DBOS.setConfig(config);
    await DBOS.launch();

    // Create a client and test version API
    const client = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
    });
    try {
      const versions = await client.listApplicationVersions();
      expect(versions.length).toBeGreaterThanOrEqual(1);
      expect(versions.map((v) => v.versionName)).toContain(v1);

      const latest = await client.getLatestApplicationVersion();
      expect(latest.versionName).toBe(v1);

      // Shutdown and launch with v2, then verify via client
      const v2 = 'client-version-v2';
      await DBOS.shutdown();
      config = generateDBOSTestConfig();
      config.applicationVersion = v2;
      DBOS.setConfig(config);
      await DBOS.launch();

      const latestAfter = await client.getLatestApplicationVersion();
      expect(latestAfter.versionName).toBe(v2);

      // Set v1 back as latest via client
      await client.setLatestApplicationVersion(v1);
      const latestReverted = await client.getLatestApplicationVersion();
      expect(latestReverted.versionName).toBe(v1);
    } finally {
      await client.destroy();
    }
  });
});
