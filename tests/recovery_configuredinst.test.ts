import { configureInstance, ConfiguredInstance, InitContext, Workflow, WorkflowContext, TestingRuntime } from '../src/';

import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import { TestingRuntimeImpl, createInternalTestRuntime } from '../src/testing/testing_runtime';

type RF = () => void;
class CCRConfig {
  resolve1: RF | undefined = undefined;
  promise1: Promise<void>;
  resolve2: RF | undefined = undefined;
  promise2: Promise<void>;

  constructor() {
    this.promise1 = new Promise<void>((resolve) => {
      this.resolve1 = resolve;
    });
    this.promise2 = new Promise<void>((resolve) => {
      this.resolve2 = resolve;
    });
  }

  count: number = 0;
}
/**
 * Test for the default local workflow recovery for configured classes.
 */
class CCRecovery extends ConfiguredInstance {
  constructor(
    name: string,
    readonly config: CCRConfig,
  ) {
    super(name);
  }

  initialize(_ctx: InitContext): Promise<void> {
    return Promise.resolve();
  }

  @Workflow()
  async testRecoveryWorkflow(ctxt: WorkflowContext, input: number) {
    this.config.count += input;

    // Signal the workflow has been executed more than once.
    if (this.config.count > input) {
      this.config.resolve2!();
    }

    await this.config.promise1;
    return this.name;
  }
}

const configA = configureInstance(CCRecovery, 'configA', new CCRConfig()); // Old way
const configB = new CCRecovery('configB', new CCRConfig()); // New way

describe('recovery-cc-tests', () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime(undefined, config);
    process.env.DBOS__VMID = '';
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test('local-recovery', async () => {
    // Run a workflow pair until pending and start recovery.
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    const handleA = await testRuntime.startWorkflow(configA).testRecoveryWorkflow(5);
    const handleB = await testRuntime.startWorkflow(configB).testRecoveryWorkflow(5);

    const recoverHandles = await dbosExec.recoverPendingWorkflows();
    await configA.config.promise2; // Wait for the recovery to be done.
    await configB.config.promise2; // Wait for the recovery to be done.
    configA.config.resolve1!(); // Both A can finish now.
    configB.config.resolve1!(); // Both B can finish now.

    expect(recoverHandles.length).toBe(2);
    await expect(recoverHandles[0].getResult()).resolves.toBeTruthy();
    await expect(recoverHandles[1].getResult()).resolves.toBeTruthy();
    await expect(handleA.getResult()).resolves.toBe('configA');
    await expect(handleB.getResult()).resolves.toBe('configB');
    expect(configA.config.count).toBe(10); // Should run twice.
    expect(configB.config.count).toBe(10); // Should run twice.
  });
});
