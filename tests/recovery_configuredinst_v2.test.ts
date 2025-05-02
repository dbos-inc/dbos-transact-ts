import { configureInstance, ConfiguredInstance, DBOS } from '../src';

import { generateDBOSTestConfig, recoverPendingWorkflows, setUpDBOSTestDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';

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

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  @DBOS.workflow()
  async testRecoveryWorkflow(input: number) {
    this.config.count += input;

    // Signal the workflow has been executed more than once.
    if (this.config.count > input) {
      this.config.resolve2!();
    }

    await this.config.promise1;
    return this.name;
  }
}

const configA = configureInstance(CCRecovery, 'configA', new CCRConfig());
const configB = configureInstance(CCRecovery, 'configB', new CCRConfig());

describe('recovery-cc-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    process.env.DBOS__VMID = '';
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('local-recovery', async () => {
    const handleA = await DBOS.startWorkflow(configA).testRecoveryWorkflow(5);
    const handleB = await DBOS.startWorkflow(configB).testRecoveryWorkflow(5);

    const recoverHandles = await recoverPendingWorkflows();
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
