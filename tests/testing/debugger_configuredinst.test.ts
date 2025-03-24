import { ConfiguredInstance, DBOS, InitContext } from '../../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import { v1 as uuidv1 } from 'uuid';
import { DBOSConfig, DebugMode } from '../../src/dbos-executor';

class DebuggerCCTest extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }
  initialize(_ctx: InitContext): Promise<void> {
    expect(this.name).toBe('configA');
    return Promise.resolve();
  }

  @DBOS.transaction({ readOnly: true })
  async testReadOnlyFunction(number: number) {
    expect(this.name).toBe('configA');
    return Promise.resolve(number);
  }

  @DBOS.workflow()
  async testWorkflow(name: string) {
    expect(this.name).toBe('configA');
    const funcResult = await this.testReadOnlyFunction(5);
    return `${name}${funcResult}`;
  }

  @DBOS.step()
  async testStep(inp: string) {
    expect(this.name).toBe('configA');
    return Promise.resolve(inp);
  }

  // Workflow that sleep, call comm, call tx, call child WF
  @DBOS.workflow()
  async mixedWorkflow(num: number) {
    expect(this.name).toBe('configA');
    await DBOS.sleepSeconds(1);
    const txResult = await this.testReadOnlyFunction(num);
    const cResult = await this.testStep('comm');
    const wfResult = await this.testWorkflow('cwf');
    return `${this.name}${txResult}${cResult}${wfResult}-${num}`;
  }
}

const configR = new DebuggerCCTest('configA');

describe('debugger-test', () => {
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let debugProxyConfig: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    debugConfig = generateDBOSTestConfig(undefined);
    debugProxyConfig = generateDBOSTestConfig(undefined);
    await setUpDBOSTestDb(config);
  });

  test('debug-workflow', async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await configR.mixedWorkflow(23);
      expect(res).toBe('configA23commcwf5-23');
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: DebugMode.ENABLED });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await configR.mixedWorkflow(23);
      expect(res).toBe('configA23commcwf5-23');
    });

    // Execute again with the provided UUID.
    await expect(DBOS.executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBe('configA23commcwf5-23');
    await DBOS.shutdown();

    // TT Mode
    DBOS.setConfig(debugProxyConfig);
    await DBOS.launch({ debugMode: DebugMode.TIME_TRAVEL });
    await expect(DBOS.executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBe('configA23commcwf5-23');
    await DBOS.shutdown();
  });
});
