import {
  Step,
  StepContext,
  ConfiguredInstance,
  InitContext,
  Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext,
} from '../../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import { v1 as uuidv1 } from 'uuid';
import { DBOSConfig, DebugMode } from '../../src/dbos-executor';
import { PoolClient } from 'pg';
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from '../../src/testing/testing_runtime';

type TestTransactionContext = TransactionContext<PoolClient>;

class DebuggerCCTest extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }
  initialize(_ctx: InitContext): Promise<void> {
    expect(this.name).toBe('configA');
    return Promise.resolve();
  }

  @Transaction({ readOnly: true })
  async testReadOnlyFunction(_txnCtxt: TestTransactionContext, number: number) {
    expect(this.name).toBe('configA');
    return Promise.resolve(number);
  }

  @Workflow()
  async testWorkflow(ctxt: WorkflowContext, name: string) {
    expect(this.name).toBe('configA');
    const funcResult = await ctxt.invoke(this).testReadOnlyFunction(5);
    return `${name}${funcResult}`;
  }

  @Step()
  async testStep(_ctxt: StepContext, inp: string) {
    expect(this.name).toBe('configA');
    return Promise.resolve(inp);
  }

  // Workflow that sleep, call comm, call tx, call child WF
  @Workflow()
  async mixedWorkflow(ctxt: WorkflowContext, num: number) {
    expect(this.name).toBe('configA');
    await ctxt.sleep(1);
    const txResult = await ctxt.invoke(this).testReadOnlyFunction(num);
    const cResult = await ctxt.invoke(this).testStep('comm');
    const wfResult = await ctxt.invokeWorkflow(this).testWorkflow('cwf');
    return `${this.name}${txResult}${cResult}${wfResult}-${num}`;
  }
}

const configR = new DebuggerCCTest('configA');

describe('debugger-test', () => {
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let debugProxyConfig: DBOSConfig;
  let testRuntime: TestingRuntime;
  let debugRuntime: TestingRuntime;
  let debugProxyRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    debugConfig = generateDBOSTestConfig(undefined);
    debugProxyConfig = generateDBOSTestConfig(undefined);
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime(undefined, config);
    debugRuntime = await createInternalTestRuntime(undefined, debugConfig, { debugMode: DebugMode.ENABLED });
    debugProxyRuntime = await createInternalTestRuntime(undefined, debugProxyConfig, {
      debugMode: DebugMode.TIME_TRAVEL,
    }); // TODO: connect to the real proxy.
  });

  afterEach(async () => {
    await debugRuntime.destroy();
    await testRuntime.destroy();
    await debugProxyRuntime.destroy();
  });

  test('debug-workflow', async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const res = await testRuntime.invokeWorkflow(configR, wfUUID).mixedWorkflow(23);
    expect(res).toBe('configA23commcwf5-23');
    await testRuntime.destroy();

    // Execute again in debug mode.
    const debugRes = await debugRuntime.invokeWorkflow(configR, wfUUID).mixedWorkflow(23);
    expect(debugRes).toBe('configA23commcwf5-23');

    // Execute again with the provided UUID.
    await expect(
      (debugRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe('configA23commcwf5-23');

    await expect(
      (debugProxyRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe('configA23commcwf5-23');
  });
});
