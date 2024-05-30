import {
  Communicator,
  CommunicatorContext,
  Configurable,
  initClassConfiguration,
  InitContext,
  Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext,
} from "../../src/";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { v1 as uuidv1 } from "uuid";
import { DBOSConfig } from "../../src/dbos-executor";
import { PoolClient } from "pg";
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from "../../src/testing/testing_runtime";

type TestTransactionContext = TransactionContext<PoolClient>;

@Configurable()
class DebuggerCCTest {
  static initConfiguration(_ctx: InitContext, _arg: {name: string}) : Promise<void> {
    return Promise.resolve();
  }

  @Transaction({readOnly: true})
  static async testReadOnlyFunction(txnCtxt: TestTransactionContext, number: number) {
    txnCtxt.getConfiguredClass(DebuggerCCTest);
    return Promise.resolve(number);
  }

  @Workflow()
  static async testWorkflow(ctxt: WorkflowContext, name: string) {
    const cc = ctxt.getConfiguredClass(DebuggerCCTest);
    const funcResult = await ctxt.invoke(cc).testReadOnlyFunction(5);
    return `${name}${funcResult}`;
  }

  @Communicator()
  static async testCommunicator(ctxt: CommunicatorContext, inp: string) {
    ctxt.getConfiguredClass(DebuggerCCTest);
    return Promise.resolve(inp);
  }

  // Workflow that sleep, call comm, call tx, call child WF
  @Workflow()
  static async mixedWorkflow(ctxt: WorkflowContext, num: number) {
    const cc = ctxt.getConfiguredClass(DebuggerCCTest);
    await ctxt.sleep(1);
    const txResult = await ctxt.invoke(cc).testReadOnlyFunction(num);
    const cResult = await ctxt.invoke(cc).testCommunicator("comm");
    const wfResult = await ctxt.invokeChildWorkflow(cc, DebuggerCCTest.testWorkflow, 'cwf');
    return `${cc.configName}${txResult}${cResult}${wfResult}-${num}`;
  }
}

const configR = initClassConfiguration(DebuggerCCTest, "configA", {name:"recover"});

describe("debugger-test", () => {
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let debugProxyConfig: DBOSConfig;
  let testRuntime: TestingRuntime;
  let debugRuntime: TestingRuntime;
  let debugProxyRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    debugConfig = generateDBOSTestConfig(undefined, true);
    debugProxyConfig = generateDBOSTestConfig(undefined, true, "localhost:5432");
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    debugRuntime = await createInternalTestRuntime([DebuggerCCTest], debugConfig);
    testRuntime = await createInternalTestRuntime([DebuggerCCTest], config);
    debugProxyRuntime = await createInternalTestRuntime([DebuggerCCTest], debugProxyConfig);     // TODO: connect to the real proxy.
  });

  afterEach(async () => {
    await debugRuntime.destroy();
    await testRuntime.destroy();
    await debugProxyRuntime.destroy();
  });

  test("debug-workflow", async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const res = await testRuntime
      .invokeWorkflow(configR, wfUUID)
      .mixedWorkflow(23);
    expect(res).toBe('configA23commcwf5-23');
    await testRuntime.destroy();

    // Execute again in debug mode.
    const debugRes = await debugRuntime
      .invokeWorkflow(configR, wfUUID)
      .mixedWorkflow(23);
    expect(debugRes).toBe('configA23commcwf5-23');

    // Execute again with the provided UUID.
    await expect((debugRuntime as TestingRuntimeImpl).getDBOSExec().executeWorkflowUUID(wfUUID).then((x) => x.getResult())).resolves.toBe('configA23commcwf5-23');

    await expect((debugProxyRuntime as TestingRuntimeImpl).getDBOSExec().executeWorkflowUUID(wfUUID).then((x) => x.getResult())).resolves.toBe('configA23commcwf5-23');
  });
});
