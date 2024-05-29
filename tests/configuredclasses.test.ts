import {
  Communicator,
  CommunicatorContext,
  Configurable,
  ConfiguredClassType,
  InitContext,
  Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext,
  initClassConfiguration,
} from "../src";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { DBOSConfig } from "../src/dbos-executor";
import { PoolClient } from "pg";
import { TestingRuntime, createInternalTestRuntime } from "../src/testing/testing_runtime";

type TestTransactionContext = TransactionContext<PoolClient>;

class ConfigTracker {
  name: string = "";
  nInit = 0;
  nByName = 0;
  nWF = 0;
  nComm = 0;
  nTrans = 0;

  constructor(name: string) {
    this.name = name;
  }
  reset() {
    this.nInit = this.nByName = this.nWF = this.nComm = this.nTrans = 0;
  }
}

@Configurable()
class SomeOtherClass
{
  static initConfiguration(_ctx: InitContext, _arg: ConfigTracker) : Promise<void> {
    return Promise.resolve();
  }
}

@Configurable()
class DBOSTestConfiguredClass {
  static configs : Map<string, ConfigTracker> = new Map();

  static initConfiguration(_ctx: InitContext, arg: ConfigTracker) : Promise<void> {
    if (!arg.name || DBOSTestConfiguredClass.configs.has(arg.name)) {
      throw new Error(`Invalid or duplicate config name: ${arg.name}`);
    }
    DBOSTestConfiguredClass.configs.set(arg.name, arg);
    ++arg.nInit;
    return Promise.resolve();
  }

  @Transaction()
  static testTransaction1(txnCtxt: TestTransactionContext) {
    const cc = txnCtxt.getConfiguredClass(DBOSTestConfiguredClass);
    expect(cc).toBeDefined();
    expect(DBOSTestConfiguredClass.configs.has(cc.configName)).toBeTruthy();
    expect(cc.config).toBe(DBOSTestConfiguredClass.configs.get(cc.configName));
    ++cc.config.nTrans;
    ++cc.config.nByName;
    return Promise.resolve();
  }

  @Transaction({readOnly: true})
  static testBadCall(txnCtxt: TestTransactionContext) {
    expect(()=>txnCtxt.getConfiguredClass(SomeOtherClass)).toThrow();
    return Promise.resolve();
  }

  @Communicator()
  static testCommunicator(ctxt: CommunicatorContext) {
    const cc = ctxt.getConfiguredClass(DBOSTestConfiguredClass);
    expect(cc).toBeDefined();
    expect(DBOSTestConfiguredClass.configs.has(cc.configName)).toBeTruthy();
    expect(cc.config).toBe(DBOSTestConfiguredClass.configs.get(cc.configName));
    ++cc.config.nComm;
    ++cc.config.nByName;
    return Promise.resolve();
  }

  @Workflow()
  static async testBasicWorkflow(ctxt: WorkflowContext, key: string) {
    expect(key).toBe("please");
    const cc = ctxt.getConfiguredClass(DBOSTestConfiguredClass);
    expect(cc).toBeDefined();
    expect(DBOSTestConfiguredClass.configs.has(cc.configName)).toBeTruthy();
    expect(cc.config).toBe(DBOSTestConfiguredClass.configs.get(cc.configName));
    ++cc.config.nWF;
    ++cc.config.nByName;

    // Invoke a transaction and a communicator
    await ctxt.invoke(cc).testCommunicator();
    await ctxt.invoke(cc).testTransaction1();
  }

  @Workflow()
  static async testChildWorkflow(ctxt: WorkflowContext) {
    const cc = ctxt.getConfiguredClass(DBOSTestConfiguredClass);
    expect(cc).toBeDefined();
    expect(DBOSTestConfiguredClass.configs.has(cc.configName)).toBeTruthy();
    expect(cc.config).toBe(DBOSTestConfiguredClass.configs.get(cc.configName));
    ++cc.config.nWF;
    ++cc.config.nByName;

    // Invoke a workflow that invokes a transaction and a communicator
    await ctxt.invokeChildWorkflow(cc, DBOSTestConfiguredClass.testBasicWorkflow, "please");
    const wfh = await ctxt.startChildWorkflow(cc, DBOSTestConfiguredClass.testBasicWorkflow, "please");
    await wfh.getResult();
  }

  @Workflow()
  static async testBadWorkflow(ctxt: WorkflowContext) {
    // Invoke a workflow function without its config
    await ctxt.invokeChildWorkflow(DBOSTestConfiguredClass.testBasicWorkflow, "please");
  }
}

const config1: ConfiguredClassType<typeof DBOSTestConfiguredClass> =
  initClassConfiguration(DBOSTestConfiguredClass, "config1", new ConfigTracker("config1"));
const configA = initClassConfiguration(DBOSTestConfiguredClass, "configA", new ConfigTracker("configA"));

describe("dbos-configclass-tests", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOSTestConfiguredClass.configs = new Map();
    config1.config.reset();
    configA.config.reset();

    testRuntime = await createInternalTestRuntime([DBOSTestConfiguredClass], config);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-functions", async () => {
    await testRuntime.invoke(config1).testCommunicator();
    await testRuntime.invoke(configA).testCommunicator();
    await testRuntime.invoke(config1).testTransaction1();
    await testRuntime.invoke(configA).testTransaction1();

    expect(config1.config.nByName).toBe(2);
    expect(configA.config.nByName).toBe(2);
    expect(config1.config.nComm).toBe(1);
    expect(configA.config.nComm).toBe(1);
    expect(config1.config.nTrans).toBe(1);
    expect(configA.config.nTrans).toBe(1);
  });

  test("simplewf", async() => {
    await testRuntime.invokeWorkflow(config1).testBasicWorkflow("please");
    expect(config1.config.nTrans).toBe(1);
    expect(config1.config.nComm).toBe(1);
    expect(config1.config.nWF).toBe(1);
    expect(config1.config.nByName).toBe(3);

    await (await testRuntime.startWorkflow(configA).testBasicWorkflow("please")).getResult();
    expect(configA.config.nTrans).toBe(1);
    expect(configA.config.nComm).toBe(1);
    expect(configA.config.nWF).toBe(1);
    expect(configA.config.nByName).toBe(3);
  });

  test("childwf", async() => {
    await testRuntime.invokeWorkflow(config1).testChildWorkflow();
    expect(config1.config.nTrans).toBe(2);
    expect(config1.config.nComm).toBe(2);
    expect(config1.config.nWF).toBe(3);
    expect(config1.config.nByName).toBe(7);
  });

  test("badcall", async () => {
    await testRuntime.invoke(config1).testBadCall();
  });

  test("badwf", async () => {
    let threw = false;
    try {
      await testRuntime.invokeWorkflow(config1).testBadWorkflow();
    }
    catch (e) {
      threw = true;
    }
    expect(threw).toBeTruthy();
  });
});
