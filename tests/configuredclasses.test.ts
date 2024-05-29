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
  WorkflowHandle,
  initClassConfiguration,
} from "../src";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { DBOSConfig } from "../src/dbos-executor";
import { PoolClient } from "pg";
import { TestingRuntime, createInternalTestRuntime } from "../src/testing/testing_runtime";

type TestTransactionContext = TransactionContext<PoolClient>;

describe("dbos-configclass-tests", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;
  let config1: ConfiguredClassType<typeof DBOSTestConfiguredClass> | undefined = undefined;
  let configA: ConfiguredClassType<typeof DBOSTestConfiguredClass> | undefined = undefined;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOSTestConfiguredClass.configs = new Map();
    config1 = initClassConfiguration(DBOSTestConfiguredClass, "config1", new ConfigTracker("config1"));
    configA = initClassConfiguration(DBOSTestConfiguredClass, "configA", new ConfigTracker("configA"));
    testRuntime = await createInternalTestRuntime([DBOSTestConfiguredClass], config);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-function", async () => {
    await testRuntime.invoke(config1!).testCommunicator();
    await testRuntime.invoke(configA!).testCommunicator();
    await testRuntime.invoke(config1!).testTransaction1();
    await testRuntime.invoke(configA!).testTransaction1();

    expect(config1!.config.nByName).toBe(2);
    expect(configA!.config.nByName).toBe(2);
    expect(config1!.config.nComm).toBe(1);
    expect(configA!.config.nComm).toBe(1);
    expect(config1!.config.nTrans).toBe(1);
    expect(configA!.config.nTrans).toBe(1);
  });
});

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
  static async testBasicWorkflow(ctxt: WorkflowContext) {
    const cc = ctxt.getConfiguredClass(DBOSTestConfiguredClass);
    expect(cc).toBeDefined();
    expect(DBOSTestConfiguredClass.configs.has(cc.configName)).toBeTruthy();
    expect(cc.config).toBe(DBOSTestConfiguredClass.configs.get(cc.configName));
    ++cc.config.nComm;
    ++cc.config.nByName;

    // Invoke a transaction and a communicator
    await ctxt.invoke(cc).testCommunicator();
    await ctxt.invoke(cc).testTransaction1();
  }
}
