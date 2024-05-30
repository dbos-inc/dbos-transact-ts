import {
  Communicator,
  CommunicatorContext,
  ConfiguredClassType,
  GetApi,
  HandlerContext,
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
import request from "supertest";

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

class DBOSTestConfiguredClass {
  static configs : Map<string, ConfigTracker> = new Map();

  tracker: ConfigTracker;
  constructor(name: string) {
    this.tracker = new ConfigTracker(name);
  }

  initialize(_ctx: InitContext) : Promise<void> {
    const arg = this.tracker;
    if (!arg.name || DBOSTestConfiguredClass.configs.has(arg.name)) {
      throw new Error(`Invalid or duplicate config name: ${arg.name}`);
    }
    DBOSTestConfiguredClass.configs.set(arg.name, arg);
    ++arg.nInit;
    return Promise.resolve();
  }

  @Transaction()
  testTransaction1(_txnCtxt: TestTransactionContext) {
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(arg.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(arg.name));
    ++arg.nTrans;
    ++arg.nByName;
    return Promise.resolve();
  }

  @Communicator()
  testCommunicator(ctxt: CommunicatorContext) {
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(arg.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(arg.name));
    ++arg.nComm;
    ++arg.nByName;
    return Promise.resolve();
  }

  @Workflow()
  async testBasicWorkflow(ctxt: WorkflowContext, key: string) {
    expect(key).toBe("please");
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(arg.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(arg.name));
    ++arg.nWF;
    ++arg.nByName;

    // Invoke a transaction and a communicator
    await ctxt.invoke(this).testCommunicator();
    await ctxt.invoke(this).testTransaction1();
  }

  @Workflow()
  async testChildWorkflow(ctxt: WorkflowContext) {
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(arg.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(arg.name));
    ++arg.nWF;
    ++arg.nByName;

    // Invoke a workflow that invokes a transaction and a communicator
//    await ctxt.invokeChildWorkflow(this, testBasicWorkflow, "please");
//    const wfh = await ctxt.startChildWorkflow(this, testBasicWorkflow, "please");
//    await wfh.getResult();
  }

  @Workflow()
  async testBadWorkflow(ctxt: WorkflowContext) {
    // Invoke a workflow function without its config
//    await ctxt.invokeChildWorkflow(DBOSTestConfiguredClass.testBasicWorkflow, "please");
  }

  @GetApi('/bad')
  static async testUnconfiguredHandler(_ctx: HandlerContext) {
    // A handler in a configured class doesn't have a configuration.
    //  The compiler won't let you ask for one.
    return Promise.resolve("This is a bad idea");
  }

  @GetApi('/instance') // You can't actually call this...
  async testInstMthd(_ctxt: HandlerContext) {
    return Promise.resolve('foo');
  }
}

/*
const config1: ConfiguredClassType<typeof DBOSTestConfiguredClass> =
  initClassConfiguration(DBOSTestConfiguredClass, "config1", new ConfigTracker("config1"));
const configA = initClassConfiguration(DBOSTestConfiguredClass, "configA", new ConfigTracker("configA"));
*/

describe("dbos-configclass-tests", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
//    DBOSTestConfiguredClass.configs = new Map();
//    config1.config.reset();
//    configA.config.reset();

    testRuntime = await createInternalTestRuntime([DBOSTestConfiguredClass], config);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-functions", async () => {
/*
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
*/
});

/*
  test("childwf", async() => {
    await testRuntime.invokeWorkflow(config1).testChildWorkflow();
    expect(config1.config.nTrans).toBe(2);
    expect(config1.config.nComm).toBe(2);
    expect(config1.config.nWF).toBe(3);
    expect(config1.config.nByName).toBe(7);
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

  test("badhandler", async() => {
    let threw = false;
    try {
      const response = await request(testRuntime.getHandlersCallback()).get("/bad");
      expect(response.statusCode).toBe(200);
    }
    catch (e) {
      threw = true;
    }
    expect(threw).toBeFalsy();

    try {
      const response = await request(testRuntime.getHandlersCallback()).get("/instance");
      expect(response.statusCode).toBe(500);
    }
    catch (e) {
      threw = true;
    }
    expect(threw).toBeFalsy();
  });
*/
});
