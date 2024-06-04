import {
  Communicator,
  CommunicatorContext,
  ConfiguredInstance,
  GetApi,
  HandlerContext,
  InitContext,
  Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext,
  configureInstance,
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

class DBOSTestConfiguredClass extends ConfiguredInstance {
  static configs : Map<string, ConfigTracker> = new Map();

  tracker: ConfigTracker;
  constructor(name: string, readonly val:number) {
    super(name);
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
    expect(DBOSTestConfiguredClass.configs.has(this.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(this.name));
    ++arg.nTrans;
    ++arg.nByName;
    return Promise.resolve();
  }

  @Communicator()
  testCommunicator(_ctxt: CommunicatorContext) {
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(this.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(this.name));
    ++arg.nComm;
    ++arg.nByName;
    return Promise.resolve();
  }

  @Workflow()
  async testBasicWorkflow(ctxt: WorkflowContext, key: string): Promise<void> {
    expect(key).toBe("please");
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(this.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(this.name));
    ++arg.nWF;
    ++arg.nByName;

    // Invoke a transaction and a communicator
    await ctxt.invoke(this).testCommunicator();
    await ctxt.invoke(this).testTransaction1();
  }

  @Workflow()
  async testChildWorkflow(ctxt: WorkflowContext) {
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(this.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(this.name));
    ++arg.nWF;
    ++arg.nByName;

    // Invoke a workflow that invokes a transaction and a communicator
    await ctxt.invokeWorkflow(this as DBOSTestConfiguredClass).testBasicWorkflow('please');
    const wfh = await ctxt.startWorkflow(this as DBOSTestConfiguredClass).testBasicWorkflow('please');
    await wfh.getResult();
  }

  @GetApi('/bad')
  static async testUnconfiguredHandler(_ctx: HandlerContext) {
    // A handler in a configured class doesn't have a configuration / this.
    return Promise.resolve("This is a bad idea");
  }

  @GetApi('/instance') // You can't actually call this...
  async testInstMthd(_ctxt: HandlerContext) {
    return Promise.resolve('foo');
  }
}

const config1 = configureInstance(DBOSTestConfiguredClass, "config1", 1);
const configA = configureInstance(DBOSTestConfiguredClass, "configA", 2);

describe("dbos-configclass-tests", () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOSTestConfiguredClass.configs = new Map();
    config1.tracker.reset();
    configA.tracker.reset();

    testRuntime = await createInternalTestRuntime([DBOSTestConfiguredClass], config);
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-functions", async () => {
    try {
    await testRuntime.invoke(config1).testCommunicator();
    }
    catch (e) {
      console.log(e);
      throw e;
    }
    await testRuntime.invoke(configA).testCommunicator();
    await testRuntime.invoke(config1).testTransaction1();
    await testRuntime.invoke(configA).testTransaction1();

    expect(config1.tracker.nInit).toBe(1);
    expect(configA.tracker.nInit).toBe(1);
    expect(config1.tracker.nByName).toBe(2);
    expect(configA.tracker.nByName).toBe(2);
    expect(config1.tracker.nComm).toBe(1);
    expect(configA.tracker.nComm).toBe(1);
    expect(config1.tracker.nTrans).toBe(1);
    expect(configA.tracker.nTrans).toBe(1);
  });

  test("simplewf", async() => {
    await testRuntime.invokeWorkflow(config1).testBasicWorkflow("please");
    expect(config1.tracker.nTrans).toBe(1);
    expect(config1.tracker.nComm).toBe(1);
    expect(config1.tracker.nWF).toBe(1);
    expect(config1.tracker.nByName).toBe(3);

    await (await testRuntime.startWorkflow(configA).testBasicWorkflow("please")).getResult();
    expect(configA.tracker.nTrans).toBe(1);
    expect(configA.tracker.nComm).toBe(1);
    expect(configA.tracker.nWF).toBe(1);
    expect(configA.tracker.nByName).toBe(3);
  });

  test("childwf", async() => {
    await testRuntime.invokeWorkflow(config1).testChildWorkflow();
    expect(config1.tracker.nTrans).toBe(2);
    expect(config1.tracker.nComm).toBe(2);
    expect(config1.tracker.nWF).toBe(3);
    expect(config1.tracker.nByName).toBe(7);
  });

  test("badhandler", async() => {
    const response1 = await request(testRuntime.getHandlersCallback()).get("/bad");
    expect(response1.statusCode).toBe(200);

    const response2 = await request(testRuntime.getHandlersCallback()).get("/instance");
    expect(response2.statusCode).toBe(404);
  });
});
