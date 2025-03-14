import {
  Step,
  StepContext,
  ConfiguredInstance,
  GetApi,
  HandlerContext,
  InitContext,
  Transaction,
  TransactionContext,
  Workflow,
  WorkflowContext,
  configureInstance,
} from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import { Client, PoolClient } from 'pg';
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from '../src/testing/testing_runtime';
import request from 'supertest';
import { v1 as uuidv1 } from 'uuid';

type TestTransactionContext = TransactionContext<PoolClient>;

class ConfigTracker {
  name: string = '';
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
  static configs: Map<string, ConfigTracker> = new Map();

  tracker: ConfigTracker;
  constructor(
    name: string,
    readonly val: number,
  ) {
    super(name);
    this.tracker = new ConfigTracker(name);
  }

  initialize(_ctx: InitContext): Promise<void> {
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

  @Step()
  testStep(_ctxt: StepContext) {
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(this.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(this.name));
    ++arg.nComm;
    ++arg.nByName;
    return Promise.resolve();
  }

  @Workflow()
  async testBasicWorkflow(ctxt: WorkflowContext, key: string): Promise<void> {
    expect(key).toBe('please');
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(this.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(this.name));
    ++arg.nWF;
    ++arg.nByName;

    // Invoke a transaction and a step
    await ctxt.invoke(this).testStep();
    await ctxt.invoke(this).testTransaction1();
  }

  @Workflow()
  async testChildWorkflow(ctxt: WorkflowContext) {
    const arg = this.tracker;
    expect(DBOSTestConfiguredClass.configs.has(this.name)).toBeTruthy();
    expect(arg).toBe(DBOSTestConfiguredClass.configs.get(this.name));
    ++arg.nWF;
    ++arg.nByName;

    // Invoke a workflow that invokes a transaction and a step
    await ctxt.invokeWorkflow(this).testBasicWorkflow('please');
    const wfh = await ctxt.startWorkflow(this).testBasicWorkflow('please');
    await wfh.getResult();
  }

  async testFunc() {
    return Promise.resolve();
  }

  @Workflow()
  async bogusChildWorkflow(ctxt: WorkflowContext) {
    // Try invokeWorkflow on something you should not invoke.
    //  If your attention has been called to this line because it stopped compiling,
    //    great!  You may have done something good.
    await ctxt.invokeWorkflow(this).testFunc();
  }

  @GetApi('/bad')
  static async testUnconfiguredHandler(_ctx: HandlerContext) {
    // A handler in a configured class doesn't have a configuration / this.
    return Promise.resolve('This is a bad idea');
  }

  @GetApi('/instance') // You can't actually call this...
  async testInstMthd(_ctxt: HandlerContext) {
    return Promise.resolve('foo');
  }
}

const config1 = new DBOSTestConfiguredClass('config1', 1); // The new way
const configA = configureInstance(DBOSTestConfiguredClass, 'configA', 2); // The old way

describe('dbos-configclass-tests', () => {
  let config: DBOSConfig;
  let testRuntime: TestingRuntime;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOSTestConfiguredClass.configs = new Map();
    config1.tracker.reset();
    configA.tracker.reset();

    testRuntime = await createInternalTestRuntime(undefined, config);
    systemDBClient = new Client({
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: config.system_database,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await testRuntime.destroy();
  });

  test('simple-functions', async () => {
    try {
      await testRuntime.invoke(config1).testStep();
    } catch (e) {
      console.log(e);
      throw e;
    }
    await testRuntime.invoke(configA).testStep();
    const wfUUID1 = uuidv1();
    const wfUUID2 = uuidv1();
    await testRuntime.invoke(config1, wfUUID1).testTransaction1();
    await testRuntime.invoke(configA, wfUUID2).testTransaction1();

    expect(config1.tracker.nInit).toBe(1);
    expect(configA.tracker.nInit).toBe(1);
    expect(config1.tracker.nByName).toBe(2);
    expect(configA.tracker.nByName).toBe(2);
    expect(config1.tracker.nComm).toBe(1);
    expect(configA.tracker.nComm).toBe(1);
    expect(config1.tracker.nTrans).toBe(1);
    expect(configA.tracker.nTrans).toBe(1);

    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    // Make sure we correctly record the function's class name and config name
    await dbosExec.flushWorkflowBuffers();
    let result = await systemDBClient.query<{ status: string; name: string; class_name: string; config_name: string }>(
      `SELECT status, name, class_name, config_name FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [wfUUID1],
    );
    expect(result.rows[0].class_name).toBe('DBOSTestConfiguredClass');
    expect(result.rows[0].name).toContain('testTransaction1');
    expect(result.rows[0].config_name).toBe('config1');
    expect(result.rows[0].status).toBe('SUCCESS');

    result = await systemDBClient.query<{ status: string; name: string; class_name: string; config_name: string }>(
      `SELECT status, name, class_name, config_name FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [wfUUID2],
    );
    expect(result.rows[0].class_name).toBe('DBOSTestConfiguredClass');
    expect(result.rows[0].name).toContain('testTransaction1');
    expect(result.rows[0].config_name).toBe('configA');
    expect(result.rows[0].status).toBe('SUCCESS');
  });

  test('simplewf', async () => {
    await testRuntime.invokeWorkflow(config1).testBasicWorkflow('please');
    expect(config1.tracker.nTrans).toBe(1);
    expect(config1.tracker.nComm).toBe(1);
    expect(config1.tracker.nWF).toBe(1);
    expect(config1.tracker.nByName).toBe(3);

    await (await testRuntime.startWorkflow(configA).testBasicWorkflow('please')).getResult();
    expect(configA.tracker.nTrans).toBe(1);
    expect(configA.tracker.nComm).toBe(1);
    expect(configA.tracker.nWF).toBe(1);
    expect(configA.tracker.nByName).toBe(3);
  });

  test('childwf', async () => {
    await testRuntime.invokeWorkflow(config1).testChildWorkflow();
    expect(config1.tracker.nTrans).toBe(2);
    expect(config1.tracker.nComm).toBe(2);
    expect(config1.tracker.nWF).toBe(3);
    expect(config1.tracker.nByName).toBe(7);
  });

  test('badhandler', async () => {
    const response1 = await request(testRuntime.getHandlersCallback()).get('/bad');
    expect(response1.statusCode).toBe(200);

    const response2 = await request(testRuntime.getHandlersCallback()).get('/instance');
    expect(response2.statusCode).toBe(404);
  });

  test('badwfinvoke', async () => {
    let threw = false;
    try {
      await testRuntime.invokeWorkflow(configA).bogusChildWorkflow();
    } catch (e) {
      threw = true;
    }
    expect(threw).toBeTruthy();
  });
});
