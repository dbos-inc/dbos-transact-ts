import {
  DBOS,
  Step,
  StepContext,
  ConfiguredInstance,
  GetApi,
  HandlerContext,
  InitContext,
  Workflow,
  WorkflowContext,
  configureInstance,
} from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSConfig } from '../src/dbos-executor';
import request from 'supertest';
import { randomUUID } from 'node:crypto';

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

  override initialize(_ctx: InitContext): Promise<void> {
    const arg = this.tracker;
    if (!arg.name || DBOSTestConfiguredClass.configs.has(arg.name)) {
      throw new Error(`Invalid or duplicate config name: ${arg.name}`);
    }
    DBOSTestConfiguredClass.configs.set(arg.name, arg);
    ++arg.nInit;
    return Promise.resolve();
  }

  @DBOS.transaction()
  testTransaction1() {
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

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    DBOSTestConfiguredClass.configs = new Map();
    config1.tracker.reset();
    configA.tracker.reset();

    await DBOS.launch();
    DBOS.setUpHandlerCallback();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-functions', async () => {
    try {
      await DBOS.invoke(config1).testStep();
    } catch (e) {
      console.log(e);
      throw e;
    }
    await DBOS.invoke(configA).testStep();
    const wfUUID1 = randomUUID();
    const wfUUID2 = randomUUID();
    await DBOS.withNextWorkflowID(wfUUID1, async () => await config1.testTransaction1());
    await DBOS.withNextWorkflowID(wfUUID2, async () => await configA.testTransaction1());

    expect(config1.tracker.nInit).toBe(1);
    expect(configA.tracker.nInit).toBe(1);
    expect(config1.tracker.nByName).toBe(2);
    expect(configA.tracker.nByName).toBe(2);
    expect(config1.tracker.nComm).toBe(1);
    expect(configA.tracker.nComm).toBe(1);
    expect(config1.tracker.nTrans).toBe(1);
    expect(configA.tracker.nTrans).toBe(1);

    // Make sure we correctly record the function's class name and config name
    let stat = await DBOS.getWorkflowStatus(wfUUID1);
    expect(stat?.workflowClassName).toBe('DBOSTestConfiguredClass');
    expect(stat?.workflowName).toContain('testTransaction1');
    expect(stat?.workflowConfigName).toBe('config1');
    expect(stat?.status).toBe('SUCCESS');

    stat = await DBOS.getWorkflowStatus(wfUUID2);
    expect(stat?.workflowClassName).toBe('DBOSTestConfiguredClass');
    expect(stat?.workflowName).toContain('testTransaction1');
    expect(stat?.workflowConfigName).toBe('configA');
    expect(stat?.status).toBe('SUCCESS');
  });

  test('simplewf', async () => {
    await DBOS.invoke(config1).testBasicWorkflow('please');
    expect(config1.tracker.nTrans).toBe(1);
    expect(config1.tracker.nComm).toBe(1);
    expect(config1.tracker.nWF).toBe(1);
    expect(config1.tracker.nByName).toBe(3);

    await DBOS.invoke(configA).testBasicWorkflow('please');
    expect(configA.tracker.nTrans).toBe(1);
    expect(configA.tracker.nComm).toBe(1);
    expect(configA.tracker.nWF).toBe(1);
    expect(configA.tracker.nByName).toBe(3);
  });

  test('childwf', async () => {
    await DBOS.invoke(config1).testChildWorkflow();
    expect(config1.tracker.nTrans).toBe(2);
    expect(config1.tracker.nComm).toBe(2);
    expect(config1.tracker.nWF).toBe(3);
    expect(config1.tracker.nByName).toBe(7);
  });

  test('badhandler', async () => {
    const response1 = await request(DBOS.getHTTPHandlersCallback()!).get('/bad');
    expect(response1.statusCode).toBe(200);

    const response2 = await request(DBOS.getHTTPHandlersCallback()!).get('/instance');
    expect(response2.statusCode).toBe(404);
  });

  test('badwfinvoke', async () => {
    let threw = false;
    try {
      await DBOS.invoke(configA).bogusChildWorkflow();
    } catch (e) {
      threw = true;
    }
    expect(threw).toBeTruthy();
  });
});
