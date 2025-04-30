import { randomUUID } from 'node:crypto';
import { DBOS, DBOSConfig } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSExecutor } from '../src/dbos-executor';

// Step variant 1: Let DBOS provide the step wrapper making
//  a reusable function that can be called from multiple places
async function stepFunctionGuts() {
  return Promise.resolve('My second step result');
}

const stepFunction = DBOS.registerStep(stepFunctionGuts, {
  name: 'MySecondStep',
});

async function wfFunctionGuts() {
  // Step variant 2: Let DBOS run a code snippet as a step
  //
  // Note that the app can write its own retry loop, based on
  // its own policy, its own understanding of retriable errors,
  // and then just replace `DBOS.runAsWorkflowStep` in the below
  // whith the app's utility. Whether retries are recorded or not
  // would then depend entirely on whether the app puts this loop
  // inside or outside its call to `DBOS.runAsWorkflowStep`.
  const p1 = await DBOS.runAsWorkflowStep(async () => {
    return Promise.resolve('My first step result');
  }, 'MyFirstStep');

  const p2 = await stepFunction();

  return p1 + '|' + p2;
}

// Workflow functions must always be registered before launch; this
//  allows recovery to occur.
const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, {
  name: 'workflow',
});

describe('decoratorless-api-basic-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-functions', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await wfFunction();
      expect(res).toBe('My first step result|My second step result');
    });

    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].function_id).toBe(0);
    expect(wfsteps[0].function_name).toBe('MyFirstStep');
    expect(wfsteps[1].function_id).toBe(1);
    expect(wfsteps[1].function_name).toBe('MySecondStep');
  });
});

// Steps on static and instance methods,
//  without bothering to configure the instance.
class StaticAndInstanceSteps {
  static staticVal = 0;
  instanceVal: number;
  constructor(iv: number) {
    this.instanceVal = iv;
  }

  static callCount = 0;

  static async getStaticVal() {
    ++StaticAndInstanceSteps.callCount;
    return Promise.resolve(StaticAndInstanceSteps.staticVal);
  }

  async getInstanceVal() {
    ++StaticAndInstanceSteps.callCount;
    return Promise.resolve(this.instanceVal);
  }
}

StaticAndInstanceSteps.getStaticVal = DBOS.registerStep(StaticAndInstanceSteps.getStaticVal, { name: 'getStaticVal' });
// eslint-disable-next-line @typescript-eslint/unbound-method
StaticAndInstanceSteps.prototype.getInstanceVal = DBOS.registerStep(StaticAndInstanceSteps.prototype.getInstanceVal, {
  name: 'getInstanceVal',
});

async function classStepsWFFuncGuts() {
  StaticAndInstanceSteps.staticVal = 1;
  const sais = new StaticAndInstanceSteps(2);
  const rv1 = await StaticAndInstanceSteps.getStaticVal();
  const rv2 = await sais.getInstanceVal();
  return `${rv1}-${rv2}`;
}

const classStepsWF = DBOS.registerWorkflow(classStepsWFFuncGuts, { name: 'classStepsWF' });

// runAsStep no config instance
describe('decoratorless-api-class-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-functions', async () => {
    const wfid = randomUUID();
    StaticAndInstanceSteps.callCount = 0;

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await classStepsWF();
      expect(res).toBe('1-2');
    });

    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].function_id).toBe(0);
    expect(wfsteps[0].function_name).toBe('getStaticVal');
    expect(wfsteps[1].function_id).toBe(1);
    expect(wfsteps[1].function_name).toBe('getInstanceVal');

    expect(StaticAndInstanceSteps.callCount).toBe(2);
  });
});

// Do this on:
//   Bare
//   Static
//   instance (wf must be configured)
// Do this with direct invocation, and with startWorkflow (in a new form)

// registerTransaction ()
// runAsTransaction (later)
