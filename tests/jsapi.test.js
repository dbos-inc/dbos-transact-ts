const { randomUUID } = require('node:crypto');
const { ConfiguredInstance, DBOS } = require('../src');
const { generateDBOSTestConfig, setUpDBOSTestDb } = require('./helpers');
const { DBOSInvalidWorkflowTransitionError } = require('../src/error');

async function stepFunctionGuts() {
  expect(DBOS.isInStep()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(true);
  return Promise.resolve('My second step result');
}

const stepFunction = DBOS.registerStep(stepFunctionGuts, {
  name: 'MySecondStep',
});

async function wfFunctionGuts() {
  const p1 = await DBOS.runStep(async () => Promise.resolve('My first step result'), { name: 'MyFirstStep' });

  const p2 = await stepFunction();

  return p1 + '|' + p2;
}

const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, 'workflow');

describe('decoratorless-api-basic-tests', () => {
  let config;

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

  test('bare-step-wf-functions', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await wfFunction();
      expect(res).toBe('My first step result|My second step result');
    });

    const wfsteps = await DBOS.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('MyFirstStep');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('MySecondStep');
  });
});

class StaticAndInstanceSteps {
  constructor(iv) {
    this.instanceVal = iv;
  }

  static staticVal = 0;
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
StaticAndInstanceSteps.prototype.getInstanceVal = DBOS.registerStep(StaticAndInstanceSteps.prototype.getInstanceVal, {
  name: 'getInstanceVal',
});

async function classStepsWFFuncGuts() {
  const sais = new StaticAndInstanceSteps(2);
  const rv1 = await StaticAndInstanceSteps.getStaticVal();
  const rv2 = await sais.getInstanceVal();
  return `${rv1}-${rv2}`;
}

const classStepsWF = DBOS.registerWorkflow(classStepsWFFuncGuts, 'classStepsWF');

describe('decoratorless-api-class-tests', () => {
  let config;

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

  test('class-step-functions', async () => {
    const wfid = randomUUID();
    StaticAndInstanceSteps.callCount = 0;
    StaticAndInstanceSteps.staticVal = 1;

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await classStepsWF();
      expect(res).toBe('1-2');
    });

    const wfsteps = await DBOS.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('getStaticVal');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('getInstanceVal');
    expect(StaticAndInstanceSteps.callCount).toBe(2);
  });
});

class StaticAndInstanceWFs extends ConfiguredInstance {
  constructor(wiv, siv) {
    super(`staticandinstwfs${wiv}`);
    this.instanceVal = wiv;
    this.steps = new StaticAndInstanceSteps(siv);
  }

  static staticVal = 0;

  static async staticWF() {
    const rv1 = await StaticAndInstanceSteps.getStaticVal();
    const rv2 = await DBOS.runStep(() => Promise.resolve(StaticAndInstanceWFs.staticVal), { name: 'step2' });
    return `${rv1}-${rv2}`;
  }

  async instanceWF() {
    const rv1 = await this.steps.getInstanceVal();
    const rv2 = await DBOS.runStep(() => Promise.resolve(this.instanceVal), { name: 'step2' });
    return `${rv1}-${rv2}`;
  }
}

const wfi34 = new StaticAndInstanceWFs(4, 3);
const wfi56 = new StaticAndInstanceWFs(6, 5);

StaticAndInstanceWFs.staticWF = DBOS.registerWorkflow(StaticAndInstanceWFs.staticWF, 'staticWF', {
  classOrInst: StaticAndInstanceWFs,
  className: 'StaticAndInstanceWFs',
});

StaticAndInstanceWFs.prototype.instanceWF = DBOS.registerWorkflow(
  StaticAndInstanceWFs.prototype.instanceWF,
  'instanceWF',
  {
    classOrInst: StaticAndInstanceWFs,
    className: 'StaticAndInstanceWFs',
  },
);

describe('decoratorless-api-class-tests', () => {
  let config;

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

  test('class-wf-functions', async () => {
    const wfid1 = randomUUID();
    const wfid2 = randomUUID();
    StaticAndInstanceSteps.callCount = 0;
    StaticAndInstanceSteps.staticVal = 1;
    StaticAndInstanceWFs.staticVal = 2;
    const wfi = wfi34;

    await DBOS.withNextWorkflowID(wfid1, async () => {
      const res = await StaticAndInstanceWFs.staticWF();
      expect(res).toBe('1-2');
    });

    await DBOS.withNextWorkflowID(wfid2, async () => {
      const res = await wfi.instanceWF();
      expect(res).toBe('3-4');
    });

    const stat1 = await DBOS.getWorkflowStatus(wfid1);
    expect(stat1.workflowClassName).toBe('StaticAndInstanceWFs');
    expect(stat1.workflowConfigName).toBeFalsy();
    expect(stat1.workflowName).toBe('staticWF');
    expect(stat1.workflowID).toBe(wfid1);

    const wfsteps1 = await DBOS.listWorkflowSteps(wfid1);
    expect(wfsteps1[0].name).toBe('getStaticVal');
    expect(wfsteps1[1].name).toBe('step2');

    const stat2 = await DBOS.getWorkflowStatus(wfid2);
    expect(stat2.workflowClassName).toBe('StaticAndInstanceWFs');
    expect(stat2.workflowConfigName).toBe('staticandinstwfs4');
    expect(stat2.workflowName).toBe('instanceWF');
    expect(stat2.workflowID).toBe(wfid2);
  });
});

async function argsWFFuncGuts(a, b) {
  return `${a}-${b}`;
}

const argsWF = DBOS.registerWorkflow(argsWFFuncGuts, 'argsWF');

async function stepFuncBare() {
  expect(DBOS.isWithinWorkflow()).toBe(false);
  return Promise.resolve('BareStep');
}

const stepFunctionBare = DBOS.registerStep(stepFuncBare, {
  name: 'MyNonWFStep',
});

describe('start-workflow-function', () => {
  let config;

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

  test('class-wf-functions', async () => {
    const wfid1 = randomUUID();
    const wfid2 = randomUUID();
    StaticAndInstanceSteps.staticVal = 1;
    StaticAndInstanceWFs.staticVal = 2;
    const wfi = wfi56;

    const wfh1 = await DBOS.startWorkflow(StaticAndInstanceWFs, { workflowID: wfid1 }).staticWF();
    await expect(wfh1.getResult()).resolves.toBe('1-2');

    const wfh2 = await DBOS.startWorkflow(wfi, { workflowID: wfid2 }).instanceWF();
    await expect(wfh2.getResult()).resolves.toBe('5-6');
  });

  test('bare-wf-functions', async () => {
    const wfid1 = randomUUID();
    const wfid2 = randomUUID();
    const wfid3 = randomUUID();
    const wfi = wfi56;

    const wfh1 = await DBOS.startWorkflow(StaticAndInstanceWFs.staticWF, { workflowID: wfid1 })();
    await expect(wfh1.getResult()).resolves.toBe('1-2');

    const wfh2 = await DBOS.startWorkflow(wfi, { workflowID: wfid2 }).instanceWF();
    await expect(wfh2.getResult()).resolves.toBe('5-6');

    const wfh3 = await DBOS.startWorkflow(argsWF, { workflowID: wfid3 })(7, 'f');
    await expect(wfh3.getResult()).resolves.toBe('7-f');
  });

  test('step-outside-wf', async () => {
    const nwsBefore = (await DBOS.listWorkflows({})).length;
    expect(nwsBefore).toBeGreaterThanOrEqual(1);

    const r1 = await stepFunctionBare();
    expect(r1).toBe('BareStep');

    const i1 = await DBOS.runStep(() => Promise.resolve('inline'), {
      name: 'MyFirstStep',
    });
    expect(i1).toBe('inline');

    const nwsAfter = (await DBOS.listWorkflows({})).length;
    expect(nwsAfter - nwsBefore).toBe(0);

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await expect(stepFunctionBare()).rejects.toThrow(DBOSInvalidWorkflowTransitionError);
    });
  });

  it('should generate uuid', async () => {
    const uuid = await DBOS.randomUUID();
    expect(uuid.length).toBeGreaterThan(16);
    expect(uuid[0]).not.toBe('y');
  });

  it('Should get dates', async () => {
    const st = Date.now();
    const t1 = await DBOS.now();
    const et = Date.now();

    expect(t1).toBeGreaterThanOrEqual(st);
    expect(t1).toBeLessThanOrEqual(et);
  });
});
