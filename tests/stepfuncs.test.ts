import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { ConfiguredInstance, DBOS, DBOSConfig, StepConfig, StepReservation } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { DBOSStepTimeoutError, DBOSWorkflowCancelledError } from '../src/error';
import { Event, generateDBOSTestConfig, reexecuteWorkflowById, retryUntilSuccess, setUpDBOSTestSysDb } from './helpers';

// Step variant 1: Let DBOS provide the step wrapper making
//  a reusable function that can be called from multiple places
async function stepFunctionGuts() {
  expect(DBOS.isInStep()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(true);
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
  // and then just replace `DBOS.runStep` in the below
  // with the app's utility. Whether retries are recorded or not
  // would then depend entirely on whether the app puts this loop
  // inside or outside its call to `DBOS.runStep`.
  const p1 = await DBOS.runStep(
    async () => {
      return Promise.resolve('My first step result');
    },
    { name: 'MyFirstStep' },
  );

  const p2 = await stepFunction();

  return p1 + '|' + p2;
}

// Workflow functions must always be registered before launch; this
//  allows recovery to occur.
const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, { name: 'workflow' });

describe('decoratorless-api-basic-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
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

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('MyFirstStep');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('MySecondStep');
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
    await setUpDBOSTestSysDb(config);
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

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('getStaticVal');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('getInstanceVal');

    expect(StaticAndInstanceSteps.callCount).toBe(2);
  });
});

// Run workflow as bare, static, and instance (which must be named)
class StaticAndInstanceWFs extends ConfiguredInstance {
  static staticVal = 0;
  instanceVal: number;
  steps: StaticAndInstanceSteps;
  constructor(wiv: number, siv: number) {
    super(`staticandinstwfs${wiv}`);
    this.instanceVal = wiv;
    this.steps = new StaticAndInstanceSteps(siv);
  }

  static async staticWF() {
    const rv1 = await StaticAndInstanceSteps.getStaticVal();
    const rv2 = await DBOS.runStep(async () => Promise.resolve(StaticAndInstanceWFs.staticVal), { name: 'step2' });
    return Promise.resolve(`${rv1}-${rv2}`);
  }

  async instanceWF() {
    const rv1 = await this.steps.getInstanceVal();
    const rv2 = await DBOS.runStep(async () => Promise.resolve(this.instanceVal), { name: 'step2' });
    return Promise.resolve(`${rv1}-${rv2}`);
  }
}

const wfi34 = new StaticAndInstanceWFs(4, 3);
const wfi56 = new StaticAndInstanceWFs(6, 5);

StaticAndInstanceWFs.staticWF = DBOS.registerWorkflow(StaticAndInstanceWFs.staticWF, {
  name: 'staticWF',
  ctorOrProto: StaticAndInstanceWFs,
  className: 'StaticAndInstanceWFs',
});

StaticAndInstanceWFs.prototype.instanceWF = DBOS.registerWorkflow(
  // eslint-disable-next-line @typescript-eslint/unbound-method
  StaticAndInstanceWFs.prototype.instanceWF,
  {
    name: 'instanceWF',
    ctorOrProto: StaticAndInstanceWFs,
    className: 'StaticAndInstanceWFs',
  },
);

describe('decoratorless-api-class-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
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
    expect(stat1?.workflowClassName).toBe('StaticAndInstanceWFs');
    expect(stat1?.workflowConfigName).toBeFalsy();
    expect(stat1?.workflowName).toBe('staticWF');
    expect(stat1?.workflowID).toBe(wfid1);

    const wfsteps1 = (await DBOS.listWorkflowSteps(wfid1))!;
    expect(wfsteps1.length).toBe(2);
    expect(wfsteps1[0].functionID).toBe(0);
    expect(wfsteps1[0].name).toBe('getStaticVal');
    expect(wfsteps1[1].functionID).toBe(1);
    expect(wfsteps1[1].name).toBe('step2');

    const stat2 = await DBOS.getWorkflowStatus(wfid2);
    expect(stat2?.workflowClassName).toBe('StaticAndInstanceWFs');
    expect(stat2?.workflowConfigName).toBe('staticandinstwfs4');
    expect(stat2?.workflowName).toBe('instanceWF');
    expect(stat2?.workflowID).toBe(wfid2);

    const wfsteps2 = (await DBOS.listWorkflowSteps(wfid2))!;
    expect(wfsteps2.length).toBe(2);
    expect(wfsteps2[0].functionID).toBe(0);
    expect(wfsteps2[0].name).toBe('getInstanceVal');
    expect(wfsteps2[1].functionID).toBe(1);
    expect(wfsteps2[1].name).toBe('step2');
  });
});

async function argsWFFuncGuts(a: number, b: string) {
  return Promise.resolve(`${a}-${b}`);
}

const argsWF = DBOS.registerWorkflow(argsWFFuncGuts, { name: 'argsWF' });

async function stepFuncBare() {
  // expect(DBOS.isInStep()).toBe(true); // TODO should be true
  expect(DBOS.isWithinWorkflow()).toBe(false);
  return Promise.resolve('BareStep');
}

const stepFunctionBare = DBOS.registerStep(stepFuncBare, {
  name: 'MyNonWFStep',
});

// Step variant 4: the `@DBOS.step` decorator, on a static and on an instance method
const stepQueue = { name: 'stepRejectQ' };

class InstanceStep extends ConfiguredInstance {
  constructor() {
    super('Instance');
  }

  initialize(): Promise<void> {
    return Promise.resolve();
  }

  @DBOS.step()
  async testStep(arg: string, rv?: string): Promise<string> {
    expect(arg).toBe('a');
    ++InstanceStep.stepCnt;
    return Promise.resolve(rv ?? '');
  }

  static stepCnt = 0;
  static reset() {
    InstanceStep.stepCnt = 0;
  }
}

const inst = new InstanceStep();

class StaticStep {
  @DBOS.step()
  static async testStep(arg: string, rv?: string): Promise<string> {
    expect(arg).toBe('a');
    ++StaticStep.stepCnt;
    return Promise.resolve(rv ?? '');
  }

  static stepCnt = 0;
  static reset() {
    StaticStep.stepCnt = 0;
  }
}

class WorkflowsCallingSteps {
  @DBOS.workflow()
  static async runFuncs() {
    expect(await StaticStep.testStep('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');
  }
}

// Do this with startWorkflow (in a new form)
describe('start-workflow-function', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.registerQueue(stepQueue.name, { onConflict: 'always_update' });
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
    StaticAndInstanceSteps.staticVal = 1;
    StaticAndInstanceWFs.staticVal = 2;
    const wfi = wfi56;

    const wfh1 = await DBOS.startWorkflow(StaticAndInstanceWFs.staticWF, {
      workflowID: wfid1,
    })();
    await expect(wfh1.getResult()).resolves.toBe('1-2');

    const wfh2 = await DBOS.startWorkflow(wfi, { workflowID: wfid2 }).instanceWF();
    await wfh2.getResult();
    await expect(wfh2.getResult()).resolves.toBe('5-6');

    const wfh3 = await DBOS.startWorkflow(argsWF, { workflowID: wfid3 })(7, 'f');
    await expect(wfh3.getResult()).resolves.toBe('7-f');

    const wfidi1 = randomUUID();
    const wfhi1 = await DBOS.startWorkflow(StaticAndInstanceWFs, { workflowID: wfidi1 }).staticWF();
    await expect(wfhi1.getResult()).resolves.toBe('1-2');
  });

  test('step-outside-wf', async () => {
    // Check that the bare step does not start a workflow
    const nwsBefore = (await DBOS.listWorkflows({})).length;
    expect(nwsBefore).toBeGreaterThanOrEqual(1);
    const r1 = await stepFunctionBare();
    expect(r1).toBe('BareStep');
    const i1 = await DBOS.runStep(
      async () => {
        return Promise.resolve('inline');
      },
      { name: 'MyFirstStep' },
    );
    expect(i1).toBe('inline');
    const nwsAfter = (await DBOS.listWorkflows({})).length;
    expect(nwsAfter - nwsBefore).toBe(0);

    // An assigned workflow ID is for the next workflow; a step in the closure neither takes nor clears it.
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await expect(stepFunctionBare()).resolves.toBe('BareStep');
      await expect(DBOS.runStep(async () => Promise.resolve('inline'), { name: 'MyFirstStep' })).resolves.toBe(
        'inline',
      );
      const handle = await DBOS.startWorkflow(wfFunction)();
      expect(handle.workflowID).toBe(wfid);
      await handle.getResult();
    });
    expect((await DBOS.listWorkflows({ workflowIDs: [wfid] })).length).toBe(1);
  });

  // The same contract for the decorator, on a static and on an instance method
  test('decorated-step-outside-wf', async () => {
    StaticStep.reset();
    InstanceStep.reset();
    const wfsBefore = (await DBOS.listWorkflows({})).length;

    // Runs as an ordinary call, checkpointing nothing
    expect(await StaticStep.testStep('a', '1')).toBe('1');
    expect(await inst.testStep('a', '1')).toBe('1');
    expect(StaticStep.stepCnt).toBe(1);
    expect(InstanceStep.stepCnt).toBe(1);
    expect((await DBOS.listWorkflows({})).length - wfsBefore).toBe(0);

    // An assigned workflow ID is for the next workflow; a step in the closure leaves it alone
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      expect(await StaticStep.testStep('a', '1')).toBe('1');
      const handle = await DBOS.startWorkflow(WorkflowsCallingSteps).runFuncs();
      expect(handle.workflowID).toBe(wfid);
      await handle.getResult();
    });
    expect(StaticStep.stepCnt).toBe(3); // two plain calls, plus the workflow's own
    expect(InstanceStep.stepCnt).toBe(2); // one plain call, plus the workflow's own

    // Steps are not workflows: neither startable nor enqueueable, and neither call runs the body
    await expect(DBOS.startWorkflow(StaticStep).testStep('a', '1')).rejects.toThrow(
      /only workflows can be started or enqueued/,
    );
    await expect(DBOS.startWorkflow(inst, { queueName: stepQueue.name }).testStep('a', '1')).rejects.toThrow(
      /only workflows can be started or enqueued/,
    );
    expect(StaticStep.stepCnt).toBe(3);
    expect(InstanceStep.stepCnt).toBe(2);
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

// Step reservations: DBOS.reserveStep draws a step ID now, and StepReservation.run runs a step at it later

// An unbounded FIFO whose `get` waits for the next item.
class AsyncQueue<T> {
  private items: T[] = [];
  private waiters: ((item: T) => void)[] = [];

  put(item: T) {
    const waiter = this.waiters.shift();
    if (waiter) waiter(item);
    else this.items.push(item);
  }

  get(): Promise<T> {
    if (this.items.length > 0) return Promise.resolve(this.items.shift()!);
    return new Promise((resolve) => this.waiters.push(resolve));
  }
}

async function journal(workflowID: string): Promise<[number, string][]> {
  const steps = await DBOS.listWorkflowSteps(workflowID);
  return steps!.map((s) => [s.functionID, s.name]);
}

// Models an agent's event dispatch: emitting an event reserves a handler step that a consumer task runs later.
const handlerConfig: StepConfig = {
  name: 'event_stream_handler',
  retriesAllowed: true,
  maxAttempts: 3,
  intervalSeconds: 0.01,
};
const attempts: Record<string, number> = {};
let storeReads = 0;

const getItem = DBOS.registerStep(
  async (item: string) => {
    storeReads++;
    // Live-only latency, so replay timing differs from the live run.
    await new Promise((resolve) => setTimeout(resolve, 20));
    return `item-${item}`;
  },
  { name: 'getItem' },
);

async function handleEvent(event: string): Promise<string> {
  attempts[event] = (attempts[event] ?? 0) + 1;
  await new Promise((resolve) => setTimeout(resolve, 10));
  // Fails the first attempt of every live execution, so forks retry it too.
  if (event === 'flaky' && DBOS.stepStatus!.currentAttempt === 1) {
    throw new Error('transient handler failure');
  }
  if (event === 'bad') {
    throw new Error('handler rejected event');
  }
  return `handled-${event}`;
}

const agentRun = DBOS.registerWorkflow(
  async () => {
    const queue = new AsyncQueue<[string, boolean] | null>();
    // One FIFO of reservations per event, since the same event can be emitted twice.
    const reservations: Record<string, StepReservation[]> = {};
    const handled: string[] = [];

    const emit = (event: string, deliver: boolean = true) => {
      (reservations[event] ??= []).push(DBOS.reserveStep());
      queue.put([event, deliver]);
    };

    const consume = async () => {
      for (let item = await queue.get(); item !== null; item = await queue.get()) {
        const [event, deliver] = item;
        const reservation = reservations[event].shift()!;
        if (!deliver) continue; // The handler never starts, leaving a gap at its step.
        try {
          handled.push(await reservation.run(() => handleEvent(event), handlerConfig));
        } catch (e) {
          // Only the message survives replay, not the error's class.
          handled.push((e as Error).message.split('.')[0]);
        }
      }
    };

    const consumer = consume();
    // A plan tool: emit a burst of events, then make durable store reads while the handlers run.
    for (const event of ['plan', 'plan', 'flaky', 'bad', 'dropped']) {
      emit(event, event !== 'dropped');
    }
    const reads = [await getItem('a'), await getItem('b')];
    emit('progress');
    reads.push(await getItem('c'));
    queue.put(null);
    await consumer;
    return [...reads, ...handled];
  },
  { name: 'agentRun' },
);

const misuseLeaked: StepReservation[] = [];

const reservingWorkflow = DBOS.registerWorkflow(
  async () => {
    const reservation = DBOS.reserveStep();
    const stepID = await reservation.run(async () => Promise.resolve(DBOS.stepID), { name: 'first' });
    expect(stepID).toBe(0);
    expect(reservation.stepID).toBe(0);
    await expect(reservation.run(async () => Promise.resolve())).rejects.toThrow('only be run once');

    // A run claims the reservation before it awaits anything.
    const concurrent = DBOS.reserveStep();
    const winner = concurrent.run(async () => Promise.resolve('won'), { name: 'concurrent' });
    await expect(concurrent.run(async () => Promise.resolve('lost'))).rejects.toThrow('only be run once');
    expect(await winner).toBe('won');

    const failing = DBOS.reserveStep();
    await expect(failing.run(async () => Promise.reject(new Error('step failed')), { name: 'fail' })).rejects.toThrow(
      'step failed',
    );
    await expect(failing.run(async () => Promise.resolve())).rejects.toThrow('only be run once');

    // An invalid configuration is rejected without using up the reservation.
    const configured = DBOS.reserveStep();
    await expect(configured.run(async () => Promise.resolve(), { timeoutMS: -1 })).rejects.toThrow('Invalid timeoutMS');
    await configured.run(async () => Promise.resolve(), { name: 'configured' });

    misuseLeaked.push(DBOS.reserveStep());
  },
  { name: 'reservingWorkflow' },
);

const otherWorkflow = DBOS.registerWorkflow(
  async () => {
    await expect(misuseLeaked[0].run(async () => Promise.resolve())).rejects.toThrow('must be run from that workflow');
  },
  { name: 'otherWorkflow' },
);

const stepRunningReservation = DBOS.registerStep(
  async (reservation: StepReservation) => {
    await reservation.run(async () => Promise.resolve());
  },
  { name: 'stepRunningReservation' },
);

const stepWorkflow = DBOS.registerWorkflow(
  async () => {
    await expect(stepRunningReservation(DBOS.reserveStep())).rejects.toThrow('must be run from that workflow');
  },
  { name: 'stepWorkflow' },
);

const reservingStep = DBOS.registerStep(
  async () => {
    DBOS.reserveStep();
    return Promise.resolve();
  },
  { name: 'reservingStep' },
);

const nestedWorkflow = DBOS.registerWorkflow(
  async () => {
    await expect(reservingStep()).rejects.toThrow('outside any step or transaction');
  },
  { name: 'nestedWorkflow' },
);

const optionsWorkflow = DBOS.registerWorkflow(
  async () => {
    const timed = DBOS.reserveStep();
    const plain = DBOS.reserveStep();
    const out = await plain.run(async () => Promise.resolve(DBOS.stepID), { name: 'plain' });
    await expect(
      timed.run(
        async () => {
          // Waits until the timeout aborts it, so no timer outlives the test.
          const signal = DBOS.stepStatus!.timeoutSignal!;
          await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason as Error)));
        },
        { name: 'timed', timeoutMS: 200 },
      ),
    ).rejects.toThrow(DBOSStepTimeoutError);
    return out;
  },
  { name: 'optionsWorkflow' },
);

let blockedStarted = new Event();
let blockedRelease = new Event();
let blockedCalls = 0;

const cancelledWorkflow = DBOS.registerWorkflow(
  async () => {
    DBOS.reserveStep(); // Never run, so the blocked step is not step 0.
    return await DBOS.reserveStep().run(
      async () => {
        blockedCalls++;
        blockedStarted.set();
        await blockedRelease.wait();
        return DBOS.stepID;
      },
      { name: 'blocked' },
    );
  },
  { name: 'cancelledWorkflow' },
);

let numberedCalls = 0;

const outOfOrderWorkflow = DBOS.registerWorkflow(
  async () => {
    const numbered = (name: string) => async () => {
      numberedCalls++;
      return Promise.resolve(`${name}-${DBOS.stepID}`);
    };
    const first = DBOS.reserveStep();
    const second = DBOS.reserveStep();
    const b = await second.run(numbered('b'), { name: 'second' });
    const a = await first.run(numbered('a'), { name: 'first' });
    return a + b;
  },
  { name: 'outOfOrderWorkflow' },
);

describe('reserve-step', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;
  let schema: string;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    schema = config.systemDatabaseSchemaName ?? 'dbos';
  });

  beforeEach(async () => {
    for (const key of Object.keys(attempts)) delete attempts[key];
    storeReads = 0;
    misuseLeaked.length = 0;
    await DBOS.launch();
    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
  });

  test('reserve-step-event-stream', async () => {
    const expected = [
      'item-a',
      'item-b',
      'item-c',
      'handled-plan',
      'handled-plan',
      'handled-flaky',
      'Step event_stream_handler has exceeded its maximum of 3 retries',
      'handled-progress',
    ];
    const expectedJournal: [number, string][] = [
      [0, 'event_stream_handler'],
      [1, 'event_stream_handler'],
      [2, 'event_stream_handler'],
      [3, 'event_stream_handler'],
      [5, 'getItem'],
      [6, 'getItem'],
      [7, 'event_stream_handler'],
      [8, 'getItem'],
    ];
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      expect(await agentRun()).toEqual(expected);
    });
    expect(attempts).toEqual({ plan: 2, flaky: 2, bad: 3, progress: 1 });
    expect(await journal(wfid)).toEqual(expectedJournal);

    // Live attempts of the handler at each step ID, and the step IDs of the store reads.
    const handlerAttempts: Record<number, [string, number]> = {
      0: ['plan', 1],
      1: ['plan', 1],
      2: ['flaky', 2],
      3: ['bad', 3],
      7: ['progress', 1],
    };
    const readIDs = [5, 6, 8];
    const afterRerun = (before: Record<string, number>, rerun: number[]) => {
      const expectedAttempts = { ...before };
      for (const fid of rerun) {
        if (fid in handlerAttempts) {
          const [event, n] = handlerAttempts[fid];
          expectedAttempts[event] += n;
        }
      }
      return expectedAttempts;
    };

    // Recovery replays every step, at its reserved ID, without running it again.
    const liveAttempts = { ...attempts };
    const liveReads = storeReads;
    expect(await (await reexecuteWorkflowById(wfid)).getResult()).toEqual(expected);
    expect(attempts).toEqual(liveAttempts);
    expect(storeReads).toBe(liveReads);

    // A crash can leave reserved handlers unrecorded below recorded steps; recovery reruns only those.
    await systemDBClient.query(
      `DELETE FROM "${schema}".operation_outputs WHERE workflow_uuid = $1 AND function_id = ANY($2)`,
      [wfid, [1, 3]],
    );
    let before = { ...attempts };
    expect(await (await reexecuteWorkflowById(wfid)).getResult()).toEqual(expected);
    expect(attempts).toEqual(afterRerun(before, [1, 3]));
    expect(storeReads).toBe(liveReads);
    expect(await journal(wfid)).toEqual(expectedJournal);

    // Forking at every step replays a prefix and runs the rest live, under different timing.
    for (const startStep of [...expectedJournal.map(([fid]) => fid), 1000]) {
      before = { ...attempts };
      const readsBefore = storeReads;
      const handle = await DBOS.forkWorkflow<string[]>(wfid, startStep);
      expect(await handle.getResult()).toEqual(expected);
      const rerun = expectedJournal.map(([fid]) => fid).filter((fid) => fid >= startStep);
      expect(attempts).toEqual(afterRerun(before, rerun));
      expect(storeReads).toBe(readsBefore + readIDs.filter((fid) => rerun.includes(fid)).length);
      expect(await journal(handle.workflowID)).toEqual(expectedJournal);
    }
  }, 60000);

  test('reserve-step-misuse', async () => {
    expect(() => DBOS.reserveStep()).toThrow('outside any step or transaction');

    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await reservingWorkflow();
    });
    expect(await journal(wfid)).toEqual([
      [0, 'first'],
      [1, 'concurrent'],
      [2, 'fail'],
      [3, 'configured'],
    ]);
    expect(misuseLeaked[0].stepID).toBe(4);

    await otherWorkflow();
    await stepWorkflow();
    await nestedWorkflow();
    await expect(misuseLeaked[0].run(async () => Promise.resolve())).rejects.toThrow('must be run from that workflow');
  });

  test('reserve-step-timeout', async () => {
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      expect(await optionsWorkflow()).toBe(1);
    });
    expect(await journal(wfid)).toEqual([
      [0, 'timed'],
      [1, 'plain'],
    ]);
  });

  test('reserve-step-cancel-and-resume', async () => {
    // Cancelling mid-step leaves the reserved step unrecorded; resuming runs it again at the same ID.
    blockedStarted = new Event();
    blockedRelease = new Event();
    blockedCalls = 0;
    const handle = await DBOS.startWorkflow(cancelledWorkflow)();
    const wfid = handle.workflowID;
    await blockedStarted.wait();
    await DBOS.cancelWorkflow(wfid);
    blockedRelease.set();
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    await retryUntilSuccess(() =>
      expect(DBOSExecutor.globalInstance!.systemDatabase.checkForRunningWorkflow(wfid)).toBe(false),
    );
    expect(await journal(wfid)).toEqual([]);

    const resumed = await DBOS.resumeWorkflow<number>(wfid);
    expect(await resumed.getResult()).toBe(1);
    expect(blockedCalls).toBe(2);
    expect(await journal(wfid)).toEqual([[1, 'blocked']]);
  });

  test('reserve-step-out-of-order', async () => {
    numberedCalls = 0;
    const wfid = randomUUID();
    await DBOS.withNextWorkflowID(wfid, async () => {
      expect(await outOfOrderWorkflow()).toBe('a-0b-1');
    });
    expect(await journal(wfid)).toEqual([
      [0, 'first'],
      [1, 'second'],
    ]);
    expect(await (await reexecuteWorkflowById(wfid)).getResult()).toBe('a-0b-1');
    expect(await (await DBOS.forkWorkflow<string>(wfid, 1000)).getResult()).toBe('a-0b-1');
    expect(numberedCalls).toBe(2);
  });
});
