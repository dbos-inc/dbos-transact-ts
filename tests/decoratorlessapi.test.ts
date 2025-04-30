// runAsStep
// runAsStep no config instance
// runAsTransaction (later)
// registerTransaction ()
// registerStep ()
// registerWorkflow ()

// Do this on:
//   Bare
//   Static
//   instance (wf must be configured)
// Do this with direct invocation, and with startWorkflow (in a new form)

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

describe('decoratorless-api-tests', () => {
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
