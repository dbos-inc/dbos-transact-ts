// runAsStep
// runAsStep no config instance
// runAsTransaction (later)
// registerTransaction ()
// registerStep ()
// registerWorkflow ()

import { DBOS, DBOSConfig } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

async function _stepFunctionGuts() {
  return Promise.resolve('My second step result');
}

async function wfFunctionGuts() {
  const p1 = await DBOS.runAsWorkflowStep(async () => {
    return Promise.resolve('My first step result');
  }, 'MyFirstStep');

  return p1;
}

const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, {
  funcName: 'workflow',
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
    await expect(wfFunction()).resolves.toBe('My first step result');
  });
});
