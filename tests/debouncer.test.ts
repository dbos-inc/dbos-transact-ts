import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { debouncerWorkflow } from '../src/debouncer';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import assert from 'node:assert';

describe('debouncer-tests', () => {
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
  }, 10000);

  DBOS.registerWorkflow(
    async (x: number) => {
      return Promise.resolve(x);
    },
    { name: 'workflow' },
  );

  test('test-debouncer-workflow', async () => {
    await debouncerWorkflow(
      0,
      { workflowClassName: '', workflowName: 'workflow', startWorkflowParams: { workflowID: '5' } },
      5,
    );
    const handle = DBOS.retrieveWorkflow('5');
    assert.equal(await handle.getResult(), 5);
  });
});
