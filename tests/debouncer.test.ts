import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { Debouncer } from '../src/debouncer';
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

  test('test-debouncer', async () => {
    const debouncer = new Debouncer({
      debouncerKey: '5',
      workflowName: 'workflow',
      workflowClassName: '',
      startWorkflowParams: { workflowID: '5' },
    });
    const handle = await debouncer.debounce(5, 5);
    assert.equal(await handle.getResult(), 5);
  });
});
