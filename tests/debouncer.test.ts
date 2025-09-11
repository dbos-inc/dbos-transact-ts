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
    const key = 'debouncer_key';
    const debouncerPeriodMs = 1000;
    const input = 5;
    const debouncer = new Debouncer({
      workflowName: 'workflow',
      workflowClassName: '',
    });
    const handle = await debouncer.debounce(key, debouncerPeriodMs, input);
    assert.equal(await handle.getResult(), input);
  });
});
