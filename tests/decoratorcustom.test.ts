// Custom step decorator (think retry policy)
// Custom tx decorator
// Custom event decorator

import { DBOS, DBOSConfig } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

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
    expect(1).toBe(1);
    return Promise.resolve();
  });
});
