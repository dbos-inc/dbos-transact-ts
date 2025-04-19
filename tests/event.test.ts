import { DBOS } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSConfigInternal } from '../src/dbos-executor';
import { v4 as uuidv4 } from 'uuid';

describe('dbos-tests', () => {
  let config: DBOSConfigInternal;

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

  test('concurrent-workflow-events', async () => {
    const wfid = uuidv4();
    const promises: Promise<string | null>[] = [];
    for (let i = 0; i < 10; ++i) {
      promises.push(DBOS.getEvent(wfid, 'key1'));
    }

    const st = Date.now();
    await DBOS.withNextWorkflowID(wfid, async () => {
      await DBOSTestClassWFS.setEventWorkflow();
    });
    const et = Date.now();
    expect(et - st).toBeLessThan(1000);

    await Promise.allSettled(promises);
  });
});

class DBOSTestClassWFS {
  @DBOS.workflow()
  static async setEventWorkflow() {
    await DBOS.setEvent('key1', 'value1');
    return 0;
  }
}
