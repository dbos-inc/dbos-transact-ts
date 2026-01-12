import { DBOS } from '../src/dbos';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

describe('clear-reg-tests', () => {
  const config = generateDBOSTestConfig();

  beforeAll(async () => {
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  test('clear-reg-between-runs', async () => {
    for (let i = 1; i <= 2; ++i) {
      const stepfunc1 = DBOS.registerStep(async () => {
        return Promise.resolve(`${i}`);
      });
      const wffunc1 = async () => {
        const so1 = await DBOS.runStep(() => {
          return Promise.resolve(`${i}`);
        });
        const so2 = await stepfunc1();
        return so1 + so2;
      };
      const wf = DBOS.registerWorkflow(wffunc1);
      await DBOS.launch();
      try {
        await expect(wf()).resolves.toBe(`${i}${i}`);
      } finally {
        expect(() => DBOS.clearRegistry()).toThrow();
        await DBOS.shutdown();
        DBOS.clearRegistry();
      }
    }
  }, 20000);
});
