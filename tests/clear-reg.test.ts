import { DBOS } from '../src/dbos';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';

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
      // Explicit registered code
      const wffunc1 = async () => {
        const so1 = await DBOS.runStep(() => {
          return Promise.resolve(`${i}`);
        });
        const so2 = await stepfunc1();
        return so1 + so2;
      };
      const wf = DBOS.registerWorkflow(wffunc1);

      // Decorated code
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const m = require(`./dynamic_code_v${i}`) as typeof import('./dynamic_code_v1'); // Load it dynamically inside expect

      await DBOS.launch();
      try {
        await expect(wf()).resolves.toBe(`${i}${i}`);
        const wfh = await DBOS.startWorkflow(wf)();
        expect(await wfh.getResult()).toBe(`${i}${i}`);
        expect(await (await reexecuteWorkflowById(wfh.workflowID))!.getResult()).toBe(`${i}${i}`);
        await expect(m.DBOSWFTest.runWF()).resolves.toBe(i === 1 ? 'A' : 'B');
        await expect(m.instA.doWorkflow()).resolves.toBe(i === 1 ? 'done A1' : 'done A2');
        await expect(m.instB.doWorkflow()).resolves.toBe('done B');
        await expect((await DBOS.startWorkflow(m.instB).doWorkflow()).getResult()).resolves.toBe('done B');
        await expect(
          (await DBOS.startWorkflow(m.instA, { queueName: m.queue.name }).doWorkflow()).getResult(),
        ).resolves.toBe(i === 1 ? 'done A1' : 'done A2');
        await expect(
          (await DBOS.startWorkflow(m.instB, { queueName: m.queue.name }).doWorkflow()).getResult(),
        ).resolves.toBe('done B');

        // Wait for scheduled WF to run
        while (!m.DBOSWFTest.ran) await sleepms(100);
      } finally {
        expect(() => DBOS.clearRegistry()).toThrow();
        await DBOS.shutdown();
        DBOS.clearRegistry();
      }
    }
  }, 20000);
});
