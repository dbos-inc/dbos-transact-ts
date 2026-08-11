/* eslint-disable @typescript-eslint/require-await */
import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { causeChaos, generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { dbRetryConfig } from '../src/utils';

let config: DBOSConfig;

class DisruptiveWFs {
  @DBOS.workflow()
  static async dbLossBetweenSteps() {
    await DBOS.runStep(
      async () => {
        return 'A';
      },
      { name: 'A' },
    );
    await DBOS.runStep(
      async () => {
        return 'B';
      },
      { name: 'B' },
    );
    await causeChaos(config!.systemDatabaseUrl!);
    await DBOS.runStep(
      async () => {
        return 'C';
      },
      { name: 'C' },
    );
    await DBOS.runStep(
      async () => {
        return 'D';
      },
      { name: 'D' },
    );
    return 'Hehehe';
  }

  @DBOS.workflow()
  static async runChildWf() {
    await causeChaos(config!.systemDatabaseUrl!);
    const wfh = await DBOS.startWorkflow(DisruptiveWFs).dbLossBetweenSteps();
    await causeChaos(config!.systemDatabaseUrl!);
    return await wfh.getResult();
  }

  @DBOS.workflow()
  static async wfPart1() {
    await causeChaos(config!.systemDatabaseUrl!);
    // Chaos kills the LISTEN/NOTIFY listener, so a wakeup can be missed; poll often enough that
    // falling back to polling costs milliseconds rather than a full default poll interval.
    const r = await DBOS.recv<string>('topic', { timeoutSeconds: 5, pollingIntervalMs: 100 });
    await causeChaos(config!.systemDatabaseUrl!);
    await DBOS.setEvent('key', 'v1');
    await causeChaos(config!.systemDatabaseUrl!);
    return 'Part1' + r;
  }

  @DBOS.workflow()
  static async wfPart2(id1: string) {
    await causeChaos(config!.systemDatabaseUrl!);
    await DBOS.send(id1, 'hello1', 'topic');
    await causeChaos(config!.systemDatabaseUrl!);
    const v1 = await DBOS.getEvent<string>(id1, 'key', { timeoutSeconds: 10, pollingIntervalMs: 100 });
    await causeChaos(config!.systemDatabaseUrl!);
    return 'Part2' + v1;
  }
}

describe('sys-db-hiccup', () => {
  const savedBackoff = { ...dbRetryConfig };

  beforeAll(async () => {
    // This test kills every connection 10 times over. The production backoff (1s, doubling to 60s)
    // compounds into minutes of pure sleeping under a slow CI runner, so bound it: pg_terminate_backend
    // leaves the server up, so reconnecting succeeds almost immediately.
    dbRetryConfig.initialBackoffSec = 0.05;
    dbRetryConfig.maxBackoffSec = 1.0;
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  afterAll(() => {
    Object.assign(dbRetryConfig, savedBackoff);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 30000);

  test('a-little-chaos', async () => {
    await expect(DisruptiveWFs.dbLossBetweenSteps()).resolves.toBe('Hehehe');
    await expect(DisruptiveWFs.runChildWf()).resolves.toBe('Hehehe');

    const h1 = await DBOS.startWorkflow(DisruptiveWFs).wfPart1();
    const h2 = await DBOS.startWorkflow(DisruptiveWFs).wfPart2(h1.workflowID);

    await expect(h1.getResult()).resolves.toBe('Part1hello1');
    await expect(h2.getResult()).resolves.toBe('Part2v1');
  });
});
