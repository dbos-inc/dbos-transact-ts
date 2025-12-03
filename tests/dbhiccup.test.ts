/* eslint-disable @typescript-eslint/require-await */
import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { causeChaos, generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

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
    const r = await DBOS.recv<string>('topic', 5);
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
    const v1 = await DBOS.getEvent<string>(id1, 'key', 10);
    await causeChaos(config!.systemDatabaseUrl!);
    return 'Part2' + v1;
  }
}

describe('sys-db-hiccup', () => {
  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('a-little-chaos', async () => {
    await expect(DisruptiveWFs.dbLossBetweenSteps()).resolves.toBe('Hehehe');
    await expect(DisruptiveWFs.runChildWf()).resolves.toBe('Hehehe');

    const h1 = await DBOS.startWorkflow(DisruptiveWFs).wfPart1();
    const h2 = await DBOS.startWorkflow(DisruptiveWFs).wfPart2(h1.workflowID);

    await expect(h1.getResult()).resolves.toBe('Part1hello1');
    await expect(h2.getResult()).resolves.toBe('Part2v1');
  }, 30000);
});
