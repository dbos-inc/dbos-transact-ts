import { Client } from 'pg';
import { DBOS, DBOSConfig } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

async function workflowFunc(s: string, x: number, o: { k: string; v: string[] }, wfid?: string): Promise<string> {
  await DBOS.setEvent('defstat', { status: 'Happy' });
  await DBOS.setEvent('nstat', { status: 'Happy' }, { serializationType: 'native' });
  await DBOS.setEvent('pstat', { status: 'Happy' }, { serializationType: 'portable' });

  await DBOS.writeStream('defstream', { stream: 'OhYeah' });
  await DBOS.writeStream('nstream', { stream: 'OhYeah' }, { serializationType: 'native' });
  await DBOS.writeStream('pstream', { stream: 'OhYeah' }, { serializationType: 'portable' });

  if (wfid) {
    await DBOS.send(wfid, { message: 'Hello!' }, 'default');
    await DBOS.send(wfid, { message: 'Hello!' }, 'native', undefined, { serializationType: 'native' });
    await DBOS.send(wfid, { message: 'Hello!' }, 'portable', undefined, { serializationType: 'portable' });
  }

  const r = await DBOS.recv('incoming');

  return `${s}-${x}-${o.k}:${o.v.join(',')}@${JSON.stringify(r)}`;
}

const defWorkflow = DBOS.registerWorkflow(workflowFunc, {
  name: 'workflowDef',
  className: 'workflows',
  serialization: undefined,
});

const portWorkflow = DBOS.registerWorkflow(
  async (s: string, x: number, o: { k: string; v: string[] }, wfid?: string) => workflowFunc(s, x, o, wfid),
  {
    name: 'workflowPortable',
    className: 'workflows',
    serialization: 'portable',
  },
);

describe('portable-serizlization-tests', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();

    systemDBClient = new Client({
      connectionString: config.systemDatabaseUrl,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
    process.env.DBOS__APPVERSION = undefined;
  });

  test('test-explicit-ser', async () => {
    // Run with default serialization
    const wfhd = await DBOS.startWorkflow(defWorkflow)('s', 1, { k: 'k', v: ['v'] });
    await DBOS.send(wfhd.workflowID, 'm', 'incoming');
    const rvd = await wfhd.getResult();
    expect(rvd).toBe('s-1-k:v@"m"');

    // Run with portable serialization
    const wfhp = await DBOS.startWorkflow(portWorkflow)('s', 1, { k: 'k', v: ['v'] });
    await DBOS.send(wfhp.workflowID, 'm', 'incoming');
    const rvp = await wfhp.getResult();
    expect(rvp).toBe('s-1-k:v@"m"');
  });

  test('test-portable-client', async () => {});

  test('test-direct-insert', async () => {});

  test('test-workflow-export-import', async () => {});

  test('test-nonserializable-stuff', async () => {});
});
