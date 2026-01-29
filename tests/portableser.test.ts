import { Client } from 'pg';
import { DBOS, DBOSConfig, WorkflowQueue } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { notifications, workflow_events, workflow_events_history, workflow_status } from '../schemas/system_db_schema';
import { DBOSJSON, DBOSPortableJSON } from '../src/serialization';
import { randomUUID } from 'node:crypto';

const _queue = new WorkflowQueue('testq');

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

const simpleRecv = DBOS.registerWorkflow(
  async (topic: string) => {
    return await DBOS.recv(topic);
  },
  { name: 'simpleRecv' },
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
    async function checkMsgSer(dstdid: string, topic: string, ser: string) {
      const mser = await systemDBClient.query<notifications>(
        `SELECT * FROM dbos.notifications where destination_uuid = $1 and topic=$2;`,
        [dstdid, topic],
      );
      expect(mser.rows[0].serialization).toBe(ser);
    }

    async function checkEvtSer(wfid: string, key: string, ser: string) {
      const eser = await systemDBClient.query<workflow_events>(
        `SELECT * FROM dbos.workflow_events where workflow_uuid = $1 and key=$2;`,
        [wfid, key],
      );
      expect(eser.rows[0].serialization).toBe(ser);
      const hser = await systemDBClient.query<workflow_events_history>(
        `SELECT * FROM dbos.workflow_events_history where workflow_uuid = $1 and key=$2;`,
        [wfid, key],
      );
      expect(hser.rows[0].serialization).toBe(ser);
    }

    // Run WF with default serialization
    //  But first, receivers
    const drpwfh = await DBOS.startWorkflow(simpleRecv)('native');
    const wfhd = await DBOS.startWorkflow(defWorkflow)('s', 1, { k: 'k', v: ['v'] }, drpwfh.workflowID);
    await DBOS.send(wfhd.workflowID, 'm', 'incoming');
    expect(await DBOS.getEvent(wfhd.workflowID, 'defstat')).toStrictEqual({ status: 'Happy' });
    expect(await DBOS.getEvent(wfhd.workflowID, 'nstat')).toStrictEqual({ status: 'Happy' });
    expect(await DBOS.getEvent(wfhd.workflowID, 'pstat')).toStrictEqual({ status: 'Happy' });
    const { value: ddread } = await DBOS.readStream(wfhd.workflowID, 'defstream').next();
    expect(ddread).toStrictEqual({ stream: 'OhYeah' });
    const { value: dnread } = await DBOS.readStream(wfhd.workflowID, 'nstream').next();
    expect(dnread).toStrictEqual({ stream: 'OhYeah' });
    const { value: dpread } = await DBOS.readStream(wfhd.workflowID, 'pstream').next();
    expect(dpread).toStrictEqual({ stream: 'OhYeah' });
    const rvd = await wfhd.getResult();
    expect(rvd).toBe('s-1-k:v@"m"');
    expect(await drpwfh.getResult()).toStrictEqual({ message: 'Hello!' });

    // Snoop the DB to make sure serialization format is correct
    // WF
    const nser = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status where workflow_uuid = $1',
      [wfhd.workflowID],
    );
    expect(nser.rows[0].serialization).toBe(DBOSJSON.name());
    // Messages
    await checkMsgSer(drpwfh.workflowID, 'default', DBOSJSON.name());
    //await checkMsgSer(drpwfh.workflowID, 'native', DBOSJSON.name()); // This got deleted
    await checkMsgSer(drpwfh.workflowID, 'portable', DBOSPortableJSON.name());

    // Events
    await checkEvtSer(wfhd.workflowID, 'defstat', DBOSJSON.name());
    await checkEvtSer(wfhd.workflowID, 'nstat', DBOSJSON.name());
    await checkEvtSer(wfhd.workflowID, 'pstat', DBOSPortableJSON.name());

    // Streams

    // Run with portable serialization
    const drdwfh = await DBOS.startWorkflow(simpleRecv)('portable');
    const wfhp = await DBOS.startWorkflow(portWorkflow)('s', 1, { k: 'k', v: ['v'] }, drdwfh.workflowID);
    await DBOS.send(wfhp.workflowID, 'm', 'incoming');
    expect(await DBOS.getEvent(wfhp.workflowID, 'defstat')).toStrictEqual({ status: 'Happy' });
    expect(await DBOS.getEvent(wfhp.workflowID, 'nstat')).toStrictEqual({ status: 'Happy' });
    expect(await DBOS.getEvent(wfhp.workflowID, 'pstat')).toStrictEqual({ status: 'Happy' });
    const { value: pdread } = await DBOS.readStream(wfhp.workflowID, 'defstream').next();
    expect(pdread).toStrictEqual({ stream: 'OhYeah' });
    const { value: pnread } = await DBOS.readStream(wfhp.workflowID, 'nstream').next();
    expect(pnread).toStrictEqual({ stream: 'OhYeah' });
    const { value: ppread } = await DBOS.readStream(wfhp.workflowID, 'pstream').next();
    expect(ppread).toStrictEqual({ stream: 'OhYeah' });
    const rvp = await wfhp.getResult();
    expect(rvp).toBe('s-1-k:v@"m"');
    expect(await drdwfh.getResult()).toStrictEqual({ message: 'Hello!' });

    // Snoop the DB to make sure serialization format is correct
    // WF
    const pser = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status where workflow_uuid = $1',
      [wfhp.workflowID],
    );
    expect(pser.rows[0].serialization).toBe(DBOSPortableJSON.name());
    expect(pser.rows[0].output).toBe('"s-1-k:v@\\"m\\""');
    // Messages
    await checkMsgSer(drdwfh.workflowID, 'default', DBOSPortableJSON.name());
    await checkMsgSer(drdwfh.workflowID, 'native', DBOSJSON.name());
    //await checkMsgSer(drdwfh.workflowID, 'portable', DBOSPortableJSON.name()); // This got deleted

    // Events
    await checkEvtSer(wfhp.workflowID, 'defstat', DBOSPortableJSON.name());
    await checkEvtSer(wfhp.workflowID, 'nstat', DBOSJSON.name());
    await checkEvtSer(wfhp.workflowID, 'pstat', DBOSPortableJSON.name());
  });

  test('test-direct-insert', async () => {
    const id = randomUUID();
    await systemDBClient.query(
      `
      INSERT INTO dbos.workflow_status(
        workflow_uuid,
        name,
        class_name,
        queue_name,
        status,
        inputs,
        created_at,
        serialization
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `,
      [
        id,
        'workflowPortable',
        'workflows',
        'testq',
        'ENQUEUED',
        JSON.stringify({ positionalArgs: ['s', 1, { k: 'k', v: ['v'] }] }),
        Date.now(),
        'portable_json',
      ],
    );

    await systemDBClient.query(
      `
      INSERT INTO dbos.notifications(
        destination_uuid,
        topic,
        message,
        serialization
      )
      VALUES ($1, $2, $3, $4);
    `,
      [id, 'incoming', JSON.stringify('M'), 'portable_json'],
    );

    // This is cheating, but we tested it above
    const wfh = DBOS.retrieveWorkflow(id);
    const res = await wfh.getResult();
    expect(res).toBe('s-1-k:v@"M"');
  });

  test('test-workflow-export-import', async () => {});

  test('test-portable-client', async () => {});

  // TODO: Test error cases
  // TODO: Custom ser interop

  test('test-nonserializable-stuff', async () => {});
});
