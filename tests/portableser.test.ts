import { Client } from 'pg';
import { DBOS, DBOSClient, DBOSConfig, WorkflowHandle, WorkflowQueue } from '../src';
import { generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';
import {
  notifications,
  PortableWorkflowError,
  streams,
  workflow_events,
  workflow_events_history,
  workflow_status,
} from '../schemas/system_db_schema';
import { DBOSJSON, DBOSPortableJSON } from '../src/serialization';
import { randomUUID } from 'node:crypto';
import { DBOSExecutor } from '../src/dbos-executor';
import { PostgresSystemDatabase } from '../src/system_database';

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

class PortableWorkflow {
  static lastWfid: string | undefined = undefined;

  @DBOS.workflow({ serialization: 'portable' })
  // eslint-disable-next-line @typescript-eslint/require-await
  static async pwfError() {
    PortableWorkflow.lastWfid = DBOS.workflowID;
    expect(DBOS.defaultSerializationType).toBe('portable');
    throw new Error('Failed!');
  }
}

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

  async function checkStreamSer(wfid: string, key: string, ser: string) {
    const mser = await systemDBClient.query<streams>(
      `SELECT * FROM dbos.streams where workflow_uuid = $1 and key=$2;`,
      [wfid, key],
    );
    expect(mser.rows[0].serialization).toBe(ser);
  }

  test('test-explicit-ser', async () => {
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
    await checkStreamSer(wfhd.workflowID, 'defstream', DBOSJSON.name());
    await checkStreamSer(wfhd.workflowID, 'nstream', DBOSJSON.name());
    await checkStreamSer(wfhd.workflowID, 'pstream', DBOSPortableJSON.name());

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

    // Streams
    await checkStreamSer(wfhp.workflowID, 'defstream', DBOSPortableJSON.name());
    await checkStreamSer(wfhp.workflowID, 'nstream', DBOSJSON.name());
    await checkStreamSer(wfhp.workflowID, 'pstream', DBOSPortableJSON.name());

    // Test copy+paste workflow
    const sysDb = DBOSExecutor.globalInstance!.systemDatabase as PostgresSystemDatabase;
    // Export with children
    const exported = await sysDb.exportWorkflow(wfhp.workflowID, true);

    // Delete the workflow so it can be reimported
    await DBOS.deleteWorkflow(wfhp.workflowID, true);

    // Importing the workflow succeeds after deletion
    await sysDb.importWorkflow(exported);

    // Check everything still there
    expect(await wfhp.getResult()).toBe('s-1-k:v@"m"');
    // Messages
    await checkMsgSer(drdwfh.workflowID, 'default', DBOSPortableJSON.name());
    await checkMsgSer(drdwfh.workflowID, 'native', DBOSJSON.name());
    //await checkMsgSer(drdwfh.workflowID, 'portable', DBOSPortableJSON.name()); // This got deleted

    // Events
    await checkEvtSer(wfhp.workflowID, 'defstat', DBOSPortableJSON.name());
    await checkEvtSer(wfhp.workflowID, 'nstat', DBOSJSON.name());
    await checkEvtSer(wfhp.workflowID, 'pstat', DBOSPortableJSON.name());

    // Streams
    await checkStreamSer(wfhp.workflowID, 'defstream', DBOSPortableJSON.name());
    await checkStreamSer(wfhp.workflowID, 'nstream', DBOSJSON.name());
    await checkStreamSer(wfhp.workflowID, 'pstream', DBOSPortableJSON.name());

    // Check reexec
    const reh = await reexecuteWorkflowById(wfhp.workflowID);
    expect(await reh?.getResult()).toBe('s-1-k:v@"m"');

    // Error handling
    // Check WF that throws an error
    await expect(PortableWorkflow.pwfError()).rejects.toThrow('Failed!');
    // The error thrown from direct invocation should be PortableWorkflowError
    //  for consistency (like we do with return values, run through ser/des)?
    // However, in JS you cannot count on this.  It's just how it is... there is no class
    //  registry.
    await expect(PortableWorkflow.pwfError()).rejects.toThrow(PortableWorkflowError);

    // Snoop the DB to make sure serialization format is correct
    // WF
    const eser = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status where workflow_uuid = $1',
      [PortableWorkflow.lastWfid],
    );
    expect(eser.rows[0].serialization).toBe(DBOSPortableJSON.name());
    expect(eser.rows[0].output).toBeNull();
    expect(eser.rows[0].error).toBe('{\"name\":\"Error\",\"message\":\"Failed!\"}');

    const errh = DBOS.retrieveWorkflow(PortableWorkflow.lastWfid!);
    await expect(errh.getResult()).rejects.toThrow('Failed!');
    try {
      await errh.getResult();
    } catch (e) {
      expect((e as PortableWorkflowError).message).toBe('Failed!');
      expect((e as PortableWorkflowError).name).toBe('Error');
      expect((e as object).constructor.name).toBe('PortableWorkflowError');
    }
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

  test('test-portable-client', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      for (const calltype of ['regular', 'portable', 'dbos']) {
        // Run WF with custom serialization
        const wfhr = await client.enqueue<typeof simpleRecv>(
          {
            workflowName: 'simpleRecv',
            queueName: 'testq',
            serializationType: 'portable',
          },
          'portable',
        );
        let wfhs: WorkflowHandle<string>;
        if (calltype === 'portable') {
          wfhs = await client.enqueuePortable<string>(
            {
              workflowName: 'workflowDef',
              workflowClassName: 'workflows',
              queueName: 'testq',
            },
            ['s', 1, { k: 'k', v: ['v'] }, wfhr.workflowID],
          );
        } else if (calltype === 'dbos') {
          wfhs = await DBOS.enqueuePortable<string>(
            {
              workflowName: 'workflowDef',
              workflowClassName: 'workflows',
              queueName: 'testq',
            },
            ['s', 1, { k: 'k', v: ['v'] }, wfhr.workflowID],
          );
        } else {
          wfhs = await client.enqueue<typeof defWorkflow>(
            {
              workflowName: 'workflowDef',
              workflowClassName: 'workflows',
              queueName: 'testq',
              serializationType: 'portable',
            },
            's',
            1,
            { k: 'k', v: ['v'] },
            wfhr.workflowID,
          );
        }
        await client.send(wfhs.workflowID, 'm', 'incoming', undefined, { serializationType: 'portable' });
        expect(await client.getEvent(wfhs.workflowID, 'defstat', 2)).toStrictEqual({ status: 'Happy' });
        expect(await client.getEvent(wfhs.workflowID, 'nstat', 2)).toStrictEqual({ status: 'Happy' });
        expect(await client.getEvent(wfhs.workflowID, 'pstat', 2)).toStrictEqual({ status: 'Happy' });
        const { value: ddread } = await client.readStream(wfhs.workflowID, 'defstream').next();
        expect(ddread).toStrictEqual({ stream: 'OhYeah' });
        const { value: dnread } = await client.readStream(wfhs.workflowID, 'nstream').next();
        expect(dnread).toStrictEqual({ stream: 'OhYeah' });
        const { value: dpread } = await client.readStream(wfhs.workflowID, 'pstream').next();
        expect(dpread).toStrictEqual({ stream: 'OhYeah' });
        const rvs = await wfhs.getResult();
        expect(rvs).toBe('s-1-k:v@"m"');
        expect(await wfhr.getResult()).toStrictEqual({ message: 'Hello!' });

        // Snoop the DB to make sure serialization format is correct
        // WF
        const pser = await systemDBClient.query<workflow_status>(
          'SELECT * FROM dbos.workflow_status where workflow_uuid = $1',
          [wfhs.workflowID],
        );
        expect(pser.rows[0].serialization).toBe(DBOSPortableJSON.name());
        expect(pser.rows[0].output).toBe('"s-1-k:v@\\"m\\""');

        // Messages
        await checkMsgSer(wfhr.workflowID, 'default', DBOSPortableJSON.name());
        await checkMsgSer(wfhr.workflowID, 'native', DBOSJSON.name());
        //await checkMsgSer(drdwfh.workflowID, 'portable', DBOSPortableJSON.name()); // This got deleted

        // Events
        await checkEvtSer(wfhs.workflowID, 'defstat', DBOSPortableJSON.name());
        await checkEvtSer(wfhs.workflowID, 'nstat', DBOSJSON.name());
        await checkEvtSer(wfhs.workflowID, 'pstat', DBOSPortableJSON.name());

        // Streams
        await checkStreamSer(wfhs.workflowID, 'defstream', DBOSPortableJSON.name());
        await checkStreamSer(wfhs.workflowID, 'nstream', DBOSJSON.name());
        await checkStreamSer(wfhs.workflowID, 'pstream', DBOSPortableJSON.name());
      }
    } finally {
      await client.destroy();
    }
  }, 15000);
});
