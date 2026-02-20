import { Client } from 'pg';
import { DBOS, DBOSClient, DBOSConfig, DBOSSerializer, WorkflowHandle, WorkflowQueue } from '../src';
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
import { z } from 'zod';

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

// Simple portable workflow that doesn't recv (for insert tests)
const simplePortWorkflow = DBOS.registerWorkflow(
  async (s: string, x: number, o: { k: string; v: string[] }) => {
    return `${s}-${x}-${o.k}:${o.v.join(',')}`;
  },
  {
    name: 'simplePortWorkflow',
    className: 'workflows',
    serialization: 'portable',
  },
);

// Workflow with inputSchema validation (Zod)
const validatedWorkflow = DBOS.registerWorkflow(
  async (s: string, x: number, o: { k: string; v: string[] }) => {
    return `${s}-${x}-${o.k}:${o.v.join(',')}`;
  },
  {
    name: 'validatedWorkflow',
    className: 'workflows',
    serialization: 'portable',
    inputSchema: z.tuple([z.string(), z.number(), z.object({ k: z.string(), v: z.array(z.string()) })]),
  },
);

// Workflow with inputSchema that coerces string → Date
const dateWorkflow = DBOS.registerWorkflow(
  async (d: Date) => {
    expect(d).toBeInstanceOf(Date);
    return `date:${d.toISOString()}`;
  },
  {
    name: 'dateWorkflow',
    className: 'workflows',
    serialization: 'portable',
    inputSchema: z.tuple([z.coerce.date()]),
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

  test('test-invalid-json-input', async () => {
    // Insert a workflow with unparseable JSON in the inputs column
    const id = randomUUID();
    await systemDBClient.query(
      `
      INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, queue_name,
        status, inputs, created_at, serialization
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `,
      [id, 'simplePortWorkflow', 'workflows', 'testq', 'ENQUEUED', 'not valid json{{', Date.now(), 'portable_json'],
    );

    // The queue poller picks this up, changes status to PENDING, then
    // deserializePositionalArgs throws a SyntaxError. The error is now
    // caught in executeWorkflowId which records it as ERROR status
    // (previously, the workflow would be stuck in PENDING forever).
    const wfh = DBOS.retrieveWorkflow(id);
    await expect(wfh.getResult()).rejects.toThrow();

    const result = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [id],
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].status).toBe('ERROR');
    expect(result.rows[0].error).toBeDefined();
  });

  test('test-mismatched-args-input', async () => {
    // Insert a workflow with valid JSON but wrong argument types.
    // simplePortWorkflow expects (s: string, x: number, o: {k: string, v: string[]})
    // We provide [123, "not_a_number", "not_an_object"].
    const id = randomUUID();
    await systemDBClient.query(
      `
      INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, queue_name,
        status, inputs, created_at, serialization
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `,
      [
        id,
        'simplePortWorkflow',
        'workflows',
        'testq',
        'ENQUEUED',
        JSON.stringify({ positionalArgs: [123, 'not_a_number', 'not_an_object'] }),
        Date.now(),
        'portable_json',
      ],
    );

    // The JSON parses fine, but when the workflow function tries o.v.join(','),
    // it crashes because 'not_an_object' is a string, not an object with a v property.
    // Unlike invalid JSON, this error occurs INSIDE the workflow execution,
    // so the workflow machinery catches it and records an ERROR status.
    const wfh = DBOS.retrieveWorkflow(id);
    await expect(wfh.getResult()).rejects.toThrow();

    const result = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [id],
    );
    expect(result.rows[0].status).toBe('ERROR');
    expect(result.rows[0].error).toBeDefined();
  });

  test('test-input-schema-rejects-bad-input', async () => {
    // validatedWorkflow has inputSchema: z.tuple([z.string(), z.number(), z.object({...})])
    // Insert with wrong types — the Zod schema should reject them with a clear error.
    const id = randomUUID();
    await systemDBClient.query(
      `
      INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, queue_name,
        status, inputs, created_at, serialization
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `,
      [
        id,
        'validatedWorkflow',
        'workflows',
        'testq',
        'ENQUEUED',
        JSON.stringify({ positionalArgs: [123, 'not_a_number', 'not_an_object'] }),
        Date.now(),
        'portable_json',
      ],
    );

    // With inputSchema, the error is a ZodError with a clear validation message,
    // not a cryptic runtime crash from within the workflow function.
    const wfh = DBOS.retrieveWorkflow(id);
    await expect(wfh.getResult()).rejects.toThrow();

    const result = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [id],
    );
    expect(result.rows[0].status).toBe('ERROR');
    // The error should contain Zod validation details
    expect(result.rows[0].error).toBeDefined();
    expect(result.rows[0].error).toContain('expected string');
  });

  test('test-input-schema-valid-input', async () => {
    // validatedWorkflow with correct types — should pass validation and succeed.
    const id = randomUUID();
    await systemDBClient.query(
      `
      INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, queue_name,
        status, inputs, created_at, serialization
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `,
      [
        id,
        'validatedWorkflow',
        'workflows',
        'testq',
        'ENQUEUED',
        JSON.stringify({ positionalArgs: ['hello', 42, { k: 'key', v: ['a', 'b'] }] }),
        Date.now(),
        'portable_json',
      ],
    );

    const wfh = DBOS.retrieveWorkflow(id);
    const res = await wfh.getResult();
    expect(res).toBe('hello-42-key:a,b');
  });

  test('test-input-schema-coercion', async () => {
    // dateWorkflow has inputSchema: z.tuple([z.coerce.date()])
    // Insert an ISO date string — Zod should coerce it to a Date object.
    const isoDate = '2025-06-15T12:00:00.000Z';
    const id = randomUUID();
    await systemDBClient.query(
      `
      INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, queue_name,
        status, inputs, created_at, serialization
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
      `,
      [
        id,
        'dateWorkflow',
        'workflows',
        'testq',
        'ENQUEUED',
        JSON.stringify({ positionalArgs: [isoDate] }),
        Date.now(),
        'portable_json',
      ],
    );

    const wfh = DBOS.retrieveWorkflow(id);
    const res = await wfh.getResult();
    // The workflow receives a real Date object (coerced from the ISO string)
    // and returns its ISO string representation.
    expect(res).toBe(`date:${isoDate}`);
  });

  test('test-input-schema-date-roundtrip', async () => {
    // Full round-trip: enqueue with a real Date → portable JSON serializes it
    // to an ISO string → queue poller deserializes → Zod coerces string back to Date.
    const testDate = new Date('2025-06-15T12:00:00.000Z');
    const wfh = await DBOS.startWorkflow(dateWorkflow, { queueName: 'testq' })(testDate);
    const res = await wfh.getResult();
    expect(res).toBe(`date:${testDate.toISOString()}`);

    // Verify the inputs column in the DB stored the date as a string (portable JSON),
    // not as a Date object — confirming the coercion happened on the way back in.
    const dbRow = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [wfh.workflowID],
    );
    expect(dbRow.rows[0].serialization).toBe('portable_json');
    const storedInputs = JSON.parse(dbRow.rows[0].inputs!);
    // Portable JSON stores the Date as an ISO string
    expect(typeof storedInputs.positionalArgs[0]).toBe('string');
    expect(storedInputs.positionalArgs[0]).toBe(testDate.toISOString());
  });

  test('test-input-schema-direct-invocation', async () => {
    // Verify inputSchema validation also works on direct (non-queue) invocation.
    // validatedWorkflow should succeed with correct args.
    const res = await validatedWorkflow('hello', 42, { k: 'key', v: ['a', 'b'] });
    expect(res).toBe('hello-42-key:a,b');

    // dateWorkflow should coerce a string to Date on direct invocation too.
    const isoDate = '2025-06-15T12:00:00.000Z';
    const dateRes = await dateWorkflow(isoDate as unknown as Date);
    expect(dateRes).toBe(`date:${isoDate}`);
  });

  test('test-portable-client', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      for (const calltype of ['regular', 'portable']) {
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

// Workflows for custom-serializer-restart tests (registered at module level)
const custSerWorkflow = DBOS.registerWorkflow(
  async (input: string) => {
    await DBOS.setEvent('key', input);
    return `result:${input}`;
  },
  { name: 'custSerWorkflow' },
);

const custSerErrorWorkflow = DBOS.registerWorkflow(
  async (msg: string) => {
    return Promise.reject(new Error(msg));
  },
  { name: 'custSerErrorWorkflow' },
);

describe('custom-serializer-restart-tests', () => {
  const base64Serializer: DBOSSerializer = {
    name: () => 'custom_base64',
    parse: (text: string | null | undefined): unknown => {
      if (text === null || text === undefined) return null;
      return JSON.parse(Buffer.from(text, 'base64').toString());
    },
    stringify: (obj: unknown): string => {
      if (obj === undefined) obj = null;
      return Buffer.from(JSON.stringify(obj)).toString('base64');
    },
  };

  let config: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(() => {
    config = generateDBOSTestConfig();
  });

  afterEach(async () => {
    if (systemDBClient) {
      await systemDBClient.end();
    }
    await DBOS.shutdown();
  });

  test('test-add-custom-serializer-reads-old-data', async () => {
    // Phase 1: Launch with default serializer, run workflows
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestSysDb(config);
    config.serializer = undefined;
    DBOS.setConfig(config);
    await DBOS.launch();

    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();

    const p1Handle = await DBOS.startWorkflow(custSerWorkflow)('hello');
    expect(await p1Handle.getResult()).toBe('result:hello');
    expect(await DBOS.getEvent(p1Handle.workflowID, 'key')).toBe('hello');

    const p1ErrHandle = await DBOS.startWorkflow(custSerErrorWorkflow)('phase1-oops');
    await expect(p1ErrHandle.getResult()).rejects.toThrow('phase1-oops');

    // Verify DB stores default serialization format
    const p1Status = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [p1Handle.workflowID],
    );
    expect(p1Status.rows[0].serialization).toBe(DBOSJSON.name());

    await DBOS.shutdown();

    // Phase 2: Relaunch with custom serializer
    config.serializer = base64Serializer;
    DBOS.setConfig(config);
    await DBOS.launch();

    // Old data (js_superjson) should still be readable via DBOS API
    expect(await DBOS.retrieveWorkflow(p1Handle.workflowID).getResult()).toBe('result:hello');
    expect(await DBOS.getEvent(p1Handle.workflowID, 'key')).toBe('hello');
    await expect(DBOS.retrieveWorkflow(p1ErrHandle.workflowID).getResult()).rejects.toThrow('phase1-oops');

    // Old data should also be readable via DBOSClient with the custom serializer
    const client = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      serializer: base64Serializer,
    });
    expect(await client.retrieveWorkflow(p1Handle.workflowID).getResult()).toBe('result:hello');
    expect(await client.getEvent(p1Handle.workflowID, 'key')).toBe('hello');

    // New workflows should use the custom serializer
    const p2Handle = await DBOS.startWorkflow(custSerWorkflow)('world');
    expect(await p2Handle.getResult()).toBe('result:world');
    expect(await DBOS.getEvent(p2Handle.workflowID, 'key')).toBe('world');

    // Client should also read new custom-serialized data
    expect(await client.retrieveWorkflow(p2Handle.workflowID).getResult()).toBe('result:world');
    expect(await client.getEvent(p2Handle.workflowID, 'key')).toBe('world');
    await client.destroy();

    // Verify DB stores custom serialization format
    const p2Status = await systemDBClient.query<workflow_status>(
      'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [p2Handle.workflowID],
    );
    expect(p2Status.rows[0].serialization).toBe('custom_base64');
    expect(p2Status.rows[0].output).toBe(base64Serializer.stringify('result:world'));

    const p2Evt = await systemDBClient.query<workflow_events>(
      'SELECT * FROM dbos.workflow_events WHERE workflow_uuid = $1 AND key = $2',
      [p2Handle.workflowID, 'key'],
    );
    expect(p2Evt.rows[0].serialization).toBe('custom_base64');

    process.env.DBOS__APPVERSION = undefined;
  }, 30000);

  test('test-remove-custom-serializer-errors', async () => {
    // Phase 1: Launch with custom serializer, create data
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestSysDb(config);
    config.serializer = base64Serializer;
    DBOS.setConfig(config);
    await DBOS.launch();

    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();

    // Insert a default-format (js_superjson) workflow directly for comparison,
    // since we can't run both serializers in one launch
    const defaultWfId = randomUUID();
    await systemDBClient.query(
      `INSERT INTO dbos.workflow_status(
        workflow_uuid, name, class_name, status, output, created_at, serialization
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        defaultWfId,
        'custSerWorkflow',
        '',
        'SUCCESS',
        DBOSJSON.stringify('result:default'),
        Date.now(),
        DBOSJSON.name(),
      ],
    );
    await systemDBClient.query(
      `INSERT INTO dbos.workflow_events(
        workflow_uuid, key, value, serialization
      ) VALUES ($1, $2, $3, $4)`,
      [defaultWfId, 'key', DBOSJSON.stringify('default'), DBOSJSON.name()],
    );

    // Run workflows with custom serializer
    const custHandle = await DBOS.startWorkflow(custSerWorkflow)('custom');
    expect(await custHandle.getResult()).toBe('result:custom');
    expect(await DBOS.getEvent(custHandle.workflowID, 'key')).toBe('custom');

    const custErrHandle = await DBOS.startWorkflow(custSerErrorWorkflow)('custom-oops');
    await expect(custErrHandle.getResult()).rejects.toThrow('custom-oops');

    await DBOS.shutdown();

    // Phase 2: Relaunch WITHOUT custom serializer
    config.serializer = undefined;
    DBOS.setConfig(config);
    await DBOS.launch();

    // Default-format data should still be readable
    expect(await DBOS.retrieveWorkflow(defaultWfId).getResult()).toBe('result:default');
    expect(await DBOS.getEvent(defaultWfId, 'key')).toBe('default');

    // Custom-format workflow result should throw TypeError
    await expect(DBOS.retrieveWorkflow(custHandle.workflowID).getResult()).rejects.toThrow('is not available');
    // Custom-format event should throw TypeError
    await expect(DBOS.getEvent(custHandle.workflowID, 'key')).rejects.toThrow('is not available');
    // Custom-format error workflow should throw TypeError (not the original error)
    await expect(DBOS.retrieveWorkflow(custErrHandle.workflowID).getResult()).rejects.toThrow('is not available');

    // DBOSClient without the serializer should behave equivalently
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });

    // Default-format data readable via client too
    expect(await client.retrieveWorkflow(defaultWfId).getResult()).toBe('result:default');
    expect(await client.getEvent(defaultWfId, 'key')).toBe('default');

    // Custom-format data: client getResult/getEvent throw TypeError
    await expect(client.retrieveWorkflow(custHandle.workflowID).getResult()).rejects.toThrow('is not available');
    await expect(client.getEvent(custHandle.workflowID, 'key')).rejects.toThrow('is not available');

    // listWorkflows falls back to raw strings for custom data (both DBOS and client)
    const allWorkflows = await DBOS.listWorkflows({});
    const custListed = allWorkflows.find((w) => w.workflowID === custHandle.workflowID);
    expect(custListed).toBeDefined();
    expect(custListed!.output).toBe(base64Serializer.stringify('result:custom'));
    const defaultListed = allWorkflows.find((w) => w.workflowID === defaultWfId);
    expect(defaultListed).toBeDefined();
    expect(defaultListed!.output).toBe('result:default');

    const clientWorkflows = await client.listWorkflows({});
    const clientCustListed = clientWorkflows.find((w) => w.workflowID === custHandle.workflowID);
    expect(clientCustListed).toBeDefined();
    expect(clientCustListed!.output).toBe(base64Serializer.stringify('result:custom'));
    const clientDefaultListed = clientWorkflows.find((w) => w.workflowID === defaultWfId);
    expect(clientDefaultListed).toBeDefined();
    expect(clientDefaultListed!.output).toBe('result:default');

    await client.destroy();

    // listWorkflowSteps for custom-format workflow should also fall back
    const custSteps = await DBOS.listWorkflowSteps(custHandle.workflowID);
    expect(custSteps).toBeDefined();
    const setEventStep = custSteps!.find((s) => s.name.includes('setEvent'));
    expect(setEventStep).toBeDefined();

    process.env.DBOS__APPVERSION = undefined;
  }, 30000);
});
