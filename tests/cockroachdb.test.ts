import { DBOS } from '../src/';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { dropPGDatabase } from '../src/database_utils';
import { ensureTestDatabase } from './helpers';
import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { garbageCollect } from '../src/workflow_management';
import { cutoffPastAllCompletions } from './helpers';

const cockroachdbUrl = process.env.DBOS_COCKROACHDB_URL;
const describeIf = cockroachdbUrl ? describe : describe.skip;

const silentDropLogger = { warn: () => {} };

const testQueueName = 'crdb-test-queue';

// Building the schema from scratch costs ~40s on CockroachDB, so hooks that may do it need well over the 60s default.
const CRDB_SCHEMA_TIMEOUT_MS = 180000;

class CRDBTestClass {
  @DBOS.workflow()
  static async testWorkflow(input: string) {
    const result = await CRDBTestClass.testStep(input);
    return result;
  }

  @DBOS.step()
  static async testStep(input: string) {
    await Promise.resolve();
    return input.toUpperCase();
  }

  @DBOS.workflow()
  static async receiveWorkflow() {
    return await DBOS.recv<string>();
  }

  @DBOS.workflow()
  static async eventWorkflow() {
    await DBOS.setEvent('key1', 'value1');
    await DBOS.setEvent('key2', 'value2');
    return 'done';
  }

  @DBOS.workflow()
  static async streamWriterWorkflow(streamKey: string, testValues: unknown[]) {
    for (const value of testValues) {
      await DBOS.writeStream(streamKey, value);
    }
    await DBOS.closeStream(streamKey);
  }
}

describeIf('cockroachdb', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    const url = new URL(cockroachdbUrl!);
    url.pathname = '/dbos_test';
    const systemDatabaseUrl = url.toString();

    await dropPGDatabase(systemDatabaseUrl, silentDropLogger);
    await ensureTestDatabase(systemDatabaseUrl);
    config = {
      name: 'cockroachdb-test',
      systemDatabaseUrl,
      useListenNotify: false,
    };
    DBOS.setConfig(config);
  }, CRDB_SCHEMA_TIMEOUT_MS);

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.registerQueue(testQueueName, { onConflict: 'always_update' });
    const sysDB = DBOSExecutor.globalInstance!.systemDatabase;
    sysDB.dbPollingIntervalResultMs = 100;
    sysDB.dbPollingIntervalEventMs = 100;
  }, CRDB_SCHEMA_TIMEOUT_MS);

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('workflow-with-step', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).testWorkflow('hello');
    const result = await handle.getResult();
    expect(result).toBe('HELLO');
  });

  test('workflow-on-queue', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass, { queueName: testQueueName }).testWorkflow('queued');
    expect(await handle.getResult()).toBe('QUEUED');
    const status = await handle.getStatus();
    expect(status?.queueName).toBe('crdb-test-queue');
  });

  test('send-and-recv', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).receiveWorkflow();
    await DBOS.send(handle.workflowID, 'hello');
    expect(await handle.getResult()).toBe('hello');
  });

  test('set-and-get-events', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).eventWorkflow();
    await handle.getResult();
    await expect(DBOS.getEvent(handle.workflowID, 'key1')).resolves.toBe('value1');
    await expect(DBOS.getEvent(handle.workflowID, 'key2')).resolves.toBe('value2');
    await expect(DBOS.getEvent(handle.workflowID, 'nonexistent', 0)).resolves.toBeNull();

    // Fork the workflow from the end and verify the forked workflow also has the events
    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    const forkedHandle = await DBOS.forkWorkflow(handle.workflowID, steps!.length);
    await forkedHandle.getResult();
    await expect(DBOS.getEvent(forkedHandle.workflowID, 'key1')).resolves.toBe('value1');
    await expect(DBOS.getEvent(forkedHandle.workflowID, 'key2')).resolves.toBe('value2');
  });

  test('list-workflows', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).testWorkflow('introspect');
    await handle.getResult();

    const workflows = await DBOS.listWorkflows({ workflowName: 'testWorkflow' });
    expect(workflows.length).toBeGreaterThan(0);
    const match = workflows.find((w) => w.workflowID === handle.workflowID);
    expect(match).toBeDefined();
    expect(match?.status).toBe('SUCCESS');
    expect(match?.priority).toBe(0);
  });

  test('list-workflow-steps', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).testWorkflow('steps');
    await handle.getResult();

    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    expect(steps).toBeDefined();
    expect(steps!.length).toBe(1);
    expect(steps![0].name).toContain('testStep');
    expect(steps![0].functionID).toBe(0);
  });

  test('streaming', async () => {
    const testValues = ['hello', 42, { key: 'value' }, [1, 2, 3], null];
    const streamKey = 'test_stream';
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      await CRDBTestClass.streamWriterWorkflow(streamKey, testValues);
    });

    const readValues: unknown[] = [];
    for await (const value of DBOS.readStream(wfid, streamKey)) {
      readValues.push(value);
    }
    expect(readValues).toEqual(testValues);
  });

  test('pg-enqueue', async () => {
    const client = new Client(config.systemDatabaseUrl);
    let wfid: string;

    try {
      await client.connect();

      // Use PostgreSQL function to enqueue
      const enqueueResult = await client.query<{ enqueue_workflow: string }>(
        `
        SELECT dbos.enqueue_workflow(
          'testWorkflow', 
          'crdb-test-queue', 
          ARRAY[$1::JSON], 
          '{}'::JSON, 
          'CRDBTestClass'
        )
      `,
        [JSON.stringify('queued')],
      );

      expect(enqueueResult.rowCount).toEqual(1);
      wfid = enqueueResult.rows[0].enqueue_workflow;
    } finally {
      await client.end();
    }

    const handle = DBOS.retrieveWorkflow<string>(wfid);
    const status = await handle.getStatus();
    expect(status).toBeDefined();

    const result = await handle.getResult();
    expect(result).toBe('QUEUED');
  });

  test('pg-send', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).receiveWorkflow();

    const client = new Client(config.systemDatabaseUrl);
    try {
      await client.connect();

      await client.query<{ enqueue_workflow: string }>(`SELECT dbos.send_message($1, $2)`, [
        handle.workflowID,
        JSON.stringify('hello'),
      ]);
    } finally {
      await client.end();
    }
    expect(await handle.getResult()).toBe('hello');
  });

  test('retention', async () => {
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const client = new Client(config.systemDatabaseUrl);
    // CockroachDB rejects VACUUM outright, so a round that tried would warn on every table,
    // every time. Silence is what proves the round skips it rather than attempting it.
    const warn = jest.spyOn(sysdb.logger, 'warn');
    try {
      await client.connect();
      // The describe shares one database, so earlier tests leave completed rows behind.
      // Collect them first, and scope every assertion below to the rows this test makes.
      await garbageCollect(sysdb, cutoffPastAllCompletions());
      const ours = new Set<string>();
      const payloadIDs = async (table: string) => {
        const { rows } = await client.query<{ workflow_uuid: string }>(`SELECT workflow_uuid FROM dbos.${table}`);
        return new Set(rows.map((r) => r.workflow_uuid).filter((id) => ours.has(id)));
      };

      // Enqueued first, so these are the oldest rows. A delay keeps them DELAYED, so the
      // status sweep cannot touch them however far the cutoff reaches: they are stragglers.
      const stragglers: string[] = [];
      for (let i = 0; i < 2; i++) {
        const handle = await DBOS.startWorkflow(CRDBTestClass, {
          queueName: testQueueName,
          enqueueOptions: { delaySeconds: 60 },
        }).testWorkflow(`crdb-straggler-${i}`);
        stragglers.push(handle.workflowID);
        ours.add(handle.workflowID);
      }

      const rowsThreshold = 3;
      const completed: string[] = [];
      for (let i = 0; i < 8; i++) {
        const handle = await DBOS.startWorkflow(CRDBTestClass).testWorkflow(`crdb-gc-${i}`);
        await expect(handle.getResult()).resolves.toBe(`CRDB-GC-${i}`);
        completed.push(handle.workflowID);
        ours.add(handle.workflowID);
      }
      expect(await payloadIDs('workflow_input')).toEqual(ours);

      // Inside a held lock: CockroachDB stubs advisory locks out, so a round must still run
      // rather than skip itself into never collecting.
      const held = await sysdb.acquireRetentionLock();
      expect(held).toBeDefined();
      try {
        await garbageCollect(sysdb, null, rowsThreshold, { batchSize: 2 });
      } finally {
        await held!.release();
      }

      // The newest completed rows survive, as do the stragglers the sweep cannot touch.
      const collected = new Set(completed.slice(0, -rowsThreshold));
      const retained = new Set([...completed.slice(-rowsThreshold), ...stragglers]);
      const listed = new Set((await DBOS.listWorkflows({})).map((w) => w.workflowID).filter((id) => ours.has(id)));
      expect(listed).toEqual(retained);

      // Payloads follow their workflows: collected ones gone, in-flight ones spared.
      expect(await payloadIDs('workflow_input')).toEqual(retained);
      const outputs = await payloadIDs('workflow_output');
      expect([...outputs].filter((id) => collected.has(id))).toEqual([]);

      expect(warn.mock.calls.flat().join(' ')).not.toMatch(/vacuum/i);

      for (const workflowID of stragglers) {
        await DBOS.cancelWorkflow(workflowID);
      }
    } finally {
      warn.mockRestore();
      await client.end();
    }
  });
});
