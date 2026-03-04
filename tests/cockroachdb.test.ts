import { DBOS, WorkflowQueue } from '../src/';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { randomUUID } from 'node:crypto';
import { Client } from 'pg';

const cockroachdbUrl = process.env.DBOS_COCKROACHDB_URL;
const describeIf = cockroachdbUrl ? describe : describe.skip;

const testQueue = new WorkflowQueue('crdb-test-queue');

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

    const client = new Client({ connectionString: cockroachdbUrl });
    await client.connect();
    await client.query('DROP DATABASE IF EXISTS dbos_test');
    await client.query('CREATE DATABASE dbos_test');
    await client.end();
    config = {
      name: 'cockroachdb-test',
      systemDatabaseUrl,
      useListenNotify: false,
    };
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    const sysDB = DBOSExecutor.globalInstance!.systemDatabase;
    sysDB.dbPollingIntervalResultMs = 100;
    sysDB.dbPollingIntervalEventMs = 100;
  }, 60000);

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('workflow-with-step', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).testWorkflow('hello');
    const result = await handle.getResult();
    expect(result).toBe('HELLO');
  });

  test('workflow-on-queue', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass, { queueName: testQueue.name }).testWorkflow('queued');
    expect(await handle.getResult()).toBe('QUEUED');
    const status = await handle.getStatus();
    expect(status?.queueName).toBe('crdb-test-queue');
  });

  test('send-and-recv', async () => {
    const handle = await DBOS.startWorkflow(CRDBTestClass).receiveWorkflow();
    await DBOS.send(handle.workflowID, 'hello');
    expect(await handle.getResult()).toBe('hello');
  }, 10000);

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
});
