import { workflow_status } from '../schemas/system_db_schema';
import { DBOS, DBOSConfig, DBOSClient, WorkflowQueue } from '../src';
import { globalParams, sleepms } from '../src/utils';
import { generateDBOSTestConfig, recoverPendingWorkflows, setUpDBOSTestDb } from './helpers';
import { Client, PoolConfig } from 'pg';
import { spawnSync } from 'child_process';
import { DBOSQueueDuplicatedError } from '../src/error';

const _queue = new WorkflowQueue('testQueue');

class ClientTest {
  @DBOS.workflow()
  static async enqueueTest(
    numVal: number,
    strVal: string,
    objVal: { first: string; last: string; age: number },
  ): Promise<string> {
    return Promise.resolve(`${numVal}-${strVal}-${JSON.stringify(objVal)}`);
  }

  @DBOS.workflow()
  static async sendTest(topic?: string) {
    return await DBOS.recv<string>(topic, 60);
  }

  @DBOS.workflow()
  static async eventTest(key: string, value: string, update: boolean = false) {
    await DBOS.setEvent(key, value);
    await DBOS.sleepSeconds(5);
    if (update) {
      await DBOS.setEvent(key, `updated-${value}`);
    }
    return `${key}-${value}`;
  }
}

type EnqueueTest = typeof ClientTest.enqueueTest;

function runClientSendWorker(workflowID: string, topic: string, appVersion: string) {
  const _child = spawnSync('npx', ['ts-node', './tests/clientSendWorker.ts', workflowID, topic], {
    cwd: process.cwd(),
    env: { ...process.env, DBOS__APPVERSION: appVersion },
  });
}

describe('DBOSClient', () => {
  let config: DBOSConfig;
  let database_url: string;
  let poolConfig: PoolConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    const $poolConfig = structuredClone(config.poolConfig!);
    $poolConfig.connectionString = undefined;
    database_url = `postgres://${$poolConfig.user}:${$poolConfig.password as string}@${$poolConfig.host}:${$poolConfig.port}/${$poolConfig.database}`;
    poolConfig = { ...$poolConfig, database: config.system_database };
    await setUpDBOSTestDb(config);
  });

  beforeEach(() => {
    DBOS.setConfig(config);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('DBOSClient-enqueue-idempotent', async () => {
    const client = await DBOSClient.create(database_url);
    const wfid = `client-enqueue-idempotent-${Date.now()}`;

    try {
      await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID: wfid,
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );

      await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID: wfid,
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );
    } finally {
      await client.destroy();
    }

    const dbClient = new Client(poolConfig);
    try {
      await dbClient.connect();
      const resultBefore = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(resultBefore.rows).toHaveLength(1);
      expect(resultBefore.rows[0].workflow_uuid).toBe(wfid);
      expect(resultBefore.rows[0].status).toBe('ENQUEUED');
      expect(resultBefore.rows[0].application_version).toBeNull();

      await DBOS.launch();
      const handle = DBOS.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const wfresult = await handle.getResult();
      expect(wfresult).toBe('42-test-{"first":"John","last":"Doe","age":30}');

      const resultAfter = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(resultAfter.rows).toHaveLength(1);
      expect(resultAfter.rows[0].workflow_uuid).toBe(wfid);
      expect(resultAfter.rows[0].status).toBe('SUCCESS');
      expect(resultAfter.rows[0].application_version).toBe(globalParams.appVersion);
    } finally {
      await dbClient.end();
    }
  }, 20000);

  test('DBOSClient-enqueue-appVer-notSet', async () => {
    const client = await DBOSClient.create(database_url);
    const wfid = `client-enqueue-${Date.now()}`;

    await DBOS.launch();

    try {
      await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID: wfid,
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );

      const handle = DBOS.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }

    const dbClient = new Client(poolConfig);
    try {
      await dbClient.connect();
      const result = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].workflow_uuid).toBe(wfid);
      expect(result.rows[0].status).toBe('SUCCESS');
      expect(result.rows[0].application_version).toBe(globalParams.appVersion);
    } finally {
      await dbClient.end();
    }
  }, 20000);

  test('DBOSClient-enqueue-and-get-result', async () => {
    const client = await DBOSClient.create(database_url);

    await DBOS.launch();

    let wfid: string;
    try {
      const handle = await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );
      wfid = handle.workflowID;

      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }

    const dbClient = new Client(poolConfig);
    try {
      await dbClient.connect();
      const result = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].workflow_uuid).toBe(wfid);
      expect(result.rows[0].status).toBe('SUCCESS');
      expect(result.rows[0].application_version).toBe(globalParams.appVersion);
    } finally {
      await dbClient.end();
    }
  }, 20000);

  test('DBOSClient-enqueue-dedupid', async () => {
    const client = await DBOSClient.create(database_url);

    await DBOS.launch();

    try {
      const handle = await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          deduplicationID: '12345',
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );

      let expectedError = false;
      try {
        await client.enqueue<EnqueueTest>(
          {
            workflowName: 'enqueueTest',
            workflowClassName: 'ClientTest',
            queueName: 'testQueue',
            deduplicationID: '12345',
          },
          42,
          'test',
          { first: 'John', last: 'Doe', age: 30 },
        );
      } catch (e) {
        expectedError = true;
        expect(e).toBeInstanceOf(DBOSQueueDuplicatedError);
      }
      expect(expectedError).toBe(true);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }
  }, 20000);

  test('DBOSClient-enqueue-appVer-set', async () => {
    const client = await DBOSClient.create(database_url);
    const wfid = `client-enqueue-${Date.now()}`;

    await DBOS.launch();

    try {
      await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID: wfid,
          appVersion: globalParams.appVersion,
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );
    } finally {
      await client.destroy();
    }

    const handle = DBOS.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
    const result = await handle.getResult();
    expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');

    const dbClient = new Client(poolConfig);
    try {
      await dbClient.connect();
      const result = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].workflow_uuid).toBe(wfid);
      expect(result.rows[0].status).toBe('SUCCESS');
      expect(result.rows[0].application_version).toBe(globalParams.appVersion);
    } finally {
      await dbClient.end();
    }
  }, 20000);

  test('DBOSClient-enqueue-wrong-appVer', async () => {
    const client = await DBOSClient.create(database_url);

    try {
      await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          appVersion: '1234567890ABCDEF',
        },
        422,
        'test2',
        { first: 'John2', last: 'Doe2', age: 32 },
      );
    } finally {
      await client.destroy();
    }

    await DBOS.launch();
    await sleepms(10000);

    const dbClient = new Client(poolConfig);
    try {
      await dbClient.connect();
      const result = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE application_version = $1',
        ['1234567890ABCDEF'],
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].status).toBe('ENQUEUED');
      expect(result.rows[0].application_version).toBe('1234567890ABCDEF');
    } finally {
      await dbClient.end();
    }
  }, 20000);

  test('DBOSClient-send-topic', async () => {
    const now = Date.now();
    const workflowID = `client-send-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;

    await DBOS.launch();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

    const client = await DBOSClient.create(database_url);
    try {
      await client.send<string>(workflowID, message, topic);
    } finally {
      await client.destroy();
    }

    const result = await handle.getResult();
    expect(result).toBe(message);
  });

  test('DBOSClient-send-no-topic', async () => {
    const now = Date.now();
    const workflowID = `client-send-${now}`;
    const message = `Hello, DBOS! (${now})`;

    await DBOS.launch();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest();

    const client = await DBOSClient.create(database_url);
    try {
      await client.send<string>(workflowID, message);
    } finally {
      await client.destroy();
    }

    const result = await handle.getResult();
    expect(result).toBe(message);
  });

  test('DBOSClient-send-failure', async () => {
    const now = Date.now();
    const workflowID = `client-send-failure-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;
    const idempotencyKey = `idempotency-key-${now}`;
    const sendWFID = `${workflowID}-${idempotencyKey}`;

    await DBOS.launch();
    runClientSendWorker(workflowID, topic, globalParams.appVersion);

    const client = await DBOSClient.create(database_url);
    const dbClient = new Client(poolConfig);
    try {
      await dbClient.connect();
      await client.send<string>(workflowID, message, topic, idempotencyKey);

      // simulate a crash in send by deleting the results of the send operation, leaving just the WF status table result
      const res1 = await dbClient.query('DELETE FROM dbos.operation_outputs WHERE workflow_uuid = $1', [sendWFID]);
      expect(res1.rowCount).toBe(1);
      const res2 = await dbClient.query('DELETE FROM dbos.notifications WHERE destination_uuid = $1', [workflowID]);
      expect(res2.rowCount).toBe(1);
      const res3 = await dbClient.query<{ recovery_attempts: string }>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [sendWFID],
      );
      expect(res3.rows).toHaveLength(1);
      expect(res3.rows[0].recovery_attempts).toBe('1');

      await client.send<string>(workflowID, message, topic, idempotencyKey);
      const res4 = await dbClient.query<{ recovery_attempts: string }>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [sendWFID],
      );
      expect(res4.rows).toHaveLength(1);
      expect(res4.rows[0].recovery_attempts).toBe('2');
    } finally {
      await dbClient.end();
      await client.destroy();
    }

    await recoverPendingWorkflows();
    const handle = DBOS.retrieveWorkflow<string>(workflowID);
    const result = await handle.getResult();
    expect(result).toBe(message);
  }, 30000);

  test('DBOSClient-send-idempotent', async () => {
    const now = Date.now();
    const workflowID = `client-send-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;
    const idempotencyKey = `idempotency-key-${now}`;
    const sendWFID = `${workflowID}-${idempotencyKey}`;

    await DBOS.launch();
    runClientSendWorker(workflowID, topic, globalParams.appVersion);

    const client = await DBOSClient.create(database_url);
    try {
      await client.send<string>(workflowID, message, topic, idempotencyKey);
      await client.send<string>(workflowID, message, topic, idempotencyKey);
    } finally {
      await client.destroy();
    }

    const dbClient = new Client(poolConfig);
    try {
      await dbClient.connect();
      const res = await dbClient.query('SELECT * FROM dbos.notifications WHERE destination_uuid = $1', [workflowID]);
      expect(res.rows).toHaveLength(1);
      const res2 = await dbClient.query('SELECT * FROM dbos.operation_outputs WHERE workflow_uuid = $1', [sendWFID]);
      expect(res2.rows).toHaveLength(1);
      const res3 = await dbClient.query('SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1', [sendWFID]);
      expect(res3.rows).toHaveLength(1);
    } finally {
      await dbClient.end();
    }

    await recoverPendingWorkflows();
    const handle = DBOS.retrieveWorkflow<string>(workflowID);
    const result = await handle.getResult();
    expect(result).toBe(message);
  }, 30000);

  test('DBOSClient-getEvent-while-running', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    const client = await DBOSClient.create(database_url);
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value);
      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(value);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);
    } finally {
      await client.destroy();
    }
  }, 10000);

  test('DBOSClient-getEvent-when-finished', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    const client = await DBOSClient.create(database_url);
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);

      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(value);
    } finally {
      await client.destroy();
    }
  }, 10000);

  test('DBOSClient-getEvent-update-while-running', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    const client = await DBOSClient.create(database_url);
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value, true);
      let eventValue = await client.getEvent<string>(workflowID, key, 1);
      expect(eventValue).toBe(value);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);
      eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(`updated-${value}`);
    } finally {
      await client.destroy();
    }
  }, 10000);

  test('DBOSClient-getEvent-update-when-finished', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    const client = await DBOSClient.create(database_url);
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value, true);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);

      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(`updated-${value}`);
    } finally {
      await client.destroy();
    }
  }, 10000);

  test('DBOSClient-retrieve-workflow', async () => {
    const wfid = `client-retrieve-${Date.now()}`;

    await DBOS.launch();
    await DBOS.startWorkflow(ClientTest, { workflowID: wfid }).enqueueTest(42, 'test', {
      first: 'John',
      last: 'Doe',
      age: 30,
    });

    const client = await DBOSClient.create(database_url);
    try {
      const handle = client.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-retrieve-workflow-done', async () => {
    const wfid = `client-retrieve-done-${Date.now()}`;

    await DBOS.launch();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID: wfid }).enqueueTest(42, 'test', {
      first: 'John',
      last: 'Doe',
      age: 30,
    });
    const result1 = await handle.getResult();
    expect(result1).toBe('42-test-{"first":"John","last":"Doe","age":30}');

    const client = await DBOSClient.create(database_url);
    try {
      const handle = client.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }
  });
});
