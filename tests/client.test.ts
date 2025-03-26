import { workflow_status } from '../schemas/system_db_schema';
import { DBOS, DBOSConfig, DBOSClient, WorkflowQueue } from '../src';
import { globalParams, sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { Client, PoolConfig } from 'pg';

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
  static async sendTestCrash(topic?: string) {
    await DBOS.sleepSeconds(5);
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

describe('DBOSClient', () => {
  let config: DBOSConfig;
  let poolConfig: PoolConfig;
  let system_database: string;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    poolConfig = config.poolConfig!;
    system_database = config.system_database!;
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('DBOSClient-enqueue-appVer-notSet)', async () => {
    const client = new DBOSClient(poolConfig, system_database);
    const wfid = `client-enqueue-${Date.now()}`;

    try {
      await client.init();
      await client.enqueue<Parameters<EnqueueTest>>(
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

    const dbClient = new Client({ ...poolConfig, database: system_database });
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
  }, 10000);

  test('DBOSClient-enqueue-appVer-set', async () => {
    const client = new DBOSClient(poolConfig, system_database);
    const wfid = `client-enqueue-${Date.now()}`;

    try {
      await client.init();
      await client.enqueue<Parameters<EnqueueTest>>(
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

      const handle = DBOS.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }

    const dbClient = new Client({ ...poolConfig, database: system_database });
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
  }, 10000);

  test('DBOSClient-enqueue-wrong-appVer', async () => {
    const client = new DBOSClient(poolConfig, system_database);

    try {
      await client.init();
      await client.enqueue<Parameters<EnqueueTest>>(
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

    await sleepms(10000);

    const dbClient = new Client({ ...poolConfig, database: system_database });
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

    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

    const client = new DBOSClient(poolConfig, system_database);
    try {
      await client.send<string>(workflowID, message, topic);
    } finally {
      await client.destroy();
    }

    const result = await handle.getResult();
    expect(result).toBe(message);
  }, 10000);

  test('DBOSClient-send-no-topic', async () => {
    const now = Date.now();
    const workflowID = `client-send-${now}`;
    const message = `Hello, DBOS! (${now})`;

    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest();

    const client = new DBOSClient(poolConfig, system_database);
    try {
      await client.send<string>(workflowID, message);
    } finally {
      await client.destroy();
    }

    const result = await handle.getResult();
    expect(result).toBe(message);
  }, 10000);

  // test('DBOSClient-send-failure', async () => {
  //   const now = Date.now();
  //   const workflowID = `client-send-failure-${now}`;
  //   const topic = `test-topic-${now}`;
  //   const message = `Hello, DBOS! (${now})`;
  //   const idempotencyKey = `idempotency-key-${now}`;

  //   await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);
  //   // shutdown DBOS w/o waiting for pending workflows so we can simulate a failure
  //   await destroyExecutorGlobalInstance();
  //   await DBOS.shutdown();

  //   const client = new DBOSClient(poolConfig, system_database);
  //   const dbClient = new Client({ ...poolConfig, database: system_database });
  //   try {
  //     await dbClient.connect();
  //     await client.send<string>(workflowID, message, topic, idempotencyKey);

  //     const sendWFID = `${workflowID}-${idempotencyKey}`;
  //     // simulate a crash in send by deleting the results of the send operation, leaving just the WF status table result
  //     const res1 = await dbClient.query('DELETE FROM dbos.operation_outputs WHERE workflow_uuid = $1', [sendWFID]);
  //     expect(res1.rowCount).toBe(1);
  //     const res2 = await dbClient.query('DELETE FROM dbos.notifications WHERE destination_uuid = $1', [workflowID]);
  //     expect(res2.rowCount).toBe(1);

  //     await client.send<string>(workflowID, message, topic, idempotencyKey);
  //     const res3 = await dbClient.query('SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1', [sendWFID]);
  //     expect(res3.rows).toHaveLength(1);
  //     expect(res3.rows[0].recovery_attempts).toBe('2');
  //   } finally {
  //     await dbClient.end();
  //     await client.destroy();
  //   }

  //   await DBOS.launch();
  //   const handle = DBOS.retrieveWorkflow<string>(workflowID);
  //   const result = await handle.getResult();
  //   expect(result).toBe(message);
  // }, 10000);

  // test('DBOSClient-send-idempotent', async () => {
  //   const now = Date.now();
  //   const workflowID = `client-send-${now}`;
  //   const topic = `test-topic-${now}`;
  //   const message = `Hello, DBOS! (${now})`;
  //   const idempotencyKey = `idempotency-key-${now}`;

  //   await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);
  //   await destroyExecutorGlobalInstance();
  //   await DBOS.shutdown();

  //   const client = new DBOSClient(poolConfig, system_database);
  //   try {
  //     await client.send<string>(workflowID, message, topic, idempotencyKey);
  //     await client.send<string>(workflowID, message, topic, idempotencyKey);
  //   } finally {
  //     await client.destroy();
  //   }

  //   const dbClient = new Client({ ...poolConfig, database: system_database });
  //   try {
  //     await dbClient.connect();
  //     const res = await dbClient.query('SELECT * FROM dbos.notifications WHERE destination_uuid = $1', [workflowID]);
  //     expect(res.rows).toHaveLength(1);
  //   } finally {
  //     await dbClient.end();
  //   }

  //   await DBOS.launch();
  //   const handle = DBOS.retrieveWorkflow<string>(workflowID);
  //   const result = await handle.getResult();
  //   expect(result).toBe(message);
  // }, 10000);

  test('DBOSClient-getEvent-while-running', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    const client = new DBOSClient(poolConfig, system_database);
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value);
      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(value);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);
    } finally {
      await client.destroy();
    }
  }, 30000);

  test('DBOSClient-getEvent-when-finished', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    const client = new DBOSClient(poolConfig, system_database);
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);

      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(value);
    } finally {
      await client.destroy();
    }
  }, 30000);

  test('DBOSClient-getEvent-update-while-running', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    const client = new DBOSClient(poolConfig, system_database);
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
  }, 30000);

  test('DBOSClient-getEvent-update-when-finished', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    const client = new DBOSClient(poolConfig, system_database);
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value, true);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);

      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(`updated-${value}`);
    } finally {
      await client.destroy();
    }
  }, 30000);

  test('DBOSClient-retrieve-workflow', async () => {
    const wfid = `client-retrieve-${Date.now()}`;
    await DBOS.startWorkflow(ClientTest, { workflowID: wfid }).enqueueTest(42, 'test', {
      first: 'John',
      last: 'Doe',
      age: 30,
    });

    const client = new DBOSClient(poolConfig, system_database);
    try {
      await client.init();
      const handle = client.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-retrieve-workflow-done', async () => {
    const wfid = `client-retrieve-done-${Date.now()}`;
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID: wfid }).enqueueTest(42, 'test', {
      first: 'John',
      last: 'Doe',
      age: 30,
    });
    const result1 = await handle.getResult();
    expect(result1).toBe('42-test-{"first":"John","last":"Doe","age":30}');

    const client = new DBOSClient(poolConfig, system_database);
    try {
      await client.init();
      const handle = client.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }
  }, 30000);
});
