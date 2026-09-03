import { workflow_status } from '../schemas/system_db_schema';
import { DBOS, DBOSClient, StatusString } from '../src';
import { globalParams, sleepms } from '../src/utils';
import {
  generateDBOSTestConfig,
  recoverPendingWorkflows,
  recoverWorkflow,
  retryUntilSuccess,
  setUpDBOSTestSysDb,
  setWfAndChildrenToPending,
} from './helpers';
import { Client, PoolConfig } from 'pg';
import { spawnSync } from 'child_process';
import {
  DBOSQueueDuplicatedError,
  DBOSAwaitedWorkflowCancelledError,
  DBOSAwaitedWorkflowExceededMaxRecoveryAttempts,
} from '../src/error';
import { randomUUID } from 'crypto';
import { DBOSConfig } from '../src/dbos-executor';
import { DEFAULT_POOL_SIZE } from '../src/system_database';

// Re-register the database-backed queue used by these tests every time DBOS
// is launched. Many tests in this file launch with their own setup, so this
// is invoked from each test rather than from a single `beforeEach`.
async function registerTestQueue(): Promise<void> {
  await DBOS.registerQueue('testQueue', { onConflict: 'always_update', priorityEnabled: true });
}

// Wait until every workflow under `prefix` reaches CANCELLED. When a parent
// times out, `handle.getResult()` rejects as soon as the parent is cancelled,
// but its children are cancelled asynchronously by the cascade, so a child can
// still read as PENDING immediately afterwards. Poll until the cascade settles.
async function waitForAllCancelled(
  client: DBOSClient,
  prefix: string,
  expectedCount: number,
): Promise<Awaited<ReturnType<DBOSClient['listWorkflows']>>> {
  const deadline = Date.now() + 10000;
  for (;;) {
    const statuses = await client.listWorkflows({ workflow_id_prefix: prefix });
    if (statuses.length === expectedCount && statuses.every((status) => status.status === StatusString.CANCELLED)) {
      return statuses;
    }
    if (Date.now() >= deadline) {
      return statuses; // Let the caller's assertions report the unsettled state.
    }
    await sleepms(100);
  }
}

class ClientTest {
  static inorder_results: string[] = [];

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

  @DBOS.workflow()
  static async priorityTest(input: string): Promise<string> {
    ClientTest.inorder_results.push(input);
    return Promise.resolve(input);
  }

  @DBOS.workflow()
  static async blockingWorkflow() {
    while (true) {
      await DBOS.sleep(100);
    }
  }

  @DBOS.workflow()
  static async blockingParentStart() {
    await DBOS.startWorkflow(ClientTest)
      .blockingWorkflow()
      .then((h) => h.getResult());
  }

  @DBOS.workflow()
  static async blockingParentDirect() {
    await ClientTest.blockingWorkflow();
  }
}

const DLQ_MAX_RECOVERY_ATTEMPTS = 2;

class ClientDLQTest {
  // Succeeds when run, but is forced back to PENDING repeatedly until it exhausts recovery attempts (DLQ).
  @DBOS.workflow({ maxRecoveryAttempts: DLQ_MAX_RECOVERY_ATTEMPTS })
  static async dlqWorkflow(): Promise<string> {
    return Promise.resolve('should-not-be-returned');
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
  let systemDatabaseUrl: string;
  let poolConfig: PoolConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    expect(config.systemDatabaseUrl).toBeDefined();
    systemDatabaseUrl = config.systemDatabaseUrl!;
    poolConfig = { connectionString: config.systemDatabaseUrl };
    await setUpDBOSTestSysDb(config);
  });

  beforeEach(() => {
    DBOS.setConfig(config);
  });

  // Rows written inside a caller's transaction must be invisible to everyone else until it commits,
  // so every in-transaction test reads back through a second connection of its own.
  async function countFromOtherConnection(sql: string, params: unknown[]): Promise<number> {
    const other = new Client(poolConfig);
    await other.connect();
    try {
      const { rowCount } = await other.query(sql, params);
      return rowCount ?? 0;
    } finally {
      await other.end();
    }
  }

  const countWorkflows = (workflowID: string) =>
    countFromOtherConnection('SELECT 1 FROM dbos.workflow_status WHERE workflow_uuid = $1', [workflowID]);
  const countNotifications = (destinationID: string) =>
    countFromOtherConnection('SELECT 1 FROM dbos.notifications WHERE destination_uuid = $1', [destinationID]);

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('create-passes-through-pool-size-and-polling-concurrency', async () => {
    // No DBOS.launch() needed: create only constructs the SystemDatabase, whose
    // pool connects lazily, so we can inspect the wiring without touching the DB.
    const client = await DBOSClient.create({
      systemDatabaseUrl,
      systemDatabaseSchemaName: 'custom_schema',
      systemDatabasePoolSize: 8,
      systemDatabasePollingConcurrency: 3,
    });
    try {
      const sysdb = client['systemDatabase'];
      expect(sysdb.pool.options.max).toBe(8);
      expect(sysdb.schemaName).toBe('custom_schema');
      // The semaphore is initialized with the requested `systemDatabasePollingConcurrency` (3),
      // not the half-the-pool default (which would be 4 for a pool size of 8).
      expect(sysdb.pollLimiter['available']).toBe(3);
    } finally {
      await client.destroy();
    }

    // When the optional parameters are omitted, the defaults apply: the default
    // pool size and a polling concurrency of half the pool.
    const defaultClient = await DBOSClient.create({ systemDatabaseUrl });
    try {
      const sysdb = defaultClient['systemDatabase'];
      expect(sysdb.pool.options.max).toBe(DEFAULT_POOL_SIZE);
      expect(sysdb.pollLimiter['available']).toBe(Math.floor(DEFAULT_POOL_SIZE / 2));
    } finally {
      await defaultClient.destroy();
    }
  });

  test('enqueue-timeout-simple', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = randomUUID();

    try {
      const handle = await client.enqueue<typeof ClientTest.blockingWorkflow>({
        workflowName: 'blockingWorkflow',
        workflowClassName: 'ClientTest',
        queueName: 'testQueue',
        workflowID: wfid,
        workflowTimeoutMS: 1000,
      });
      await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(wfid));

      const wfstatus = await client.getWorkflow(wfid);
      expect(wfstatus?.status).toBe(StatusString.CANCELLED);
    } finally {
      await client.destroy();
    }
  });

  test('enqueue-timeout-direct-parent', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = randomUUID();

    try {
      const handle = await client.enqueue<typeof ClientTest.blockingParentDirect>({
        workflowName: 'blockingParentDirect',
        workflowClassName: 'ClientTest',
        queueName: 'testQueue',
        workflowID: wfid,
        workflowTimeoutMS: 1000,
      });
      await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(wfid));

      const statuses = await waitForAllCancelled(client, wfid, 2);
      expect(statuses.length).toBe(2);
      statuses.forEach((status) => {
        expect(status.status).toBe(StatusString.CANCELLED);
      });
      const deadline = statuses[0].deadlineEpochMS;
      statuses.slice(1).forEach((status) => {
        expect(status.deadlineEpochMS).toBe(deadline);
      });
    } finally {
      await client.destroy();
    }
  });

  test('enqueue-timeout-startwf-parent', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = randomUUID();

    try {
      const handle = await client.enqueue<typeof ClientTest.blockingParentStart>({
        workflowName: 'blockingParentStart',
        workflowClassName: 'ClientTest',
        queueName: 'testQueue',
        workflowID: wfid,
        workflowTimeoutMS: 1000,
      });
      await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(wfid));

      const statuses = await waitForAllCancelled(client, wfid, 2);
      expect(statuses.length).toBe(2);
      statuses.forEach((status) => {
        expect(status.status).toBe(StatusString.CANCELLED);
      });
      const deadline = statuses[0].deadlineEpochMS;
      statuses.slice(1).forEach((status) => {
        expect(status.deadlineEpochMS).toBe(deadline);
      });
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-enqueue-idempotent', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl });
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
      await registerTestQueue();
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
  });

  test('DBOSClient-enqueue-appVer-notSet', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = `client-enqueue-${Date.now()}`;

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
  });

  test('DBOSClient-enqueue-and-get-result', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });

    const version = globalParams.appVersion;

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

      let result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
      // Shut down DBOS and retrieve again.
      // It should work because the client and DBOS are isolated.
      await DBOS.shutdown();
      result = await handle.getResult();
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
      expect(result.rows[0].application_version).toBe(version);
    } finally {
      await dbClient.end();
    }
  });

  test('DBOSClient-getResult-throws-on-max-recovery-attempts-exceeded', async () => {
    // A workflow past its max recovery attempts must make client getResult throw, not silently return null.
    await DBOS.launch();
    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      const handle = await DBOS.startWorkflow(ClientDLQTest).dlqWorkflow();
      await handle.getResult();

      // Force back to PENDING and recover repeatedly until it exhausts recovery attempts and lands in the DLQ.
      for (let i = 0; i < DLQ_MAX_RECOVERY_ATTEMPTS; i++) {
        await setWfAndChildrenToPending(handle.workflowID, false);
        await (await recoverWorkflow(handle.workflowID)).getResult();
      }
      await setWfAndChildrenToPending(handle.workflowID, false);
      await recoverPendingWorkflows();
      // Recovery re-enqueues, so the DLQ transition happens when the queue dequeues the workflow.
      await retryUntilSuccess(async () => {
        expect((await handle.getStatus())?.status).toBe(StatusString.MAX_RECOVERY_ATTEMPTS_EXCEEDED);
      });

      await expect(client.retrieveWorkflow(handle.workflowID).getResult()).rejects.toThrow(
        DBOSAwaitedWorkflowExceededMaxRecoveryAttempts,
      );
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-enqueue-and-get-result-portable', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });

    const version = globalParams.appVersion;

    let wfid: string;
    try {
      const handle = await client.enqueue<EnqueueTest>(
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          serializationType: 'portable',
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );
      wfid = handle.workflowID;

      let result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
      // Shut down DBOS and retrieve again.
      // It should work because the client and DBOS are isolated.
      await DBOS.shutdown();
      result = await handle.getResult();
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
      expect(result.rows[0].application_version).toBe(version);
    } finally {
      await dbClient.end();
    }
  });

  test('DBOSClient-enqueue-dedupid', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });

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
  });

  test('DBOSClient-enqueue-priority', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });

    type PriorityTest = typeof ClientTest.priorityTest;

    try {
      const handle1 = await client.enqueue<PriorityTest>(
        {
          workflowName: 'priorityTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
        },
        'abc',
      );

      const handle2 = await client.enqueue<PriorityTest>(
        {
          workflowName: 'priorityTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          priority: 5,
        },
        'def',
      );

      const handle3 = await client.enqueue<PriorityTest>(
        {
          workflowName: 'priorityTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          priority: 1,
        },
        'ghi',
      );

      const result1 = await handle1.getResult();
      const result2 = await handle2.getResult();
      const result3 = await handle3.getResult();

      expect(result1).toBe('abc');
      expect(result2).toBe('def');
      expect(result3).toBe('ghi');
      // They should be processed in order of priority
      expect(ClientTest.inorder_results).toEqual(['abc', 'ghi', 'def']);
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-enqueue-appVer-set', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = `client-enqueue-${Date.now()}`;

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
  });

  test('DBOSClient-enqueue-wrong-appVer', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl });

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
    await registerTestQueue();
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
  });

  test('DBOSClient-send-topic', async () => {
    const now = Date.now();
    const workflowID = `client-send-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;

    await DBOS.launch();
    await registerTestQueue();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

    const client = await DBOSClient.create({ systemDatabaseUrl });
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
    await registerTestQueue();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      await client.send<string>(workflowID, message);
    } finally {
      await client.destroy();
    }

    const result = await handle.getResult();
    expect(result).toBe(message);
  });

  test('DBOSClient-send-idempotent', async () => {
    const now = Date.now();
    const workflowID = `client-send-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;
    const idempotencyKey = `idempotency-key-${now}`;

    await DBOS.launch();
    await registerTestQueue();
    runClientSendWorker(workflowID, topic, globalParams.appVersion);

    const client = await DBOSClient.create({ systemDatabaseUrl });
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
    } finally {
      await dbClient.end();
    }

    await recoverPendingWorkflows();
    const handle = DBOS.retrieveWorkflow<string>(workflowID);
    const result = await handle.getResult();
    expect(result).toBe(message);
  });

  test('DBOSClient-send-portable', async () => {
    const now = Date.now();
    const workflowID = `client-send-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;

    await DBOS.launch();
    await registerTestQueue();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      await client.send<string>(workflowID, message, topic, undefined, { serializationType: 'portable' });
    } finally {
      await client.destroy();
    }

    const result = await handle.getResult();
    expect(result).toBe(message);
  });

  test('DBOSClient-enqueueInTransaction-commit', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = `client-enqueue-tx-${Date.now()}`;
    const txClient = new Client(poolConfig);
    const otherClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await otherClient.connect();
      await txClient.query('BEGIN');

      const handle = await client.enqueueInTransaction<EnqueueTest>(
        txClient,
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
      expect(handle.workflowID).toBe(wfid);

      // The row belongs to the caller's transaction until it commits.
      const beforeCommit = await otherClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(beforeCommit.rows).toHaveLength(0);

      await txClient.query('COMMIT');
      expect(await handle.getResult()).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await txClient.end();
      await otherClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-enqueueInTransaction-rollback', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = `client-enqueue-tx-rollback-${Date.now()}`;
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      await client.enqueueInTransaction<EnqueueTest>(
        txClient,
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
      await txClient.query('ROLLBACK');

      const result = await txClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(result.rows).toHaveLength(0);
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-enqueueInTransaction-idempotent', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = `client-enqueue-tx-idempotent-${Date.now()}`;
    const options = {
      workflowName: 'enqueueTest',
      workflowClassName: 'ClientTest',
      queueName: 'testQueue',
      workflowID: wfid,
    };
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      const handle = await client.enqueueInTransaction<EnqueueTest>(txClient, options, 42, 'test', {
        first: 'John',
        last: 'Doe',
        age: 30,
      });
      // Re-enqueuing the same workflow ID keeps the inputs of the first enqueue.
      const handle2 = await client.enqueueInTransaction<EnqueueTest>(txClient, options, 99, 'other', {
        first: 'Jane',
        last: 'Roe',
        age: 40,
      });
      await txClient.query('COMMIT');

      expect(await handle.getResult()).toBe('42-test-{"first":"John","last":"Doe","age":30}');
      expect(await handle2.getResult()).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-enqueueInTransaction-deduplication', async () => {
    // DBOS is not launched, so the enqueued workflow keeps holding the deduplication slot.
    const client = await DBOSClient.create({ systemDatabaseUrl });
    const now = Date.now();
    const wfid = `client-enqueue-tx-dedup-${now}`;
    const wfid2 = `${wfid}-other`;
    const options = {
      workflowName: 'enqueueTest',
      workflowClassName: 'ClientTest',
      queueName: 'testQueue',
      workflowID: wfid,
      deduplicationID: `dedup-${now}`,
    };
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      await client.enqueueInTransaction<EnqueueTest>(txClient, options, 42, 'test', {
        first: 'John',
        last: 'Doe',
        age: 30,
      });
      expect(await countWorkflows(wfid)).toBe(0);
      await txClient.query('COMMIT');

      await txClient.query('BEGIN');
      await expect(
        client.enqueueInTransaction<EnqueueTest>(txClient, { ...options, workflowID: wfid2 }, 42, 'test', {
          first: 'John',
          last: 'Doe',
          age: 30,
        }),
      ).rejects.toThrow(DBOSQueueDuplicatedError);
      // The conflict aborted the caller's transaction, which is the caller's to roll back.
      await txClient.query('ROLLBACK');

      const result = await txClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = ANY($1)',
        [[wfid, wfid2]],
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].workflow_uuid).toBe(wfid);
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-enqueueInTransaction-rejects-return-existing', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl });
    const now = Date.now();
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      await expect(
        client.enqueueInTransaction<EnqueueTest>(
          txClient,
          {
            workflowName: 'enqueueTest',
            workflowClassName: 'ClientTest',
            queueName: 'testQueue',
            workflowID: `client-enqueue-tx-singleton-${now}`,
            deduplicationID: `dedup-${now}`,
            duplicationPolicy: 'return-existing',
          },
          42,
          'test',
          { first: 'John', last: 'Doe', age: 30 },
        ),
      ).rejects.toThrow("`duplicationPolicy: 'return-existing'` is not supported in a caller-owned transaction");
      await txClient.query('ROLLBACK');
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-enqueuePortableInTransaction-commit', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const wfid = `client-enqueue-portable-tx-${Date.now()}`;
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      const handle = await client.enqueuePortableInTransaction<string>(
        txClient,
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID: wfid,
        },
        [42, 'test', { first: 'John', last: 'Doe', age: 30 }],
      );
      expect(await countWorkflows(wfid)).toBe(0);

      // The row must carry the portable encoding, which is the whole point of this entry
      // point: a peer in another language has to be able to read these inputs.
      const written = await txClient.query<workflow_status>(
        `SELECT ws.serialization, wi.inputs
         FROM dbos.workflow_status ws
         JOIN dbos.workflow_input wi ON wi.workflow_uuid = ws.workflow_uuid
         WHERE ws.workflow_uuid = $1`,
        [wfid],
      );
      expect(written.rows[0].serialization).toBe('portable_json');
      expect(JSON.parse(written.rows[0].inputs)).toEqual({
        positionalArgs: [42, 'test', { first: 'John', last: 'Doe', age: 30 }],
      });

      await txClient.query('COMMIT');
      expect(await handle.getResult()).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-sendInTransaction-commit', async () => {
    const now = Date.now();
    const workflowID = `client-send-tx-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;

    await DBOS.launch();
    await registerTestQueue();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const txClient = new Client(poolConfig);
    const otherClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await otherClient.connect();
      await txClient.query('BEGIN');
      await client.sendInTransaction<string>(txClient, workflowID, message, topic);

      const beforeCommit = await otherClient.query('SELECT * FROM dbos.notifications WHERE destination_uuid = $1', [
        workflowID,
      ]);
      expect(beforeCommit.rows).toHaveLength(0);

      await txClient.query('COMMIT');
      expect(await handle.getResult()).toBe(message);
    } finally {
      await txClient.end();
      await otherClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-sendInTransaction-rollback', async () => {
    const now = Date.now();
    const workflowID = `client-send-tx-rollback-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;

    await DBOS.launch();
    await registerTestQueue();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      await client.sendInTransaction<string>(txClient, workflowID, `rolled back (${now})`, topic);
      await txClient.query('ROLLBACK');

      const rows = await txClient.query('SELECT * FROM dbos.notifications WHERE destination_uuid = $1', [workflowID]);
      expect(rows.rows).toHaveLength(0);

      // The workflow is still waiting, so only a committed send unblocks it.
      await client.send<string>(workflowID, message, topic);
      expect(await handle.getResult()).toBe(message);
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-sendInTransaction-idempotent', async () => {
    const now = Date.now();
    const workflowID = `client-send-tx-idempotent-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;
    const idempotencyKey = `idempotency-key-${now}`;

    await DBOS.launch();
    await registerTestQueue();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      await client.sendInTransaction<string>(txClient, workflowID, message, topic, idempotencyKey);
      await client.sendInTransaction<string>(txClient, workflowID, message, topic, idempotencyKey);
      expect(await countNotifications(workflowID)).toBe(0);
      await txClient.query('COMMIT');

      const rows = await txClient.query('SELECT * FROM dbos.notifications WHERE destination_uuid = $1', [workflowID]);
      expect(rows.rows).toHaveLength(1);
      expect(await handle.getResult()).toBe(message);
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-enqueue-and-send-in-transaction', async () => {
    await DBOS.launch();
    await registerTestQueue();

    const now = Date.now();
    const workflowID = `client-enqueue-send-tx-${now}`;
    const topic = `test-topic-${now}`;
    const message = `Hello, DBOS! (${now})`;

    const client = await DBOSClient.create({ systemDatabaseUrl });
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query('BEGIN');
      const handle = await client.enqueueInTransaction<typeof ClientTest.sendTest>(
        txClient,
        {
          workflowName: 'sendTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID,
        },
        topic,
      );
      await client.sendInTransaction<string>(txClient, workflowID, message, topic);
      // Neither half of the pair escapes before the caller commits.
      expect(await countWorkflows(workflowID)).toBe(0);
      expect(await countNotifications(workflowID)).toBe(0);
      await txClient.query('COMMIT');

      expect(await handle.getResult()).toBe(message);
    } finally {
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-in-transaction-with-caller-writes', async () => {
    // The documented use case: the caller's own rows and the DBOS work commit or roll back together.
    await DBOS.launch();
    await registerTestQueue();

    const now = Date.now();
    const table = `client_tx_orders_${now}`;
    const client = await DBOSClient.create({ systemDatabaseUrl });
    const txClient = new Client(poolConfig);
    try {
      await txClient.connect();
      await txClient.query(`CREATE TABLE ${table} (id TEXT PRIMARY KEY)`);

      // Rolled back together: neither the order nor the workflow survives.
      const rolledBackID = `client-tx-caller-rollback-${now}`;
      await txClient.query('BEGIN');
      await txClient.query(`INSERT INTO ${table} VALUES ($1)`, [rolledBackID]);
      await client.enqueueInTransaction<EnqueueTest>(
        txClient,
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID: rolledBackID,
        },
        1,
        'rolled-back',
        { first: 'John', last: 'Doe', age: 30 },
      );
      await txClient.query('ROLLBACK');

      expect(await countWorkflows(rolledBackID)).toBe(0);
      const rolledBackOrders = await txClient.query(`SELECT 1 FROM ${table} WHERE id = $1`, [rolledBackID]);
      expect(rolledBackOrders.rowCount).toBe(0);

      // Committed together: the order is durable and the workflow runs.
      const committedID = `client-tx-caller-commit-${now}`;
      await txClient.query('BEGIN');
      await txClient.query(`INSERT INTO ${table} VALUES ($1)`, [committedID]);
      const handle = await client.enqueueInTransaction<EnqueueTest>(
        txClient,
        {
          workflowName: 'enqueueTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowID: committedID,
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );
      expect(await countWorkflows(committedID)).toBe(0);
      await txClient.query('COMMIT');

      expect(await handle.getResult()).toBe('42-test-{"first":"John","last":"Doe","age":30}');
      const committedOrders = await txClient.query(`SELECT 1 FROM ${table} WHERE id = $1`, [committedID]);
      expect(committedOrders.rowCount).toBe(1);
    } finally {
      await txClient.query(`DROP TABLE IF EXISTS ${table}`).catch(() => {});
      await txClient.end();
      await client.destroy();
    }
  });

  test('DBOSClient-in-transaction-rejects-unusable-clients', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl });
    const now = Date.now();
    const options = {
      workflowName: 'enqueueTest',
      workflowClassName: 'ClientTest',
      queueName: 'testQueue',
      workflowID: `client-tx-unusable-${now}`,
    };
    // A client whose connection cannot serve the DBOS schema, as one on the wrong database cannot.
    const missingSchemaClient = await DBOSClient.create({
      systemDatabaseUrl,
      systemDatabaseSchemaName: `absent_schema_${now}`,
    });
    const txClient = new Client(poolConfig);
    const args = [42, 'test', { first: 'John', last: 'Doe', age: 30 }] as Parameters<EnqueueTest>;
    try {
      await txClient.connect();

      await txClient.query('BEGIN');
      await expect(txClient.query('SELECT 1/0')).rejects.toThrow();
      // The caller's transaction is already aborted, so the enqueue cannot join it.
      await expect(client.enqueueInTransaction<EnqueueTest>(txClient, options, ...args)).rejects.toThrow();
      await txClient.query('ROLLBACK');

      await txClient.query('BEGIN');
      await expect(missingSchemaClient.enqueueInTransaction<EnqueueTest>(txClient, options, ...args)).rejects.toThrow(
        /workflow_status/,
      );
      await txClient.query('ROLLBACK');
    } finally {
      await txClient.end();
      await missingSchemaClient.destroy();
      await client.destroy();
    }
  });

  test('DBOSClient-getEvent-while-running', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    await registerTestQueue();
    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value);
      const eventValue = await client.getEvent<string>(workflowID, key, 60);
      expect(eventValue).toBe(value);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-getEvent-when-finished', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    await registerTestQueue();
    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);

      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(value);
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-getEvent-update-while-running', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    await registerTestQueue();
    const client = await DBOSClient.create({ systemDatabaseUrl });
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
  });

  test('DBOSClient-getEvent-update-when-finished', async () => {
    const now = Date.now();

    const workflowID = `client-event-${now}`;
    const key = `event-key-${now}`;
    const value = `event-value-${now}`;

    await DBOS.launch();
    await registerTestQueue();
    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).eventTest(key, value, true);
      const result = await handle.getResult();
      expect(result).toBe(`${key}-${value}`);

      const eventValue = await client.getEvent<string>(workflowID, key, 10);
      expect(eventValue).toBe(`updated-${value}`);
    } finally {
      await client.destroy();
    }
  });

  test('DBOSClient-retrieve-workflow', async () => {
    const wfid = `client-retrieve-${Date.now()}`;

    await DBOS.launch();
    await registerTestQueue();
    await DBOS.startWorkflow(ClientTest, { workflowID: wfid }).enqueueTest(42, 'test', {
      first: 'John',
      last: 'Doe',
      age: 30,
    });

    const client = await DBOSClient.create({ systemDatabaseUrl });
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
    await registerTestQueue();
    const handle = await DBOS.startWorkflow(ClientTest, { workflowID: wfid }).enqueueTest(42, 'test', {
      first: 'John',
      last: 'Doe',
      age: 30,
    });
    const result1 = await handle.getResult();
    expect(result1).toBe('42-test-{"first":"John","last":"Doe","age":30}');

    const client = await DBOSClient.create({ systemDatabaseUrl });
    try {
      const handle = client.retrieveWorkflow<ReturnType<EnqueueTest>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }
  });

  test('queue-crud', async () => {
    await DBOS.launch();
    await registerTestQueue();
    const client = await DBOSClient.create({ systemDatabaseUrl });
    const queueName = `test_client_queue_${randomUUID()}`;

    try {
      expect(await client.retrieveQueue(queueName)).toBeNull();

      // Register persists configuration without a launched DBOS executor
      // ever having seen the queue name.
      const registered = await client.registerQueue(queueName, {
        concurrency: 4,
        rateLimit: { limitPerPeriod: 5, periodSec: 1.5 },
        workerConcurrency: 2,
        priorityEnabled: true,
        minPollingIntervalMs: 2500,
      });
      expect(registered.name).toBe(queueName);
      expect(registered.databaseBacked).toBe(true);
      expect(registered.clientBound).toBe(true);

      const retrieved = await client.retrieveQueue(queueName);
      expect(retrieved).not.toBeNull();
      expect(retrieved!.clientBound).toBe(true);
      expect(retrieved!.concurrency).toBe(4);
      expect(retrieved!.workerConcurrency).toBe(2);
      expect(retrieved!.rateLimit).toEqual({ limitPerPeriod: 5, periodSec: 1.5 });
      expect(retrieved!.priorityEnabled).toBe(true);
      expect(retrieved!.minPollingIntervalMs).toBe(2500);

      // Partition limits persist through the client's own registration path.
      const partitionedName = `test_client_partition_queue_${randomUUID()}`;
      await client.registerQueue(partitionedName, {
        globalConcurrency: 6,
        partitionConcurrency: 2,
        partitionWorkerConcurrency: 1,
        partitionRateLimit: { limitPerPeriod: 3, periodSec: 2 },
      });
      const partitioned = await client.retrieveQueue(partitionedName);
      expect(partitioned!.concurrency).toBe(6);
      expect(partitioned!.partitionConcurrency).toBe(2);
      expect(partitioned!.partitionWorkerConcurrency).toBe(1);
      expect(partitioned!.partitionRateLimit).toEqual({ limitPerPeriod: 3, periodSec: 2 });
      // Any per-partition limit partitions the queue, without the deprecated flag.
      expect(partitioned!.partitionQueue).toBe(true);
      await client.deleteQueue(partitionedName);

      // Setters write through the client's database; the launched DBOS
      // executor sees the same row.
      await retrieved!.setConcurrency(8);
      const fromDbos = await DBOS.retrieveQueue(queueName);
      expect(fromDbos).not.toBeNull();
      expect(fromDbos!.concurrency).toBe(8);
      // Queues retrieved through DBOS are not client-bound.
      expect(fromDbos!.clientBound).toBe(false);

      // Clients have no application version, so update_if_latest_version
      // is rejected.
      await expect(
        client.registerQueue(queueName, { concurrency: 1, onConflict: 'update_if_latest_version' }),
      ).rejects.toThrow(/update_if_latest_version/);

      // Default for clients is always_update: re-registering with new config
      // overwrites the existing row.
      await client.registerQueue(queueName, { concurrency: 99 });
      const overwritten = await DBOS.retrieveQueue(queueName);
      expect(overwritten!.concurrency).toBe(99);

      // delete removes the row; subsequent retrievals from either side
      // return null, and a second delete is a no-op.
      await client.deleteQueue(queueName);
      expect(await client.retrieveQueue(queueName)).toBeNull();
      expect(await DBOS.retrieveQueue(queueName)).toBeNull();
      await client.deleteQueue(queueName);
    } finally {
      await client.destroy();
    }
  });
});
