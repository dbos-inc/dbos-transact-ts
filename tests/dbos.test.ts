import { WorkflowHandle, DBOS, DBOSSerializer, WorkflowQueue } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { randomUUID } from 'node:crypto';
import { StatusString } from '../src/workflow';
import { DBOSConfig } from '../src/dbos-executor';
import { Client, Pool } from 'pg';
import { DBOSWorkflowCancelledError, DBOSAwaitedWorkflowCancelledError, DBOSInitializationError } from '../src/error';
import assert from 'node:assert';
import { DBOSClient } from '../dist/src';
import { dropPGDatabase, ensurePGDatabase } from '../src/database_utils';

describe('dbos-tests', () => {
  let username: string;
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    expect(config.systemDatabaseUrl).toBeDefined();
    const url = new URL(config.systemDatabaseUrl!);
    username = url.username;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    DBOSTestClass.cnt = 0;
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-function', async () => {
    const workflowHandle: WorkflowHandle<string> = await DBOS.startWorkflow(DBOSTestClass).testWorkflow(username);
    const workflowResult: string = await workflowHandle.getResult();
    expect(workflowResult).toEqual(username);
  });

  test('simple-workflow-attempts-counter', async () => {
    const systemDBClient = new Client({
      connectionString: config.systemDatabaseUrl,
    });
    try {
      await systemDBClient.connect();
      const handle = await DBOS.startWorkflow(DBOSTestClass).noopWorkflow();
      for (let i = 0; i < 10; i++) {
        await DBOS.startWorkflow(DBOSTestClass, { workflowID: handle.workflowID }).noopWorkflow();
        const result = await systemDBClient.query<{ status: string; attempts: number }>(
          `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
          [handle.workflowID],
        );
        expect(result.rows[0].attempts).toBe(String(1));
      }
    } finally {
      await systemDBClient.end();
    }
  });

  test('return-void', async () => {
    const workflowUUID = randomUUID();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await DBOSTestClass.testVoidFunction();
    });
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(DBOSTestClass.testVoidFunction()).resolves.toBeFalsy();
    });
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(DBOSTestClass.testVoidFunction()).resolves.toBeFalsy();
    });
  });

  test('tight-loop', async () => {
    for (let i = 0; i < 100; i++) {
      await expect(DBOSTestClass.testNameWorkflow(username)).resolves.toBe(username);
    }
  });

  test('abort-function', async () => {
    await expect(DBOSTestClass.testFailWorkflow('fail')).rejects.toThrow('fail');
  });

  test('simple-step', async () => {
    const workflowUUID: string = randomUUID();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(DBOSTestClass.testStep()).resolves.toBe(0);
    });
    await expect(DBOSTestClass.testStep()).resolves.toBe(1);
  });

  test('simple-workflow-notifications', async () => {
    // Send to non-existent workflow should fail
    await expect(DBOSTestClass.sendWorkflow('1234567')).rejects.toThrow(
      'Sent to non-existent destination workflow UUID',
    );

    const workflowUUID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTestClass, { workflowID: workflowUUID }).receiveWorkflow();
    await expect(DBOSTestClass.sendWorkflow(handle.workflowID)).resolves.toBeFalsy(); // return void.
    expect(await handle.getResult()).toBe(true);
  });

  test('simple-workflow-events', async () => {
    const handle: WorkflowHandle<number> = await DBOS.startWorkflow(DBOSTestClass).setEventWorkflow();
    const workflowUUID = handle.workflowID;
    await handle.getResult();
    await expect(DBOS.getEvent(workflowUUID, 'key1')).resolves.toBe('value1');
    await expect(DBOS.getEvent(workflowUUID, 'key2')).resolves.toBe('value2');
    await expect(DBOS.getEvent(workflowUUID, 'fail', 0)).resolves.toBe(null);
  });

  test('simple-workflow-events-multiple', async () => {
    const handle: WorkflowHandle<number> = await DBOS.startWorkflow(DBOSTestClass).setEventMultipleWorkflow();
    const workflowUUID = handle.workflowID;
    await handle.getResult();
    await expect(DBOS.getEvent(workflowUUID, 'key1')).resolves.toBe('value1b');
    await expect(DBOS.getEvent(workflowUUID, 'key2')).resolves.toBe('value2');
    await expect(DBOS.getEvent(workflowUUID, 'fail', 0)).resolves.toBe(null);
  });

  class RetrieveWorkflowStatus {
    // Test workflow status changes correctly.
    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      RetrieveWorkflowStatus.resolve1 = resolve;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      RetrieveWorkflowStatus.resolve2 = resolve;
    });

    static resolve3: () => void;
    static promise3 = new Promise<void>((resolve) => {
      RetrieveWorkflowStatus.resolve3 = resolve;
    });

    @DBOS.workflow()
    static async testStatusWorkflow(id: number, name: string) {
      await RetrieveWorkflowStatus.promise1;
      RetrieveWorkflowStatus.resolve3(); // Signal the execution has done.
      await RetrieveWorkflowStatus.promise2;
      return name;
    }
  }

  test('retrieve-workflowstatus', async () => {
    const workflowUUID = randomUUID();

    const workflowHandle = await DBOS.startWorkflow(RetrieveWorkflowStatus, {
      workflowID: workflowUUID,
    }).testStatusWorkflow(123, 'hello');

    expect(workflowHandle.workflowID).toBe(workflowUUID);
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      workflowID: workflowUUID,
      status: StatusString.PENDING,
      workflowName: RetrieveWorkflowStatus.testStatusWorkflow.name,
    });
    await expect(workflowHandle.getWorkflowInputs()).resolves.toMatchObject([123, 'hello']);

    // getResult with a timeout ... it'll time out.
    await expect(DBOS.getResult<string>(workflowUUID, 0.1)).resolves.toBeNull();

    RetrieveWorkflowStatus.resolve1();
    await RetrieveWorkflowStatus.promise3;

    // Retrieve handle, should get the pending status.
    await expect(DBOS.retrieveWorkflow<string>(workflowUUID).getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
      workflowName: RetrieveWorkflowStatus.testStatusWorkflow.name,
    });

    // Proceed to the end.
    RetrieveWorkflowStatus.resolve2();
    await expect(workflowHandle.getResult()).resolves.toBe('hello');

    // The status should transition to SUCCESS.
    const retrievedHandle = DBOS.retrieveWorkflow<string>(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    expect(retrievedHandle.workflowID).toBe(workflowUUID);
    await expect(retrievedHandle.getResult()).resolves.toBe('hello');
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  describe('workflow-timeout', () => {
    test('workflow-withWorkflowTimeout', async () => {
      const workflowID: string = randomUUID();
      await DBOS.withNextWorkflowID(workflowID, async () => {
        await DBOS.withWorkflowTimeout(100, async () => {
          await expect(DBOSTimeoutTestClass.blockedWorkflow()).rejects.toThrow(
            new DBOSWorkflowCancelledError(workflowID),
          );
        });
      });
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
    });

    test('workflow-timeout-startWorkflow-params', async () => {
      const workflowID = randomUUID();
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 100 }).blockedWorkflow();
      await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
    });

    test('parent-workflow-withWorkflowTimeout', async () => {
      const workflowID: string = randomUUID();
      const childID = `${workflowID}-0`;
      await DBOS.withNextWorkflowID(workflowID, async () => {
        await DBOS.withWorkflowTimeout(100, async () => {
          await expect(DBOSTimeoutTestClass.blockingParentStartWF()).rejects.toThrow(
            new DBOSWorkflowCancelledError(workflowID),
          );
        });
      });
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
      const childStatus = await DBOS.getWorkflowStatus(childID);
      expect(childStatus?.status).toBe(StatusString.CANCELLED);
      expect(status?.deadlineEpochMS).toBe(childStatus?.deadlineEpochMS);
    });

    test('parent-workflow-timeout-startWorkflow-params', async () => {
      const workflowID = randomUUID();
      const childID = `${workflowID}-0`;
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
        workflowID,
        timeoutMS: 100,
      }).blockingParentStartWF();
      await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
      const childStatus = await DBOS.getWorkflowStatus(childID);
      expect(childStatus?.status).toBe(StatusString.CANCELLED);
      expect(status?.deadlineEpochMS).toBe(childStatus?.deadlineEpochMS);
    });

    test('direct-parent-workflow-withWorkflowTimeout', async () => {
      const workflowID: string = randomUUID();
      const childID = `${workflowID}-0`;
      await DBOS.withNextWorkflowID(workflowID, async () => {
        await DBOS.withWorkflowTimeout(100, async () => {
          await expect(DBOSTimeoutTestClass.blockingParentDirect()).rejects.toThrow(
            new DBOSWorkflowCancelledError(workflowID),
          );
        });
      });
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
      const childStatus = await DBOS.getWorkflowStatus(childID);
      expect(childStatus?.status).toBe(StatusString.CANCELLED);
      expect(status?.deadlineEpochMS).toBe(childStatus?.deadlineEpochMS);
    });

    test('direct-parent-workflow-timeout-startWorkflow-params', async () => {
      const workflowID = randomUUID();
      const childID = `${workflowID}-0`;
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
        workflowID,
        timeoutMS: 100,
      }).blockingParentDirect();
      await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
      const childStatus = await DBOS.getWorkflowStatus(childID);
      expect(childStatus?.status).toBe(StatusString.CANCELLED);
      expect(status?.deadlineEpochMS).toBe(childStatus?.deadlineEpochMS);
    });

    test('child-wf-timeout-simple', async () => {
      const workflowID = randomUUID();
      const childID = `${workflowID}-0`;
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID }).timeoutParentStartWF(100);
      await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(childID));
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.ERROR);
      const childStatus = await DBOS.getWorkflowStatus(childID);
      expect(childStatus?.status).toBe(StatusString.CANCELLED);
      expect(status?.deadlineEpochMS).toBe(undefined);
    });

    test('child-wf-timeout-before-parent', async () => {
      const workflowID = randomUUID();
      const childID = `${workflowID}-0`;
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
        workflowID,
        timeoutMS: 1000,
      }).timeoutParentStartWF(100);
      await expect(handle.getResult()).rejects.toThrow(new DBOSAwaitedWorkflowCancelledError(childID));
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.ERROR);
      const childStatus = await DBOS.getWorkflowStatus(childID);
      expect(childStatus?.status).toBe(StatusString.CANCELLED);
      expect(status?.deadlineEpochMS).toBeGreaterThan(childStatus?.deadlineEpochMS as number);
    });

    test('child-wf-timeout-after-parent', async () => {
      const workflowID = randomUUID();
      const childID = `${workflowID}-0`;
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
        workflowID,
        timeoutMS: 100,
      }).timeoutParentStartWF(1000);
      await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
      const childStatus = await DBOS.getWorkflowStatus(childID);
      expect(childStatus?.status).toBe(StatusString.CANCELLED);
      expect(childStatus?.deadlineEpochMS).toBeGreaterThan(status?.deadlineEpochMS as number);
    });

    test('sleeping-workflow-timed-out', async () => {
      const workflowID = randomUUID();
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 100 }).sleepingWorkflow(
        1000,
      );
      await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.CANCELLED);
    });

    test('sleeping-workflow-not-timed-out', async () => {
      const workflowID = randomUUID();
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 2000 }).sleepingWorkflow(
        1000,
      );
      await expect(handle.getResult()).resolves.toBe(42);
      const status = await DBOS.getWorkflowStatus(workflowID);
      expect(status?.status).toBe(StatusString.SUCCESS);
    });

    test('child-wf-detach-deadline', async () => {
      const workflowID = randomUUID();
      const childID = `${workflowID}-0`;
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
        workflowID,
        timeoutMS: 100,
      }).timeoutParentStartDetachedChild(100);
      await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
      await expect(handle.getStatus()).resolves.toMatchObject({
        status: StatusString.CANCELLED,
      });
      const childHandle = DBOS.retrieveWorkflow(childID);
      await expect(childHandle.getResult()).resolves.toBe(42);
      await expect(childHandle.getStatus()).resolves.toMatchObject({
        status: StatusString.SUCCESS,
      });
    });

    test('child-wf-detach-deadline-with-syntax', async () => {
      const workflowID = randomUUID();
      const childID = `${workflowID}-0`;
      const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
        workflowID,
        timeoutMS: 100,
      }).timeoutParentStartDetachedChildWithSyntax(100);
      await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
      await expect(handle.getStatus()).resolves.toMatchObject({
        status: StatusString.CANCELLED,
      });
      const childHandle = DBOS.retrieveWorkflow(childID);
      await expect(childHandle.getResult()).resolves.toBe(42);
      await expect(childHandle.getStatus()).resolves.toMatchObject({
        status: StatusString.SUCCESS,
      });
    });

    test('custom-serializer-test', async () => {
      await DBOS.shutdown();

      const key = 'key';
      const value = 'value';
      const message = 'message';
      const workflow = DBOS.registerWorkflow(
        async (input: string) => {
          await DBOS.setEvent(key, input);
          return DBOS.recv();
        },
        { name: 'custom-serializer-test' },
      );
      const queue = new WorkflowQueue('example-queue');

      // Configure DBOS with a JSON-based custom serializer
      const config = generateDBOSTestConfig();
      const jsonSerializer: DBOSSerializer = {
        parse: (text: string | null | undefined): unknown => {
          if (text === null || text === undefined) return null;
          return JSON.parse(text);
        },
        stringify: JSON.stringify,
      };
      config.serializer = jsonSerializer;
      DBOS.setConfig(config);
      await DBOS.launch();

      // Test workflow operations with a custom serializer
      const handle = await DBOS.startWorkflow(workflow, { queueName: queue.name })(value);
      await DBOS.send(handle.workflowID, message);
      assert.equal(await handle.getResult(), message);
      assert.equal(await DBOS.getEvent(handle.workflowID, key), value);
      const steps = await DBOS.listWorkflowSteps(handle.workflowID);
      assert.ok(steps);
      assert.equal(steps.length, 2);
      assert.ok(steps[0].name.includes('DBOS.setEvent'));
      assert.ok(steps[1].name.includes('DBOS.recv'));
      assert.equal(steps[1].output, message);

      // Test the client with a custom serializer
      const client = await DBOSClient.create({
        systemDatabaseUrl: config.systemDatabaseUrl!,
        serializer: jsonSerializer,
      });
      const clientEnqueue = await client.enqueue(
        { workflowName: 'custom-serializer-test', queueName: queue.name },
        message,
      );
      await client.send(clientEnqueue.workflowID, message);
      assert.equal(await clientEnqueue.getResult(), message);
      const clientHandle = client.retrieveWorkflow(handle.workflowID);
      assert.equal(await clientHandle.getResult(), message);
      assert.equal(await client.getEvent(handle.workflowID, key), value);
      await client.destroy();
    }, 10000);
  });
});

class DBOSTimeoutTestClass {
  @DBOS.workflow()
  static async sleepingWorkflow(duration: number) {
    await DBOS.sleep(duration);
    return 42;
  }

  @DBOS.workflow()
  static async blockedWorkflow() {
    while (true) {
      await DBOS.sleep(100);
    }
  }

  @DBOS.workflow()
  static async blockingParentStartWF() {
    await DBOS.startWorkflow(DBOSTimeoutTestClass)
      .blockedWorkflow()
      .then((h) => h.getResult());
  }

  @DBOS.workflow()
  static async blockingParentDirect() {
    await DBOSTimeoutTestClass.blockedWorkflow();
  }

  @DBOS.workflow()
  static async timeoutParentStartWF(timeout: number) {
    await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: timeout })
      .blockedWorkflow()
      .then((h) => h.getResult());
  }

  @DBOS.workflow()
  static async timeoutParentStartDetachedChild(duration: number) {
    await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: null })
      .sleepingWorkflow(duration * 2)
      .then((h) => h.getResult());
  }

  @DBOS.workflow()
  static async timeoutParentStartDetachedChildWithSyntax(duration: number) {
    await DBOS.withWorkflowTimeout(null, async () => {
      await DBOSTimeoutTestClass.sleepingWorkflow(duration * 2);
    });
  }
}

class DBOSTestClass {
  static cnt: number = 0;

  @DBOS.step()
  static async testFunction(name: string) {
    return Promise.resolve(name);
  }

  @DBOS.workflow()
  static async testWorkflow(name: string) {
    const funcResult = await DBOSTestClass.testFunction(name);
    return funcResult;
  }

  @DBOS.step()
  static async testVoidFunction() {
    return Promise.resolve();
  }

  @DBOS.step()
  static async testNameFunction(name: string) {
    return Promise.resolve(name);
  }

  @DBOS.workflow()
  static async testNameWorkflow(name: string) {
    return DBOSTestClass.testNameFunction(name); // Missing await is intentional
  }

  @DBOS.step()
  static async testFailFunction(name: string) {
    if (name === 'fail') {
      throw new Error('fail');
    }
    return Promise.resolve(name);
  }

  @DBOS.workflow()
  static async testFailWorkflow(name: string) {
    await DBOSTestClass.testFailFunction(name);
  }

  @DBOS.step()
  static async testStep() {
    return Promise.resolve(DBOSTestClass.cnt++);
  }

  @DBOS.workflow()
  static async receiveWorkflow() {
    const message1 = await DBOS.recv<string>();
    const message2 = await DBOS.recv<string>();
    const fail = await DBOS.recv('fail', 0);
    return message1 === 'message1' && message2 === 'message2' && fail === null;
  }

  @DBOS.workflow()
  static async sendWorkflow(destinationId: string) {
    await DBOS.send(destinationId, 'message1');
    await DBOS.send(destinationId, 'message2');
  }

  @DBOS.workflow()
  static async setEventWorkflow() {
    await DBOS.setEvent('key1', 'value1');
    await DBOS.setEvent('key2', 'value2');
    return 0;
  }

  @DBOS.workflow()
  static async setEventMultipleWorkflow() {
    await DBOS.setEvent('key1', 'value1');
    await DBOS.setEvent('key2', 'value2');
    await DBOS.setEvent('key1', 'value1b');
    return 0;
  }

  @DBOS.workflow()
  static async noopWorkflow() {
    return Promise.resolve();
  }
}

describe('custom-pool-test', () => {
  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('custom-pool-test', async () => {
    const baseConfig = generateDBOSTestConfig();
    // Destroy the system database
    await dropPGDatabase({ urlToDrop: baseConfig.systemDatabaseUrl, logger: () => {} });
    const systemDatabaseURL = baseConfig.systemDatabaseUrl;
    assert(systemDatabaseURL);
    let pool = new Pool({ connectionString: systemDatabaseURL });
    let config: DBOSConfig = {
      systemDatabaseUrl: 'postgres://fake:nonsense@badhost:1111/no_database',
      systemDatabasePool: pool,
    };
    DBOS.setConfig(config);
    const workflow = DBOS.registerWorkflow(
      () => {
        return DBOS.recv();
      },
      { name: 'custom-pool-test' },
    );
    // Launching with a custom pool but nonexistent database should fail
    await expect(DBOS.launch()).rejects.toThrow(DBOSInitializationError);
    await DBOS.shutdown();
    // Create the system database, launch should succeed with a custom pool but fake URL
    await ensurePGDatabase({ urlToEnsure: baseConfig.systemDatabaseUrl, logger: () => {} });
    pool = new Pool({ connectionString: systemDatabaseURL });
    config = {
      systemDatabaseUrl: 'postgres://fake:nonsense@badhost:1111/no_database',
      systemDatabasePool: pool,
    };
    DBOS.setConfig(config);
    await DBOS.launch();

    const message = 'message';
    const handle = await DBOS.startWorkflow(workflow)();
    await DBOS.send(handle.workflowID, message);
    assert.equal(await handle.getResult(), message);

    const client = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      systemDatabasePool: config.systemDatabasePool,
    });
    const clientHandle = client.retrieveWorkflow(handle.workflowID);
    assert.equal(await clientHandle.getResult(), message);
  });
});
