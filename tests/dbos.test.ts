import { WorkflowHandle, DBOS, DBOSSerializer, WorkflowQueue } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, Event } from './helpers';
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

  class SendIdempotencyTestClass {
    static recvTwoMessagesEvent = new Event();

    @DBOS.workflow()
    static async recvTwoMsgs() {
      const msg1 = await DBOS.recv<string>(undefined, 10);
      SendIdempotencyTestClass.recvTwoMessagesEvent.set();
      const msg2 = await DBOS.recv<string>(undefined, 2);
      return `${msg1}-${msg2}`;
    }

    @DBOS.workflow()
    static async recvTwoOnTopic() {
      const msg1 = await DBOS.recv<string>('t', 10);
      const msg2 = await DBOS.recv<string>('t', 10);
      return `${msg1}-${msg2}`;
    }

    @DBOS.workflow()
    static async badSendWorkflow(destUUID: string) {
      await DBOS.send(destUUID, 'msg', undefined, 'should-fail');
    }

    @DBOS.workflow()
    static async recvOneMsg() {
      return String(await DBOS.recv<string>('s', 10));
    }

    @DBOS.step()
    static async sendFromStep(dest: string, msg: string, topic: string) {
      await DBOS.send(dest, msg, topic);
    }

    @DBOS.workflow()
    static async sendFromStepWF(dest: string, msg: string, topic: string) {
      await SendIdempotencyTestClass.sendFromStep(dest, msg, topic);
    }

    @DBOS.step()
    static async sendFromStepWithKey(dest: string, msg: string, key: string) {
      await DBOS.send(dest, msg, undefined, key);
    }

    @DBOS.workflow()
    static async sendFromStepIdemWF(dest: string, msg: string, key: string) {
      await SendIdempotencyTestClass.sendFromStepWithKey(dest, msg, key);
    }
  }

  test('send-idempotency-key', async () => {
    // Test 1: Sending with the same idempotency key twice delivers only one message.
    SendIdempotencyTestClass.recvTwoMessagesEvent.clear();
    const destUUID = randomUUID();
    const handle = await DBOS.startWorkflow(SendIdempotencyTestClass, { workflowID: destUUID }).recvTwoMsgs();

    const idemKey = randomUUID();
    await DBOS.send(destUUID, 'hello', undefined, idemKey);
    await SendIdempotencyTestClass.recvTwoMessagesEvent.wait();
    // Duplicate send with the same key should be silently ignored.
    await DBOS.send(destUUID, 'hello_duplicate', undefined, idemKey);
    // The second recv times out (returns null), proving only one message was delivered.
    expect(await handle.getResult()).toBe('hello-null');

    // Test 2: Different idempotency keys deliver separate messages.
    const destUUID2 = randomUUID();
    const handle2 = await DBOS.startWorkflow(SendIdempotencyTestClass, { workflowID: destUUID2 }).recvTwoOnTopic();

    await DBOS.send(destUUID2, 'a', 't', randomUUID());
    await DBOS.send(destUUID2, 'b', 't', randomUUID());
    expect(await handle2.getResult()).toBe('a-b');

    // Test 3: idempotency_key inside a workflow raises an error.
    await expect(SendIdempotencyTestClass.badSendWorkflow(destUUID)).rejects.toThrow('idempotency key');

    // Test 4: Send from a step (without idempotency key).
    const destUUID3 = randomUUID();
    const handle3 = await DBOS.startWorkflow(SendIdempotencyTestClass, { workflowID: destUUID3 }).recvOneMsg();

    await SendIdempotencyTestClass.sendFromStepWF(destUUID3, 'from_step', 's');
    expect(await handle3.getResult()).toBe('from_step');

    // Test 5: Send from a step with same idempotency key twice delivers only one message.
    SendIdempotencyTestClass.recvTwoMessagesEvent.clear();
    const destUUID4 = randomUUID();
    const handle4 = await DBOS.startWorkflow(SendIdempotencyTestClass, { workflowID: destUUID4 }).recvTwoMsgs();

    const stepIdemKey = randomUUID();
    await SendIdempotencyTestClass.sendFromStepIdemWF(destUUID4, 'hello_step', stepIdemKey);
    await SendIdempotencyTestClass.recvTwoMessagesEvent.wait();

    // Duplicate send with the same key should be silently ignored.
    await SendIdempotencyTestClass.sendFromStepIdemWF(destUUID4, 'hello_step_dup', stepIdemKey);
    // The second recv times out (returns null), proving only one message was delivered.
    expect(await handle4.getResult()).toBe('hello_step-null');
  }, 30000);

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

    test('test_wait_first', async () => {
      const handleFast = await DBOS.startWorkflow(WaitFirstTestClass).fastWorkflow();
      const handleSlow = await DBOS.startWorkflow(WaitFirstTestClass).slowWorkflow();

      const resultHandle = await DBOS.waitFirst([handleFast, handleSlow]);
      expect(resultHandle.workflowID).toBe(handleFast.workflowID);
      expect(await resultHandle.getResult()).toBe('fast');
      // Wait for slow workflow to finish so it doesn't hang
      await handleSlow.getResult();

      // Test waitFirst via the client
      const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
      try {
        const handleFast2 = await DBOS.startWorkflow(WaitFirstTestClass).fastWorkflow();
        const handleSlow2 = await DBOS.startWorkflow(WaitFirstTestClass).slowWorkflow();

        const clientHandleFast = client.retrieveWorkflow(handleFast2.workflowID);
        const clientHandleSlow = client.retrieveWorkflow(handleSlow2.workflowID);

        const clientResult = await client.waitFirst([clientHandleFast, clientHandleSlow]);
        expect(clientResult.workflowID).toBe(handleFast2.workflowID);
        expect(await clientResult.getResult()).toBe('fast');
        await handleSlow2.getResult();

        // Client waitFirst with empty handles should throw
        await expect(client.waitFirst([])).rejects.toThrow('handles must not be empty');
      } finally {
        await client.destroy();
      }
    }, 10000);

    test('test_wait_first_empty', async () => {
      await expect(DBOS.waitFirst([])).rejects.toThrow('handles must not be empty');
    });

    // This test should run last in the block as it changes some global state
    test('custom-serializer-test', async () => {
      await DBOS.shutdown();
      const config = generateDBOSTestConfig();
      // Reset the test database
      await setUpDBOSTestSysDb(config);

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
      const errorWorkflow = DBOS.registerWorkflow(
        async () => {
          await Promise.resolve();
          throw new Error(message);
        },
        { name: 'custom-serializer-test-error' },
      );
      const queue = new WorkflowQueue('example-queue');

      // Configure DBOS with a custom serializer to base64-encoded JSON
      const jsonSerializer: DBOSSerializer = {
        name: () => 'custom_base64',
        parse: (text: string | null | undefined): unknown => {
          // Parsers must always return null when receiving null or undefined
          if (text === null || text === undefined) return null;
          return JSON.parse(Buffer.from(text, 'base64').toString());
        },
        stringify: (obj: unknown): string => {
          // JSON.stringify doesn't handle undefined, so convert it to null instead
          if (obj === undefined) {
            obj = null;
          }
          return Buffer.from(JSON.stringify(obj)).toString('base64');
        },
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
      const errorHandle = await DBOS.startWorkflow(errorWorkflow, { queueName: queue.name })();
      await expect(errorHandle.getResult()).rejects.toThrow(message);

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

      // Verify a client without the custom serializer does not fail,
      // but falls back to returning raw strings
      const badClient = await DBOSClient.create({
        systemDatabaseUrl: config.systemDatabaseUrl!,
      });
      const workflows = await badClient.listWorkflows({});
      assert.equal(workflows.length, 3);
      assert.equal(workflows[0].output, jsonSerializer.stringify(message));
      const badClientSteps = await badClient.listWorkflowSteps(workflows[0].workflowID);
      assert.ok(badClientSteps);
      assert.ok(badClientSteps[1].name.includes('DBOS.recv'));
      assert.equal(badClientSteps[1].output, jsonSerializer.stringify(message));
      assert.equal(badClientSteps.length, 2);
      await badClient.destroy();
    }, 10000);
  });
});

class WaitFirstTestClass {
  @DBOS.workflow()
  static async fastWorkflow() {
    await Promise.resolve();
    return 'fast';
  }

  @DBOS.workflow()
  static async slowWorkflow() {
    await DBOS.sleep(2);
    return 'slow';
  }
}

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

class TimeoutTestClass {
  @DBOS.workflow()
  static async getEventTimeoutWorkflow() {
    const workflowID = DBOS.workflowID!;
    return DBOS.getEvent(workflowID, 'key', 0);
  }

  @DBOS.workflow()
  static async recvTimeoutWorkflow() {
    return DBOS.recv(undefined, 0);
  }
}

describe('timeout-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('get-event-timeout', async () => {
    const handle = await DBOS.startWorkflow(TimeoutTestClass).getEventTimeoutWorkflow();
    expect(await handle.getResult()).toBeNull();

    const forkedHandle = await DBOS.forkWorkflow(handle.workflowID, 5);
    expect(await forkedHandle.getResult()).toBeNull();
  });

  test('recv-timeout', async () => {
    const handle = await DBOS.startWorkflow(TimeoutTestClass).recvTimeoutWorkflow();
    expect(await handle.getResult()).toBeNull();

    const forkedHandle = await DBOS.forkWorkflow(handle.workflowID, 5);
    expect(await forkedHandle.getResult()).toBeNull();
  });
});

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
