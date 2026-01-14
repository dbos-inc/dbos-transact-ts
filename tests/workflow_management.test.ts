import { GetWorkflowsInput, StatusString, DBOS, WorkflowQueue } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import {
  generateDBOSTestConfig,
  setUpDBOSTestSysDb,
  Event,
  recoverPendingWorkflows,
  reexecuteWorkflowById,
} from './helpers';
import { Client } from 'pg';
import { WorkflowHandle, WorkflowStatus } from '../src/workflow';
import { randomUUID } from 'node:crypto';
import { globalParams, sleepms } from '../src/utils';
import { PostgresSystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { getWorkflow, globalTimeout, listQueuedWorkflows, listWorkflows } from '../src/workflow_management';
import {
  DBOSAwaitedWorkflowCancelledError,
  DBOSNonExistentWorkflowError,
  DBOSWorkflowCancelledError,
} from '../src/error';
import assert from 'node:assert';
import { DBOSJSON } from '../src/serialization';

describe('workflow-management-tests', () => {
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

  test('simple-getworkflows', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const workflows = await DBOS.listWorkflows({});
    expect(workflows.length).toBe(1);
  });

  test('getworkflows-with-dates', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      startTime: new Date(Date.now() - 10000).toISOString(),
      endTime: new Date(Date.now()).toISOString(),
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.endTime = new Date(Date.now() - 10000).toISOString();
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-status', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      status: StatusString.SUCCESS,
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.status = StatusString.PENDING;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-wfname', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      workflowName: 'testWorkflow',
    };
    const workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);
  });

  test('getworkflows-with-applicationVersion', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      applicationVersion: DBOS.applicationVersion,
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.applicationVersion = 'v1';
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-executorID', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      executorId: DBOS.executorID,
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.executorId = 'fake-id';
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-limit', async () => {
    const workflowIDs: string[] = [];
    let wfid = await TestEndpoints.testWorkflowGetID();
    assert.ok(wfid);
    expect(wfid).toBeTruthy();
    expect(wfid.length).toBeGreaterThan(0);
    workflowIDs.push(wfid);

    const input: GetWorkflowsInput = {
      limit: 10,
    };

    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);
    expect(workflows[0].workflowID).toBe(workflowIDs[0]);

    for (let i = 0; i < 10; i++) {
      wfid = await TestEndpoints.testWorkflowGetID();
      assert.ok(wfid);
      expect(wfid.length).toBeGreaterThan(0);
      workflowIDs.push(wfid);
    }

    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      // The order should be ascending by default
      expect(workflows[i].workflowID).toBe(workflowIDs[i]);
    }

    // Test sort_desc inverts the order
    input.sortDesc = true;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      expect(workflows[i].workflowID).toBe(workflowIDs[10 - i]);
    }

    // Test LIMIT 2 OFFSET 2 returns the third and fourth workflows
    input.limit = 2;
    input.offset = 2;
    input.sortDesc = false;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(2);
    for (let i = 0; i < workflows.length; i++) {
      expect(workflows[i].workflowID).toBe(workflowIDs[i + 2]);
    }

    // Test OFFSET 10 returns the last workflow
    input.offset = 10;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);
    for (let i = 0; i < workflows.length; i++) {
      expect(workflows[i].workflowID).toBe(workflowIDs[i + 10]);
    }

    // Test search by workflow ID.
    const wfidInput: GetWorkflowsInput = {
      workflowIDs: [workflowIDs[5], workflowIDs[7]],
    };
    workflows = await DBOS.listWorkflows(wfidInput);
    expect(workflows.length).toBe(2);
    expect(workflows[0].workflowID).toBe(workflowIDs[5]);
    expect(workflows[1].workflowID).toBe(workflowIDs[7]);
  });

  test('getworkflows-cli', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    await expect(TestEndpoints.failWorkflow('alice')).rejects.toThrow();

    const logger = new GlobalLogger();
    expect(config.systemDatabaseUrl).toBeDefined();
    const sysdb = new PostgresSystemDatabase(config.systemDatabaseUrl!, logger, DBOSJSON);
    try {
      const input: GetWorkflowsInput = {};
      const infos = await listWorkflows(sysdb, input);
      expect(infos.length).toBe(2);
      let info = infos[0];
      expect(info.workflowName).toBe('testWorkflow');
      expect(info.status).toBe(StatusString.SUCCESS);
      expect(info.workflowClassName).toBe('TestEndpoints');
      expect(info.assumedRole).toBe('');
      expect(info.workflowConfigName).toBe('');
      expect(info.error).toBeUndefined();
      expect(info.output).toBe('alice');
      expect(info.input).toEqual(['alice']);
      expect(info.applicationVersion).toBe(globalParams.appVersion);
      expect(info.createdAt).toBeGreaterThan(0);
      expect(info.updatedAt).toBeGreaterThan(0);
      expect(info.executorId).toBe(globalParams.executorID);
      expect(info.deduplicationID).toBeUndefined();
      expect(info.priority).toBe(0);
      expect(info.queuePartitionKey).toBeUndefined();
      expect(info.forkedFrom).toBeUndefined();

      info = infos[1];
      expect(info.workflowName).toBe('failWorkflow');
      expect(info.status).toBe(StatusString.ERROR);
      expect(info.workflowClassName).toBe('TestEndpoints');
      expect(info.assumedRole).toBe('');
      expect(info.workflowConfigName).toBe('');
      const error = info.error as Error;
      expect(error.message).toBe('alice');
      expect(info.output).toBeUndefined();
      expect(info.input).toEqual(['alice']);
      expect(info.applicationVersion).toBe(globalParams.appVersion);
      expect(info.createdAt).toBeGreaterThan(0);
      expect(info.updatedAt).toBeGreaterThan(0);
      expect(info.executorId).toBe(globalParams.executorID);

      const getInfo = await getWorkflow(sysdb, info.workflowID);
      expect(info).toEqual(getInfo);

      // Test ignoring input and output
      input.loadInput = false;
      input.loadOutput = false;
      const noIOInfos = await listWorkflows(sysdb, input);
      expect(noIOInfos.length).toBe(2);
      expect(noIOInfos[0].input).toBeUndefined();
      expect(noIOInfos[0].output).toBeUndefined();
      expect(noIOInfos[0].error).toBeUndefined();
      expect(noIOInfos[1].input).toBeUndefined();
      expect(noIOInfos[1].output).toBeUndefined();
      expect(noIOInfos[1].error).toBeUndefined();
    } finally {
      await sysdb.destroy();
    }
  });

  test('test-cancel-after-completion', async () => {
    TestEndpoints.tries = 0;

    const workflowID = `test-cancel-after-completion-${Date.now()}`;
    const handle = await DBOS.startWorkflow(TestEndpoints, { workflowID }).waitingWorkflow(42);
    await DBOS.send(workflowID, 'message');
    await expect(handle.getResult()).resolves.toEqual(`42-message`);

    let result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    let rows = result.rows;
    expect(rows[0].attempts).toBe(String(1));
    expect(rows[0].status).toBe(StatusString.SUCCESS);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    await DBOS.cancelWorkflow(workflowID);

    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    rows = result.rows;
    expect(rows[0].attempts).toBe(String(1));
    expect(rows[0].status).toBe(StatusString.SUCCESS);
  });

  test('test-cancel-retry-restart', async () => {
    TestEndpoints.tries = 0;

    const workflowID = `test-cancel-resume-fork-${Date.now()}`;
    const handle = await DBOS.startWorkflow(TestEndpoints, { workflowID }).waitingWorkflow(42);
    expect(TestEndpoints.tries).toBe(1);
    expect(handle.workflowID).toBe(workflowID);

    // waitingWorkflow is blocked waiting for a message to be sent, but we're going to cancel instead
    await DBOS.cancelWorkflow(workflowID);

    let result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.CANCELLED);

    await recoverPendingWorkflows(); // Does nothing as the workflow is CANCELLED
    expect(TestEndpoints.tries).toBe(1);

    // Retry the workflow, resetting the attempts counter
    const handle2 = await DBOS.resumeWorkflow<number>(workflowID);
    await DBOS.send(workflowID, 'message');
    await expect(handle2.getResult()).resolves.toEqual(`42-message`);

    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(TestEndpoints.tries).toBe(2);
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);

    // Resume a non-existant workflow should throw an error
    await expect(DBOS.resumeWorkflow('fake-workflow')).rejects.toThrow(
      new DBOSNonExistentWorkflowError(`Workflow fake-workflow does not exist`),
    );

    // fork the workflow
    const wfh = await DBOS.forkWorkflow(workflowID, 0);
    await DBOS.send(wfh.workflowID, 'fork-message');
    await expect(wfh.getResult()).resolves.toEqual(`42-fork-message`);
    expect(TestEndpoints.tries).toBe(3);

    // Validate a new workflow is started and successful
    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid!=$1`,
      [wfh.workflowID],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);

    // Validate the original workflow status hasn't changed
    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    // expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
  }, 30000);

  test('systemdb-migration-backward-compatible', async () => {
    // Make sure the system DB migration failure is handled correctly.
    // If there is a migration failure, the system DB should still be able to start.
    // This happens when the old code is running with a new system DB schema.
    await DBOS.shutdown();
    await systemDBClient.query(`UPDATE "dbos"."dbos_migrations" SET "version" = 10000;`);
    await DBOS.launch();
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    // Test schema install idempotence
    await DBOS.shutdown();
    await systemDBClient.query(`UPDATE "dbos"."dbos_migrations" SET "version" = 0;`);
    await DBOS.launch();
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');
  });

  class TestEndpoints {
    @DBOS.workflow()
    static async testWorkflow(name: string) {
      return Promise.resolve(name);
    }

    @DBOS.workflow()
    static async testWorkflowGetID() {
      return Promise.resolve(DBOS.workflowID);
    }

    @DBOS.workflow()
    static async failWorkflow(name: string) {
      await Promise.resolve(name);
      throw new Error(name);
    }

    static tries = 0;
    static testResolve: () => void;
    static testPromise = new Promise<void>((resolve) => {
      TestEndpoints.testResolve = resolve;
    });

    @DBOS.workflow()
    static async waitingWorkflow(value: number) {
      TestEndpoints.tries += 1;
      const msg = await DBOS.recv<string>();
      await TestEndpoints.stepOne();
      return `${value}-${msg}`;
    }

    @DBOS.step()
    static async stepOne() {
      return Promise.resolve();
    }
  }
});

describe('test-list-queues', () => {
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
  }, 20000);

  class TestListQueues {
    static queuedSteps = 5;
    static event = new Event();
    static taskEvents = Array.from({ length: TestListQueues.queuedSteps }, () => new Event());
    static queue = new WorkflowQueue('testQueueRecovery');

    @DBOS.workflow()
    static async testWorkflow() {
      const handles: WorkflowHandle<unknown>[] = [];
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        const h = await DBOS.startWorkflow(TestListQueues, { queueName: TestListQueues.queue.name }).blockingTask(i);
        handles.push(h);
      }
      return await Promise.all(handles.map((h) => h.getResult()));
    }

    @DBOS.workflow()
    static async blockingTask(i: number) {
      TestListQueues.taskEvents[i].set();
      await TestListQueues.event.wait();
      return i;
    }
  }

  test('test-list-queues', async () => {
    const wfid = randomUUID();

    // Start the workflow. Wait for all five tasks to start. Verify that they started.
    const originalHandle = await DBOS.startWorkflow(TestListQueues, { workflowID: wfid }).testWorkflow();
    for (const e of TestListQueues.taskEvents) {
      await e.wait();
    }

    const logger = new GlobalLogger();
    expect(config.systemDatabaseUrl).toBeDefined();
    const sysdb = new PostgresSystemDatabase(config.systemDatabaseUrl!, logger, DBOSJSON);
    try {
      let input: GetWorkflowsInput = {};
      let output: WorkflowStatus[] = [];
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);

      // Test workflowName
      input = {
        workflowName: 'blockingTask',
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        expect(output[i].input).toEqual([i]);
      }

      // Test ignoring input
      input.loadInput = false;
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        expect(output[i].input).toBeUndefined();
      }

      input = {
        workflowName: 'no',
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test sortDesc reverts the order
      input = {
        sortDesc: true,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        expect(output[i].input).toEqual([TestListQueues.queuedSteps - i - 1]);
      }

      // Test startTime and endTime
      input = {
        startTime: new Date(Date.now() - 10000).toISOString(),
        endTime: new Date(Date.now()).toISOString(),
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      input = {
        startTime: new Date(Date.now() + 10000).toISOString(),
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test status
      input = {
        status: 'PENDING',
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      input = {
        status: 'SUCCESS',
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test queue name
      input = {
        queueName: TestListQueues.queue.name,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);

      input = {
        queueName: 'no',
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test limit
      input = {
        limit: 2,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(input.limit);
      for (let i = 0; i < input.limit!; i++) {
        expect(output[i].input).toEqual([i]);
      }

      // Test offset
      input = {
        limit: 2,
        offset: 2,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(input.limit);
      for (let i = 0; i < input.limit!; i++) {
        expect(output[i].input).toEqual([i + 2]);
      }

      // Confirm the workflow finishes and nothing is in the queue afterwards
      TestListQueues.event.set();
      await expect(originalHandle.getResult()).resolves.toEqual([0, 1, 2, 3, 4]);

      input = {};
      await expect(listQueuedWorkflows(sysdb, input)).resolves.toEqual([]);
    } finally {
      await sysdb.destroy();
    }
  });

  class TestGarbageCollection {
    static event = new Event();

    @DBOS.step()
    static async testStep(x: number) {
      return Promise.resolve(x);
    }

    @DBOS.workflow()
    static async testWorkflow(x: number) {
      await TestGarbageCollection.testStep(x);
      return x;
    }

    @DBOS.workflow()
    static async blockedWorkflow() {
      await TestGarbageCollection.event.wait();
      return DBOS.workflowID;
    }
  }

  test('test-garbage-collection', async () => {
    const numWorkflows = 10;

    // Start one blocked workflow and 100 normal workflows
    const handle = await DBOS.startWorkflow(TestGarbageCollection).blockedWorkflow();
    for (let i = 0; i < numWorkflows; i++) {
      await expect(TestGarbageCollection.testWorkflow(i)).resolves.toBe(i);
    }

    // Garbage collect all but one workflow
    await DBOSExecutor.globalInstance!.systemDatabase.garbageCollect(undefined, 1);
    // Verify two workflows remain: the newest and blocked workflow
    let workflows = await DBOS.listWorkflows({});
    expect(workflows.length).toBe(2);
    expect(workflows[0].workflowID).toEqual(handle.workflowID);

    // Garbage collect all completed workflows
    await DBOSExecutor.globalInstance!.systemDatabase.garbageCollect(Date.now(), undefined);
    // Verify only the blocked workflow remains
    workflows = await DBOS.listWorkflows({});
    expect(workflows.length).toBe(1);
    expect(workflows[0].workflowID).toEqual(handle.workflowID);

    // Finish the blocked workflow, garbage collect everything
    TestGarbageCollection.event.set();
    await expect(handle.getResult()).resolves.toBeTruthy();
    await DBOSExecutor.globalInstance!.systemDatabase.garbageCollect(Date.now(), undefined);
    workflows = await DBOS.listWorkflows({});
    expect(workflows.length).toBe(0);

    // Verify GC runs without errors on a blank table
    await DBOSExecutor.globalInstance!.systemDatabase.garbageCollect(undefined, 1);

    // Run workflows, wait, run them again
    for (let i = 0; i < numWorkflows; i++) {
      await expect(TestGarbageCollection.testWorkflow(i)).resolves.toBe(i);
    }
    await sleepms(1000);
    for (let i = 0; i < numWorkflows; i++) {
      await expect(TestGarbageCollection.testWorkflow(i)).resolves.toBe(i);
    }
    // GC the first half, verify only half were GC'ed
    await DBOSExecutor.globalInstance!.systemDatabase.garbageCollect(Date.now() - 1000, undefined);
    workflows = await DBOS.listWorkflows({});
    expect(workflows.length).toBe(numWorkflows);
  });

  class TestGlobalTimeout {
    static blocked: boolean = true;

    @DBOS.workflow()
    static async blockedWorkflow() {
      while (TestGlobalTimeout.blocked) {
        await DBOS.sleep(100);
      }
      return DBOS.workflowID as string;
    }
  }

  test('test-global-timeout', async () => {
    const numWorkflows = 10;
    const handles: WorkflowHandle<string>[] = [];
    for (let i = 0; i < numWorkflows; i++) {
      handles.push(await DBOS.startWorkflow(TestGlobalTimeout).blockedWorkflow());
    }

    // Wait one second, start one final workflow, then timeout all workflows started more than one second ago
    await sleepms(1000);
    const finalHandle = await DBOS.startWorkflow(TestGlobalTimeout).blockedWorkflow();
    await globalTimeout(DBOSExecutor.globalInstance?.systemDatabase as PostgresSystemDatabase, Date.now() - 1000);

    // Verify all workflows started before the global timeout are cancelled
    for (const handle of handles) {
      await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    }
    TestGlobalTimeout.blocked = false;
    await expect(finalHandle.getResult()).resolves.toBeTruthy();
  });
});

describe('test-list-steps', () => {
  let config: DBOSConfig;
  const queue = new WorkflowQueue('child_queue');
  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });
  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
  });
  afterEach(async () => {
    await DBOS.shutdown();
  });

  class TestListSteps {
    @DBOS.workflow()
    static async testWorkflow() {
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      await DBOS.sleep(10);
      return DBOS.workflowID;
    }

    @DBOS.step()
    static async stepOne() {
      return Promise.resolve(DBOS.workflowID);
    }
    @DBOS.step()
    static async stepTwo() {
      return Promise.resolve(DBOS.workflowID);
    }

    @DBOS.workflow()
    static async sendWorkflow(target: string) {
      await DBOS.send(target, 'message1');
    }

    @DBOS.workflow()
    static async recvWorkflow(target: string) {
      const msg = await DBOS.recv(target, 1);
      console.log('received message:', msg);
    }

    @DBOS.workflow()
    static async setEventWorkflow() {
      await DBOS.setEvent('key', 'value');
      await DBOS.getEvent('fakewid', 'key', 1);
    }

    @DBOS.workflow()
    static async callChildWorkflowfirst() {
      const handle = await DBOS.startWorkflow(TestListSteps).testWorkflow();
      const childID = await handle.getResult();
      await handle.getStatus();
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      return childID;
    }
    @DBOS.workflow()
    static async callChildWorkflowMiddle() {
      await TestListSteps.stepOne();
      const handle = await DBOS.startWorkflow(TestListSteps).testWorkflow();
      await handle.getStatus();
      const childID = await handle.getResult();
      await TestListSteps.stepTwo();
      return childID;
    }
    @DBOS.workflow()
    static async callChildWorkflowLast() {
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      const handle = await DBOS.startWorkflow(TestListSteps).testWorkflow();
      await handle.getStatus();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async enqueueChildWorkflowFirst() {
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).testWorkflow();
      const childID = await handle.getResult();
      await handle.getStatus();
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      return childID;
    }

    @DBOS.workflow()
    static async enqueueChildWorkflowMiddle() {
      await TestListSteps.stepOne();
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).testWorkflow();
      await handle.getStatus();
      const childID = await handle.getResult();
      await TestListSteps.stepTwo();
      return childID;
    }

    @DBOS.workflow()
    static async enqueueChildWorkflowLast() {
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).testWorkflow();
      await handle.getStatus();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async directCallWorkflow() {
      const childID = await TestListSteps.testWorkflow();
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      return childID;
    }

    @DBOS.workflow()
    // eslint-disable-next-line  @typescript-eslint/require-await
    static async childWorkflowWithCounter(id: string) {
      return id;
    }

    @DBOS.step()
    static async failingStep() {
      await Promise.resolve();
      throw Error('fail');
    }

    @DBOS.workflow()
    static async callFailingStep() {
      await TestListSteps.failingStep();
    }

    @DBOS.workflow()
    static async startFailingStep() {
      const handle = await DBOS.startWorkflow(TestListSteps).failingStep();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async enqueueFailingStep() {
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).failingStep();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async CounterParent() {
      const childwfid = randomUUID();
      const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: childwfid }).childWorkflowWithCounter(
        childwfid,
      );
      return await handle.getResult();
    }
  }

  class ListWorkflows {
    @DBOS.workflow()
    static async listingWorkflow() {
      return (await DBOS.listWorkflows({})).length;
    }

    @DBOS.workflow()
    static async simpleWorkflow() {
      return Promise.resolve();
    }
  }

  const numStepTimingSteps = 5;
  async function stepTimingStep() {
    await sleepms(100);
  }

  const stepTimingWorkflow = DBOS.registerWorkflow(async () => {
    for (let i = 0; i < numStepTimingSteps; i++) {
      await DBOS.runStep(() => stepTimingStep());
    }
    await DBOS.setEvent('key', 'value');
    await DBOS.listWorkflows({});
    await DBOS.recv(undefined, 0);
  });

  test('test-step-timing', async () => {
    const startTime = Date.now();
    const handle = await DBOS.startWorkflow(stepTimingWorkflow)();

    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    assert(steps);
    for (const s of steps) {
      assert(s.startedAtEpochMs);
      assert(s.completedAtEpochMs);
      assert(s.startedAtEpochMs >= startTime);
      assert(s.completedAtEpochMs >= s.startedAtEpochMs);
      if (s.functionID < numStepTimingSteps) {
        assert(s.completedAtEpochMs - s.startedAtEpochMs >= 100);
      }
    }
  });

  test('test-list-steps', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('stepTwo');
    expect(wfsteps[2].functionID).toBe(2);
    expect(wfsteps[2].name).toBe('DBOS.sleep');
  });

  test('test-list-steps-invalid-wfid', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(randomUUID());
    expect(wfsteps).toBeUndefined();
  });

  test('test-send-recv', async () => {
    const wfid1 = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid1 }).recvWorkflow('message1');

    const wfid2 = randomUUID();
    await DBOS.startWorkflow(TestListSteps, { workflowID: wfid2 }).sendWorkflow(wfid1);

    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid1);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[1].name).toBe('DBOS.sleep');
    expect(wfsteps[0].name).toBe('DBOS.recv');

    const wfsteps2 = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid2);
    if (!wfsteps2) {
      throw new Error('wfsteps2 is undefined');
    }
    expect(wfsteps2[0].functionID).toBe(0);
    expect(wfsteps2[0].name).toBe('DBOS.send');
  });

  test('test-set-getEvent', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).setEventWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].name).toBe('DBOS.setEvent');
    expect(wfsteps[1].name).toBe('DBOS.getEvent');
  });

  test('test-call-child-workflow-first', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowfirst();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('testWorkflow');
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(childID);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].childWorkflowID).toBe(childID);
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[2].functionID).toBe(2);
    expect(wfsteps[2].output).toBeTruthy();
    expect(wfsteps[2].error).toBe(null);
    expect(wfsteps[2].childWorkflowID).toBe(null);
    expect(wfsteps[3].name).toBe('stepOne');
    expect(wfsteps[3].functionID).toBe(3);
    expect(wfsteps[3].output).toBe(wfid);
    expect(wfsteps[3].error).toBe(null);
    expect(wfsteps[3].childWorkflowID).toBe(null);
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-call-child-workflow-middle', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowMiddle();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('testWorkflow');
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[3].name).toBe('DBOS.getResult');
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-call-child-workflow-last', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowLast();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('stepTwo');
    expect(wfsteps[2].name).toBe('testWorkflow');
    expect(wfsteps[3].name).toBe('getStatus');
    expect(wfsteps[4].name).toBe('DBOS.getResult');
  });

  test('test-queue-child-workflow-first', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowFirst();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('testWorkflow');
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(childID);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].childWorkflowID).toBe(childID);
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[3].name).toBe('stepOne');
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-queue-child-workflow-middle', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowMiddle();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('testWorkflow');
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[3].name).toBe('DBOS.getResult');
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-queue-child-workflow-last', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowLast();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('stepTwo');
    expect(wfsteps[2].name).toBe('testWorkflow');
    expect(wfsteps[3].name).toBe('getStatus');
    expect(wfsteps[4].name).toBe('DBOS.getResult');
  });

  test('test-direct-call-workflow', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).directCallWorkflow();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(4);
    expect(wfsteps[0].name).toBe('testWorkflow');
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(childID);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].childWorkflowID).toBe(childID);
    expect(wfsteps[2].name).toBe('stepOne');
    expect(wfsteps[3].name).toBe('stepTwo');
  });

  test('test-list-failing-step', async () => {
    // Test calling a failing step directly
    let wfid = randomUUID();
    let handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callFailingStep();
    await expect(handle.getResult()).rejects.toThrow(new Error('fail'));
    let wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].name).toBe('failingStep');
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBeInstanceOf(Error);
    expect(wfsteps[0].childWorkflowID).toBe(null);
    // Test starting a failing step
    wfid = randomUUID();
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).startFailingStep();
    await expect(handle.getResult()).rejects.toThrow(new Error('fail'));
    wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].name).toBe('temp_workflow-step-failingStep');
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(`${wfid}-0`);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].output).toBe(null);
    expect(wfsteps[1].error).toBeInstanceOf(Error);
    expect(wfsteps[1].childWorkflowID).toBe(`${wfid}-0`);
    // Test enqueueing a failing step
    wfid = randomUUID();
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueFailingStep();
    await expect(handle.getResult()).rejects.toThrow(new Error('fail'));

    wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].name).toBe('temp_workflow-step-failingStep');
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(`${wfid}-0`);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].output).toBe(null);
    expect(wfsteps[1].error).toBeInstanceOf(Error);
    expect(wfsteps[1].childWorkflowID).toBe(`${wfid}-0`);
  });

  test('test-child-rerun', async () => {
    const wfid = randomUUID();
    let handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).CounterParent();
    const result1 = await handle.getResult();
    // call again with same wfid
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).CounterParent();
    const result2 = await handle.getResult();
    expect(result1).toEqual(result2);

    expect(config.systemDatabaseUrl).toBeDefined();
    const sysdb = new PostgresSystemDatabase(config.systemDatabaseUrl!, new GlobalLogger(), DBOSJSON);
    try {
      const wfs = await listWorkflows(sysdb, {});
      expect(wfs.length).toBe(2);

      const wfid1 = randomUUID();
      // call with different wfid we should get different result
      handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid1 }).CounterParent();
      const result3 = await handle.getResult();

      expect(result3).not.toEqual(result1);
    } finally {
      await sysdb.destroy();
    }
  });

  test('test-list-workflows-as-step', async () => {
    const wfid = randomUUID();
    const c1 = await DBOS.withNextWorkflowID(wfid, async () => {
      return await ListWorkflows.listingWorkflow();
    });
    expect(c1).toBe(1);

    await ListWorkflows.simpleWorkflow();

    // Let this start over
    const c2 = await (await reexecuteWorkflowById(wfid))?.getResult();
    expect(c2).toBe(1);
  });
});

describe('test-fork', () => {
  let config: DBOSConfig;
  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });
  beforeEach(async () => {
    ExampleWorkflow.stepOneCount = 0;
    ExampleWorkflow.stepTwoCount = 0;
    ExampleWorkflow.stepThreeCount = 0;
    ExampleWorkflow.stepFourCount = 0;
    ExampleWorkflow.stepFiveCount = 0;
    ExampleWorkflow.transactionOneCount = 0;
    ExampleWorkflow.transactionTwoCount = 0;
    ExampleWorkflow.transactionThreeCount = 0;
    ExampleWorkflow.childWorkflowCount = 0;
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
  });
  afterEach(async () => {
    await DBOS.shutdown();
  });

  class ExampleWorkflow {
    static stepOneCount = 0;
    static stepTwoCount = 0;
    static stepThreeCount = 0;
    static stepFourCount = 0;
    static stepFiveCount = 0;
    static transactionOneCount = 0;
    static transactionTwoCount = 0;
    static transactionThreeCount = 0;
    static childWorkflowCount = 0;

    @DBOS.workflow()
    static async stepsWorkflow(input: number): Promise<number> {
      let result = await ExampleWorkflow.stepOne(1);
      result += await ExampleWorkflow.stepTwo(2);
      result += await ExampleWorkflow.stepThree(3);
      result += await ExampleWorkflow.stepFour(4);
      result += await ExampleWorkflow.stepFive(5);
      return result * input;
    }

    @DBOS.step()
    static async stepOne(input: number): Promise<number> {
      ExampleWorkflow.stepOneCount += 1;
      return Promise.resolve(1 * input);
    }

    @DBOS.step()
    static async stepTwo(input: number): Promise<number> {
      ExampleWorkflow.stepTwoCount += 1;
      return Promise.resolve(2 * input);
    }

    @DBOS.step()
    static async stepThree(input: number): Promise<number> {
      ExampleWorkflow.stepThreeCount += 1;
      return Promise.resolve(3 * input);
    }

    @DBOS.step()
    static async stepFour(input: number): Promise<number> {
      ExampleWorkflow.stepFourCount += 1;
      return Promise.resolve(4 * input);
    }

    @DBOS.step()
    static async stepFive(input: number): Promise<number> {
      ExampleWorkflow.stepFiveCount += 1;
      return Promise.resolve(5 * input);
    }

    @DBOS.workflow()
    static async childWorkflow() {
      ExampleWorkflow.childWorkflowCount += 1;
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async forkWorkflow(id: string, stepID: number): Promise<string> {
      const handle = await DBOS.forkWorkflow(id, stepID);
      await handle.getResult();
      return handle.workflowID;
    }

    @DBOS.workflow()
    static async parentWorkflow() {
      await ExampleWorkflow.stepOne(1);
      const handle = await DBOS.startWorkflow(ExampleWorkflow).childWorkflow();
      await handle.getResult();
      await ExampleWorkflow.stepTwo(1);
    }
  }

  test('test-fork-steps', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).stepsWorkflow(10);
    const result: number = await handle.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);
    expect(ExampleWorkflow.stepThreeCount).toBe(1);
    expect(ExampleWorkflow.stepFourCount).toBe(1);
    expect(ExampleWorkflow.stepFiveCount).toBe(1);

    const forkedHandle = await DBOS.forkWorkflow(wfid, 0);
    expect((await forkedHandle.getStatus())?.forkedFrom).toBe(wfid);
    let forkresult = await forkedHandle.getResult();
    expect(forkresult).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(2);
    expect(ExampleWorkflow.stepFourCount).toBe(2);
    expect(ExampleWorkflow.stepFiveCount).toBe(2);

    const forkedHandle2 = await DBOS.forkWorkflow(wfid, 2);
    expect((await forkedHandle2.getStatus())?.forkedFrom).toBe(wfid);
    forkresult = await forkedHandle2.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(3);
    expect(ExampleWorkflow.stepFourCount).toBe(3);
    expect(ExampleWorkflow.stepFiveCount).toBe(3);

    const forkedHandle3 = await DBOS.forkWorkflow(wfid, 4);
    expect((await forkedHandle3.getStatus())?.forkedFrom).toBe(wfid);
    forkresult = await forkedHandle3.getResult();
    expect(forkresult).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(3);
    expect(ExampleWorkflow.stepFourCount).toBe(3);
    expect(ExampleWorkflow.stepFiveCount).toBe(4);

    const forkedWorkflows = await DBOS.listWorkflows({ forkedFrom: handle.workflowID });
    expect(forkedWorkflows.length).toBe(3);
    expect(forkedWorkflows[0].workflowID).toBe(forkedHandle.workflowID);
    expect(forkedWorkflows[1].workflowID).toBe(forkedHandle2.workflowID);
    expect(forkedWorkflows[2].workflowID).toBe(forkedHandle3.workflowID);
  }, 10000);

  test('test-fork-childwf', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).parentWorkflow();
    await handle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.childWorkflowCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);

    const forkedHandle = await DBOS.forkWorkflow(wfid, 2);
    expect((await forkedHandle.getStatus())?.forkedFrom).toBe(wfid);
    await forkedHandle.getResult();
    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.childWorkflowCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
  });

  test('test-fork-fromaworklow', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).parentWorkflow();
    await handle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.childWorkflowCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);

    const forkwfid = randomUUID();
    const forkHandle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: forkwfid }).forkWorkflow(wfid, 0);
    const firstforkedid = await forkHandle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.childWorkflowCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);

    // Fork the workflow again
    const forkHandle2 = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: forkwfid }).forkWorkflow(wfid, 0);
    const secondforkedid = await forkHandle2.getResult();

    expect(firstforkedid).toEqual(secondforkedid);
    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.childWorkflowCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
  });

  test('test-fork-WithNextWorkflowId', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).stepsWorkflow(10);
    const result: number = await handle.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);
    expect(ExampleWorkflow.stepThreeCount).toBe(1);
    expect(ExampleWorkflow.stepFourCount).toBe(1);
    expect(ExampleWorkflow.stepFiveCount).toBe(1);

    const forkedWfid = randomUUID();

    await DBOS.withNextWorkflowID(forkedWfid, async () => {
      const forkedHandle = await DBOS.forkWorkflow(wfid, 0);
      expect((await forkedHandle.getStatus())?.forkedFrom).toBe(wfid);
      const forkresult = await forkedHandle.getResult();
      expect(forkresult).toBe(550);
      expect(forkedHandle.workflowID).toBe(forkedWfid);
    });

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(2);
    expect(ExampleWorkflow.stepFourCount).toBe(2);
    expect(ExampleWorkflow.stepFiveCount).toBe(2);
  });

  test('test-fork-version', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).stepsWorkflow(10);
    const result: number = await handle.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);
    expect(ExampleWorkflow.stepThreeCount).toBe(1);
    expect(ExampleWorkflow.stepFourCount).toBe(1);
    expect(ExampleWorkflow.stepFiveCount).toBe(1);

    const applicationVersion = 'newVersion';

    globalParams.appVersion = applicationVersion;
    const forkedHandle = await DBOS.forkWorkflow(wfid, 0, { applicationVersion });

    const status = await forkedHandle.getStatus();
    const returnedVersion = status?.applicationVersion;
    expect(returnedVersion).toBe(applicationVersion);

    const forkresult = await forkedHandle.getResult();
    expect(forkresult).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(2);
    expect(ExampleWorkflow.stepFourCount).toBe(2);
    expect(ExampleWorkflow.stepFiveCount).toBe(2);
  });

  const testForkStreamsKey = 'key';
  const testForkStreamsEvent = new Event();
  // Step that writes to stream
  const streamStep = DBOS.registerStep(
    async (val: number) => {
      await DBOS.writeStream(testForkStreamsKey, val);
    },
    { name: 'stream-step' },
  );

  // Workflow: waits on event, writes 0, writes 1, calls step(2), closes stream
  const streamWorkflow = DBOS.registerWorkflow(
    async () => {
      await testForkStreamsEvent.wait();
      await DBOS.writeStream(testForkStreamsKey, 0); // function_id = 0
      await DBOS.writeStream(testForkStreamsKey, 1); // function_id = 1
      await streamStep(2); // function_id = 2
      await DBOS.closeStream(testForkStreamsKey); // function_id = 3
      return DBOS.workflowID!;
    },
    { name: 'stream-fork-workflow' },
  );

  test('test-fork-streams', async () => {
    // Helper to read N values from a stream without blocking forever
    async function readStreamN(workflowID: string, n: number): Promise<number[]> {
      if (n === 0) return [];
      const values: number[] = [];
      for await (const value of DBOS.readStream(workflowID, testForkStreamsKey)) {
        values.push(value as number);
        if (values.length >= n) break;
      }
      return values;
    }

    // Run workflow to completion first
    testForkStreamsEvent.set();
    const handle = await DBOS.startWorkflow(streamWorkflow, {})();
    expect(await handle.getResult()).toBe(handle.workflowID);

    // Verify original stream has [0, 1, 2]
    const allValues: number[] = [];
    for await (const v of DBOS.readStream(handle.workflowID, testForkStreamsKey)) {
      allValues.push(v as number);
    }
    expect(allValues).toEqual([0, 1, 2]);

    // Block workflow so forks can't advance
    testForkStreamsEvent.clear();

    // Fork from different points, verify streams have appropriate values
    const forkOne = await DBOS.forkWorkflow(handle.workflowID, 0);
    expect(await readStreamN(forkOne.workflowID, 0)).toEqual([]);

    const forkTwo = await DBOS.forkWorkflow(handle.workflowID, 1);
    expect(await readStreamN(forkTwo.workflowID, 1)).toEqual([0]);

    const forkThree = await DBOS.forkWorkflow(handle.workflowID, 2);
    expect(await readStreamN(forkThree.workflowID, 2)).toEqual([0, 1]);

    const forkFour = await DBOS.forkWorkflow(handle.workflowID, 3);
    expect(await readStreamN(forkFour.workflowID, 3)).toEqual([0, 1, 2]);

    const forkFive = await DBOS.forkWorkflow(handle.workflowID, 4);
    const forkFiveValues: number[] = [];
    for await (const value of DBOS.readStream(forkFive.workflowID, testForkStreamsKey)) {
      forkFiveValues.push(value as number);
    }
    expect(forkFiveValues).toEqual([0, 1, 2]);

    // Unblock the forked workflows, verify they successfully complete
    testForkStreamsEvent.set();
    for (const forkHandle of [forkOne, forkTwo, forkThree, forkFour, forkFive]) {
      expect(await forkHandle.getResult()).toBeTruthy();
      const finalValues: number[] = [];
      for await (const value of DBOS.readStream(forkHandle.workflowID, testForkStreamsKey)) {
        finalValues.push(value as number);
      }
      expect(finalValues).toEqual([0, 1, 2]);
    }
  }, 10000);

  const testForkEventsKey = 'event_key';
  const testForkEventsEvent = new Event();

  // Workflow: waits on event, sets event to 0, 1, 2
  const eventWorkflow = DBOS.registerWorkflow(
    async () => {
      await testForkEventsEvent.wait();
      await DBOS.setEvent(testForkEventsKey, 0); // function_id = 0
      await DBOS.setEvent(testForkEventsKey, 1); // function_id = 1
      await DBOS.setEvent(testForkEventsKey, 2); // function_id = 2
      return DBOS.workflowID!;
    },
    { name: 'event-fork-workflow' },
  );

  test('test-fork-events', async () => {
    // Run workflow to completion first
    testForkEventsEvent.set();
    const handle = await DBOS.startWorkflow(eventWorkflow, {})();
    expect(await handle.getResult()).toBe(handle.workflowID);

    // Verify the event's final value is 2
    expect(await DBOS.getEvent(handle.workflowID, testForkEventsKey)).toBe(2);

    // Block workflow so forks can't advance
    testForkEventsEvent.clear();

    // Fork from different points, verify events have appropriate values
    const forkOne = await DBOS.forkWorkflow(handle.workflowID, 0);
    expect(await DBOS.getEvent(forkOne.workflowID, testForkEventsKey, 0)).toBeNull();

    const forkTwo = await DBOS.forkWorkflow(handle.workflowID, 1);
    expect(await DBOS.getEvent(forkTwo.workflowID, testForkEventsKey)).toBe(0);

    const forkThree = await DBOS.forkWorkflow(handle.workflowID, 2);
    expect(await DBOS.getEvent(forkThree.workflowID, testForkEventsKey)).toBe(1);

    const forkFour = await DBOS.forkWorkflow(handle.workflowID, 3);
    expect(await DBOS.getEvent(forkFour.workflowID, testForkEventsKey)).toBe(2);

    // Fork from a fork
    const forkFive = await DBOS.forkWorkflow(forkFour.workflowID, 3);
    expect(await DBOS.getEvent(forkFive.workflowID, testForkEventsKey)).toBe(2);

    // Unblock the forked workflows, verify they successfully complete
    testForkEventsEvent.set();
    for (const forkHandle of [forkOne, forkTwo, forkThree, forkFour, forkFive]) {
      expect(await forkHandle.getResult()).toBeTruthy();
      expect(await DBOS.getEvent(forkHandle.workflowID, testForkEventsKey)).toBe(2);
    }
  }, 10000);
});

describe('wf-cancel-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    WFwith2Steps.stepsExecuted = 0;
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  test('test-two-steps-base', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    await wfh.getResult();

    expect(WFwith2Steps.stepsExecuted).toBe(2);
  });

  test('test-two-steps-cancel', async () => {
    const wfid = randomUUID();

    try {
      const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

      await DBOS.cancelWorkflow(wfid);
      await wfh.getResult();
    } catch (e) {
      console.log(`number executed  ${WFwith2Steps.stepsExecuted}`);

      expect(WFwith2Steps.stepsExecuted).toBe(1);

      const wfstatus = await DBOS.getWorkflowStatus(wfid);

      expect(wfstatus?.status).toBe(StatusString.CANCELLED);
    }
  });

  test('test-two-steps-cancel-resume', async () => {
    const wfid = randomUUID();

    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    try {
      await DBOS.cancelWorkflow(wfid);

      await wfh.getResult();
    } catch (e) {
      console.log(`number executed  ${WFwith2Steps.stepsExecuted}`);

      expect(WFwith2Steps.stepsExecuted).toBe(1);

      const wfstatus = await DBOS.getWorkflowStatus(wfid);

      expect(wfstatus?.status).toBe(StatusString.CANCELLED);
    }

    const wfh2 = await DBOS.resumeWorkflow(wfid);
    await wfh2.getResult();
    const resstatus = await DBOS.getWorkflowStatus(wfid);
    expect(resstatus?.status).toBe(StatusString.SUCCESS);
  });

  test('test-resume-on-a-completed-ws', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    await wfh.getResult();

    expect(WFwith2Steps.stepsExecuted).toBe(2);

    await DBOS.resumeWorkflow(wfid);
    await DBOS.getWorkflowStatus(wfid);

    expect(WFwith2Steps.stepsExecuted).toBe(2);
  });

  test('test-preempt-sleepms', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).sleepTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-getresult', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).getResultTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-getevent', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).getEventTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-recv', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).recvTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  class WFwith2Steps {
    static stepsExecuted = 0 as number;

    @DBOS.step()
    static async step1() {
      WFwith2Steps.stepsExecuted++;
      console.log(`Step 1  ${WFwith2Steps.stepsExecuted}`);
      await DBOS.sleepSeconds(1);
    }

    @DBOS.step()
    // eslint-disable-next-line @typescript-eslint/require-await
    static async step2() {
      WFwith2Steps.stepsExecuted++;
      console.log(`Step 1  ${WFwith2Steps.stepsExecuted}`);
    }

    @DBOS.workflow()
    static async workflowWithSteps() {
      await WFwith2Steps.step1();
      await WFwith2Steps.step2();
      return Promise.resolve();
    }
  }

  class DeepSleep {
    @DBOS.workflow()
    static async sleepTooLong() {
      await DBOS.sleepms(1000 * 1000);
      return 'Done';
    }

    @DBOS.workflow()
    static async getResultTooLong() {
      await DBOS.getResult('bogusbogusbogus', 1000);
      return 'Done';
    }

    @DBOS.workflow()
    static async recvTooLong() {
      await DBOS.recv('bogusbogusbogus', 1000);
      return 'Done';
    }

    @DBOS.workflow()
    static async getEventTooLong() {
      await DBOS.getEvent('bogusbogusbogus', 'notopic', 1000);
      return 'Done';
    }
  }

  // Delete workflow test
  class DeleteWorkflowTest {
    @DBOS.workflow()
    static async childWorkflow(x: number): Promise<number> {
      return Promise.resolve(x * 2);
    }

    @DBOS.workflow()
    static async parentWorkflow(x: number): Promise<number> {
      const handle = await DBOS.startWorkflow(DeleteWorkflowTest).childWorkflow(x);
      return handle.getResult();
    }
  }

  test('test-delete-workflow', async () => {
    // Run the parent workflow which starts a child workflow
    const parentWfid = randomUUID();
    const handle = await DBOS.startWorkflow(DeleteWorkflowTest, { workflowID: parentWfid }).parentWorkflow(5);
    const result = await handle.getResult();
    expect(result).toBe(10);

    // Get the child workflow ID
    const steps = await DBOS.listWorkflowSteps(parentWfid);
    const childWfid = steps!.find((s) => s.childWorkflowID)?.childWorkflowID;
    expect(childWfid).toBeDefined();

    // Verify both workflows exist
    expect(await DBOS.getWorkflowStatus(parentWfid)).not.toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid!)).not.toBeNull();

    // Delete without deleteChildren - only parent should be deleted
    await DBOS.deleteWorkflow(parentWfid, false);
    expect(await DBOS.getWorkflowStatus(parentWfid)).toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid!)).not.toBeNull();

    // Run again to test deleteChildren=true
    const parentWfid2 = randomUUID();
    const handle2 = await DBOS.startWorkflow(DeleteWorkflowTest, { workflowID: parentWfid2 }).parentWorkflow(7);
    const result2 = await handle2.getResult();
    expect(result2).toBe(14);

    const steps2 = await DBOS.listWorkflowSteps(parentWfid2);
    const childWfid2 = steps2!.find((s) => s.childWorkflowID)?.childWorkflowID;
    expect(childWfid2).toBeDefined();

    // Verify both workflows exist
    expect(await DBOS.getWorkflowStatus(parentWfid2)).not.toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid2!)).not.toBeNull();

    // Delete with deleteChildren=true - both should be deleted
    await DBOS.deleteWorkflow(parentWfid2, true);
    expect(await DBOS.getWorkflowStatus(parentWfid2)).toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid2!)).toBeNull();

    // Verify deleting a non-existent workflow doesn't error
    await DBOS.deleteWorkflow(parentWfid2, false);
  });

  // Workflow export/import test
  class ExportImportTest {
    @DBOS.step()
    static async testStep(): Promise<void> {
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async grandchildWorkflow(): Promise<string> {
      return Promise.resolve('grandchild-result');
    }

    @DBOS.workflow()
    static async childWorkflow(): Promise<string> {
      const handle = await DBOS.startWorkflow(ExportImportTest).grandchildWorkflow();
      await handle.getResult();
      return 'child-result';
    }

    @DBOS.workflow()
    static async parentWorkflow(): Promise<string> {
      const handle = await DBOS.startWorkflow(ExportImportTest).childWorkflow();
      await handle.getResult();
      // Run multiple steps
      for (let i = 0; i < 10; i++) {
        await ExportImportTest.testStep();
      }
      await DBOS.setEvent('key', 'value');
      await DBOS.writeStream('key', 'value');
      await DBOS.closeStream('key');
      return DBOS.workflowID!;
    }
  }

  test('test-workflow-export-import', async () => {
    const workflowId = randomUUID();
    const handle = await DBOS.startWorkflow(ExportImportTest, { workflowID: workflowId }).parentWorkflow();
    const result = await handle.getResult();
    expect(result).toBe(workflowId);

    const sysDb = DBOSExecutor.globalInstance!.systemDatabase as PostgresSystemDatabase;

    // Export with children
    const exported = await sysDb.exportWorkflow(workflowId, true);
    const originalSteps = await DBOS.listWorkflowSteps(workflowId);

    // Importing into an existing database fails with a primary key conflict
    await expect(sysDb.importWorkflow(exported)).rejects.toThrow();

    // Delete the workflows
    await DBOS.deleteWorkflow(workflowId, true);

    // Importing the workflow succeeds after deletion
    await sysDb.importWorkflow(exported);

    // All workflow information is present - verify event
    const eventValue = await DBOS.getEvent(workflowId, 'key');
    expect(eventValue).toBe('value');

    // Verify stream is restored
    const streamValues: unknown[] = [];
    for await (const v of DBOS.readStream(workflowId, 'key')) {
      streamValues.push(v);
    }
    expect(streamValues).toEqual(['value']);

    // Verify steps are restored with same content
    const importedSteps = await DBOS.listWorkflowSteps(workflowId);
    expect(importedSteps!.length).toBe(originalSteps!.length);
    for (let i = 0; i < importedSteps!.length; i++) {
      expect(importedSteps![i].functionID).toBe(originalSteps![i].functionID);
      expect(importedSteps![i].name).toBe(originalSteps![i].name);
    }

    // The child workflows are also copied over (parent + child + grandchild = 3)
    expect(exported.length).toBe(3);
    const allWorkflows = await DBOS.listWorkflows({
      workflowIDs: exported.map((w) => w.workflow_status.workflow_uuid),
    });
    expect(allWorkflows.length).toBe(3);

    // The imported workflow can be forked
    const forkedHandle = await DBOS.forkWorkflow(workflowId, importedSteps!.length);
    const forkedResult = await forkedHandle.getResult();
    expect(forkedResult).toBe(forkedHandle.workflowID);

    // The forked workflow has the event
    const forkedEventValue = await DBOS.getEvent(forkedHandle.workflowID, 'key');
    expect(forkedEventValue).toBe('value');
  });
});
