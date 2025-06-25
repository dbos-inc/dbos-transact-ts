import { GetWorkflowsInput, StatusString, Authentication, MiddlewareContext, DBOS, WorkflowQueue } from '../src';
import request from 'supertest';
import { DBOSConfigInternal, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event, recoverPendingWorkflows } from './helpers';
import { Client } from 'pg';
import { GetQueuedWorkflowsInput, WorkflowHandle, WorkflowStatus } from '../src/workflow';
import { randomUUID } from 'node:crypto';
import { globalParams, sleepms } from '../src/utils';
import { PostgresSystemDatabase } from '../src/system_database';
import { GlobalLogger as Logger } from '../src/telemetry/logs';
import {
  getWorkflow,
  globalTimeout,
  listQueuedWorkflows,
  listWorkflows,
} from '../src/dbos-runtime/workflow_management';
import { DBOSNonExistentWorkflowError, DBOSWorkflowCancelledError } from '../src/error';

describe('workflow-management-tests', () => {
  const testTableName = 'dbos_test_kv';

  let config: DBOSConfigInternal;
  let systemDBClient: Client;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestDb(config);
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`);

    systemDBClient = new Client({
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: config.system_database,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
    process.env.DBOS__APPVERSION = undefined;
  });

  test('simple-getworkflows', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input: {} });
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);
  });

  test('getworkflows-with-dates', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      startTime: new Date(Date.now() - 10000).toISOString(),
      endTime: new Date(Date.now()).toISOString(),
    };
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);

    input.endTime = new Date(Date.now() - 10000).toISOString();
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(0);
  });

  test('getworkflows-with-status', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      status: StatusString.SUCCESS,
    };
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);

    input.status = StatusString.PENDING;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(0);
  });

  test('getworkflows-with-wfname', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      workflowName: 'testWorkflow',
    };
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);
  });

  test('getworkflows-with-authentication', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      authenticatedUser: 'alice',
    };
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);
  });

  test('getworkflows-with-authentication', async () => {
    let response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      applicationVersion: globalParams.appVersion,
    };
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);

    input.applicationVersion = 'v1';
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(0);
  });

  test('getworkflows-with-limit', async () => {
    const workflowIDs: string[] = [];
    let response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow_get_id');
    expect(response.statusCode).toBe(200);
    expect(response.text.length).toBeGreaterThan(0);
    workflowIDs.push(response.text);

    const input: GetWorkflowsInput = {
      limit: 10,
    };

    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);
    expect(workflowUUIDs[0].workflowID).toBe(workflowIDs[0]);

    for (let i = 0; i < 10; i++) {
      response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow_get_id');
      expect(response.statusCode).toBe(200);
      expect(response.text.length).toBeGreaterThan(0);
      workflowIDs.push(response.text);
    }

    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      // The order should be ascending by default
      expect(workflowUUIDs[i].workflowID).toBe(workflowIDs[i]);
    }

    // Test sort_desc inverts the order
    input.sortDesc = true;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      expect(workflowUUIDs[i].workflowID).toBe(workflowIDs[10 - i]);
    }

    // Test LIMIT 2 OFFSET 2 returns the third and fourth workflows
    input.limit = 2;
    input.offset = 2;
    input.sortDesc = false;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(2);
    for (let i = 0; i < workflowUUIDs.length; i++) {
      expect(workflowUUIDs[i].workflowID).toBe(workflowIDs[i + 2]);
    }

    // Test OFFSET 10 returns the last workflow
    input.offset = 10;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(1);
    for (let i = 0; i < workflowUUIDs.length; i++) {
      expect(workflowUUIDs[i].workflowID).toBe(workflowIDs[i + 10]);
    }

    // Test search by workflow ID.
    const wfidInput: GetWorkflowsInput = {
      workflowIDs: [workflowIDs[5], workflowIDs[7]],
    };
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input: wfidInput });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as WorkflowStatus[];
    expect(workflowUUIDs.length).toBe(2);
    expect(workflowUUIDs[0].workflowID).toBe(workflowIDs[5]);
    expect(workflowUUIDs[1].workflowID).toBe(workflowIDs[7]);
  });

  test('getworkflows-cli', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const failResponse = await request(DBOS.getHTTPHandlersCallback()!).post('/fail/alice');
    expect(failResponse.statusCode).toBe(500);

    const logger = new Logger();
    const sysdb = new PostgresSystemDatabase(config.poolConfig, config.system_database, logger);
    try {
      const input: GetWorkflowsInput = {};
      const infos = await listWorkflows(sysdb, input);
      expect(infos.length).toBe(2);
      let info = infos[0];
      expect(info.authenticatedUser).toBe('alice');
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

      info = infos[1];
      expect(info.authenticatedUser).toBe('alice');
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

  test('test-restart-transaction', async () => {
    TestEndpoints.tries = 0;

    await TestEndpoints.testTransaction();
    expect(TestEndpoints.tries).toBe(1);

    let result = await systemDBClient.query<{ status: string; workflow_uuid: string; name: string }>(
      `SELECT status, workflow_uuid, name FROM dbos.workflow_status`,
      [],
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
    expect(result.rows[0].name).toBe('temp_workflow-transaction-testTransaction');
    const workflowUUID = result.rows[0].workflow_uuid;

    let wfh = await DBOS.forkWorkflow(workflowUUID, 0);
    await wfh.getResult();
    expect(TestEndpoints.tries).toBe(2);

    result = await systemDBClient.query<{ status: string; workflow_uuid: string; name: string }>(
      `SELECT status, workflow_uuid, name FROM dbos.workflow_status WHERE workflow_uuid!=$1`,
      [workflowUUID],
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
    expect(result.rows[0].name).toBe('temp_workflow-transaction-testTransaction');
    const restartedWorkflowUUID = result.rows[0].workflow_uuid;

    wfh = await DBOS.forkWorkflow(restartedWorkflowUUID, 0);
    await wfh.getResult();
    expect(TestEndpoints.tries).toBe(3);
  });

  test('systemdb-migration-backward-compatible', async () => {
    // Make sure the system DB migration failure is handled correctly.
    // If there is a migration failure, the system DB should still be able to start.
    // This happens when the old code is running with a new system DB schema.
    await DBOS.shutdown();
    await systemDBClient.query(
      `INSERT INTO knex_migrations (name, batch, migration_time) VALUES ('faketest.js', 1, now());`,
    );
    await DBOS.launch();
    DBOS.setUpHandlerCallback();
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');
  });

  async function testAuthMiddleware(_ctx: MiddlewareContext) {
    return Promise.resolve({
      authenticatedUser: 'alice',
      authenticatedRoles: ['aliceRole'],
    });
  }

  @Authentication(testAuthMiddleware)
  class TestEndpoints {
    @DBOS.postApi('/workflow/:name')
    @DBOS.workflow()
    static async testWorkflow(name: string) {
      return Promise.resolve(name);
    }

    @DBOS.postApi('/workflow_get_id')
    @DBOS.workflow()
    static async testWorkflowGetID() {
      return Promise.resolve(DBOS.workflowID);
    }

    @DBOS.postApi('/fail/:name')
    @DBOS.workflow()
    static async failWorkflow(name: string) {
      await Promise.resolve(name);
      throw new Error(name);
    }

    @DBOS.postApi('/getWorkflows')
    static async getWorkflows(input: GetWorkflowsInput) {
      return await DBOS.listWorkflows(input);
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

    @DBOS.transaction()
    static async testTransaction() {
      TestEndpoints.tries += 1;
      return Promise.resolve();
    }
  }
});

describe('test-list-queues', () => {
  let config: DBOSConfigInternal;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
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

    const logger = new Logger();
    const sysdb = new PostgresSystemDatabase(config.poolConfig, config.system_database, logger);
    try {
      let input: GetQueuedWorkflowsInput = {};
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
  let config: DBOSConfigInternal;
  const queue = new WorkflowQueue('child_queue');
  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });
  beforeEach(async () => {
    await setUpDBOSTestDb(config);
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

    @DBOS.transaction()
    static async transaction() {
      return Promise.resolve(DBOS.workflowID);
    }

    @DBOS.transaction()
    static async transactionWithError() {
      await Promise.resolve();
      throw Error('transaction error');
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

    @DBOS.workflow()
    static async workflowWithTransaction() {
      await TestListSteps.transaction();
    }

    @DBOS.workflow()
    static async workflowWithTransactionError() {
      try {
        await TestListSteps.transactionWithError();
      } catch (e) {
        console.log('transaction error', e);
      }
    }

    @DBOS.workflow()
    static async workflowWithTransactionAndSteps() {
      await TestListSteps.stepOne();
      await TestListSteps.transaction();
      await TestListSteps.stepTwo();
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

    const sysdb = new PostgresSystemDatabase(config.poolConfig, config.system_database, new Logger());
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

  test('test-transaction', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).workflowWithTransaction();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].name).toBe('transaction');
    expect(wfsteps[0].output).toBe(wfid);
    expect(wfsteps[0].error).toBe(null);
  });

  test('test-transaction-error', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).workflowWithTransactionError();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].name).toBe('transactionWithError');
    expect(wfsteps[0].error).toBeInstanceOf(Error);
    expect(wfsteps[0].output).toBe(null);
  });

  test('test-transaction-steps', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).workflowWithTransactionAndSteps();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('transaction');
    expect(wfsteps[2].name).toBe('stepTwo');
  });

  test('test-list-workflows-as-step', async () => {
    const wfid = randomUUID();
    const c1 = await DBOS.withNextWorkflowID(wfid, async () => {
      return await ListWorkflows.listingWorkflow();
    });
    expect(c1).toBe(1);

    await ListWorkflows.simpleWorkflow();

    // Let this start over
    await DBOSExecutor.globalInstance?.systemDatabase.setWorkflowStatus(wfid, StatusString.PENDING, true);

    // This value was stored
    const c2 = await DBOS.withNextWorkflowID(wfid, async () => {
      return await ListWorkflows.listingWorkflow();
    });
    expect(c2).toBe(1);
  });
});

describe('test-fork', () => {
  let config: DBOSConfigInternal;
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
    await setUpDBOSTestDb(config);
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

    @DBOS.workflow()
    static async stepsAndTransactionWorkflow() {
      await ExampleWorkflow.stepOne(1);
      await ExampleWorkflow.transactionOne();
      await ExampleWorkflow.stepTwo(1);
      await ExampleWorkflow.transactionTwo();
      await ExampleWorkflow.transactionThree();
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

    @DBOS.transaction()
    static async transactionOne() {
      ExampleWorkflow.transactionOneCount += 1;
      return Promise.resolve();
    }
    @DBOS.transaction()
    static async transactionTwo() {
      ExampleWorkflow.transactionTwoCount += 1;
      return Promise.resolve();
    }
    @DBOS.transaction()
    static async transactionThree() {
      ExampleWorkflow.transactionThreeCount += 1;
      return Promise.resolve();
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
    let forkresult = await forkedHandle.getResult();
    expect(forkresult).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(2);
    expect(ExampleWorkflow.stepFourCount).toBe(2);
    expect(ExampleWorkflow.stepFiveCount).toBe(2);

    const forkedHandle2 = await DBOS.forkWorkflow(wfid, 2);
    forkresult = await forkedHandle2.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(3);
    expect(ExampleWorkflow.stepFourCount).toBe(3);
    expect(ExampleWorkflow.stepFiveCount).toBe(3);

    const forkedHandle3 = await DBOS.forkWorkflow(wfid, 4);
    forkresult = await forkedHandle3.getResult();
    expect(forkresult).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(3);
    expect(ExampleWorkflow.stepFourCount).toBe(3);
    expect(ExampleWorkflow.stepFiveCount).toBe(4);
  }, 10000);

  test('test-fork-steps-transactions', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).stepsAndTransactionWorkflow();
    await handle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.transactionOneCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);
    expect(ExampleWorkflow.transactionTwoCount).toBe(1);
    expect(ExampleWorkflow.transactionThreeCount).toBe(1);

    const forkedHandle = await DBOS.forkWorkflow(wfid, 0);
    await forkedHandle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.transactionOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.transactionTwoCount).toBe(2);
    expect(ExampleWorkflow.transactionThreeCount).toBe(2);

    const forkedHandle2 = await DBOS.forkWorkflow(wfid, 1);
    await forkedHandle2.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.transactionOneCount).toBe(3);
    expect(ExampleWorkflow.stepTwoCount).toBe(3);
    expect(ExampleWorkflow.transactionTwoCount).toBe(3);
    expect(ExampleWorkflow.transactionThreeCount).toBe(3);

    const forkedHandle3 = await DBOS.forkWorkflow(wfid, 4);
    await forkedHandle3.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.transactionOneCount).toBe(3);
    expect(ExampleWorkflow.stepTwoCount).toBe(3);
    expect(ExampleWorkflow.transactionTwoCount).toBe(3);
    expect(ExampleWorkflow.transactionThreeCount).toBe(4);
  }, 10000);

  test('test-fork-childwf', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).parentWorkflow();
    await handle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.childWorkflowCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);

    const forkedHandle = await DBOS.forkWorkflow(wfid, 2);
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
});
