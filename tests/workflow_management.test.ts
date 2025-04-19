import {
  GetWorkflowsOutput,
  GetWorkflowsInput,
  StatusString,
  Authentication,
  MiddlewareContext,
  DBOS,
  WorkflowQueue,
} from '../src';
import request from 'supertest';
import { DBOSConfigInternal, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event } from './helpers';
import {
  WorkflowInformation,
  getWorkflow,
  listWorkflows,
  listQueuedWorkflows,
} from '../src/dbos-runtime/workflow_management';
import { Client } from 'pg';
import { v4 as uuidv4 } from 'uuid';
import { GetQueuedWorkflowsInput, WorkflowHandle } from '../src/workflow';
import { globalParams } from '../src/utils';

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
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
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
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.endTime = new Date(Date.now() - 10000).toISOString();
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
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
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.status = StatusString.PENDING;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
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
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
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
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
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
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.applicationVersion = 'v1';
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
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
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
    expect(workflowUUIDs.workflowUUIDs[0]).toBe(workflowIDs[0]);

    for (let i = 0; i < 10; i++) {
      response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow_get_id');
      expect(response.statusCode).toBe(200);
      expect(response.text.length).toBeGreaterThan(0);
      workflowIDs.push(response.text);
    }

    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      // The order should be ascending by default
      expect(workflowUUIDs.workflowUUIDs[i]).toBe(workflowIDs[i]);
    }

    // Test sort_desc inverts the order
    input.sortDesc = true;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      expect(workflowUUIDs.workflowUUIDs[i]).toBe(workflowIDs[10 - i]);
    }

    // Test LIMIT 2 OFFSET 2 returns the third and fourth workflows
    input.limit = 2;
    input.offset = 2;
    input.sortDesc = false;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(2);
    for (let i = 0; i < workflowUUIDs.workflowUUIDs.length; i++) {
      expect(workflowUUIDs.workflowUUIDs[i]).toBe(workflowIDs[i + 2]);
    }

    // Test OFFSET 10 returns the last workflow
    input.offset = 10;
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
    for (let i = 0; i < workflowUUIDs.workflowUUIDs.length; i++) {
      expect(workflowUUIDs.workflowUUIDs[i]).toBe(workflowIDs[i + 10]);
    }

    // Test search by workflow ID.
    const wfidInput: GetWorkflowsInput = {
      workflowIDs: [workflowIDs[5], workflowIDs[7]],
    };
    response = await request(DBOS.getHTTPHandlersCallback()!).post('/getWorkflows').send({ input: wfidInput });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(2);
    expect(workflowUUIDs.workflowUUIDs[0]).toBe(workflowIDs[5]);
    expect(workflowUUIDs.workflowUUIDs[1]).toBe(workflowIDs[7]);
  });

  test('getworkflows-cli', async () => {
    const response = await request(DBOS.getHTTPHandlersCallback()!).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const failResponse = await request(DBOS.getHTTPHandlersCallback()!).post('/fail/alice');
    expect(failResponse.statusCode).toBe(500);

    const input: GetWorkflowsInput = {};
    const infos = await listWorkflows(config, input, false);
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

    const getInfo = await getWorkflow(config, info.workflowUUID, false);
    expect(info).toEqual(getInfo);
  });

  test('test-cancel-retry-restart', async () => {
    TestEndpoints.tries = 0;

    const handle = await DBOS.startWorkflow(TestEndpoints).waitingWorkflow();

    expect(TestEndpoints.tries).toBe(1);
    await DBOS.cancelWorkflow(handle.getWorkflowUUID());

    let result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.CANCELLED);

    await DBOS.recoverPendingWorkflows(); // Does nothing as the workflow is CANCELLED
    expect(TestEndpoints.tries).toBe(1);

    TestEndpoints.testResolve();
    // Retry the workflow, resetting the attempts counter
    const handle2 = await DBOS.resumeWorkflow(handle.getWorkflowUUID());
    await handle2.getResult();

    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(TestEndpoints.tries).toBe(2);
    await handle.getResult();

    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);

    // Restart the workflow
    const wfh = await DBOS.executeWorkflowById(handle.getWorkflowUUID(), true);
    await wfh.getResult();
    expect(TestEndpoints.tries).toBe(3);
    // Validate a new workflow is started and successful
    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid!=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
    // Validate the original workflow status hasn't changed
    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
  });

  test('test-restart-transaction', async () => {
    TestEndpoints.tries = 0;

    await DBOS.invoke(TestEndpoints).testTransaction();
    expect(TestEndpoints.tries).toBe(1);

    let result = await systemDBClient.query<{ status: string; workflow_uuid: string; name: string }>(
      `SELECT status, workflow_uuid, name FROM dbos.workflow_status`,
      [],
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
    expect(result.rows[0].name).toBe('temp_workflow-transaction-testTransaction');
    const workflowUUID = result.rows[0].workflow_uuid;

    let wfh = await DBOS.executeWorkflowById(workflowUUID, true);
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

    wfh = await DBOS.executeWorkflowById(restartedWorkflowUUID, true);
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
      return await DBOS.getWorkflows(input);
    }

    static tries = 0;
    static testResolve: () => void;
    static testPromise = new Promise<void>((resolve) => {
      TestEndpoints.testResolve = resolve;
    });

    @DBOS.workflow()
    static async waitingWorkflow() {
      TestEndpoints.tries += 1;
      await TestEndpoints.testPromise;
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
    console.log('starting test-list-queues');

    const wfid = uuidv4();

    // Start the workflow. Wait for all five tasks to start. Verify that they started.
    const originalHandle = await DBOS.startWorkflow(TestListQueues, { workflowID: wfid }).testWorkflow();
    for (const e of TestListQueues.taskEvents) {
      await e.wait();
    }

    let input: GetQueuedWorkflowsInput = {};
    let output: WorkflowInformation[] = [];
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(TestListQueues.queuedSteps);

    // Test workflowName
    input = {
      workflowName: 'blockingTask',
    };

    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(TestListQueues.queuedSteps);
    for (let i = 0; i < TestListQueues.queuedSteps; i++) {
      expect(output[i].input).toEqual([i]);
    }

    input = {
      workflowName: 'no',
    };
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(0);

    // Test sortDesc reverts the order
    input = {
      sortDesc: true,
    };
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(TestListQueues.queuedSteps);
    for (let i = 0; i < TestListQueues.queuedSteps; i++) {
      expect(output[i].input).toEqual([TestListQueues.queuedSteps - i - 1]);
    }

    // Test startTime and endTime
    input = {
      startTime: new Date(Date.now() - 10000).toISOString(),
      endTime: new Date(Date.now()).toISOString(),
    };
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(TestListQueues.queuedSteps);
    input = {
      startTime: new Date(Date.now() + 10000).toISOString(),
    };

    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(0);

    // Test status
    input = {
      status: 'PENDING',
    };
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(TestListQueues.queuedSteps);
    input = {
      status: 'SUCCESS',
    };

    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(0);

    // Test queue name
    input = {
      queueName: TestListQueues.queue.name,
    };
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(TestListQueues.queuedSteps);

    input = {
      queueName: 'no',
    };

    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(0);

    // Test limit
    input = {
      limit: 2,
    };
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(input.limit);
    for (let i = 0; i < input.limit!; i++) {
      expect(output[i].input).toEqual([i]);
    }

    // Test offset
    input = {
      limit: 2,
      offset: 2,
    };
    output = await listQueuedWorkflows(config, input, false);
    expect(output.length).toBe(input.limit);
    for (let i = 0; i < input.limit!; i++) {
      expect(output[i].input).toEqual([i + 2]);
    }

    // Confirm the workflow finishes and nothing is in the queue afterwards
    TestListQueues.event.set();
    await expect(originalHandle.getResult()).resolves.toEqual([0, 1, 2, 3, 4]);

    input = {};
    await expect(listQueuedWorkflows(config, input, false)).resolves.toEqual([]);
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
      const childwfid = uuidv4();
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
      return (await DBOS.getWorkflows({})).workflowUUIDs.length;
    }

    @DBOS.workflow()
    static async simpleWorkflow() {
      return Promise.resolve();
    }
  }

  test('test-list-steps', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].function_id).toBe(0);
    expect(wfsteps[0].function_name).toBe('stepOne');
    expect(wfsteps[1].function_id).toBe(1);
    expect(wfsteps[1].function_name).toBe('stepTwo');
    expect(wfsteps[2].function_id).toBe(2);
    expect(wfsteps[2].function_name).toBe('DBOS.sleep');
  });

  test('test-send-recv', async () => {
    const wfid1 = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid1 }).recvWorkflow('message1');

    const wfid2 = uuidv4();
    await DBOS.startWorkflow(TestListSteps, { workflowID: wfid2 }).sendWorkflow(wfid1);

    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid1);
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[1].function_name).toBe('DBOS.sleep');
    expect(wfsteps[0].function_name).toBe('DBOS.recv');

    const wfsteps2 = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid2);
    expect(wfsteps2[0].function_id).toBe(0);
    expect(wfsteps2[0].function_name).toBe('DBOS.send');
  });

  test('test-set-getEvent', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).setEventWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].function_name).toBe('DBOS.setEvent');
    expect(wfsteps[1].function_name).toBe('DBOS.getEvent');
  });

  test('test-call-child-workflow-first', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowfirst();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].function_name).toBe('testWorkflow');
    expect(wfsteps[0].function_id).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].child_workflow_id).toBe(childID);
    expect(wfsteps[1].function_name).toBe('DBOS.getResult');
    expect(wfsteps[1].function_id).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].child_workflow_id).toBe(childID);
    expect(wfsteps[2].function_name).toBe('getStatus');
    expect(wfsteps[2].function_id).toBe(2);
    expect(wfsteps[2].output).toBeTruthy();
    expect(wfsteps[2].error).toBe(null);
    expect(wfsteps[2].child_workflow_id).toBe(null);
    expect(wfsteps[3].function_name).toBe('stepOne');
    expect(wfsteps[3].function_id).toBe(3);
    expect(wfsteps[3].output).toBe(wfid);
    expect(wfsteps[3].error).toBe(null);
    expect(wfsteps[3].child_workflow_id).toBe(null);
    expect(wfsteps[4].function_name).toBe('stepTwo');
  });

  test('test-call-child-workflow-middle', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowMiddle();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].function_name).toBe('stepOne');
    expect(wfsteps[1].function_name).toBe('testWorkflow');
    expect(wfsteps[2].function_name).toBe('getStatus');
    expect(wfsteps[3].function_name).toBe('DBOS.getResult');
    expect(wfsteps[4].function_name).toBe('stepTwo');
  });

  test('test-call-child-workflow-last', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowLast();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].function_name).toBe('stepOne');
    expect(wfsteps[1].function_name).toBe('stepTwo');
    expect(wfsteps[2].function_name).toBe('testWorkflow');
    expect(wfsteps[3].function_name).toBe('getStatus');
    expect(wfsteps[4].function_name).toBe('DBOS.getResult');
  });

  test('test-queue-child-workflow-first', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowFirst();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].function_name).toBe('testWorkflow');
    expect(wfsteps[0].function_id).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].child_workflow_id).toBe(childID);
    expect(wfsteps[1].function_name).toBe('DBOS.getResult');
    expect(wfsteps[1].function_id).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].child_workflow_id).toBe(childID);
    expect(wfsteps[2].function_name).toBe('getStatus');
    expect(wfsteps[3].function_name).toBe('stepOne');
    expect(wfsteps[4].function_name).toBe('stepTwo');
  });

  test('test-queue-child-workflow-middle', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowMiddle();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].function_name).toBe('stepOne');
    expect(wfsteps[1].function_name).toBe('testWorkflow');
    expect(wfsteps[2].function_name).toBe('getStatus');
    expect(wfsteps[3].function_name).toBe('DBOS.getResult');
    expect(wfsteps[4].function_name).toBe('stepTwo');
  });

  test('test-queue-child-workflow-last', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowLast();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].function_name).toBe('stepOne');
    expect(wfsteps[1].function_name).toBe('stepTwo');
    expect(wfsteps[2].function_name).toBe('testWorkflow');
    expect(wfsteps[3].function_name).toBe('getStatus');
    expect(wfsteps[4].function_name).toBe('DBOS.getResult');
  });

  test('test-direct-call-workflow', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).directCallWorkflow();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(4);
    expect(wfsteps[0].function_name).toBe('testWorkflow');
    expect(wfsteps[0].function_id).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].child_workflow_id).toBe(childID);
    expect(wfsteps[1].function_name).toBe('DBOS.getResult');
    expect(wfsteps[1].function_id).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].child_workflow_id).toBe(childID);
    expect(wfsteps[2].function_name).toBe('stepOne');
    expect(wfsteps[3].function_name).toBe('stepTwo');
  });

  test('test-list-failing-step', async () => {
    // Test calling a failing step directly
    let wfid = uuidv4();
    let handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callFailingStep();
    await expect(handle.getResult()).rejects.toThrow(new Error('fail'));
    let wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].function_name).toBe('failingStep');
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBeInstanceOf(Error);
    expect(wfsteps[0].child_workflow_id).toBe(null);
    // Test starting a failing step
    wfid = uuidv4();
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).startFailingStep();
    await expect(handle.getResult()).rejects.toThrow(new Error('fail'));
    wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].function_name).toBe('temp_workflow-step-failingStep');
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].child_workflow_id).toBe(`${wfid}-0`);
    expect(wfsteps[1].function_name).toBe('DBOS.getResult');
    expect(wfsteps[1].output).toBe(null);
    expect(wfsteps[1].error).toBeInstanceOf(Error);
    expect(wfsteps[1].child_workflow_id).toBe(`${wfid}-0`);
    // Test enqueueing a failing step
    wfid = uuidv4();
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueFailingStep();
    await expect(handle.getResult()).rejects.toThrow(new Error('fail'));

    wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);

    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].function_name).toBe('temp_workflow-step-failingStep');
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].child_workflow_id).toBe(`${wfid}-0`);
    expect(wfsteps[1].function_name).toBe('DBOS.getResult');
    expect(wfsteps[1].output).toBe(null);
    expect(wfsteps[1].error).toBeInstanceOf(Error);
    expect(wfsteps[1].child_workflow_id).toBe(`${wfid}-0`);
  });

  test('test-child-rerun', async () => {
    const wfid = uuidv4();
    let handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).CounterParent();
    const result1 = await handle.getResult();
    // call again with same wfid
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).CounterParent();
    const result2 = await handle.getResult();
    expect(result1).toEqual(result2);

    const wfs = await listWorkflows(config, {}, false);
    expect(wfs.length).toBe(2);

    const wfid1 = uuidv4();
    // call with different wfid we should get different result
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid1 }).CounterParent();
    const result3 = await handle.getResult();

    expect(result3).not.toEqual(result1);
  });

  test('test-transaction', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).workflowWithTransaction();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    console.log(wfsteps);
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].function_name).toBe('transaction');
    expect(wfsteps[0].output).toBe(wfid);
    expect(wfsteps[0].error).toBe(null);
  });

  test('test-transaction-error', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).workflowWithTransactionError();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    console.log(wfsteps);
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].function_name).toBe('transactionWithError');
    expect(wfsteps[0].error).toBeInstanceOf(Error);
    expect(wfsteps[0].output).toBe(null);
  });

  test('test-transaction-steps', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).workflowWithTransactionAndSteps();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    console.log(wfsteps);
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].function_name).toBe('stepOne');
    expect(wfsteps[1].function_name).toBe('transaction');
    expect(wfsteps[2].function_name).toBe('stepTwo');
  });

  test('test-list-workflows-as-step', async () => {
    const wfid = uuidv4();
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
