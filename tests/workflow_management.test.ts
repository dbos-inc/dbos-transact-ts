import {
  Workflow,
  HandlerContext,
  PostApi,
  WorkflowContext,
  GetWorkflowsOutput,
  GetWorkflowsInput,
  StatusString,
  Authentication,
  MiddlewareContext,
  TransactionContext,
  Transaction,
  DBOS,
  WorkflowQueue,
} from '../src';
import request from 'supertest';
import { DBOSConfig } from '../src/dbos-executor';
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from '../src/testing/testing_runtime';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event } from './helpers';
import {
  WorkflowInformation,
  getWorkflow,
  listWorkflows,
  listQueuedWorkflows,
  listWorkflowSteps,
} from '../src/dbos-runtime/workflow_management';
import { Client } from 'pg';
import { Knex } from 'knex';
import { v4 as uuidv4 } from 'uuid';
import { GetQueuedWorkflowsInput } from '../src/workflow';
import { globalParams } from '../src/utils';

describe('workflow-management-tests', () => {
  const testTableName = 'dbos_test_kv';

  let testRuntime: TestingRuntime;
  let config: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(() => {
    config = generateDBOSTestConfig();
  });

  beforeEach(async () => {
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestDb(config);
    testRuntime = await createInternalTestRuntime([TestEndpoints], config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`);

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
    await testRuntime.destroy();
    process.env.DBOS__APPVERSION = undefined;
  });

  test('simple-getworkflows', async () => {
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input: {} });
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
  });

  test('getworkflows-with-dates', async () => {
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      startTime: new Date(Date.now() - 10000).toISOString(),
      endTime: new Date(Date.now()).toISOString(),
    };
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.endTime = new Date(Date.now() - 10000).toISOString();
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
  });

  test('getworkflows-with-status', async () => {
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    await dbosExec.flushWorkflowBuffers();

    const input: GetWorkflowsInput = {
      status: StatusString.SUCCESS,
    };
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.status = StatusString.PENDING;
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
  });

  test('getworkflows-with-wfname', async () => {
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      workflowName: 'testWorkflow',
    };
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
  });

  test('getworkflows-with-authentication', async () => {
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      authenticatedUser: 'alice',
    };
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
  });

  test('getworkflows-with-authentication', async () => {
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      applicationVersion: globalParams.appVersion,
    };
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.applicationVersion = 'v1';
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
  });

  test('getworkflows-with-limit', async () => {
    const workflowIDs: string[] = [];
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow_get_id');
    expect(response.statusCode).toBe(200);
    expect(response.text.length).toBeGreaterThan(0);
    workflowIDs.push(response.text);

    const input: GetWorkflowsInput = {
      limit: 10,
    };

    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
    expect(workflowUUIDs.workflowUUIDs[0]).toBe(workflowIDs[0]);

    for (let i = 0; i < 10; i++) {
      response = await request(testRuntime.getHandlersCallback()).post('/workflow_get_id');
      expect(response.statusCode).toBe(200);
      expect(response.text.length).toBeGreaterThan(0);
      workflowIDs.push(response.text);
    }

    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      // The order should be ascending by default
      expect(workflowUUIDs.workflowUUIDs[i]).toBe(workflowIDs[i]);
    }

    // Test sort_desc inverts the order
    input.sortDesc = true;
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
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
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(2);
    for (let i = 0; i < workflowUUIDs.workflowUUIDs.length; i++) {
      expect(workflowUUIDs.workflowUUIDs[i]).toBe(workflowIDs[i + 2]);
    }

    // Test OFFSET 10 returns the last workflow
    input.offset = 10;
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
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
    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input: wfidInput });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(2);
    expect(workflowUUIDs.workflowUUIDs[0]).toBe(workflowIDs[5]);
    expect(workflowUUIDs.workflowUUIDs[1]).toBe(workflowIDs[7]);
  });

  test('getworkflows-cli', async () => {
    const response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const failResponse = await request(testRuntime.getHandlersCallback()).post('/fail/alice');
    expect(failResponse.statusCode).toBe(500);

    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    await dbosExec.flushWorkflowBuffers();

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
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    const handle = await testRuntime.startWorkflow(TestEndpoints).waitingWorkflow();
    expect(TestEndpoints.tries).toBe(1);
    await dbosExec.cancelWorkflow(handle.getWorkflowUUID());

    let result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.CANCELLED);

    await dbosExec.recoverPendingWorkflows(); // Does nothing as the workflow is CANCELLED
    expect(TestEndpoints.tries).toBe(1);

    TestEndpoints.testResolve();
    // Retry the workflow, resetting the attempts counter
    await dbosExec.resumeWorkflow(handle.getWorkflowUUID());

    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(TestEndpoints.tries).toBe(2);
    await handle.getResult();

    await dbosExec.flushWorkflowBuffers();
    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);

    // Restart the workflow
    const wfh = await dbosExec.executeWorkflowUUID(handle.getWorkflowUUID(), true);
    await wfh.getResult();
    expect(TestEndpoints.tries).toBe(3);
    await dbosExec.flushWorkflowBuffers();
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
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    await testRuntime.invoke(TestEndpoints).testTransaction();
    expect(TestEndpoints.tries).toBe(1);
    await dbosExec.flushWorkflowBuffers();

    let result = await systemDBClient.query<{ status: string; workflow_uuid: string; name: string }>(
      `SELECT status, workflow_uuid, name FROM dbos.workflow_status`,
      [],
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
    expect(result.rows[0].name).toBe('temp_workflow-transaction-testTransaction');
    const workflowUUID = result.rows[0].workflow_uuid;

    let wfh = await dbosExec.executeWorkflowUUID(workflowUUID, true);
    await wfh.getResult();
    expect(TestEndpoints.tries).toBe(2);
    await dbosExec.flushWorkflowBuffers();

    result = await systemDBClient.query<{ status: string; workflow_uuid: string; name: string }>(
      `SELECT status, workflow_uuid, name FROM dbos.workflow_status WHERE workflow_uuid!=$1`,
      [workflowUUID],
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
    expect(result.rows[0].name).toBe('temp_workflow-transaction-testTransaction');
    const restartedWorkflowUUID = result.rows[0].workflow_uuid;

    wfh = await dbosExec.executeWorkflowUUID(restartedWorkflowUUID, true);
    await wfh.getResult();
    expect(TestEndpoints.tries).toBe(3);
  });

  test('systemdb-migration-backward-compatible', async () => {
    // Make sure the system DB migration failure is handled correctly.
    // If there is a migration failure, the system DB should still be able to start.
    // This happens when the old code is running with a new system DB schema.
    await testRuntime.destroy();
    await systemDBClient.query(
      `INSERT INTO knex_migrations (name, batch, migration_time) VALUES ('faketest.js', 1, now());`,
    );
    testRuntime = await createInternalTestRuntime([TestEndpoints], config);
    const response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
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
    @PostApi('/workflow/:name')
    @Workflow()
    static async testWorkflow(_ctxt: WorkflowContext, name: string) {
      return Promise.resolve(name);
    }

    @PostApi('/workflow_get_id')
    @Workflow()
    static async testWorkflowGetID(ctxt: WorkflowContext) {
      return Promise.resolve(ctxt.workflowUUID);
    }

    @PostApi('/fail/:name')
    @Workflow()
    static async failWorkflow(_ctxt: WorkflowContext, name: string) {
      await Promise.resolve(name);
      throw new Error(name);
    }

    @PostApi('/getWorkflows')
    static async getWorkflows(ctxt: HandlerContext, input: GetWorkflowsInput) {
      return await ctxt.getWorkflows(input);
    }

    static tries = 0;
    static testResolve: () => void;
    static testPromise = new Promise<void>((resolve) => {
      TestEndpoints.testResolve = resolve;
    });

    @Workflow()
    static async waitingWorkflow(_ctxt: WorkflowContext) {
      TestEndpoints.tries += 1;
      await TestEndpoints.testPromise;
    }

    @Transaction()
    static async testTransaction(_ctxt: TransactionContext<Knex>) {
      TestEndpoints.tries += 1;
      return Promise.resolve();
    }
  }
});

describe('test-list-queues', () => {
  let config: DBOSConfig;

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
      const handles = [];
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        const h = await DBOS.startWorkflow(TestListQueues, { queueName: TestListQueues.queue.name }).blockingTask(i);
        handles.push(h);
      }
      return Promise.all(handles.map((h) => h.getResult()));
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
  let config: DBOSConfig;
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
  });
  class TestListSteps {
    @DBOS.workflow()
    static async testWorkflow() {
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      await DBOS.sleep(10);
    }
    @DBOS.step()
    // eslint-disable-next-line @typescript-eslint/require-await
    static async stepOne() {
      console.log('executed stepOne');
    }
    @DBOS.step()
    // eslint-disable-next-line @typescript-eslint/require-await
    static async stepTwo() {
      console.log('executed stepTwo');
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
  }
  test('test-list-steps', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();
    const wfsteps = await listWorkflowSteps(config, wfid);
    expect(wfsteps.workflow_uuid).toBe(wfid);
    expect(wfsteps.steps.length).toBe(3);
    expect(wfsteps.steps[0].function_id).toBe(0);
    expect(wfsteps.steps[0].function_name).toBe('stepOne');
    expect(wfsteps.steps[1].function_id).toBe(1);
    expect(wfsteps.steps[1].function_name).toBe('stepTwo');
    expect(wfsteps.steps[2].function_id).toBe(2);
    expect(wfsteps.steps[2].function_name).toBe('DBOS.sleep');
  });

  test('test-send-recv', async () => {
    const wfid1 = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid1 }).recvWorkflow('message1');

    const wfid2 = uuidv4();
    await DBOS.startWorkflow(TestListSteps, { workflowID: wfid2 }).sendWorkflow(wfid1);

    await handle.getResult();
    const wfsteps = await listWorkflowSteps(config, wfid1);
    console.log(wfsteps);
    expect(wfsteps.workflow_uuid).toBe(wfid1);
    expect(wfsteps.steps.length).toBe(2);
    expect(wfsteps.steps[0].function_name).toBe('DBOS.sleep');
    expect(wfsteps.steps[1].function_name).toBe('DBOS.recv');

    const wfsteps2 = await listWorkflowSteps(config, wfid2);
    console.log(wfsteps2);
    expect(wfsteps2.steps[0].function_id).toBe(0);
    expect(wfsteps2.steps[0].function_name).toBe('DBOS.send');
  });

  test('test-set-getEvent', async () => {
    const wfid = uuidv4();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).setEventWorkflow();
    await handle.getResult();
    const wfsteps = await listWorkflowSteps(config, wfid);
    console.log(wfsteps);
    expect(wfsteps.workflow_uuid).toBe(wfid);
    expect(wfsteps.steps.length).toBe(3);
    expect(wfsteps.steps[0].function_name).toBe('DBOS.setEvent');
    expect(wfsteps.steps[2].function_name).toBe('DBOS.getEvent');
  });
});
