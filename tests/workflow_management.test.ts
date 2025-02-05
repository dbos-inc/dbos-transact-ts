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
} from '../src';
import request from 'supertest';
import { DBOSConfig } from '../src/dbos-executor';
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from '../src/testing/testing_runtime';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import {
  WorkflowInformation,
  cancelWorkflow,
  getWorkflow,
  listWorkflows,
  reattemptWorkflow,
} from '../src/dbos-runtime/workflow_management';
import { Client } from 'pg';
import { Knex } from 'knex';

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
      applicationVersion: 'v0',
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
    let response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe('alice');

    const input: GetWorkflowsInput = {
      limit: 10,
    };

    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
    const firstUUID = workflowUUIDs.workflowUUIDs[0];

    for (let i = 0; i < 10; i++) {
      response = await request(testRuntime.getHandlersCallback()).post('/workflow/alice');
      expect(response.statusCode).toBe(200);
      expect(response.text).toBe('alice');
    }

    response = await request(testRuntime.getHandlersCallback()).post('/getWorkflows').send({ input });
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(10);
    expect(workflowUUIDs.workflowUUIDs).not.toContain(firstUUID);
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
    let info = infos[0] as WorkflowInformation;
    expect(info.authenticatedUser).toBe('alice');
    expect(info.workflowName).toBe('testWorkflow');
    expect(info.status).toBe(StatusString.SUCCESS);
    expect(info.workflowClassName).toBe('TestEndpoints');
    expect(info.assumedRole).toBe('');
    expect(info.workflowConfigName).toBe('');
    expect(info.error).toBeUndefined();
    expect(info.output).toBe('alice');
    expect(info.input).toEqual(['alice']);

    info = infos[1] as WorkflowInformation;
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

    const getInfo = (await getWorkflow(config, info.workflowUUID, false)) as WorkflowInformation;
    expect(info).toEqual(getInfo);
  });

  test('test-cancel-retry-restart', async () => {
    TestEndpoints.tries = 0;
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    const handle = await testRuntime.startWorkflow(TestEndpoints).waitingWorkflow();
    expect(TestEndpoints.tries).toBe(1);
    await cancelWorkflow(config, handle.getWorkflowUUID());

    let result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.getWorkflowUUID()],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.CANCELLED);

    await dbosExec.recoverPendingWorkflows(); // Does nothing as the workflow is CANCELLED
    expect(TestEndpoints.tries).toBe(1);

    TestEndpoints.testResolve();
    await reattemptWorkflow(config, null, handle.getWorkflowUUID(), false); // Retry the workflow, resetting the attempts counter
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

    await reattemptWorkflow(config, null, handle.getWorkflowUUID(), true); // Restart the workflow
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

    await reattemptWorkflow(config, null, workflowUUID, true);
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

    await reattemptWorkflow(config, null, restartedWorkflowUUID, true);
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
