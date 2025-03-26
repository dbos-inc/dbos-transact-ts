import {
  WorkflowContext,
  TransactionContext,
  Transaction,
  Workflow,
  DBOSInitializer,
  InitContext,
  StepContext,
  Step,
} from '../../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from '../helpers';
import { v1 as uuidv1 } from 'uuid';
import { DBOSConfig, DebugMode } from '../../src/dbos-executor';
import { Client, PoolClient } from 'pg';
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from '../../src/testing/testing_runtime';

type TestTransactionContext = TransactionContext<PoolClient>;
const testTableName = 'debugger_test_kv';

describe('debugger-test', () => {
  let username: string;
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let debugProxyConfig: DBOSConfig;
  let testRuntime: TestingRuntime;
  let debugRuntime: TestingRuntime;
  let timeTravelRuntime: TestingRuntime;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    debugConfig = generateDBOSTestConfig(undefined);
    debugProxyConfig = generateDBOSTestConfig(undefined);
    username = config.poolConfig!.user || 'postgres';
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime(undefined, config, { debugMode: DebugMode.DISABLED });
    debugRuntime = await createInternalTestRuntime(undefined, debugConfig, { debugMode: DebugMode.ENABLED });
    timeTravelRuntime = await createInternalTestRuntime(undefined, debugProxyConfig, {
      debugMode: DebugMode.TIME_TRAVEL,
    });
    DebuggerTest.count = 0;
    systemDBClient = new Client({
      user: config.poolConfig!.user,
      port: config.poolConfig!.port,
      host: config.poolConfig!.host,
      password: config.poolConfig!.password,
      database: config.system_database,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await debugRuntime.destroy();
    await testRuntime.destroy();
    await timeTravelRuntime.destroy();
  });

  class DebuggerTest {
    static count: number = 0;

    @DBOSInitializer()
    static async init(ctx: InitContext) {
      await ctx.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
      await ctx.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    }

    @Transaction({ readOnly: true })
    static async testReadOnlyFunction(txnCtxt: TestTransactionContext, number: number) {
      const { rows } = await txnCtxt.client.query<{ one: number }>(`SELECT 1 AS one`);
      return Number(rows[0].one) + number;
    }

    @Transaction()
    static async testFunction(txnCtxt: TestTransactionContext, name: string) {
      const { rows } = await txnCtxt.client.query<TestKvTable>(
        `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
        [name],
      );
      return Number(rows[0].id);
    }

    @Workflow()
    static async testWorkflow(ctxt: WorkflowContext, name: string) {
      const funcResult = await ctxt.invoke(DebuggerTest).testFunction(name);
      return funcResult;
    }

    @Step()
    static async testStep(_ctxt: StepContext) {
      return Promise.resolve(++DebuggerTest.count);
    }

    @Workflow()
    static async receiveWorkflow(ctxt: WorkflowContext) {
      const message1 = await ctxt.recv<string>();
      const message2 = await ctxt.recv<string>();
      const fail = await ctxt.recv('message3', 0);
      return message1 === 'message1' && message2 === 'message2' && fail === null;
    }

    @Workflow()
    static async sendWorkflow(ctxt: WorkflowContext, destinationUUID: string) {
      await ctxt.send(destinationUUID, 'message1');
      await ctxt.send(destinationUUID, 'message2');
    }

    @Workflow()
    static async setEventWorkflow(ctxt: WorkflowContext) {
      await ctxt.setEvent('key1', 'value1');
      await ctxt.setEvent('key2', 'value2');
      return 0;
    }

    @Workflow()
    static async getEventWorkflow(ctxt: WorkflowContext, targetUUID: string) {
      const val1 = await ctxt.getEvent<string>(targetUUID, 'key1');
      const val2 = await ctxt.getEvent<string>(targetUUID, 'key2');
      return val1 + '-' + val2;
    }

    @Transaction()
    static async voidFunction(_txnCtxt: TestTransactionContext) {
      if (DebuggerTest.count > 0) {
        return Promise.resolve(DebuggerTest.count);
      }
      DebuggerTest.count++;
      return;
    }

    // Workflow with different results.
    @Workflow()
    static async diffWorkflow(_ctxt: WorkflowContext, num: number) {
      DebuggerTest.count += num;
      return Promise.resolve(DebuggerTest.count);
    }

    // Workflow that sleep
    @Workflow()
    static async sleepWorkflow(ctxt: WorkflowContext, num: number) {
      await ctxt.sleep(1);
      const funcResult = await ctxt.invoke(DebuggerTest).testReadOnlyFunction(num);
      return funcResult;
    }

    static debugCount: number = 0;

    @Workflow()
    static async debugWF(ctxt: WorkflowContext, value: number) {
      return await ctxt.invoke(DebuggerTest).debugTx(value);
    }

    @Transaction()
    static async debugTx(_ctxt: TestTransactionContext, value: number) {
      DebuggerTest.debugCount++;
      return Promise.resolve(value * 10);
    }
  }

  // Test we're robust to duplicated function names
  class DebuggerTestDup {
    @Transaction()
    static async voidFunction(_txnCtxt: TestTransactionContext) {
      // Nothing here
      return Promise.resolve();
    }
  }

  test('debug-workflow', async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const res = await testRuntime.invokeWorkflow(DebuggerTest, wfUUID).testWorkflow(username);
    expect(res).toBe(1);
    await testRuntime.destroy();

    // Execute again in debug mode.
    const debugRes = await debugRuntime.invokeWorkflow(DebuggerTest, wfUUID).testWorkflow(username);
    expect(debugRes).toBe(1);

    // Execute again with the provided UUID.
    await expect(
      (debugRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(1);

    await expect(
      (timeTravelRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID2).testWorkflow(username)).rejects.toThrow(
      `DEBUGGER: Failed to find inputs for workflow UUID ${wfUUID2}`,
    );

    // Execute a workflow without specifying the UUID should fail.
    await expect(debugRuntime.invoke(DebuggerTest).testWorkflow(username)).rejects.toThrow(
      /DEBUGGER: Failed to find inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gm,
    );
  });

  test('tt-debug-workflow', async () => {
    DebuggerTest.debugCount = 0;
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const res = await testRuntime.invokeWorkflow(DebuggerTest, wfUUID).debugWF(100);
    expect(res).toBe(1000);
    expect(DebuggerTest.debugCount).toBe(1);
    await testRuntime.destroy();

    // Execute again in debug mode.
    const debugRes = await debugRuntime.invokeWorkflow(DebuggerTest, wfUUID).debugWF(100);
    expect(DebuggerTest.debugCount).toBe(1);
    expect(debugRes).toBe(1000);

    const ttdbgRes = await timeTravelRuntime.invokeWorkflow(DebuggerTest, wfUUID).debugWF(100);
    expect(DebuggerTest.debugCount).toBe(2);
    expect(ttdbgRes).toBe(1000);
  });

  test('debug-sleep-workflow', async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const res = await testRuntime.invokeWorkflow(DebuggerTest, wfUUID).sleepWorkflow(2);
    expect(res).toBe(3);
    await testRuntime.destroy();

    // Execute again in debug mode, should return the correct value
    const debugRes = await debugRuntime.invokeWorkflow(DebuggerTest, wfUUID).sleepWorkflow(2);
    expect(debugRes).toBe(3);

    // Proxy mode should return the same result
    const debugProxyRes = await timeTravelRuntime.invokeWorkflow(DebuggerTest, wfUUID).sleepWorkflow(2);
    expect(debugProxyRes).toBe(3);
  });

  test('debug-void-transaction', async () => {
    const wfUUID = uuidv1();
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invoke(DebuggerTest, wfUUID).voidFunction()).resolves.toBeUndefined();
    expect(DebuggerTest.count).toBe(1);

    // Duplicated function name should not affect the execution
    await expect(testRuntime.invoke(DebuggerTestDup).voidFunction()).resolves.toBeUndefined();
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID).voidFunction()).resolves.toBeFalsy();
    expect(DebuggerTest.count).toBe(1);

    // Execute again with the provided UUID.
    await expect(
      (debugRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBeFalsy();
    expect(DebuggerTest.count).toBe(1);

    // Make sure we correctly record the function's class name
    await dbosExec.flushWorkflowBuffers();
    const result = await systemDBClient.query<{ status: string; name: string; class_name: string }>(
      `SELECT status, name, class_name FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [wfUUID],
    );
    expect(result.rows[0].class_name).toBe('DebuggerTest');
    expect(result.rows[0].name).toContain('voidFunction');
    expect(result.rows[0].status).toBe('SUCCESS');
  });

  test('debug-transaction', async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invoke(DebuggerTest, wfUUID).testFunction(username)).resolves.toBe(1);
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID).testFunction(username)).resolves.toBe(1);

    // Execute again with the provided UUID.
    await expect(
      (debugRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(1);

    // Proxy mode should return the same result.
    await expect(
      (timeTravelRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID2).testFunction(username)).rejects.toThrow(
      `DEBUGGER: Failed to find the recorded output for the transaction: workflow UUID ${wfUUID2}, step number 0`,
    );

    // Execute a workflow without specifying the UUID should fail.
    await expect(debugRuntime.invoke(DebuggerTest).testFunction(username)).rejects.toThrow(
      /DEBUGGER: Failed to find the recorded output for the transaction: workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gm,
    );
  });

  test('debug-read-only-transaction', async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invoke(DebuggerTest, wfUUID).testReadOnlyFunction(1)).resolves.toBe(2);
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID).testReadOnlyFunction(1)).resolves.toBe(2);

    // Execute again with the provided UUID.
    await expect(
      (debugRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(2);

    // Proxy mode should return the same result.
    await expect(
      (timeTravelRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(2);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID2).testReadOnlyFunction(1)).rejects.toThrow(
      `DEBUGGER: Failed to find the recorded output for the transaction: workflow UUID ${wfUUID2}, step number 0`,
    );

    // Execute a workflow without specifying the UUID should fail.
    await expect(debugRuntime.invoke(DebuggerTest).testReadOnlyFunction(1)).rejects.toThrow(
      /DEBUGGER: Failed to find the recorded output for the transaction: workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gm,
    );
  });

  test('debug-step', async () => {
    const wfUUID = uuidv1();
    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();

    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invoke(DebuggerTest, wfUUID).testStep()).resolves.toBe(1);
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID).testStep()).resolves.toBe(1);

    // Execute again with the provided UUID.
    await expect(
      (debugRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = uuidv1();
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID2).testStep()).rejects.toThrow(
      `DEBUGGER: Failed to find inputs for workflow UUID ${wfUUID2}`,
    );

    // Execute a workflow without specifying the UUID should fail.
    await expect(debugRuntime.invoke(DebuggerTest).testStep()).rejects.toThrow(
      /DEBUGGER: Failed to find inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gm,
    );

    // Make sure we correctly record the function's class name
    await dbosExec.flushWorkflowBuffers();
    const result = await systemDBClient.query<{ status: string; name: string; class_name: string }>(
      `SELECT status, name, class_name FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [wfUUID],
    );
    expect(result.rows[0].class_name).toBe('DebuggerTest');
    expect(result.rows[0].name).toContain('testStep');
    expect(result.rows[0].status).toBe('SUCCESS');
  });

  test('debug-workflow-notifications', async () => {
    const recvUUID = uuidv1();
    const sendUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const handle = await testRuntime.invoke(DebuggerTest, recvUUID).receiveWorkflow();
    await expect(testRuntime.invokeWorkflow(DebuggerTest, sendUUID).sendWorkflow(recvUUID)).resolves.toBeFalsy(); // return void.
    expect(await handle.getResult()).toBe(true);
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invokeWorkflow(DebuggerTest, recvUUID).receiveWorkflow()).resolves.toBe(true);
    await expect(debugRuntime.invokeWorkflow(DebuggerTest, sendUUID).sendWorkflow(recvUUID)).resolves.toBeFalsy();
  });

  test('debug-workflow-events', async () => {
    const getUUID = uuidv1();
    const setUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    await expect(testRuntime.invokeWorkflow(DebuggerTest, setUUID).setEventWorkflow()).resolves.toBe(0);
    await expect(testRuntime.invokeWorkflow(DebuggerTest, getUUID).getEventWorkflow(setUUID)).resolves.toBe(
      'value1-value2',
    );
    await testRuntime.destroy();

    // Execute again in debug mode.
    await expect(debugRuntime.invokeWorkflow(DebuggerTest, setUUID).setEventWorkflow()).resolves.toBe(0);
    await expect(debugRuntime.invokeWorkflow(DebuggerTest, getUUID).getEventWorkflow(setUUID)).resolves.toBe(
      'value1-value2',
    );
  });

  test('debug-workflow-input-output', async () => {
    const wfUUID = uuidv1();
    // Execute the workflow and destroy the runtime
    const res = await testRuntime.invokeWorkflow(DebuggerTest, wfUUID).diffWorkflow(1);
    expect(res).toBe(1);
    await testRuntime.destroy();

    // Execute again with the provided UUID, should still get the same output.
    await expect(
      (debugRuntime as TestingRuntimeImpl)
        .getDBOSExec()
        .executeWorkflowUUID(wfUUID)
        .then((x) => x.getResult()),
    ).resolves.toBe(1);
    expect(DebuggerTest.count).toBe(2);

    // Execute again with different input, should still get the same output.
    await expect(debugRuntime.invoke(DebuggerTest, wfUUID).diffWorkflow(2)).rejects.toThrow(
      /DEBUGGER: Detected different inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.\s*Received: \[2\]\s*Original: \[1\]/gm,
    );
  });
});
