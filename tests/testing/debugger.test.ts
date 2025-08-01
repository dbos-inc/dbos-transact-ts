import { DBOSInitializer, DBOS } from '../../src/';
import { executeWorkflowById, generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from '../helpers';
import { randomUUID } from 'node:crypto';
import { DBOSConfig } from '../../src/dbos-executor';
import { Client } from 'pg';

const testTableName = 'debugger_test_kv';

describe('debugger-test', () => {
  let username: string;
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig('pg-node');
    debugConfig = generateDBOSTestConfig('pg-node');
    expect(config.databaseUrl).toBeDefined();
    const url = new URL(config.databaseUrl!);
    username = url.username ?? 'postgres';
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DebuggerTest.count = 0;
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.shutdown();
    expect(config.systemDatabaseUrl).toBeDefined();
    systemDBClient = new Client({
      connectionString: config.systemDatabaseUrl,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
  });

  class DebuggerTest {
    static count: number = 0;

    @DBOSInitializer()
    static async init() {
      await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
      await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    }

    @DBOS.transaction({ readOnly: true })
    static async testReadOnlyFunction(number: number) {
      const { rows } = await DBOS.pgClient.query<{ one: number }>(`SELECT 1 AS one`);
      return Number(rows[0].one) + number;
    }

    @DBOS.transaction()
    static async testFunction(name: string) {
      const { rows } = await DBOS.pgClient.query<TestKvTable>(
        `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
        [name],
      );
      return Number(rows[0].id);
    }

    @DBOS.workflow()
    static async testWorkflow(name: string) {
      const funcResult = await DebuggerTest.testFunction(name);
      return funcResult;
    }

    @DBOS.step()
    static async testStep() {
      return Promise.resolve(++DebuggerTest.count);
    }

    @DBOS.workflow()
    static async receiveWorkflow() {
      const message1 = await DBOS.recv<string>();
      const message2 = await DBOS.recv<string>();
      const fail = await DBOS.recv('message3', 0);
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
    static async getEventWorkflow(targetUUID: string) {
      const val1 = await DBOS.getEvent<string>(targetUUID, 'key1');
      const val2 = await DBOS.getEvent<string>(targetUUID, 'key2');
      return val1 + '-' + val2;
    }

    @DBOS.transaction()
    static async voidFunction() {
      if (DebuggerTest.count > 0) {
        return Promise.resolve(DebuggerTest.count);
      }
      DebuggerTest.count++;
      return;
    }

    // Workflow with different results.
    @DBOS.workflow()
    static async diffWorkflow(num: number) {
      DebuggerTest.count += num;
      return Promise.resolve(DebuggerTest.count);
    }

    // Workflow that sleep
    @DBOS.workflow()
    static async sleepWorkflow(num: number) {
      await DBOS.sleepSeconds(1);
      const funcResult = await DebuggerTest.testReadOnlyFunction(num);
      return funcResult;
    }

    static debugCount: number = 0;

    @DBOS.workflow()
    static async debugWF(value: number) {
      return await DebuggerTest.debugTx(value);
    }

    @DBOS.transaction()
    static async debugTx(value: number) {
      DebuggerTest.debugCount++;
      return Promise.resolve(value * 10);
    }
  }

  // Test we're robust to duplicated function names
  class DebuggerTestDup {
    @DBOS.transaction()
    static async voidFunction() {
      // Nothing here
      return Promise.resolve();
    }
  }

  test('debug-workflow', async () => {
    const wfUUID = randomUUID();
    // Execute the workflow and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await DebuggerTest.testWorkflow(username);
      expect(res).toBe(1);
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const debugRes = await DebuggerTest.testWorkflow(username);
      expect(debugRes).toBe(1);
    });
    await DBOS.shutdown();

    // Execute again with the provided UUID.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    const debugRes1 = await (await executeWorkflowById(wfUUID)).getResult();
    expect(debugRes1).toBe(1);
    await DBOS.shutdown();

    // Execute a non-exist UUID should fail under debugger.
    const wfUUID2 = randomUUID();
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID2, async () => {
      await expect(DebuggerTest.testWorkflow(username)).rejects.toThrow(
        `DEBUGGER: Failed to find inputs for workflow UUID ${wfUUID2}`,
      );
    });

    // Execute a workflow without specifying the UUID should fail.
    await expect(DebuggerTest.testWorkflow(username)).rejects.toThrow(
      /DEBUGGER: Failed to find inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gm,
    );

    await DBOS.shutdown();
  });

  test('tt-debug-workflow', async () => {
    DebuggerTest.debugCount = 0;
    const wfUUID = randomUUID();
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      // Execute the workflow and destroy the runtime
      const res = await DebuggerTest.debugWF(100);
      expect(res).toBe(1000);
      expect(DebuggerTest.debugCount).toBe(1);
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const debugRes = await DebuggerTest.debugWF(100);
      expect(DebuggerTest.debugCount).toBe(1);
      expect(debugRes).toBe(1000);
    });
    await DBOS.shutdown();
  });

  test('debug-sleep-workflow', async () => {
    const wfUUID = randomUUID();
    DBOS.setConfig(config);
    // Execute the workflow and destroy the runtime
    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await DebuggerTest.sleepWorkflow(2);
      expect(res).toBe(3);
    });
    await DBOS.shutdown();

    // Execute again in debug mode, should return the correct value
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await DebuggerTest.sleepWorkflow(2);
      expect(res).toBe(3);
    });
    await DBOS.shutdown();
  });

  test('debug-void-transaction', async () => {
    const wfUUID = randomUUID();

    DBOS.setConfig(config);
    await DBOS.launch();

    // Execute the workflow and destroy the runtime
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.voidFunction()).resolves.toBeUndefined();
      expect(DebuggerTest.count).toBe(1);
    });

    // Duplicated function name should not affect the execution
    await expect(DebuggerTestDup.voidFunction()).resolves.toBeUndefined();
    // Dup wf function invocation
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.voidFunction()).resolves.toBeFalsy();
      expect(DebuggerTest.count).toBe(1);
    });

    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.voidFunction()).resolves.toBeFalsy();
      expect(DebuggerTest.count).toBe(1);
    });
    expect(DebuggerTest.count).toBe(1);

    // Execute again with the provided UUID.
    await expect(executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBeFalsy();
    expect(DebuggerTest.count).toBe(1);
    await DBOS.shutdown();

    // Make sure we correctly record the function's class name
    const result = await systemDBClient.query<{ status: string; name: string; class_name: string }>(
      `SELECT status, name, class_name FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [wfUUID],
    );
    expect(result.rows[0].class_name).toBe('DebuggerTest');
    expect(result.rows[0].name).toContain('voidFunction');
    expect(result.rows[0].status).toBe('SUCCESS');
  });

  test('debug-transaction', async () => {
    const wfUUID = randomUUID();

    DBOS.setConfig(config);
    await DBOS.launch();

    // Execute the workflow and destroy the runtime
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.testFunction(username)).resolves.toBe(1);
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.testFunction(username)).resolves.toBe(1);
    });

    // Execute again with the provided UUID.
    await expect(executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = randomUUID();
    await DBOS.withNextWorkflowID(wfUUID2, async () => {
      await expect(DebuggerTest.testFunction(username)).rejects.toThrow(
        `DEBUGGER: Failed to find inputs for workflow UUID ${wfUUID2}`,
      );
    });

    // Execute a workflow without specifying the UUID should fail.
    await expect(DebuggerTest.testFunction(username)).rejects.toThrow(
      /DEBUGGER: Failed to find inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gm,
    );
    await DBOS.shutdown();
  });

  test('debug-step', async () => {
    const wfUUID = randomUUID();

    // Execute the step and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();

    // Execute the workflow and destroy the runtime
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.testStep()).resolves.toBe(1);
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.testStep()).resolves.toBe(1);
    });

    // Execute again with the provided UUID.
    await expect(executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBe(1);

    // Execute a non-exist UUID should fail.
    const wfUUID2 = randomUUID();
    await DBOS.withNextWorkflowID(wfUUID2, async () => {
      await expect(DebuggerTest.testStep()).rejects.toThrow(
        `DEBUGGER: Failed to find inputs for workflow UUID ${wfUUID2}`,
      );
    });

    // Execute a workflow without specifying the UUID should fail.
    await expect(DebuggerTest.testStep()).rejects.toThrow(
      /DEBUGGER: Failed to find inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gm,
    );

    // Make sure we correctly record the function's class name
    await DBOS.shutdown();
    const result = await systemDBClient.query<{ status: string; name: string; class_name: string }>(
      `SELECT status, name, class_name FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [wfUUID],
    );
    expect(result.rows[0].class_name).toBe('DebuggerTest');
    expect(result.rows[0].name).toContain('testStep');
    expect(result.rows[0].status).toBe('SUCCESS');
  });

  test('debug-workflow-notifications', async () => {
    const recvUUID = randomUUID();
    const sendUUID = randomUUID();

    // Execute the workflow and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();
    const handle = await DBOS.startWorkflow(DebuggerTest, { workflowID: recvUUID }).receiveWorkflow();
    await DBOS.withNextWorkflowID(sendUUID, async () => {
      await expect(DebuggerTest.sendWorkflow(recvUUID)).resolves.toBeFalsy(); // return void.
    });
    expect(await handle.getResult()).toBe(true);
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(recvUUID, async () => {
      await expect(DebuggerTest.receiveWorkflow()).resolves.toBeTruthy();
    });
    await DBOS.withNextWorkflowID(sendUUID, async () => {
      await expect(DebuggerTest.sendWorkflow(recvUUID)).resolves.toBeFalsy();
    });
    await DBOS.shutdown();
  });

  test('debug-workflow-events', async () => {
    const getUUID = randomUUID();
    const setUUID = randomUUID();
    // Execute the workflow and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.withNextWorkflowID(setUUID, async () => {
      await expect(DebuggerTest.setEventWorkflow()).resolves.toBe(0);
    });
    await DBOS.withNextWorkflowID(getUUID, async () => {
      await expect(DebuggerTest.getEventWorkflow(setUUID)).resolves.toBe('value1-value2');
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(setUUID, async () => {
      await expect(DebuggerTest.setEventWorkflow()).resolves.toBe(0);
    });
    await DBOS.withNextWorkflowID(getUUID, async () => {
      await expect(DebuggerTest.getEventWorkflow(setUUID)).resolves.toBe('value1-value2');
    });
    await DBOS.shutdown();
  });

  test('debug-workflow-input-output', async () => {
    const wfUUID = randomUUID();
    // Execute the workflow and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await DebuggerTest.diffWorkflow(1);
      expect(res).toBe(1);
    });
    await DBOS.shutdown();

    // Execute again with the provided UUID, should still get the same output.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await expect(executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBe(1);
    expect(DebuggerTest.count).toBe(2);

    // Execute again with different input, should still get the same output.
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      await expect(DebuggerTest.diffWorkflow(2)).rejects.toThrow(
        /DEBUGGER: Detected different inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.\s*Received: \[2\]\s*Original: \[1\]/gm,
      );
    });
    await DBOS.shutdown();
  });
});
