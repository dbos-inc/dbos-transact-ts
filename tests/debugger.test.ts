import { ConfiguredInstance, DBOS } from '../src/';
import { executeWorkflowById, generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { randomUUID } from 'node:crypto';
import { DBOSConfig } from '../src/dbos-executor';
import { Client } from 'pg';

describe('debugger-test', () => {
  let username: string;
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    debugConfig = generateDBOSTestConfig();
    expect(config.systemDatabaseUrl).toBeDefined();
    const url = new URL(config.systemDatabaseUrl!);
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

    @DBOS.step()
    static async testFunction(name: string) {
      return Promise.resolve(name);
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

    @DBOS.step()
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
    static async sleepWorkflow(val: string) {
      await DBOS.sleepSeconds(1);
      const funcResult = await DebuggerTest.testFunction(val);
      return funcResult;
    }

    static debugCount: number = 0;

    @DBOS.workflow()
    static async debugWF(value: number) {
      return await DebuggerTest.debugStep(value);
    }

    @DBOS.step()
    static async debugStep(value: number) {
      DebuggerTest.debugCount++;
      return Promise.resolve(value * 10);
    }
  }

  // Test we're robust to duplicated function names
  class DebuggerTestDup {
    @DBOS.step()
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
      expect(res).toBe(username);
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const debugRes = await DebuggerTest.testWorkflow(username);
      expect(debugRes).toBe(username);
    });
    await DBOS.shutdown();

    // Execute again with the provided UUID.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    const debugRes1 = await (await executeWorkflowById(wfUUID)).getResult();
    expect(debugRes1).toBe(username);
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
      const res = await DebuggerTest.sleepWorkflow('val');
      expect(res).toBe('val');
    });
    await DBOS.shutdown();

    // Execute again in debug mode, should return the correct value
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await DebuggerTest.sleepWorkflow('val');
      expect(res).toBe('val');
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
        /DEBUGGER: Detected different inputs for workflow UUID [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.\s*Received:.*?\[2\].*?\s*Original:.*?\[1\].*?/gm,
      );
    });
    await DBOS.shutdown();
  });
});

export class TestApp {
  static bgTaskValue: number = 0;

  @DBOS.workflow()
  static async backgroundTask(n: number): Promise<void> {
    TestApp.bgTaskValue = n;
    for (let i = 1; i <= n; i++) {
      await TestApp.backgroundTaskStep(i);
    }
  }

  static stepCount: number = 0;
  @DBOS.step()
  static async backgroundTaskStep(step: number): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 100));
    TestApp.stepCount += step;
  }
}

describe('dbos-debug-v2-library', () => {
  it('should run a background task', async () => {
    const wfUUID = `wf-${Date.now()}`;

    const config = generateDBOSTestConfig(); // Optional.  If you don't, it'll open the YAML file...
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);

    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      TestApp.bgTaskValue = 0;
      TestApp.stepCount = 0;
      await TestApp.backgroundTask(10);
      expect(TestApp.bgTaskValue).toBe(10);
      expect(TestApp.stepCount).toBe(55);
    });
    await DBOS.shutdown();

    process.env.DBOS_DEBUG_WORKFLOW_ID = wfUUID;
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-explicit-any
    const mockExit = jest.spyOn(process, 'exit').mockImplementation((() => {}) as any);
    try {
      TestApp.bgTaskValue = 0;
      TestApp.stepCount = 0;
      await DBOS.launch();
      expect(TestApp.bgTaskValue).toBe(10);
      expect(TestApp.stepCount).toBe(0);
      expect(mockExit).toHaveBeenCalledWith(0);
    } finally {
      mockExit.mockRestore();
      delete process.env.DBOS_DEBUG_WORKFLOW_ID;
    }
  }, 30000);
});

class DebuggerCCTest extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  @DBOS.workflow()
  async testWorkflow(name: string) {
    expect(this.name).toBe('configA');
    const funcResult = await this.testStep(name);
    return `${name}${funcResult}`;
  }

  @DBOS.step()
  async testStep(inp: string) {
    expect(this.name).toBe('configA');
    return Promise.resolve(inp);
  }

  // Workflow that sleep, call comm, call tx, call child WF
  @DBOS.workflow()
  async mixedWorkflow(num: number) {
    expect(this.name).toBe('configA');
    await DBOS.sleepSeconds(1);
    const cResult = await this.testStep('comm');
    const wfResult = await this.testWorkflow('cwf');
    return `${this.name}${cResult}${wfResult}-${num}`;
  }
}

const configR = new DebuggerCCTest('configA');

describe('debugger-test', () => {
  let config: DBOSConfig;
  let debugConfig: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    debugConfig = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  test('debug-workflow', async () => {
    const wfUUID = randomUUID();
    // Execute the workflow and destroy the runtime
    DBOS.setConfig(config);
    await DBOS.launch();
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await configR.mixedWorkflow(23);
      expect(res).toBe('configAcommcwfcwf-23');
    });
    await DBOS.shutdown();

    // Execute again in debug mode.
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: true });
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await configR.mixedWorkflow(23);
      expect(res).toBe('configAcommcwfcwf-23');
    });

    // Execute again with the provided UUID.
    await expect(executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toBe('configAcommcwfcwf-23');
    await DBOS.shutdown();
  });
});
