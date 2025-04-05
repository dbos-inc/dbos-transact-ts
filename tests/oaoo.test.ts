import { DBOS } from '../src';
import { DBOSConfigInternal } from '../src/dbos-executor';
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { v1 as uuidv1 } from 'uuid';

const testTableName = 'dbos_test_kv';

describe('oaoo-tests', () => {
  let username: string;
  let config: DBOSConfigInternal;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    username = config.poolConfig?.user || 'postgres';
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.dropSystemDB();
    await DBOS.launch();

    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  /**
   * Step OAOO tests.
   */
  class StepOAOO {
    static #counter = 0;
    static get counter() {
      return StepOAOO.#counter;
    }
    @DBOS.step()
    static async testStep() {
      return Promise.resolve(StepOAOO.#counter++);
    }

    @DBOS.workflow()
    static async testCommWorkflow() {
      const funcResult = StepOAOO.testStep();
      return funcResult ?? -1;
    }
  }

  test('step-oaoo', async () => {
    const workflowUUID: string = uuidv1();

    let result: number = -222;
    result = await DBOS.withNextWorkflowID(workflowUUID, async () => await StepOAOO.testCommWorkflow());
    expect(result).toBe(0);
    expect(StepOAOO.counter).toBe(1);

    // Test OAOO. Should return the original result.
    result = await DBOS.withNextWorkflowID(workflowUUID, async () => await StepOAOO.testCommWorkflow());
    expect(result).toBe(0);
    expect(StepOAOO.counter).toBe(1);

    // Should be a new run.
    expect(await StepOAOO.testCommWorkflow()).toBe(1);
    expect(StepOAOO.counter).toBe(2);
  });

  /**
   * Workflow OAOO tests.
   */
  class WorkflowOAOO {
    @DBOS.transaction()
    static async testInsertTx(name: string) {
      expect(DBOS.getConfig<number>('counter')).toBe(3);
      const { rows } = await DBOS.pgClient.query<TestKvTable>(
        `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
        [name],
      );
      return Number(rows[0].id);
    }

    @DBOS.transaction({ readOnly: true })
    static async testReadTx(id: number) {
      const { rows } = await DBOS.pgClient.query<TestKvTable>(`SELECT id FROM ${testTableName} WHERE id=$1`, [id]);
      if (rows.length > 0) {
        return Number(rows[0].id);
      } else {
        // Cannot find, return a negative number.
        return -1;
      }
    }

    @DBOS.workflow()
    static async testTxWorkflow(name: string) {
      expect(DBOS.getConfig<number>('counter')).toBe(3);
      const funcResult: number = await WorkflowOAOO.testInsertTx(name);
      const checkResult: number = await WorkflowOAOO.testReadTx(funcResult);
      return checkResult;
    }

    @DBOS.workflow()
    static async nestedWorkflow(name: string) {
      return await WorkflowOAOO.testTxWorkflow(name);
    }

    static numberOfChildInvocationsMax1 = 0;

    @DBOS.step()
    static async nestedWorkflowStepToRunOnce() {
      ++WorkflowOAOO.numberOfChildInvocationsMax1;
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async nestedWorkflowChildToRunOnce() {
      return await WorkflowOAOO.nestedWorkflowStepToRunOnce();
    }

    @DBOS.workflow()
    static async nestedWorkflowRunChildOnce() {
      return DBOS.withNextWorkflowID(
        'constant-idempotency-run-once',
        async () => await WorkflowOAOO.nestedWorkflowChildToRunOnce(),
      );
    }

    @DBOS.workflow()
    static async sleepWorkflow(durationSec: number) {
      await DBOS.sleepSeconds(durationSec);
      return;
    }

    @DBOS.workflow()
    static async recvWorkflow(timeoutSeconds: number) {
      await DBOS.recv('a-topic', timeoutSeconds);
      return;
    }

    @DBOS.workflow()
    static async getEventWorkflow(timeoutSeconds: number) {
      await DBOS.getEvent(uuidv1(), 'a-key', timeoutSeconds);
      return;
    }
  }

  test('workflow-sleep-oaoo', async () => {
    const workflowUUID = uuidv1();
    const initTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.sleepWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - initTime).toBeGreaterThanOrEqual(1950);

    // Rerunning should skip the sleep
    const startTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.sleepWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - startTime).toBeLessThanOrEqual(200);
  });

  test('workflow-recv-oaoo', async () => {
    const workflowUUID = uuidv1();
    const initTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.recvWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - initTime).toBeGreaterThanOrEqual(1950);

    // Rerunning should skip the sleep
    const startTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.recvWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - startTime).toBeLessThanOrEqual(200);
  });

  test('workflow-getEvent-oaoo', async () => {
    const workflowUUID = uuidv1();
    const initTime = Date.now();
    await DBOS.withNextWorkflowID(
      workflowUUID,
      async () => await expect(WorkflowOAOO.getEventWorkflow(2)).resolves.toBeFalsy(),
    );
    expect(Date.now() - initTime).toBeGreaterThanOrEqual(1950);

    // Rerunning should skip the sleep
    const startTime = Date.now();
    await DBOS.withNextWorkflowID(
      workflowUUID,
      async () => await expect(WorkflowOAOO.getEventWorkflow(2)).resolves.toBeFalsy(),
    );
    expect(Date.now() - startTime).toBeLessThanOrEqual(200);
  });

  test('workflow-oaoo', async () => {
    let workflowResult: number;
    const uuidArray: string[] = [];

    for (let i = 0; i < 10; i++) {
      const workflowHandle = await DBOS.startWorkflow(WorkflowOAOO).testTxWorkflow(username);
      const workflowUUID: string = workflowHandle.getWorkflowUUID();
      uuidArray.push(workflowUUID);
      workflowResult = await workflowHandle.getResult();
      expect(workflowResult).toEqual(i + 1);
    }

    // Rerunning with the same workflow UUID should return the same output.
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidArray[i];
      const workflowResult: number = await DBOS.withNextWorkflowID(
        workflowUUID,
        async () => await WorkflowOAOO.testTxWorkflow(username),
      );
      expect(workflowResult).toEqual(i + 1);
    }
  });

  test('nested-workflow-oaoo', async () => {
    const workflowUUID = uuidv1();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.nestedWorkflow(username)).resolves.toBe(1);
    });

    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.nestedWorkflow(username)).resolves.toBe(1);
    });

    // Retrieve output of the child workflow.
    const retrievedHandle = DBOS.retrieveWorkflow(workflowUUID + '-0');
    await expect(retrievedHandle.getResult()).resolves.toBe(1);

    // Nested with OAOO key calculated
    await WorkflowOAOO.nestedWorkflowRunChildOnce();
    await WorkflowOAOO.nestedWorkflowRunChildOnce();
    expect(WorkflowOAOO.numberOfChildInvocationsMax1).toBe(1);
  });

  /**
   * Workflow notification OAOO tests.
   */
  class NotificationOAOO {
    @DBOS.workflow()
    static async receiveOaooWorkflow(topic: string, timeout: number) {
      // This returns true if and only if exactly one message is sent to it.
      const succeeds = await DBOS.recv<number>(topic, timeout);
      const fails = await DBOS.recv<number>(topic, 0);
      return succeeds === 123 && fails === null;
    }
  }

  test('notification-oaoo', async () => {
    const recvWorkflowUUID = uuidv1();
    const idempotencyKey = 'test-suffix';

    // Receive twice with the same UUID.  Each should get the same result of true.
    const recvHandle1 = await DBOS.startWorkflow(NotificationOAOO, {
      workflowID: recvWorkflowUUID,
    }).receiveOaooWorkflow('testTopic', 1);
    const recvHandle2 = await DBOS.startWorkflow(NotificationOAOO, {
      workflowID: recvWorkflowUUID,
    }).receiveOaooWorkflow('testTopic', 1);

    // Send twice with the same idempotency key.  Only one message should be sent.
    await expect(DBOS.send(recvWorkflowUUID, 123, 'testTopic', idempotencyKey)).resolves.not.toThrow();
    await expect(DBOS.send(recvWorkflowUUID, 123, 'testTopic', idempotencyKey)).resolves.not.toThrow();

    await expect(recvHandle1.getResult()).resolves.toBe(true);
    await expect(recvHandle2.getResult()).resolves.toBe(true);

    // A receive with a different UUID should return false.
    await expect(NotificationOAOO.receiveOaooWorkflow('testTopic', 0)).resolves.toBe(false);
  });

  /**
   * GetEvent/Status OAOO tests.
   */
  class EventStatusOAOO {
    static wfCnt: number = 0;
    static resolve: () => void;
    static promise = new Promise<void>((r) => {
      EventStatusOAOO.resolve = r;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((r) => {
      EventStatusOAOO.resolve2 = r;
    });

    static resolve3: () => void;
    static promise3 = new Promise<void>((r) => {
      EventStatusOAOO.resolve3 = r;
    });

    @DBOS.workflow()
    static async setEventWorkflow() {
      await DBOS.setEvent('key1', 'value1');
      await DBOS.setEvent('key2', 'value2');
      await EventStatusOAOO.promise;
      throw Error('Failed workflow');
    }

    @DBOS.workflow()
    static async getEventRetrieveWorkflow(targetUUID: string): Promise<string> {
      let res = '';
      const getValue = await DBOS.getEvent<string>(targetUUID, 'key1', 0);
      EventStatusOAOO.wfCnt++;
      if (getValue === null) {
        res = 'valueNull';
      } else {
        res = getValue;
      }

      const handle = DBOS.retrieveWorkflow(targetUUID);
      const status = await handle.getStatus();
      EventStatusOAOO.wfCnt++;
      if (status === null) {
        res += '-statusNull';
      } else {
        res += '-' + status.status;
      }

      // Set the child workflow UUID to targetUUID.
      const invokedHandle = await DBOS.startWorkflow(EventStatusOAOO, { workflowID: targetUUID }).setEventWorkflow();
      const ires = await invokedHandle.getStatus();
      res += '-' + ires?.status;
      try {
        if (EventStatusOAOO.wfCnt > 2) {
          await invokedHandle.getResult();
        }
      } catch (e) {
        // Ignore error.
        DBOS.logger.error(e);
      }
      EventStatusOAOO.resolve3();
      await EventStatusOAOO.promise2;
      return res;
    }
  }

  test('workflow-getevent-retrieve', async () => {
    // Execute a workflow (w/ getUUID) to get an event and retrieve a workflow that doesn't exist, then invoke the setEvent workflow as a child workflow.
    // If we execute the get workflow without UUID, both getEvent and retrieveWorkflow should return values.
    // But if we run the get workflow again with getUUID, getEvent/retrieveWorkflow should still return null.
    const getUUID = uuidv1();
    const setUUID = uuidv1();

    const handle1 = await DBOS.startWorkflow(EventStatusOAOO, { workflowID: getUUID }).getEventRetrieveWorkflow(
      setUUID,
    );

    await EventStatusOAOO.promise3;
    expect(EventStatusOAOO.wfCnt).toBe(2);
    await expect(DBOS.getEvent(setUUID, 'key1')).resolves.toBe('value1');

    EventStatusOAOO.resolve();

    // Wait for the child workflow to finish.
    const handle = DBOS.retrieveWorkflow(setUUID);
    await expect(handle.getResult()).rejects.toThrow('Failed workflow');

    // Test OAOO for getEvent and getWorkflowStatus.
    const handle2 = await DBOS.startWorkflow(EventStatusOAOO, { workflowID: getUUID }).getEventRetrieveWorkflow(
      setUUID,
    );
    EventStatusOAOO.resolve2();
    await expect(handle2.getResult()).resolves.toBe('valueNull-statusNull-PENDING');
    await expect(handle1.getResult()).resolves.toBe('valueNull-statusNull-PENDING');

    // Run without UUID, should get the new result.
    await expect(EventStatusOAOO.getEventRetrieveWorkflow(setUUID)).resolves.toBe('value1-ERROR-ERROR');

    // TODO(Qian): look at this test
    expect(EventStatusOAOO.wfCnt).toBe(6); // Should re-execute the workflow because we have concurrent workflows
  });
});
