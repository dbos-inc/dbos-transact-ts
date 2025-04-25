import { StatusString, DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { DBOSTargetWorkflowCancelledError, DBOSWorkflowCancelledError } from '../src/error';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { randomUUID } from 'node:crypto';

describe('wf-cancel-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
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

  test('test-two-transactions-cancel-resume', async () => {
    const wfid = randomUUID();

    const wfh = await DBOS.startWorkflow(WFwith2Transactions, { workflowID: wfid }).workflowWithTransactions();

    try {
      // NB - This is a race to cancel a WF with a sleep in it.
      await DBOS.cancelWorkflow(wfid);

      await wfh.getResult();
    } catch (e) {
      console.log(`number executed  ${WFwith2Transactions.transExecuted}`);

      expect(WFwith2Transactions.transExecuted).toBe(1);

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

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSTargetWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-getresult', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).getResultTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSTargetWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-getevent', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).getEventTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSTargetWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-recv', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).recvTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSTargetWorkflowCancelledError);
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

  class WFwith2Transactions {
    static transExecuted = 0 as number;

    @DBOS.transaction()
    // eslint-disable-next-line @typescript-eslint/require-await
    static async transaction1() {
      WFwith2Transactions.transExecuted++;
      console.log(`Step 1  ${WFwith2Steps.stepsExecuted}`);
    }

    @DBOS.transaction()
    // eslint-disable-next-line @typescript-eslint/require-await
    static async transaction2() {
      WFwith2Transactions.transExecuted++;
      console.log(`Step 1  ${WFwith2Steps.stepsExecuted}`);
    }

    @DBOS.workflow()
    static async workflowWithTransactions() {
      await WFwith2Transactions.transaction1();
      await DBOS.sleepSeconds(1);
      await WFwith2Transactions.transaction2();
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
});
