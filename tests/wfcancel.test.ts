import { StatusString, DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { v4 as uuidv4 } from 'uuid';

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
  }, 20000);

  test('test-two-steps-base', async () => {
    const wfid = uuidv4();
    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    await wfh.getResult();

    expect(WFwith2Steps.stepsExecuted).toBe(2);
  });

  test('test-two-steps-cancel', async () => {
    const wfid = uuidv4();

    try {
      const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

      await DBOS.executor.cancelWorkflow(wfid);
      await wfh.getResult();
    } catch (e) {
      console.log(`number executed  ${WFwith2Steps.stepsExecuted}`);

      expect(WFwith2Steps.stepsExecuted).toBe(1);

      const wfstatus = await DBOS.getWorkflowStatus(wfid);

      expect(wfstatus?.status).toBe(StatusString.CANCELLED);
    }
  });

  test('test-two-steps-cancel-resume', async () => {
    const wfid = uuidv4();

    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    try {
      await DBOS.executor.cancelWorkflow(wfid);

      await wfh.getResult();
    } catch (e) {
      console.log(`number executed  ${WFwith2Steps.stepsExecuted}`);

      expect(WFwith2Steps.stepsExecuted).toBe(1);

      const wfstatus = await DBOS.getWorkflowStatus(wfid);

      expect(wfstatus?.status).toBe(StatusString.CANCELLED);
    }

    await DBOS.executor.resumeWorkflow(wfid);
    const resstatus = await DBOS.getWorkflowStatus(wfid);
    expect(resstatus?.status).toBe(StatusString.PENDING);
  });

  test('test-two-transactions-cancel-resume', async () => {
    const wfid = uuidv4();

    const wfh = await DBOS.startWorkflow(WFwith2Transactions, { workflowID: wfid }).workflowWithTransactions();

    try {
      await DBOS.executor.cancelWorkflow(wfid);

      await wfh.getResult();
    } catch (e) {
      console.log(`number executed  ${WFwith2Transactions.transExecuted}`);

      expect(WFwith2Transactions.transExecuted).toBe(1);

      const wfstatus = await DBOS.getWorkflowStatus(wfid);

      expect(wfstatus?.status).toBe(StatusString.CANCELLED);
    }

    await DBOS.executor.resumeWorkflow(wfid);
    const resstatus = await DBOS.getWorkflowStatus(wfid);
    expect(resstatus?.status).toBe(StatusString.PENDING);
  });

  test('test-resume-on-a-completed-ws', async () => {
    const wfid = uuidv4();
    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    await wfh.getResult();

    expect(WFwith2Steps.stepsExecuted).toBe(2);

    await DBOS.executor.resumeWorkflow(wfid);
    await DBOS.getWorkflowStatus(wfid);

    expect(WFwith2Steps.stepsExecuted).toBe(2);
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
});
