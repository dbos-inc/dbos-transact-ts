import { StatusString, WorkflowHandle, DBOS, ConfiguredInstance } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event } from './helpers';
import { WorkflowQueue } from '../src';
import { v4 as uuidv4 } from 'uuid';
import { WF } from './wfqtestprocess';
import { Console } from 'console';
import { WorkflowStatus } from '../src/workflow';

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
    const wfid = uuidv4();
    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    await wfh.getResult();

    console.log(`number executed  ${WFwith2Steps.stepsExecuted}`);

    expect(WFwith2Steps.stepsExecuted).toBe(2);
  });

  test('test-two-steps-cancel', async () => {
    const wfid = uuidv4();

    try {
      const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

      DBOS.executor.cancelWorkflow(wfid);

      await wfh.getResult();
    } catch (e) {
      console.log(`number executed  ${WFwith2Steps.stepsExecuted}`);

      expect(WFwith2Steps.stepsExecuted).toBe(1);

      const wfstatus = await DBOS.getWorkflowStatus(wfid);

      expect(wfstatus?.status).toBe(StatusString.CANCELLED);
    }
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
});
