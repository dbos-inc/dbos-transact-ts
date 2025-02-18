import { StatusString, WorkflowHandle, DBOS, ConfiguredInstance } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestDb, Event } from './helpers';
import { WorkflowQueue } from '../src';
import { v4 as uuidv4 } from 'uuid';
import { WF } from './wfqtestprocess';
import { Console } from 'console';

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
      expect(DBOS.executor.getWorkflowStatus(wfid)).toBe(StatusString.CANCELLED);
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

class TestWFs {
  static wfCounter = 0;
  static stepCounter = 0;
  static wfid: string;

  static reset() {
    TestWFs.wfCounter = 0;
    TestWFs.stepCounter = 0;
  }

  @DBOS.workflow()
  static async testWorkflow(var1: string, var2: string) {
    expect(DBOS.workflowID).toBe(TestWFs.wfid);
    ++TestWFs.wfCounter;
    var1 = await TestWFs.testStep(var1);
    return Promise.resolve(var1 + var2);
  }

  @DBOS.workflow()
  static async testWorkflowSimple(var1: string, var2: string) {
    ++TestWFs.wfCounter;
    return Promise.resolve(var1 + var2);
  }

  @DBOS.step()
  static async testStep(str: string) {
    ++TestWFs.stepCounter;
    return Promise.resolve(str + 'd');
  }

  @DBOS.workflow()
  static async testWorkflowTime(var1: string, var2: string): Promise<number> {
    expect(var1).toBe('abc');
    expect(var2).toBe('123');
    return Promise.resolve(new Date().getTime());
  }

  @DBOS.workflow()
  static async noop() {
    return Promise.resolve();
  }
}

class TestWFs2 {
  static wfCounter = 0;
  static flag = false;
  static wfid: string;
  static mainResolve?: () => void;
  static wfPromise?: Promise<void>;

  static reset() {
    TestWFs2.wfCounter = 0;
    TestWFs2.flag = false;
  }

  @DBOS.workflow()
  static async workflowOne() {
    ++TestWFs2.wfCounter;
    TestWFs2.mainResolve?.();
    await TestWFs2.wfPromise;
    return Promise.resolve();
  }

  @DBOS.workflow()
  static async workflowTwo() {
    TestWFs2.flag = true; // Tell if this ran yet
    return Promise.resolve();
  }
}
