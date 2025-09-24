import { DBOS, Debouncer, DBOSConfig, WorkflowQueue, StatusString, DBOSClient, DebouncerClient } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import assert from 'node:assert';

describe('debouncer-tests', () => {
  let config: DBOSConfig;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  // Register a simple workflow for testing
  const workflow = DBOS.registerWorkflow(
    async (x: number) => {
      return Promise.resolve(x);
    },
    { name: 'debouncerWorkflow' },
  );

  const queue = new WorkflowQueue('test-queue');

  test('test-debouncer-workflow', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const debouncePeriodMs = 2000;

    const test = async () => {
      const debouncer1 = new Debouncer({
        workflow,
      });
      const firstHandle = await debouncer1.debounce('key', debouncePeriodMs, firstValue);

      const debouncer2 = new Debouncer({
        workflow,
      });
      const secondHandle = await debouncer2.debounce('key', debouncePeriodMs, secondValue);

      assert.equal(firstHandle.workflowID, secondHandle.workflowID);
      assert.equal(await firstHandle.getResult(), secondValue);
      assert.equal(await secondHandle.getResult(), secondValue);

      const debouncer3 = new Debouncer({
        workflow,
      });
      const thirdHandle = await debouncer3.debounce('key', debouncePeriodMs, thirdValue);

      const debouncer4 = new Debouncer({
        workflow,
      });
      const fourthHandle = await debouncer4.debounce('key', debouncePeriodMs, fourthValue);

      assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
      assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
      assert.equal(await thirdHandle.getResult(), fourthValue);
      assert.equal(await fourthHandle.getResult(), fourthValue);
    };
    // First, run the test operations directly
    await test();
    // Then, run the test operations inside a workflow and verify they work there
    await DBOS.shutdown();
    const testWorkflow = DBOS.registerWorkflow(test);
    await DBOS.launch();
    const originalHandle = await DBOS.startWorkflow(testWorkflow)();
    await originalHandle.getResult();

    // Rerun the workflow, verify it still works
    await DBOSExecutor.globalInstance?.systemDatabase.setWorkflowStatus(
      originalHandle.workflowID,
      StatusString.PENDING,
      true,
    );
    const recoverHandle = await DBOS.startWorkflow(testWorkflow, { workflowID: originalHandle.workflowID })();
    await recoverHandle.getResult();
    const status = await recoverHandle.getStatus();
    assert.equal(status?.status, StatusString.SUCCESS);
  }, 30000);

  test('test-debouncer-timeout', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const longDebouncePeriodMs = 10000000000;
    const shortDebouncePeriodMs = 1000;

    // Set a huge period but small timeout, verify workflows start after the timeout
    const debouncer1 = new Debouncer({
      workflow,
      debounceTimeoutMs: 2000,
    });

    const firstHandle = await debouncer1.debounce('key', longDebouncePeriodMs, firstValue);
    const secondHandle = await debouncer1.debounce('key', longDebouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const thirdHandle = await debouncer1.debounce('key', longDebouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer1.debounce('key', longDebouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const debouncer2 = new Debouncer({
      workflow,
    });

    const fifthHandle = await debouncer2.debounce('key', longDebouncePeriodMs, firstValue);
    const sixthHandle = await debouncer2.debounce('key', shortDebouncePeriodMs, secondValue);

    assert.notEqual(fourthHandle.workflowID, fifthHandle.workflowID);
    assert.equal(fifthHandle.workflowID, sixthHandle.workflowID);
    assert.equal(await fifthHandle.getResult(), secondValue);
    assert.equal(await sixthHandle.getResult(), secondValue);
  }, 30000);

  test('test-multiple-debouncers', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const debouncePeriodMs = 2000;

    // Create two debouncers and use two different keys
    const debouncerOne = new Debouncer({
      workflow,
    });

    const debouncerTwo = new Debouncer({
      workflow,
    });

    const firstHandle = await debouncerOne.debounce('key_one', debouncePeriodMs, firstValue);
    const secondHandle = await debouncerOne.debounce('key_one', debouncePeriodMs, secondValue);
    const thirdHandle = await debouncerTwo.debounce('key_two', debouncePeriodMs, thirdValue);
    const fourthHandle = await debouncerTwo.debounce('key_two', debouncePeriodMs, fourthValue);

    // Verify that debouncers with different keys create different workflows
    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.notEqual(firstHandle.workflowID, thirdHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);

    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);
  }, 30000);

  test('test-debouncer-queue', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const debouncePeriodMs = 2000;

    const debouncer = new Debouncer({
      workflow,
      startWorkflowParams: { queueName: queue.name, timeoutMS: 5000 },
    });

    const firstHandle = await debouncer.debounce('key', debouncePeriodMs, firstValue);
    const secondHandle = await debouncer.debounce('key', debouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const secondStatus = await secondHandle.getStatus();
    assert.equal(secondStatus?.queueName, queue.name);

    const thirdHandle = await debouncer.debounce('key', debouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer.debounce('key', debouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const fourthStatus = await fourthHandle.getStatus();
    assert.equal(fourthStatus?.queueName, queue.name);
    assert.equal(fourthStatus?.timeoutMS, 5000);
    assert(fourthStatus?.deadlineEpochMS);
  }, 30000);

  test('test-debouncer-client', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const longDebouncePeriodMs = 10000000000;
    const shortDebouncePeriodMs = 1000;

    // Set a huge period but small timeout, verify workflows start after the timeout
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    const debouncer1 = new DebouncerClient(client, {
      workflowName: 'debouncerWorkflow',
      debounceTimeoutMs: 2000,
    });

    const firstHandle = await debouncer1.debounce('key', longDebouncePeriodMs, firstValue);
    const secondHandle = await debouncer1.debounce('key', longDebouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const thirdHandle = await debouncer1.debounce('key', longDebouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer1.debounce('key', longDebouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const debouncer2 = new DebouncerClient(client, {
      workflowName: 'debouncerWorkflow',
    });

    const fifthHandle = await debouncer2.debounce('key', longDebouncePeriodMs, firstValue);
    const sixthHandle = await debouncer2.debounce('key', shortDebouncePeriodMs, secondValue);

    assert.notEqual(fourthHandle.workflowID, fifthHandle.workflowID);
    assert.equal(fifthHandle.workflowID, sixthHandle.workflowID);
    assert.equal(await fifthHandle.getResult(), secondValue);
    assert.equal(await sixthHandle.getResult(), secondValue);
    await client.destroy();
  }, 60000);

  class TestClass {
    @DBOS.workflow()
    static async exampleWorkflow(x: number) {
      return Promise.resolve(x);
    }
  }

  test('test-debouncer-class', async () => {
    const firstValue = 0;
    const secondValue = 1;
    const thirdValue = 2;
    const fourthValue = 3;
    const fifthValue = 4;
    const sixthValue = 5;
    const debouncePeriodMs = 2000;

    // Test that two calls with the same key merge into one workflow
    const debouncer = new Debouncer({
      workflow: TestClass.exampleWorkflow,
    });
    const firstHandle = await debouncer.debounce('key', debouncePeriodMs, firstValue);

    const secondHandle = await debouncer.debounce('key', debouncePeriodMs, secondValue);

    assert.equal(firstHandle.workflowID, secondHandle.workflowID);
    assert.equal(await firstHandle.getResult(), secondValue);
    assert.equal(await secondHandle.getResult(), secondValue);

    const thirdHandle = await debouncer.debounce('key', debouncePeriodMs, thirdValue);
    const fourthHandle = await debouncer.debounce('key', debouncePeriodMs, fourthValue);

    assert.notEqual(thirdHandle.workflowID, firstHandle.workflowID);
    assert.equal(thirdHandle.workflowID, fourthHandle.workflowID);
    assert.equal(await thirdHandle.getResult(), fourthValue);
    assert.equal(await fourthHandle.getResult(), fourthValue);

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    const clientDebouncer = new DebouncerClient(client, {
      workflowClassName: 'TestClass',
      workflowName: 'exampleWorkflow',
      debounceTimeoutMs: 2000,
    });

    const fifthHandle = await clientDebouncer.debounce('key', debouncePeriodMs, fifthValue);
    const sixthHandle = await clientDebouncer.debounce('key', debouncePeriodMs, sixthValue);

    assert.notEqual(fourthHandle.workflowID, fifthHandle.workflowID);
    assert.equal(fifthHandle.workflowID, sixthHandle.workflowID);
    assert.equal(await fifthHandle.getResult(), sixthValue);
    assert.equal(await fifthHandle.getResult(), sixthValue);
    await client.destroy();
  }, 30000);
});
