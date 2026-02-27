import { DBOS, DBOSConfig } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import assert from 'node:assert';

describe('metrics-tests', () => {
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

  const sysDB = () => DBOSExecutor.globalInstance!.systemDatabase;

  const testWorkflowA = DBOS.registerWorkflow(
    async () => {
      await DBOS.runStep(() => Promise.resolve('x'), { name: 'testStepX' });
      await DBOS.runStep(() => Promise.resolve('x'), { name: 'testStepX' });
      return 'a';
    },
    { name: 'testWorkflowA' },
  );

  const testWorkflowB = DBOS.registerWorkflow(
    async () => {
      await DBOS.runStep(() => Promise.resolve('y'), { name: 'testStepY' });
      return 'b';
    },
    { name: 'testWorkflowB' },
  );

  test('test-get-metrics', async () => {
    // Record start time before creating workflows
    const startTime = new Date().toISOString();

    // Execute workflows to create metrics data
    await testWorkflowA();
    await testWorkflowA();
    await testWorkflowB();

    // Record end time after creating workflows
    const endTime = new Date().toISOString();

    // Query metrics
    const metrics = await sysDB().getMetrics(startTime, endTime);
    assert.equal(metrics.length, 4);

    // Convert to map for easier assertion
    const metricsMap = new Map(metrics.map((m) => [`${m.metricType}:${m.metricName}`, m.value]));

    // Verify workflow counts
    assert.equal(metricsMap.get('workflow_count:testWorkflowA'), 2);
    assert.equal(metricsMap.get('workflow_count:testWorkflowB'), 1);

    // Verify step counts
    assert.equal(metricsMap.get('step_count:testStepX'), 4);
    assert.equal(metricsMap.get('step_count:testStepY'), 1);
  }, 30000);
});
