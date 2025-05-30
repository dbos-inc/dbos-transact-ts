import { Pool } from 'pg';
import { DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { randomUUID } from 'node:crypto';

class TestWorkflow {
  @DBOS.workflow()
  static async testRunAsStep(value: string): Promise<string> {
    const result = await DBOS.runAsWorkflowStep(() => {
      return Promise.resolve(`testStep-${value}`);
    }, 'testStep');
    return `testRunAsStep-${result}`;
  }

  @DBOS.workflow()
  static async testRunAsStepThrows(value: string): Promise<string> {
    const result = await DBOS.runAsWorkflowStep(async () => {
      throw new Error(`testStepThrows-${value}`);
    }, 'testStepThrows');
    return `testRunAsStep-${result}`;
  }
}

describe('runAsWorkflowStep tests', () => {
  const config = generateDBOSTestConfig();
  let sysdb: Pool;

  beforeAll(async () => {
    sysdb = new Pool({
      host: config.poolConfig.host,
      port: config.poolConfig.port,
      user: config.poolConfig.user,
      password: config.poolConfig.password,
      database: config.system_database,
    });

    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
    await sysdb.end();
  });

  test('runAsWorkflowStep-works', async () => {
    const wfid = randomUUID();
    const result = await DBOS.withNextWorkflowID(wfid, () => TestWorkflow.testRunAsStep('testValue'));

    expect(result).toBe('testRunAsStep-testStep-testValue');

    const client = await sysdb.connect();
    try {
      const res = await client.query(
        'SELECT * FROM dbos.operation_outputs WHERE workflow_uuid = $1 AND function_id = $2',
        [wfid, 0],
      );
      expect(res.rows.length).toBe(1);
      expect(res.rows[0].function_name).toBe('testStep');
      expect(res.rows[0].output).toBe('"testStep-testValue"');
    } finally {
      client.release();
    }
  });

  test('throwing-runAsWorkflowStep', async () => {
    const wfid = randomUUID();
    await expect(DBOS.withNextWorkflowID(wfid, () => TestWorkflow.testRunAsStepThrows('testValue'))).rejects.toThrow(
      'testStepThrows-testValue',
    );

    const client = await sysdb.connect();
    try {
      const res = await client.query(
        'SELECT * FROM dbos.operation_outputs WHERE workflow_uuid = $1 AND function_id = $2',
        [wfid, 0],
      );
      expect(res.rows.length).toBe(1);
      expect(res.rows[0].function_name).toBe('testStepThrows');
      const error = JSON.parse(res.rows[0].error);
      expect(error.name).toBe('Error');
      expect(error.message).toBe('testStepThrows-testValue');
    } finally {
      client.release();
    }
  });
});
