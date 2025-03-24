import { workflow_status } from '../schemas/system_db_schema';
import { DBOS, DBOSConfig, DBOSClient, WorkflowQueue } from '../src';
import { globalParams } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { Client } from 'pg';

const queue = new WorkflowQueue('testQueue');

class ClientTest {
  @DBOS.workflow()
  static async invokeTest(
    numVal: number,
    strVal: string,
    objVal: { first: string; last: string; age: number },
  ): Promise<string> {
    return `${numVal}-${strVal}-${JSON.stringify(objVal)}`;
  }
}

describe('DBOSClient', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('invokeTest', async () => {
    const client = new DBOSClient(config.poolConfig, config.system_database);
    const wfid = `invokeTest-${Date.now()}`;

    try {
      type InvokeTest = typeof ClientTest.invokeTest;

      await client.init();
      await client.enqueue<Parameters<InvokeTest>>(
        {
          workflowName: 'invokeTest',
          workflowClassName: 'ClientTest',
          queueName: 'testQueue',
          workflowUUID: wfid,
        },
        42,
        'test',
        { first: 'John', last: 'Doe', age: 30 },
      );

      const handle = DBOS.retrieveWorkflow<Awaited<ReturnType<InvokeTest>>>(wfid);
      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await client.destroy();
    }

    const dbClient = new Client({ ...config.poolConfig, database: config.system_database });
    try {
      await dbClient.connect();
      const result = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].workflow_uuid).toBe(wfid);
      expect(result.rows[0].application_version).toBe(globalParams.appVersion);
    } finally {
      await dbClient.end();
    }
  }, 20000);
});
