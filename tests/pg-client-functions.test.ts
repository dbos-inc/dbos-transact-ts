import { workflow_status } from '../schemas/system_db_schema';
import { DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { Client, PoolConfig } from 'pg';
import { DBOSConfig } from '../src/dbos-executor';

class ClientTest {
  @DBOS.workflow()
  static async enqueueTest(
    numVal: number,
    strVal: string,
    objVal: { first: string; last: string; age: number },
  ): Promise<string> {
    return Promise.resolve(`${numVal}-${strVal}-${JSON.stringify(objVal)}`);
  }

  @DBOS.workflow()
  static async sendTest(topic?: string) {
    return await DBOS.recv<string>(topic, 60);
  }
}

describe('PostgreSQL Client Functions', () => {
  let config: DBOSConfig;
  let poolConfig: PoolConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    expect(config.systemDatabaseUrl).toBeDefined();
    poolConfig = { connectionString: config.systemDatabaseUrl };
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  // Test PostgreSQL send message function
  test('pg-send-message-function', async () => {
    const workflowID = `pg-send-${Date.now()}`;
    const message = `Hello from PG function! ${Date.now()}`;
    const topic = `test-topic-${Date.now()}`;

    try {
      await DBOS.launch();

      const handle = await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);

      // Send message using PostgreSQL function
      const dbClient = new Client(poolConfig);
      await dbClient.connect();

      await dbClient.query(
        `
        SELECT dbos.send_message(
          destination_id => $1,
          message => $2::JSON,
          topic => $3
        )
      `,
        [workflowID, JSON.stringify(message), topic],
      );

      await dbClient.end();

      const result = await handle.getResult();
      expect(result).toBe(message);

      await DBOS.shutdown();
    } catch (error) {
      await DBOS.shutdown();
      throw error;
    }
  }, 20000);

  // Test error handling for non-existent workflow
  test('pg-send-to-nonexistent-workflow', async () => {
    const nonExistentID = `nonexistent-${Date.now()}`;
    const message = 'Test message';
    const dbClient = new Client(poolConfig);

    try {
      await dbClient.connect();

      await expect(
        dbClient.query(
          `
          SELECT dbos.send_message(
            destination_id => $1,
            message => $2::JSON
          )
        `,
          [nonExistentID, JSON.stringify(message)],
        ),
      ).rejects.toThrow();

      await dbClient.end();
    } catch (error) {
      throw error;
    }
  });

  // Test PostgreSQL enqueue function (database-level only)
  test('pg-enqueue-function-database-test', async () => {
    const wfid = `pg-db-test-${Date.now()}`;
    const dbClient = new Client(poolConfig);

    try {
      await dbClient.connect();

      // Use PostgreSQL function to enqueue
      const result = await dbClient.query(
        `
        SELECT dbos.enqueue_workflow(
          workflow_name => 'enqueueTest',
          queue_name => 'testQueue',
          class_name => 'ClientTest', 
          workflow_id => $1,
          positional_args => ARRAY[$2::JSON, $3::JSON, $4::JSON]
        )
      `,
        [wfid, JSON.stringify(42), JSON.stringify('test'), JSON.stringify({ first: 'John', last: 'Doe', age: 30 })],
      );

      expect(result.rowCount).toBeGreaterThan(0);

      // Verify workflow was enqueued in database
      const checkResult = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );

      expect(checkResult.rows).toHaveLength(1);
      expect(checkResult.rows[0].workflow_uuid).toBe(wfid);
      expect(checkResult.rows[0].status).toBe('ENQUEUED');

      await dbClient.end();
    } catch (error) {
      throw error;
    }
  });
});
