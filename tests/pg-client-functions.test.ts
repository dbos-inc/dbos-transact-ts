import { workflow_status } from '../schemas/system_db_schema';
import { DBOS, WorkflowQueue } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { Client, PoolConfig } from 'pg';
import { DBOSConfig } from '../src/dbos-executor';

const testQueue = new WorkflowQueue('test-queue');

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
    const message = `Hello from PG function! ${Date.now()}`;
    const topic = `test-topic-${Date.now()}`;

    try {
      await DBOS.launch();

      const handle = await DBOS.startWorkflow(ClientTest).sendTest(topic);

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
        [handle.workflowID, JSON.stringify(message), topic],
      );

      await dbClient.end();

      const result = await handle.getResult();
      expect(result).toBe(message);
    } finally {
      await DBOS.shutdown();
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
    } finally {
      await dbClient.end();
    }
  });

  // Test PostgreSQL enqueue function
  test('pg-enqueue', async () => {
    const wfid = `pg-db-test-${Date.now()}`;
    const dbClient = new Client(poolConfig);

    try {
      await dbClient.connect();

      // Use PostgreSQL function to enqueue
      const enqueueResult = await dbClient.query<{ enqueue_workflow: string }>(
        `
        SELECT dbos.enqueue_workflow(
          workflow_name => 'enqueueTest',
          queue_name => $5,
          class_name => 'ClientTest', 
          workflow_id => $1,
          positional_args => ARRAY[$2::JSON, $3::JSON, $4::JSON]
        )
      `,
        [
          wfid,
          JSON.stringify(42),
          JSON.stringify('test'),
          JSON.stringify({ first: 'John', last: 'Doe', age: 30 }),
          testQueue.name,
        ],
      );

      expect(enqueueResult.rowCount).toEqual(1);
      expect(enqueueResult.rows[0].enqueue_workflow).toEqual(wfid);

      // Verify workflow was enqueued in database
      const checkResult = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );

      expect(checkResult.rows).toHaveLength(1);
      expect(checkResult.rows[0].workflow_uuid).toBe(wfid);
      expect(checkResult.rows[0].status).toBe('ENQUEUED');
    } finally {
      await dbClient.end();
    }

    try {
      await DBOS.launch();

      const handle = DBOS.retrieveWorkflow<string>(wfid);
      const status = await handle.getStatus();
      expect(status).toBeDefined();

      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await DBOS.shutdown();
    }
  }, 20000);

  test('pg-enqueue-gen-wfid', async () => {
    const dbClient = new Client(poolConfig);
    let wfid: string;

    try {
      await dbClient.connect();

      // Use PostgreSQL function to enqueue
      const enqueueResult = await dbClient.query<{ enqueue_workflow: string }>(
        `
        SELECT dbos.enqueue_workflow(
          workflow_name => 'enqueueTest',
          queue_name => $4,
          class_name => 'ClientTest', 
          positional_args => ARRAY[$1::JSON, $2::JSON, $3::JSON]
        )
      `,
        [
          JSON.stringify(42),
          JSON.stringify('test'),
          JSON.stringify({ first: 'John', last: 'Doe', age: 30 }),
          testQueue.name,
        ],
      );

      expect(enqueueResult.rowCount).toEqual(1);
      wfid = enqueueResult.rows[0].enqueue_workflow;

      // Verify workflow was enqueued in database
      const checkResult = await dbClient.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfid],
      );

      expect(checkResult.rows).toHaveLength(1);
      expect(checkResult.rows[0].workflow_uuid).toBe(wfid);
      expect(checkResult.rows[0].status).toBe('ENQUEUED');
    } finally {
      await dbClient.end();
    }

    try {
      await DBOS.launch();

      const handle = DBOS.retrieveWorkflow<string>(wfid);
      const status = await handle.getStatus();
      expect(status).toBeDefined();

      const result = await handle.getResult();
      expect(result).toBe('42-test-{"first":"John","last":"Doe","age":30}');
    } finally {
      await DBOS.shutdown();
    }
  }, 20000);
});
