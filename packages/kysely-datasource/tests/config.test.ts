import { Client, Pool, PoolClient } from 'pg';
import { KyselyDataSource } from '..';
import { dropDB, ensureDB } from './test-helpers';

const config = { user: 'postgres', database: 'kysely_ds_config_test' };

describe('KyselyDataSource.initializeDBOSSchema', () => {
  beforeEach(async () => {
    const client = new Client({ ...config, database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, config.database, true);
      await ensureDB(client, config.database);
    } finally {
      await client.end();
    }
  });

  async function queryTxCompletionTable(client: PoolClient) {
    const result = await client.query(
      'SELECT workflow_id, function_num, output, error FROM dbos.transaction_completion',
    );
    return result.rowCount;
  }

  async function txCompletionTableExists(client: PoolClient) {
    const result = await client.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_schema = 'dbos' AND table_name = 'transaction_completion');",
    );
    if (result.rowCount !== 1) throw new Error(`unexpected rowcount ${result.rowCount}`);
    return result.rows[0].exists;
  }

  test('initializeDBOSSchema', async () => {
    const db = new Pool(config);
    await KyselyDataSource.initializeDBOSSchema(db);

    try {
      const client = await db.connect();
      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual(0);

      await KyselyDataSource.uninitializeDBOSSchema(db);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
      client.release();
    } finally {
      await db.end();
    }
  });
});
