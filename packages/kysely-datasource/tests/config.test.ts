import { Client } from 'pg';
import { KyselyDataSource } from '..';
import { dropDB, ensureDB, getKyselyDB } from './test-helpers';

const config = { user: 'postgres', database: 'kysely_ds_config_test' };
const db = getKyselyDB(config);

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

  async function queryTxCompletionTable(client: Client) {
    const result = await client.query(
      'SELECT workflow_id, function_num, output, error FROM dbos.transaction_completion',
    );
    return result.rowCount;
  }

  async function txCompletionTableExists(client: Client) {
    const result = await client.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_schema = 'dbos' AND table_name = 'transaction_completion');",
    );
    if (result.rowCount !== 1) throw new Error(`unexpected rowcount ${result.rowCount}`);
    return result.rows[0].exists;
  }

  test('initializeDBOSSchema', async () => {
    await KyselyDataSource.initializeDBOSSchema(db);

    const client = new Client(config);
    try {
      await client.connect();
      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual(0);

      await KyselyDataSource.uninitializeDBOSSchema(db);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
    } finally {
      await client.end();
    }
  });
});
