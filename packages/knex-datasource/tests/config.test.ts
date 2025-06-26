import { Client } from 'pg';
import { KnexDataSource } from '../index';
import { dropDB, ensureDB } from './test-helpers';
import knex from 'knex';

describe('KnexDataSource.initializeDBOSSchema', () => {
  const config = { user: 'postgres', database: 'knex_ds_config_test' };

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

  test('initializeDBOSSchema-with-config', async () => {
    const knexConfig = { client: 'pg', connection: config };
    await KnexDataSource.initializeDBOSSchema(knexConfig);

    const client = new Client(config);
    try {
      await client.connect();
      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual(0);

      await KnexDataSource.uninitializeDBOSSchema(knexConfig);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
    } finally {
      await client.end();
    }
  });

  test('initializeDBOSSchema-with-knex', async () => {
    const knexDB = knex({ client: 'pg', connection: config });
    const client = new Client(config);
    try {
      await KnexDataSource.initializeDBOSSchema(knexDB);
      await client.connect();

      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual(0);

      await KnexDataSource.uninitializeDBOSSchema(knexDB);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
    } finally {
      await client.end();
      await knexDB.destroy();
    }
  });
});
