import { Client } from 'pg';
import { KnexDataSource } from '../index';
import { dropDB, ensureDB } from './test-helpers';
import knex, { type Knex } from 'knex';

describe('KnexDataSource.initializeSchema', () => {
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
    const result = await client.query('SELECT workflow_id, function_num, output FROM dbos.transaction_completion');
    return result.rows;
  }

  async function txCompletionTableExists(client: Client) {
    const result = await client.query<{ exists: boolean }>(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_schema = 'dbos' AND table_name = 'transaction_completion');",
    );
    if (result.rowCount !== 1) throw new Error(`unexpected rowcount ${result.rowCount}`);
    return result.rows[0].exists;
  }

  test('initializeSchema-with-config', async () => {
    const knexConfig = { client: 'pg', connection: config };
    await KnexDataSource.initializeSchema(knexConfig);

    const client = new Client(config);
    try {
      await client.connect();
      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual([]);

      await KnexDataSource.uninitializeSchema(knexConfig);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
    } finally {
      await client.end();
    }
  });

  test('initializeSchema-with-knex', async () => {
    const knexDB = knex({ client: 'pg', connection: config });
    const client = new Client(config);
    try {
      await KnexDataSource.initializeSchema(knexDB);
      await client.connect();

      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual([]);

      await KnexDataSource.uninitializeSchema(knexDB);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
    } finally {
      await client.end();
      await knexDB.destroy();
    }
  });
});
