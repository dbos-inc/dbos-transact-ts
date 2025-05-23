import { Client } from 'pg';
import { KnexDataSource } from '../index';
import { dropDB, ensureDB } from './test-helpers';

describe('KnexDataSource.configure', () => {
  const config = { user: 'postgres', database: 'knex_ds_config_test' };

  beforeAll(async () => {
    const client = new Client({ ...config, database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, config.database);
      await ensureDB(client, config.database);
    } finally {
      await client.end();
    }
  });

  test('configure creates tx outputs table', async () => {
    await KnexDataSource.configure({ client: 'pg', connection: config });

    const client = new Client(config);
    try {
      await client.connect();
      const result = await client.query('SELECT workflow_id, function_num, output FROM dbos.transaction_outputs');
      expect(result.rows.length).toBe(0);
    } finally {
      await client.end();
    }
  });
});
