import { Client } from 'pg';
import { NodePostgresDataSource } from '../index';
import { dropDB, ensureDB } from './test-helpers';

describe('NodePostgresDataSource.configure', () => {
  const config = { user: 'postgres', database: 'nodepg_ds_config_test' };

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
    await NodePostgresDataSource.configure(config);

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
