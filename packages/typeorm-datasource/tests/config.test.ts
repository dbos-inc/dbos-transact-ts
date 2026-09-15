import { Client } from 'pg';
import { TypeOrmDataSource } from '../index';
import { dropDB, ensureDB } from './test-helpers';

describe('TypeOrmDataSource.configure', () => {
  const config = { user: 'postgres', database: 'typeorm_ds_config_test' };

  beforeAll(async () => {
    const client = new Client({ ...config, database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, config.database, true);
      await ensureDB(client, config.database);
    } finally {
      await client.end();
    }
  });

  test('configure creates tx outputs table', async () => {
    await TypeOrmDataSource.initializeDBOSSchema(config);

    const client = new Client(config);
    try {
      await client.connect();
      const result = await client.query('SELECT workflow_id, function_num, output FROM dbos.transaction_completion');
      expect(result.rows.length).toBe(0);
    } finally {
      await client.end();
    }
  });

  describe('connection settings', () => {
    const password = process.env['PGPASSWORD'] || 'dbos';
    const savedEnv = { PGPASSWORD: process.env['PGPASSWORD'], PGPASSFILE: process.env['PGPASSFILE'] };

    // Hide the password from the environment and ~/.pgpass, so only the configured one can authenticate.
    beforeEach(() => {
      delete process.env['PGPASSWORD'];
      process.env['PGPASSFILE'] = '/nonexistent/pgpass';
    });

    afterEach(() => {
      for (const [key, value] of Object.entries(savedEnv)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    });

    test('uses the configured password', async () => {
      await expect(TypeOrmDataSource.initializeDBOSSchema({ ...config, password })).resolves.toBeUndefined();
    });

    test('uses the configured ssl setting', async () => {
      // Fails either way SSL is attempted: a server without SSL refuses it, and a local server's certificate is self-signed.
      await expect(
        TypeOrmDataSource.initializeDBOSSchema({ ...config, password, ssl: { rejectUnauthorized: true } }),
      ).rejects.toThrow(/SSL|certificate/);
    });
  });
});
