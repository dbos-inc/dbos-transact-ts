import { execSync } from 'child_process';
import { Client } from 'pg';
import { generateDBOSTestConfig } from './helpers';
import { ExistenceCheck } from '../src/system_database';

describe('schema-command-tests', () => {
  test('test schema command with system database URL argument', async () => {
    const config = generateDBOSTestConfig();
    const systemDatabaseUrl = config.systemDatabaseUrl;

    // Drop the system database if it exists
    const url = new URL(systemDatabaseUrl!);
    const systemDbName = url.pathname.slice(1);
    url.pathname = `/postgres`;
    const pgClient = new Client({
      connectionString: url.toString(),
    });
    await pgClient.connect();
    await pgClient.query(`DROP DATABASE IF EXISTS ${systemDbName} WITH (FORCE);`);
    await pgClient.end();

    // Run schema command with system database URL as argument
    execSync(`npx dbos schema ${systemDatabaseUrl}`, { env: process.env, stdio: 'inherit' });

    // Verify the system database and tables were created
    const pgSystemClient = new Client({
      connectionString: systemDatabaseUrl!,
    });
    await pgSystemClient.connect();

    const tableExists = await pgSystemClient.query<ExistenceCheck>(
      `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'operation_outputs')`,
    );
    expect(tableExists.rows[0].exists).toBe(true);

    await pgSystemClient.end();
  });
});
