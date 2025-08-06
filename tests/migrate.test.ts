import { execSync } from 'child_process';
import { Client } from 'pg';
import { generateDBOSTestConfig } from './helpers';
import { ExistenceCheck } from '../src/system_database';
import { DBOS } from '../src';
import { DBOSConfig } from '../dist/src';

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

  test('test schema command with permissions for application role', async () => {
    const databaseName = 'schema_migrate_test';
    const roleName = 'schema-migrate-test-role';
    const rolePassword = 'schema_migrate_test_password';

    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${databaseName}`;

    // Drop the DBOS database if it exists. Create a test role with no permissions.
    const pgClient = new Client({
      connectionString: baseUrl.toString(),
    });
    await pgClient.connect();
    await pgClient.query(`DROP DATABASE IF EXISTS ${databaseName} WITH (FORCE)`);
    await pgClient.query(`DROP ROLE IF EXISTS "${roleName}"`);
    await pgClient.query(`CREATE ROLE "${roleName}" WITH LOGIN PASSWORD '${rolePassword}'`);
    await pgClient.end();

    // Using the admin role, create the DBOS database and verify it exists.
    // Set permissions for the test role.
    try {
      const result = execSync(`npx dbos schema ${dbUrl.toString()} -r ${roleName}`, {
        env: process.env,
        stdio: 'pipe',
      });
      console.log(result.toString());
    } catch (error) {
      console.error('Schema command failed:');
      const execError = error as { stdout?: Buffer; stderr?: Buffer; status?: number };
      console.error('stdout:', execError.stdout?.toString());
      console.error('stderr:', execError.stderr?.toString());
      throw error;
    }

    const pgVerifyClient = new Client({
      connectionString: baseUrl.toString(),
    });
    await pgVerifyClient.connect();
    const result = await pgVerifyClient.query<{ count: string }>(
      `SELECT COUNT(*) FROM pg_database WHERE datname = $1`,
      [databaseName],
    );
    expect(result.rows[0].count).toBe('1');
    await pgVerifyClient.end();

    // Initialize DBOS with the test role. Verify various operations work.
    const testDbUrl = new URL(dbUrl.toString());
    testDbUrl.username = roleName;
    testDbUrl.password = rolePassword;

    await DBOS.shutdown();
    const roleConfig: DBOSConfig = {
      name: 'schema-migrate-test',
      databaseUrl: testDbUrl.toString(),
      systemDatabaseUrl: testDbUrl.toString(),
    };
    DBOS.setConfig(roleConfig);

    const testWorkflow = DBOS.registerWorkflow(
      async () => {
        const id = DBOS.workflowID;
        expect(id).toBeTruthy();
        await DBOS.setEvent(id!, id!);
        return id!;
      },
      { name: 'migrate-test-workflow' },
    );

    await DBOS.launch();

    const workflowId = await testWorkflow();
    expect(workflowId).toBeTruthy();
    expect(await DBOS.getEvent(workflowId, workflowId)).toBe(workflowId);

    const steps = await DBOS.listWorkflowSteps(workflowId);
    expect(steps).toHaveLength(1);
    expect(steps![0].name).toBe('DBOS.setEvent');

    await DBOS.shutdown();
  });
});
