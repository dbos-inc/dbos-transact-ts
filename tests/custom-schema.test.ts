import { DBOS } from '../src';
import { generateDBOSTestConfig } from './helpers';
import { Client } from 'pg';

interface ExistenceCheck {
  exists: boolean;
}

describe('custom-schema-tests', () => {
  beforeAll(async () => {
    // Clean up any existing test database
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    const customSchemaDbName = 'custom_schema_test_db';
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${customSchemaDbName}`;

    const pgClient = new Client({
      connectionString: baseUrl.toString(),
    });
    await pgClient.connect();
    await pgClient.query(`DROP DATABASE IF EXISTS ${customSchemaDbName} WITH (FORCE);`);
    await pgClient.end();
  });

  afterAll(async () => {
    // Clean up test database
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    const customSchemaDbName = 'custom_schema_test_db';

    const pgClient = new Client({
      connectionString: baseUrl.toString(),
    });
    await pgClient.connect();
    await pgClient.query(`DROP DATABASE IF EXISTS ${customSchemaDbName} WITH (FORCE);`);
    await pgClient.end();
  });

  test('test custom schema name initialization', async () => {
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    const customSchemaDbName = 'custom_schema_test_db';
    const customSchemaName = 'my_custom_schema';
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${customSchemaDbName}`;

    // Set up DBOS with custom schema name
    DBOS.setConfig({
      name: 'custom-schema-test',
      systemDatabaseUrl: dbUrl.toString(),
      systemDatabaseSchemaName: customSchemaName,
    });
    await DBOS.launch();

    // Verify the custom schema was created
    const pgSystemClient = new Client({
      connectionString: dbUrl.toString(),
    });
    await pgSystemClient.connect();

    // Check that the custom schema exists
    const schemaExists = await pgSystemClient.query<ExistenceCheck>(
      `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = '${customSchemaName}')`,
    );
    expect(schemaExists.rows[0].exists).toBe(true);

    // Check that tables were created in the custom schema
    const tableExists = await pgSystemClient.query<ExistenceCheck>(
      `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '${customSchemaName}' AND table_name = 'workflow_status')`,
    );
    expect(tableExists.rows[0].exists).toBe(true);

    // Check that the default 'dbos' schema was NOT created
    const defaultSchemaExists = await pgSystemClient.query<ExistenceCheck>(
      `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`,
    );
    expect(defaultSchemaExists.rows[0].exists).toBe(false);

    await pgSystemClient.end();
    await DBOS.shutdown();
  });

  test('test workflow execution with custom schema', async () => {
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    const customSchemaDbName = 'custom_schema_test_db_2';
    const customSchemaName = 'workflow_test_schema';
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${customSchemaDbName}`;

    // Clean up any existing test database
    const pgClient = new Client({
      connectionString: baseUrl.toString(),
    });
    await pgClient.connect();
    await pgClient.query(`DROP DATABASE IF EXISTS ${customSchemaDbName} WITH (FORCE);`);
    await pgClient.end();

    try {
      // Set up DBOS with custom schema name
      DBOS.setConfig({
        name: 'custom-schema-workflow-test',
        systemDatabaseUrl: dbUrl.toString(),
        systemDatabaseSchemaName: customSchemaName,
      });
      await DBOS.launch();

      // Define a simple workflow
      async function testStepFunction() {
        return Promise.resolve('step completed');
      }

      async function testWorkflowFunction() {
        const result = await DBOS.runStep(() => testStepFunction(), { name: 'testStep' });
        return result;
      }
      const testWorkflow = DBOS.registerWorkflow(testWorkflowFunction);

      // Execute the workflow
      const result = await testWorkflow();
      expect(result).toBe('step completed');

      // Verify workflow status was recorded in custom schema
      const pgSystemClient = new Client({
        connectionString: dbUrl.toString(),
      });
      await pgSystemClient.connect();

      const workflowCount = await pgSystemClient.query<{ count: string }>(
        `SELECT COUNT(*) as count FROM ${customSchemaName}.workflow_status`,
      );
      expect(parseInt(workflowCount.rows[0].count)).toBeGreaterThan(0);

      await pgSystemClient.end();
      await DBOS.shutdown();
    } finally {
      // Clean up test database
      const cleanupClient = new Client({
        connectionString: baseUrl.toString(),
      });
      await cleanupClient.connect();
      await cleanupClient.query(`DROP DATABASE IF EXISTS ${customSchemaDbName} WITH (FORCE);`);
      await cleanupClient.end();
    }
  });

  test('test default schema when not specified', async () => {
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    const defaultSchemaDbName = 'default_schema_test_db';
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${defaultSchemaDbName}`;

    // Clean up any existing test database
    const pgClient = new Client({
      connectionString: baseUrl.toString(),
    });
    await pgClient.connect();
    await pgClient.query(`DROP DATABASE IF EXISTS ${defaultSchemaDbName} WITH (FORCE);`);
    await pgClient.end();

    try {
      // Set up DBOS WITHOUT custom schema name (should default to 'dbos')
      DBOS.setConfig({
        name: 'default-schema-test',
        systemDatabaseUrl: dbUrl.toString(),
        // systemDatabaseSchemaName is not specified
      });
      await DBOS.launch();

      // Verify the default 'dbos' schema was created
      const pgSystemClient = new Client({
        connectionString: dbUrl.toString(),
      });
      await pgSystemClient.connect();

      const schemaExists = await pgSystemClient.query<ExistenceCheck>(
        `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`,
      );
      expect(schemaExists.rows[0].exists).toBe(true);

      const tableExists = await pgSystemClient.query<ExistenceCheck>(
        `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_status')`,
      );
      expect(tableExists.rows[0].exists).toBe(true);

      await pgSystemClient.end();
      await DBOS.shutdown();
    } finally {
      // Clean up test database
      const cleanupClient = new Client({
        connectionString: baseUrl.toString(),
      });
      await cleanupClient.connect();
      await cleanupClient.query(`DROP DATABASE IF EXISTS ${defaultSchemaDbName} WITH (FORCE);`);
      await cleanupClient.end();
    }
  });

  test('test custom schema with special characters', async () => {
    const config = generateDBOSTestConfig();
    const baseUrl = new URL(config.systemDatabaseUrl!);
    const specialSchemaDbName = 'special_schema_test_db';
    const specialSchemaName = 'F8nny_sCHem@-n@m3';
    const dbUrl = new URL(baseUrl.toString());
    dbUrl.pathname = `/${specialSchemaDbName}`;

    // Clean up any existing test database
    const pgClient = new Client({
      connectionString: baseUrl.toString(),
    });
    await pgClient.connect();
    await pgClient.query(`DROP DATABASE IF EXISTS ${specialSchemaDbName} WITH (FORCE);`);
    await pgClient.end();

    try {
      // Set up DBOS with special character schema name
      DBOS.setConfig({
        name: 'special-schema-test',
        systemDatabaseUrl: dbUrl.toString(),
        systemDatabaseSchemaName: specialSchemaName,
      });
      await DBOS.launch();

      // Verify the special character schema was created
      const pgSystemClient = new Client({
        connectionString: dbUrl.toString(),
      });
      await pgSystemClient.connect();

      // Check that the special character schema exists (using parameterized query)
      const schemaExists = await pgSystemClient.query<ExistenceCheck>(
        `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = $1)`,
        [specialSchemaName],
      );
      expect(schemaExists.rows[0].exists).toBe(true);

      // Check that tables were created in the special character schema
      const tableExists = await pgSystemClient.query<ExistenceCheck>(
        `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'workflow_status')`,
        [specialSchemaName],
      );
      expect(tableExists.rows[0].exists).toBe(true);

      // Check that the default 'dbos' schema was NOT created
      const defaultSchemaExists = await pgSystemClient.query<ExistenceCheck>(
        `SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = 'dbos')`,
      );
      expect(defaultSchemaExists.rows[0].exists).toBe(false);

      // Define and execute a simple workflow to ensure full functionality
      async function testStepFunction() {
        return Promise.resolve('special schema step completed');
      }

      async function testWorkflowFunction() {
        const result = await DBOS.runStep(() => testStepFunction(), { name: 'specialSchemaStep' });
        return result;
      }
      const testWorkflow = DBOS.registerWorkflow(testWorkflowFunction);

      // Execute the workflow
      const result = await testWorkflow();
      expect(result).toBe('special schema step completed');

      // Verify workflow status was recorded in the special character schema
      const workflowCount = await pgSystemClient.query<{ count: string }>(
        `SELECT COUNT(*) as count FROM "${specialSchemaName}".workflow_status`,
      );
      expect(parseInt(workflowCount.rows[0].count)).toBeGreaterThan(0);

      await pgSystemClient.end();
      await DBOS.shutdown();
    } finally {
      // Clean up test database
      const cleanupClient = new Client({
        connectionString: baseUrl.toString(),
      });
      await cleanupClient.connect();
      await cleanupClient.query(`DROP DATABASE IF EXISTS ${specialSchemaDbName} WITH (FORCE);`);
      await cleanupClient.end();
    }
  });
});
