/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-call */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { execSync } from 'child_process';
import { Client } from 'pg';
import { generateDBOSTestConfig } from './helpers';
import { ExistenceCheck } from '../src/system_database';
import { DBOS, WorkflowQueue } from '../src';
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

describe('workflow-management-cli-tests', () => {
  let config: DBOSConfig;
  let systemDbUrl: string;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    systemDbUrl = config.systemDatabaseUrl!;
    DBOS.setConfig(config);

    // Reset system database
    const resetOutput = execSync(`npx dbos reset -y -s ${systemDbUrl}`, { env: process.env, stdio: 'pipe' });
    console.log(resetOutput.toString());
    const schemaOutput = execSync(`npx dbos schema ${systemDbUrl}`, { env: process.env, stdio: 'pipe' });
    console.log(schemaOutput.toString());

    await DBOS.launch();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  // Define test workflows
  const queue = new WorkflowQueue('testQ');
  const TestWorkflows = {
    simpleWorkflow: DBOS.registerWorkflow(
      async (input: string) => {
        await DBOS.sleep(100);
        return `completed: ${input}`;
      },
      { name: 'simpleWorkflow' },
    ),

    multiStepWorkflow: DBOS.registerWorkflow(
      async () => {
        await DBOS.runStep(() => Promise.resolve(1), { name: 'step1' });
        await DBOS.runStep(() => Promise.resolve(2), { name: 'step2' });
        await DBOS.runStep(() => Promise.resolve(3), { name: 'step3' });
      },
      { name: 'multiStepWorkflow' },
    ),

    longRunningWorkflow: DBOS.registerWorkflow(
      async () => {
        for (let i = 0; i < 5; i++) {
          await DBOS.sleep(1000);
        }
        return 'completed';
      },
      { name: 'longRunningWorkflow' },
    ),

    queuedWorkflow: DBOS.registerWorkflow(
      async (id: number) => {
        await DBOS.sleep(200);
        return id * 2;
      },
      { name: 'queuedWorkflow' },
    ),
  };

  function runCommand(command: string): string {
    try {
      const output = execSync(command, { encoding: 'utf8', env: process.env });
      console.log(output);
      return output.trim();
    } catch (error: unknown) {
      throw new Error(`Command failed: ${command}\nError: ${String(error)}`);
    }
  }

  test('workflow list command', async () => {
    // Start some workflows
    const handle1 = await DBOS.startWorkflow(TestWorkflows.simpleWorkflow)('test1');
    const handle2 = await DBOS.startWorkflow(TestWorkflows.simpleWorkflow)('test2');
    await handle1.getResult();
    await handle2.getResult();

    // Test list command
    const output = runCommand(`npx dbos workflow list --sys-db-url "${systemDbUrl}"`);
    const workflows = JSON.parse(output);

    expect(Array.isArray(workflows)).toBe(true);
    expect(workflows.length).toBeGreaterThanOrEqual(2);

    const workflowIds = workflows.map((w: any) => w.workflowID);
    expect(workflowIds).toContain(handle1.workflowID);
    expect(workflowIds).toContain(handle2.workflowID);
  });

  test('workflow get command', async () => {
    const handle = await DBOS.startWorkflow(TestWorkflows.simpleWorkflow)('test-get');
    await handle.getResult();
    const workflowId = handle.workflowID;

    const output = runCommand(`npx dbos workflow get ${workflowId} --sys-db-url "${systemDbUrl}"`);
    const workflow = JSON.parse(output);

    expect(workflow.workflowID).toBe(workflowId);
    expect(workflow.status).toBe('SUCCESS');
    expect(workflow.workflowName).toBe('simpleWorkflow');
  });

  test('workflow steps command', async () => {
    const handle = await DBOS.startWorkflow(TestWorkflows.multiStepWorkflow)();
    await handle.getResult();
    const workflowId = handle.workflowID;

    const output = runCommand(`npx dbos workflow steps ${workflowId} --sys-db-url "${systemDbUrl}"`);
    const steps = JSON.parse(output);

    expect(Array.isArray(steps)).toBe(true);
    expect(steps.length).toBeGreaterThanOrEqual(3);
  });

  test('workflow cancel and resume commands', async () => {
    let handle = await DBOS.startWorkflow(TestWorkflows.longRunningWorkflow)();
    const workflowId = handle.workflowID;

    // Wait a bit for workflow to start
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Cancel the workflow
    runCommand(`npx dbos workflow cancel ${workflowId} --sys-db-url "${systemDbUrl}"`);

    // Verify it's cancelled
    const cancelOutput = runCommand(`npx dbos workflow get ${workflowId} --sys-db-url "${systemDbUrl}"`);
    const cancelledWorkflow = JSON.parse(cancelOutput);
    expect(cancelledWorkflow.status).toBe('CANCELLED');

    // Resume the workflow
    runCommand(`npx dbos workflow resume ${workflowId} --sys-db-url "${systemDbUrl}"`);

    // Wait a bit for resumption
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Verify it's running/enqueued
    const resumeOutput = runCommand(`npx dbos workflow get ${workflowId} --sys-db-url "${systemDbUrl}"`);
    const resumedWorkflow = JSON.parse(resumeOutput);
    expect(['ENQUEUED', 'PENDING', 'SUCCESS']).toContain(resumedWorkflow.status);
    handle = DBOS.retrieveWorkflow(workflowId);
    await handle.getResult();
  }, 20000);

  test('workflow fork command', async () => {
    const handle = await DBOS.startWorkflow(TestWorkflows.multiStepWorkflow)();
    await handle.getResult();
    const workflowId = handle.workflowID;

    // Test fork with custom workflow ID
    const customId = `custom-fork-${Date.now()}`;
    runCommand(
      `npx dbos workflow fork ${workflowId} --step 0 --forked-workflow-id "${customId}" --sys-db-url "${systemDbUrl}"`,
    );
    const forkOutput = runCommand(`npx dbos workflow get ${customId} --sys-db-url "${systemDbUrl}"`);
    const forkedWorkflow = JSON.parse(forkOutput);
    expect(['ENQUEUED', 'PENDING', 'SUCCESS']).toContain(forkedWorkflow.status);
  });

  test('workflow queue list command', async () => {
    // Start some queued workflows
    const handles = [];
    for (let i = 0; i < 3; i++) {
      const handle = await DBOS.startWorkflow(TestWorkflows.queuedWorkflow, {
        queueName: queue.name,
      })(i);
      handles.push(handle);
    }

    // Test queue list command
    const output = runCommand(`npx dbos workflow queue list --queue ${queue.name} --sys-db-url "${systemDbUrl}"`);
    const queuedWorkflows = JSON.parse(output);

    expect(Array.isArray(queuedWorkflows)).toBe(true);
    expect(queuedWorkflows.length).toBeGreaterThanOrEqual(3);

    // Wait for workflows to complete
    for (const handle of handles) {
      await handle.getResult();
    }
  });

  test('workflow list with filters', async () => {
    const handle = await DBOS.startWorkflow(TestWorkflows.simpleWorkflow)('filtered');
    await handle.getResult();

    // Test with status filter
    const successOutput = runCommand(`npx dbos workflow list --status SUCCESS --limit 5 --sys-db-url "${systemDbUrl}"`);
    const successWorkflows = JSON.parse(successOutput);
    expect(Array.isArray(successWorkflows)).toBe(true);
    expect(successWorkflows.length).toBeGreaterThanOrEqual(1);
    successWorkflows.forEach((w: any) => {
      expect(w.status).toBe('SUCCESS');
    });

    // Test with name filter
    const nameOutput = runCommand(
      `npx dbos workflow list --name simpleWorkflow --limit 5 --sys-db-url "${systemDbUrl}"`,
    );
    const nameWorkflows = JSON.parse(nameOutput);
    expect(Array.isArray(nameWorkflows)).toBe(true);
    expect(nameWorkflows.length).toBeGreaterThanOrEqual(1);
    nameWorkflows.forEach((w: any) => {
      expect(w.workflowName).toBe('simpleWorkflow');
    });
  });

  test('workflow list with time filters', async () => {
    const startTime = new Date();

    const handle = await DBOS.startWorkflow(TestWorkflows.simpleWorkflow)('time-test');
    await handle.getResult();

    const endTime = new Date();

    // Test with time range that includes the workflow
    const includeOutput = runCommand(
      `npx dbos workflow list --start-time "${startTime.toISOString()}" --end-time "${endTime.toISOString()}" --sys-db-url "${systemDbUrl}"`,
    );
    const includeWorkflows = JSON.parse(includeOutput);
    const workflowIds = includeWorkflows.map((w: any) => w.workflowID);
    expect(workflowIds).toContain(handle.workflowID);

    // Test with time range before the workflow
    const excludeEndTime = new Date(startTime.getTime() - 1000);
    const excludeOutput = runCommand(
      `npx dbos workflow list --end-time "${excludeEndTime.toISOString()}" --sys-db-url "${systemDbUrl}"`,
    );
    const excludeWorkflows = JSON.parse(excludeOutput);
    const excludeWorkflowIds = excludeWorkflows.map((w: any) => w.workflowID);
    expect(excludeWorkflowIds).not.toContain(handle.workflowID);
  });
});
