import { spawn, execSync, ChildProcess } from 'child_process';
import { Writable } from 'stream';
import { Client } from 'pg';
import { generateDBOSTestConfig } from './helpers';
import { HealthUrl } from '../src/adminserver';
import { sleepms } from '../src/utils';

async function waitForMessageTest(
  command: ChildProcess,
  port: string,
  adminPort?: string,
  checkResponse: boolean = true,
) {
  const stdout = command.stdout as unknown as Writable;
  const stdin = command.stdin as unknown as Writable;
  const stderr = command.stderr as unknown as Writable;

  // Capture and print stdout/stderr
  stdout?.on('data', (data) => {
    process.stdout.write(`[child stdout]: ${data}`);
  });

  stderr?.on('data', (data) => {
    process.stderr.write(`[child stderr]: ${data}`);
  });

  if (!adminPort) {
    adminPort = (Number(port) + 1).toString();
  }

  try {
    const maxAttempts = 10;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        if (checkResponse) {
          const response = await fetch(`http://127.0.0.1:${port}/greeting/dbos`);
          expect(response.status).toBe(200);
        }
        const healthRes = await fetch(`http://127.0.0.1:${adminPort}${HealthUrl}`);
        expect(healthRes.status).toBe(200);
      } catch (error) {
        if (attempt < maxAttempts - 1) {
          await sleepms(1000);
        } else {
          const errMsg = `Error sending test request: ${(error as Error).message}`;
          console.error(errMsg);
          throw error;
        }
      }
    }
  } finally {
    stdin.end();
    stdout.destroy();
    stderr.destroy();
    command.kill();
  }
}

async function resetTemplateDatabases() {
  const config = generateDBOSTestConfig();
  expect(config.systemDatabaseUrl).toBeDefined();
  const url = new URL(config.systemDatabaseUrl!);
  url.pathname = `/postgres`;
  const pgSystemClient = new Client({
    connectionString: url.toString(),
  });
  await pgSystemClient.connect();
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_typeorm_dbos_sys WITH (FORCE);`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_prisma_dbos_sys WITH (FORCE);`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_drizzle_dbos_sys WITH (FORCE);`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_knex_dbos_sys WITH (FORCE);`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_typeorm WITH (FORCE);`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_prisma WITH (FORCE);`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_drizzle WITH (FORCE);`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS dbos_knex WITH (FORCE);`);
  await pgSystemClient.query(`CREATE DATABASE dbos_knex;`);
  await pgSystemClient.query(`CREATE DATABASE dbos_prisma;`);
  await pgSystemClient.query(`CREATE DATABASE dbos_drizzle;`);
  await pgSystemClient.query(`CREATE DATABASE dbos_typeorm;`);
  await pgSystemClient.end();
}

function configureTemplate() {
  execSync('npm i');
  execSync('npm run build');
  if (process.env.PGPASSWORD === undefined) {
    process.env.PGPASSWORD = 'dbos';
  }
  execSync('npx dbos migrate', { env: process.env, stdio: 'inherit' });
}

describe('runtime-tests-knex', () => {
  beforeAll(async () => {
    await resetTemplateDatabases();
    process.chdir('packages/create/templates/dbos-knex');
    configureTemplate();
  });

  afterAll(() => {
    process.chdir('../../../..');
  });

  test('test hello-knex tests', () => {
    execSync('npm run test', { env: process.env }); // Make sure hello-knex passes its own tests.
    execSync('npm run lint', { env: process.env }); // Pass linter rules.
  });

  test('test hello-knex runtime', async () => {
    const command = spawn('node', ['dist/main.js'], {
      env: process.env,
    });
    await waitForMessageTest(command, '3000');
  });
});

describe('runtime-tests-typeorm', () => {
  beforeAll(async () => {
    await resetTemplateDatabases();
    process.chdir('packages/create/templates/dbos-typeorm');
    configureTemplate();
  });

  afterAll(() => {
    process.chdir('../../../..');
  });

  test('test hello-typeorm tests', () => {
    execSync('npm run test', { env: process.env }); // Make sure hello-typeorm passes its own tests.
    execSync('npm run lint', { env: process.env }); // Pass linter rules.
  });

  test('test hello-typeorm runtime', async () => {
    const command = spawn('node', ['node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js', 'start'], {
      env: process.env,
    });
    await waitForMessageTest(command, '3000');
  });
});

describe('runtime-tests-prisma', () => {
  beforeAll(async () => {
    await resetTemplateDatabases();
    process.chdir('packages/create/templates/dbos-prisma');
    configureTemplate();
  });

  afterAll(() => {
    process.chdir('../../../..');
  });

  test('test hello-prisma tests', () => {
    execSync('npm run test', { env: process.env }); // Make sure hello-prisma passes its own tests.
    execSync('npm run lint', { env: process.env }); // Pass linter rules.
  });

  test('test hello-prisma runtime', async () => {
    const command = spawn('node', ['node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js', 'start'], {
      env: process.env,
    });
    await waitForMessageTest(command, '3000');
  });
});

describe('runtime-tests-drizzle', () => {
  beforeAll(async () => {
    await resetTemplateDatabases();
    process.chdir('packages/create/templates/dbos-drizzle');
    configureTemplate();
  });

  afterAll(() => {
    process.chdir('../../../..');
  });

  test('test hello-drizzle tests', () => {
    execSync('npm run test', { env: process.env }); // Make sure hello-drizzle passes its own tests.
    execSync('npm run lint', { env: process.env, stdio: 'inherit' }); // Pass linter rules.
  });

  test('test hello-drizzle runtime', async () => {
    const command = spawn('node', ['node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js', 'start'], {
      env: process.env,
    });
    await waitForMessageTest(command, '3000');
  });
});
