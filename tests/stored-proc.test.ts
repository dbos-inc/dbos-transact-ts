import { execSync } from 'child_process';
import { getDatabaseUrl, readConfigFile } from '../src';
import { Client, ClientConfig } from 'pg';
import assert from 'assert';

async function runSql(config: ClientConfig, func: (client: Client) => Promise<void>) {
  const client = new Client(config);
  try {
    await client.connect();
    await func(client);
  } finally {
    await client.end();
  }
}

describe('stored-proc-tests', () => {
  let cwd: string;

  beforeAll(async () => {
    cwd = process.cwd();
    process.chdir('tests/proc-test');

    const config = readConfigFile();
    const databaseUrl = getDatabaseUrl(config);
    assert(databaseUrl);
    const url = new URL(databaseUrl);
    const database = url.pathname.slice(1);
    const sysDbName = new URL(databaseUrl).pathname.slice(1);
    url.pathname = '/postgres';

    await runSql({ connectionString: url.toString() }, async (client) => {
      await client.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE);`);
      await client.query(`DROP DATABASE IF EXISTS ${sysDbName} WITH (FORCE);`);
      await client.query(`CREATE DATABASE ${database};`);
    });

    execSync('npm install');
    execSync('npm run build');
  }, 120000);

  afterAll(() => {
    process.chdir(cwd);
  });

  test('npm run test', () => {
    execSync('npm run test', { env: process.env });
  });
});
