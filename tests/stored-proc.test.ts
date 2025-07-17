import { execSync } from 'child_process';
import { readConfigFile } from '../src';
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

describe.skip('stored-proc-tests', () => {
  let cwd: string;

  beforeAll(async () => {
    cwd = process.cwd();
    process.chdir('tests/proc-test');

    const config = readConfigFile();
    assert(config.database_url);
    const url = new URL(config.database_url);
    const database = url.pathname.slice(1);
    const sysDbName = config.database?.sys_db_name ?? `${database}_dbos_sys`;
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
