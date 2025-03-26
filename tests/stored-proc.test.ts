import { execSync } from 'child_process';
import { parseConfigFile } from '../src';
import { Client, ClientConfig } from 'pg';

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

    const [config] = parseConfigFile();
    await runSql({ ...config.poolConfig, database: 'postgres' }, async (client) => {
      await client.query(`DROP DATABASE IF EXISTS ${config.poolConfig.database};`);
      await client.query(`DROP DATABASE IF EXISTS ${config.system_database};`);
      await client.query(`CREATE DATABASE ${config.poolConfig.database};`);
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
