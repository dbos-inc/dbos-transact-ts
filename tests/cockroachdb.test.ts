import { DBOS } from '../src/';
import { DBOSConfig } from '../src/dbos-executor';
import { Client } from 'pg';

const cockroachdbUrl = process.env.DBOS_COCKROACHDB_URL;
const describeIf = cockroachdbUrl ? describe : describe.skip;

describeIf('cockroachdb', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    const url = new URL(cockroachdbUrl!);
    url.pathname = '/dbos_test';
    const systemDatabaseUrl = url.toString();

    const client = new Client({ connectionString: cockroachdbUrl });
    await client.connect();
    await client.query('DROP DATABASE IF EXISTS dbos_test');
    await client.query('CREATE DATABASE dbos_test');
    await client.end();
    config = {
      name: 'cockroachdb-test',
      systemDatabaseUrl,
    };
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  }, 10000);

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('launch-and-shutdown', async () => {
    // DBOS launched successfully against CockroachDB
    await expect(DBOS.listWorkflows({})).resolves.toBeDefined();
  });
});
