import { DBOS, parseConfigFile } from '@dbos-inc/dbos-sdk';
import { app, dbos_hello, Hello } from './main';
import request from 'supertest';
import { Client, ClientConfig } from 'pg';

async function runSql<T>(config: ClientConfig, func: (client: Client) => Promise<T>) {
  const client = new Client(config);
  try {
    await client.connect();
    return await func(client);
  } finally {
    await client.end();
  }
}

async function dropLocalProcs() {
  const localProcs = ['Hello_helloProcedureLocal'];
  const sqlDropLocalProcs = localProcs
    .map((proc) => `DROP ROUTINE IF EXISTS "${proc}_p"; DROP ROUTINE IF EXISTS "${proc}_f";`)
    .join('\n');
  await DBOS.queryUserDB(sqlDropLocalProcs);
}

describe('operations-test', () => {
  beforeAll(async () => {
    const [config] = parseConfigFile();
    await DBOS.launch({ expressApp: app });
    await dropLocalProcs();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('test-workflow', async () => {
    const regex = /^Hello, dbos! You have been greeted (\d+) times.$/s;

    const res = await Hello.helloWorkflow('dbos');
    const match = res.match(regex);
    expect(match).not.toBeNull();
    const greetCount = match ? parseInt(match[1], 10) : 0;

    // Check the greet count in the DB matches the result of helloWorkflow.
    const rows = (await DBOS.queryUserDB('SELECT * FROM dbos_hello WHERE name=$1', ['dbos'])) as dbos_hello[];
    expect(rows[0].greet_count).toBe(greetCount);
  });

  test('test-workflow-local', async () => {
    const regex = /^Hello, dbos! You have been greeted (\d+) times.$/s;

    const res = await Hello.helloWorkflowLocal('dbos');
    const match = res.match(regex);
    expect(match).not.toBeNull();
    const greetCount = match ? parseInt(match[1], 10) : 0;

    // Check the greet count in the DB matches the result of helloWorkflow.
    const rows = (await DBOS.queryUserDB('SELECT * FROM dbos_hello WHERE name=$1', ['dbos'])) as dbos_hello[];
    expect(rows[0].greet_count).toBe(greetCount);
  });

  test('test-endpoint', async () => {
    const res = await request(app).get('/greeting/dbos');
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch('Hello, dbos! You have been greeted');
  });
});
