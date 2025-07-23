import { DBOS } from '@dbos-inc/dbos-sdk';
import { app, dbos_hello, Hello } from './main';
import request from 'supertest';

async function dropLocalProcs() {
  const localProcs = ['Hello_helloProcedureLocal'];
  const sqlDropLocalProcs = localProcs
    .map((proc) => `DROP ROUTINE IF EXISTS "${proc}_p"; DROP ROUTINE IF EXISTS "${proc}_f";`)
    .join('\n');
  await DBOS.queryUserDB(sqlDropLocalProcs);
}

describe('operations-test', () => {
  beforeAll(async () => {
    await DBOS.launch();
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
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
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
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const rows = (await DBOS.queryUserDB('SELECT * FROM dbos_hello WHERE name=$1', ['dbos'])) as dbos_hello[];
    expect(rows[0].greet_count).toBe(greetCount);
  });

  test('test-endpoint', async () => {
    const res = await request(app).get('/greeting/dbos');
    expect(res.statusCode).toBe(200);
    expect(res.text).toMatch('Hello, dbos! You have been greeted');
  });
});
