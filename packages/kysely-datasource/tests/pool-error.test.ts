import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { sql } from 'kysely';
import { KyselyDataSource } from '..';
import { Database, dropDB, ensureDB } from './test-helpers';

const config = { user: 'postgres', database: 'kysely_ds_test_pool_error' };
const dataSource = new KyselyDataSource<Database>('pool-error-test', config);

let admin: Client;

// Loses the connection the way a Postgres restart or failover would: the backend serving this
// transaction goes away while the transaction still holds the client.
async function terminateOwnBackend() {
  const { rows } = await sql<{ pid: number }>`SELECT pg_backend_pid() AS pid`.execute(dataSource.client);
  await admin.query('SELECT pg_terminate_backend($1)', [rows[0].pid]);
  await sql`SELECT 1`.execute(dataSource.client);
}

const regTerminateOwnBackend = dataSource.registerTransaction(terminateOwnBackend);

async function terminatedWorkflow() {
  await regTerminateOwnBackend();
}

const regTerminatedWorkflow = DBOS.registerWorkflow(terminatedWorkflow);

async function selectOne() {
  const { rows } = await sql<{ one: number }>`SELECT 1 AS one`.execute(dataSource.client);
  return rows[0].one;
}

const regSelectOne = dataSource.registerTransaction(selectOne);

async function selectOneWorkflow() {
  return await regSelectOne();
}

const regSelectOneWorkflow = DBOS.registerWorkflow(selectOneWorkflow);

describe('KyselyDataSource internal pool', () => {
  beforeAll(async () => {
    admin = new Client({ ...config, database: 'postgres' });
    await admin.connect();
    await dropDB(admin, 'kysely_pool_error_test_dbos_sys', true);
    await dropDB(admin, config.database, true);
    await ensureDB(admin, config.database);
  });

  afterAll(async () => {
    await admin.end();
  });

  test('attaches an error listener on the pool it creates', async () => {
    const spy = jest
      .spyOn(Pool.prototype, 'connect')
      .mockImplementation(() => Promise.reject(new Error('stop initialize')));
    DBOS.setConfig({ name: 'kysely-pool-error-test' });
    try {
      await DBOS.launch().catch(() => undefined);
      const pool = (spy.mock.contexts as Pool[]).find((p) => p.options.database === config.database);
      expect(pool).toBeDefined();
      expect(() => pool!.emit('error', new Error('boom'))).not.toThrow();
    } finally {
      await DBOS.shutdown().catch(() => undefined);
      spy.mockRestore();
    }
  });

  // A checked-out client that loses its connection emits 'error'. With no listener that is an
  // uncaught exception, which fails this test here and takes the process down in production.
  test('survives losing the connection mid-transaction', async () => {
    DBOS.setConfig({ name: 'kysely-pool-error-test' });
    await DBOS.launch();
    try {
      await expect(regTerminatedWorkflow()).rejects.toThrow();

      // The pool replaces the dead connection.
      await expect(regSelectOneWorkflow()).resolves.toBe(1);
    } finally {
      await DBOS.shutdown();
    }
  });
});
