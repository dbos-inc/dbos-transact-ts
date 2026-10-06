import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { NodePostgresDataSource } from '..';
import { dropDB, ensureDB } from './test-helpers';

const config = { user: 'postgres', database: 'node_pg_ds_test_pool_error' };
const dataSource = new NodePostgresDataSource('pool-error-test', config);

let admin: Client;

// Loses the connection the way a Postgres restart or failover would: the backend serving this
// transaction goes away while the transaction still holds the client.
async function terminateOwnBackend() {
  const { rows } = await dataSource.client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
  await admin.query('SELECT pg_terminate_backend($1)', [rows[0].pid]);
  await dataSource.client.query('SELECT 1');
}

const regTerminateOwnBackend = dataSource.registerTransaction(terminateOwnBackend);

async function terminatedWorkflow() {
  await regTerminateOwnBackend();
}

const regTerminatedWorkflow = DBOS.registerWorkflow(terminatedWorkflow);

async function selectOne() {
  const { rows } = await dataSource.client.query<{ one: number }>('SELECT 1 AS one');
  return rows[0].one;
}

const regSelectOne = dataSource.registerTransaction(selectOne);

async function selectOneWorkflow() {
  return await regSelectOne();
}

const regSelectOneWorkflow = DBOS.registerWorkflow(selectOneWorkflow);

describe('NodePostgresDataSource internal pool', () => {
  beforeAll(async () => {
    admin = new Client({ ...config, database: 'postgres' });
    await admin.connect();
    await dropDB(admin, 'nodepg_pool_error_test_dbos_sys', true);
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
    DBOS.setConfig({ name: 'nodepg-pool-error-test' });
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
    DBOS.setConfig({ name: 'nodepg-pool-error-test' });
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
