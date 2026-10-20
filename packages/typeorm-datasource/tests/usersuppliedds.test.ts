import 'reflect-metadata';

import { User } from './demoentities/User';
import { Photo } from './demoentities/Photo';
import { DataSource } from 'typeorm';

import { TypeOrmDataSource } from '..';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { randomUUID } from 'crypto';
import { SuperJSON } from 'superjson';
import { ensurePGDatabase } from '../../../dist/src/database_utils';

const config = { user: process.env.PGUSER || 'postgres', database: 'typeorm_demo_ds' };

export const AppDataSource = new DataSource({
  type: 'postgres',
  database: config.database,
  username: config.user,
  synchronize: true, // auto-create schema in dev
  logging: false,
  entities: ['./tests/demoentities/**/*.{ts,js}'], // glob pattern
});

const dataSource: TypeOrmDataSource = TypeOrmDataSource.createFromDataSource('appdb', AppDataSource);

const workflow = DBOS.registerWorkflow(
  async () => {
    await dataSource.runTransaction(
      async () => {
        const user = dataSource.entityManager.create(User, { name: 'Alice' });
        await dataSource.entityManager.save(user);

        // attach a photo
        const photo = dataSource.entityManager.create(Photo, { url: 'https://example.com/me.jpg', user });
        await dataSource.entityManager.save(photo);
      },
      { name: 'insert' },
    );

    const nusers = await dataSource.runTransaction(
      async () => {
        const users = await AppDataSource.getRepository(User).find({
          relations: { photos: true },
        });
        return users.length;
      },
      { name: 'select' },
    );

    return nusers;
  },
  { name: 'testTypeOrm' },
);

const txState = { calls: 0 };

async function insertUser(name: string) {
  txState.calls += 1;
  const user = await dataSource.entityManager.save(dataSource.entityManager.create(User, { name }));
  return user.id;
}

async function insertUserThenFail(name: string) {
  txState.calls += 1;
  await dataSource.entityManager.save(dataSource.entityManager.create(User, { name }));
  throw new Error(`failed after inserting ${name}`);
}

const regInsertUser = dataSource.registerTransaction(insertUser);
const regInsertUserThenFail = dataSource.registerTransaction(insertUserThenFail);
const insertUserWorkflow = DBOS.registerWorkflow((name: string) => regInsertUser(name), { name: 'insertUserWorkflow' });
const failingWorkflow = DBOS.registerWorkflow((name: string) => regInsertUserThenFail(name), {
  name: 'failingWorkflow',
});

function countUsers(name: string) {
  return AppDataSource.getRepository(User).countBy({ name });
}

// ensurePGDatabase swallows every failure, so verify the postcondition and fail with the reason.
async function ensureTestDatabase(databaseUrl: string) {
  const notes: string[] = [];
  const record = (msg: string) => {
    notes.push(msg);
  };
  await ensurePGDatabase(databaseUrl, { info: record, warn: record });
  const client = new Client({ connectionString: databaseUrl });
  try {
    await client.connect();
    // CockroachDB connects to a nonexistent database, so hit a catalog that requires it to exist.
    await client.query('SELECT 1 FROM information_schema.schemata LIMIT 1');
  } catch (e) {
    const why = notes.length ? ` [${notes.join('; ')}]` : '';
    throw new Error(`Could not provision the test database: ${(e as Error).message}${why}`);
  } finally {
    await client.end().catch(() => {});
  }
}

describe('TypeOrmDataSourceDemo', () => {
  beforeAll(async () => {
    await ensureTestDatabase(
      `postgresql://${config.user}:${process.env['PGPASSWORD'] || 'dbos'}@${process.env['PGHOST'] || 'localhost'}:${process.env['PGPORT'] || '5432'}/${config.database}`,
    );
    await TypeOrmDataSource.initializeDBOSSchema(config);
    await AppDataSource.initialize();
  });

  afterAll(async () => {
    if (AppDataSource.isInitialized) await AppDataSource.destroy();
  });

  beforeEach(async () => {
    await AppDataSource.query('TRUNCATE "photo", "user" RESTART IDENTITY');
    txState.calls = 0;

    DBOS.setConfig({ name: 'typeorm-demo-ds' });
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('insert dataSource.register function', async () => {
    const nusers = await workflow();
    expect(nusers).toBe(1);
  });

  // Leaves the outcome a committed execution of the workflow's first step would have checkpointed.
  async function plantCompletion(workflowID: string, column: 'output' | 'error', value: unknown) {
    await AppDataSource.query(
      `INSERT INTO dbos.transaction_completion (workflow_id, function_num, ${column}) VALUES ($1, 0, $2)`,
      [workflowID, SuperJSON.stringify(value)],
    );
  }

  test('a recorded transaction output is replayed without rerunning the transaction', async () => {
    const workflowID = randomUUID();
    await plantCompletion(workflowID, 'output', 42);

    await expect(DBOS.withNextWorkflowID(workflowID, () => insertUserWorkflow('bob'))).resolves.toBe(42);
    expect(txState.calls).toBe(0);
    await expect(countUsers('bob')).resolves.toBe(0);
  });

  test('a recorded transaction error is replayed without rerunning the transaction', async () => {
    const workflowID = randomUUID();
    await plantCompletion(workflowID, 'error', new Error('recorded failure'));

    await expect(DBOS.withNextWorkflowID(workflowID, () => insertUserWorkflow('bob'))).rejects.toThrow(
      'recorded failure',
    );
    expect(txState.calls).toBe(0);
    await expect(countUsers('bob')).resolves.toBe(0);
  });

  test('a thrown error rolls back the transaction', async () => {
    await expect(failingWorkflow('carol')).rejects.toThrow('failed after inserting carol');
    expect(txState.calls).toBe(1);
    await expect(countUsers('carol')).resolves.toBe(0);
  });
});
