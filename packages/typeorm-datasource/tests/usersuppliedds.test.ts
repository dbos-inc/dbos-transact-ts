import 'reflect-metadata';

import { User } from './demoentities/User';
import { Photo } from './demoentities/Photo';
import { DataSource } from 'typeorm';

import { TypeOrmDataSource } from '..';

import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client } from 'pg';
import { ensurePGDatabase } from '../../../dist/src/database_utils';

const config = { user: process.env.PGUSER || 'postgres', database: 'typeorm_ds_test_datasource' };

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
          relations: ['photos'],
        });
        return users.length;
      },
      { name: 'select' },
    );

    return nusers;
  },
  { name: 'testTypeOrm' },
);

// ensurePGDatabase swallows every failure, so verify the postcondition and fail with the reason.
async function ensureTestDatabase(databaseUrl: string) {
  const notes: string[] = [];
  await ensurePGDatabase(databaseUrl, (msg: string) => notes.push(msg));
  const client = new Client({ connectionString: databaseUrl });
  try {
    await client.connect();
  } catch (e) {
    const why = notes.length ? ` [${notes.join('; ')}]` : '';
    throw new Error(`Could not provision the test database: ${(e as Error).message}${why}`);
  } finally {
    await client.end().catch(() => {});
  }
}

describe('TypeOrmDataSourceDemo', () => {
  beforeEach(async () => {
    await ensureTestDatabase(
      `postgresql://${config.user}:${process.env['PGPASSWORD'] || 'dbos'}@${process.env['PGHOST'] || 'localhost'}:${process.env['PGPORT'] || '5432'}/typeorm_demo_ds`,
    );
    await TypeOrmDataSource.initializeDBOSSchema(config);
    await AppDataSource.initialize();

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
});
