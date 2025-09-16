import 'reflect-metadata';

import { User } from './demoentities/User';
import { Photo } from './demoentities/Photo';
import { DataSource } from 'typeorm';

import { TypeOrmDataSource } from '..';

import { DBOS } from '@dbos-inc/dbos-sdk';
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

describe('TypeOrmDataSourceDemo', () => {
  beforeEach(async () => {
    await ensurePGDatabase({
      dbToEnsure: 'typeorm_demo_ds',
      adminUrl: `postgresql://${config.user}:${process.env['PGPASSWORD'] || 'dbos'}@${process.env['PGHOST'] || 'localhost'}:${process.env['PGPORT'] || '5432'}/postgres`,
    });
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
