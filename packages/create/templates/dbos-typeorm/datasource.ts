import { readConfigFile, getDatabaseUrl } from '@dbos-inc/dbos-sdk';
import { DataSource } from 'typeorm';

const dbosConfig = readConfigFile();
const databaseUrl = getDatabaseUrl(dbosConfig);

const AppDataSource = new DataSource({
  type: 'postgres',
  url: databaseUrl,
  entities: ['dist/entities/*.js'],
  migrations: ['dist/migrations/*.js'],
});

AppDataSource.initialize()
  .then(() => {
    console.log('Data Source has been initialized!');
  })
  .catch((err) => {
    console.error('Error during Data Source initialization', err);
  });

export default AppDataSource;
