import { parseConfigFile } from '@dbos-inc/dbos-sdk';
import { TlsOptions } from 'tls';
import { DataSource } from 'typeorm';

const [dbosConfig] = parseConfigFile();

const AppDataSource = new DataSource({
  type: 'postgres',
  url: dbosConfig.poolConfig.connectionString,
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
