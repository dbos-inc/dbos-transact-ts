import { Client } from 'pg';
import { DBOSConfig } from '@dbos-inc/dbos-sdk';

export async function setUpDBOSTestDb(config: DBOSConfig) {
  const pgSystemClient = new Client({
    user: config.poolConfig?.user,
    port: config.poolConfig?.port,
    host: config.poolConfig?.host,
    password: config.poolConfig?.password,
    database: 'postgres',
  });

  console.log('Setting up test database with config:', config.poolConfig);

  try {
    await pgSystemClient.connect();
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${config.poolConfig?.database};`);
    await pgSystemClient.query(`CREATE DATABASE ${config.poolConfig?.database};`);
    await pgSystemClient.query(`DROP DATABASE IF EXISTS ${config.system_database};`);
    await pgSystemClient.end();
  } catch (e) {
    if (e instanceof AggregateError) {
      console.error(`Test database setup failed: AggregateError containing ${e.errors.length} errors:`);
      e.errors.forEach((err, index) => {
        console.error(`  Error ${index + 1}:`, err);
      });
    } else {
      console.error(`Test database setup failed:`, e);
    }
    throw e;
  }
}
