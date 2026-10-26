import { DataSource } from 'typeorm';

const databaseUrl =
  process.env.DBOS_DATABASE_URL ||
  `postgresql://${process.env.PGUSER || 'postgres'}:${process.env.PGPASSWORD || 'dbos'}@${process.env.PGHOST || 'localhost'}:${process.env.PGPORT || '5432'}/${process.env.PGDATABASE || 'dbos_typeorm'}`;

const AppDataSource = new DataSource({
  type: 'postgres',
  url: databaseUrl,
  entities: ['dist/entities/*.js'],
  migrations: ['dist/migrations/*.js'],
});

export default AppDataSource;
