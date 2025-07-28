const databaseUrl =
  process.env.DBOS_DATABASE_URL ||
  `postgresql://${process.env.PGUSER || 'postgres'}:${process.env.PGPASSWORD || 'dbos'}@${process.env.PGHOST || 'localhost'}:${process.env.PGPORT || '5432'}/${process.env.PGDATABASE || 'dbos_knex'}`;

const config = {
  client: 'pg',
  connection: databaseUrl,
  migrations: {
    directory: './migrations',
  },
};

module.exports = config;
