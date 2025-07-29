import { defineConfig } from 'drizzle-kit';

const databaseUrl =
  process.env.DBOS_DATABASE_URL ||
  `postgresql://${process.env.PGUSER || 'postgres'}:${process.env.PGPASSWORD || 'dbos'}@${process.env.PGHOST || 'localhost'}:${process.env.PGPORT || '5432'}/${process.env.PGDATABASE || 'dbos_drizzle'}`;

export default defineConfig({
  schema: './src/schema.ts',
  out: './drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    url: databaseUrl,
  },
});
