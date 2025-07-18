import { defineConfig } from 'drizzle-kit';

const { readConfigFile, getDatabaseUrl } = require('@dbos-inc/dbos-sdk');

const dbosConfig = readConfigFile();
const databaseUrl = getDatabaseUrl(dbosConfig);

export default defineConfig({
  schema: './src/schema.ts',
  out: './drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    url: databaseUrl,
  },
});
