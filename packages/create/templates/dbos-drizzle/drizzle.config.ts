import { defineConfig } from 'drizzle-kit';

const { parseConfigFile } = require('@dbos-inc/dbos-sdk');

const [dbosConfig] = parseConfigFile();

export default defineConfig({
  schema: './src/schema.ts',
  out: './drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    url: dbosConfig.poolConfig.connectionString,
  },
});
