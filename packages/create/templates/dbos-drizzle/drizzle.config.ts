import { defineConfig } from 'drizzle-kit';

const { parseConfigFile } = require('@dbos-inc/dbos-sdk');

const [dbosConfig] = parseConfigFile();

export default defineConfig({
  schema: './src/schema.ts',
  out: './drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    host: dbosConfig.poolConfig.host,
    port: dbosConfig.poolConfig.port,
    user: dbosConfig.poolConfig.user,
    password: dbosConfig.poolConfig.password,
    database: dbosConfig.poolConfig.database,
    ssl: dbosConfig.poolConfig.ssl,
  },
});
