const { parseConfigFile } = require('@dbos-inc/dbos-sdk');
const fs = require('node:fs');
const path = require('node:path');

// Load the configuration file
const [dbosConfig, ] = parseConfigFile();

// Write out the .env file
const databaseURL = `postgresql://${dbosConfig.poolConfig.user}:${dbosConfig.poolConfig.password}@${dbosConfig.poolConfig.host}:${dbosConfig.poolConfig.port}/${dbosConfig.poolConfig.database}`;

try {
  fs.writeFileSync(path.join(process.cwd(), 'prisma', '.env'), `DATABASE_URL="${databaseURL}"`);
  console.log("Wrote database URL to the prisma/.env file.");
} catch (error) {
  console.error("Error writing prisma/.env file:", error.message);
}