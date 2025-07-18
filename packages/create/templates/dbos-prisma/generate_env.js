const { readConfigFile, getDatabaseUrl } = require('@dbos-inc/dbos-sdk');
const fs = require('node:fs');
const path = require('node:path');

// Load the configuration file
const dbosConfig = readConfigFile();
const databaseUrl = getDatabaseUrl(dbosConfig);

try {
  fs.writeFileSync(path.join(process.cwd(), 'prisma', '.env'), `DATABASE_URL="${databaseUrl}"`);
  console.log('Wrote database URL to the prisma/.env file.');
} catch (error) {
  console.error('Error writing prisma/.env file:', error.message);
}
