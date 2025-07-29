const fs = require('node:fs');
const path = require('node:path');

// Load the configuration file
const databaseUrl =
  process.env['DBOS_DATABASE_URL'] ||
  process.env['DATABASE_URL'] ||
  `postgresql://${process.env.PGUSER || 'postgres'}:${process.env.PGPASSWORD || 'dbos'}@${process.env.PGHOST || 'localhost'}:${process.env.PGPORT || '5432'}/${process.env.PGDATABASE || 'dbos_prisma'}`;

try {
  fs.writeFileSync(path.join(process.cwd(), 'prisma', '.env'), `DATABASE_URL="${databaseUrl}"`);
  console.log('Wrote database URL to the prisma/.env file.');
} catch (error) {
  console.error('Error writing prisma/.env file:', error.message);
}
