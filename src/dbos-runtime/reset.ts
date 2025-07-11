import { confirm } from '@inquirer/prompts';
import { getDatabaseInfo, readConfigFile } from './config';
import { Client } from 'pg';

export async function reset(cnf: boolean, appDir?: string) {
  if (cnf) {
    const userConfirmed = await confirm({
      message:
        'This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?',
      default: false, // Default value for confirmation
    });

    if (!userConfirmed) {
      console.log('Operation cancelled.');
      process.exit(0); // Exit the process if the user cancels
    }
  }
  const configFile = readConfigFile(appDir);
  const { databaseUrl, sysDbName } = getDatabaseInfo(configFile);
  if (!sysDbName) {
    console.error('System database name is not defined in the configuration.');
    return 1;
  }

  console.log(`Resetting ${sysDbName} if it exists`);

  const url = new URL(databaseUrl);
  url.pathname = `/postgres`;
  const client = new Client({
    connectionString: url.toString(),
  });
  try {
    await client.connect();
    await client.query(`DROP DATABASE IF EXISTS ${sysDbName};`);
  } finally {
    await client.end();
  }

  return 0;
}
