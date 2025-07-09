import { confirm } from '@inquirer/prompts';
import { PostgresSystemDatabase } from '../system_database';
import { getDatabaseInfo, readConfigFile } from './config';

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
  const configFile = await readConfigFile(appDir);
  const { databaseUrl, sysDbName } = getDatabaseInfo(configFile);
  if (!sysDbName) {
    console.error('System database name is not defined in the configuration.');
    return 1;
  }

  console.log(`Resetting ${sysDbName} if it exists`);
  await PostgresSystemDatabase.dropSystemDB(databaseUrl, sysDbName);
  return 0;
}
