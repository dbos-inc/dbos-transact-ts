import { GlobalLogger } from '../telemetry/logs';
import { confirm } from '@inquirer/prompts';
import { DBOSConfigInternal } from '../dbos-executor';
import { PostgresSystemDatabase } from '../system_database';
import { ConfigFile, getDatabaseUrl, getSystemDatabaseName } from './config';

export async function reset(config: ConfigFile, logger: GlobalLogger, cnf: boolean) {
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

  const databaseUrl = getDatabaseUrl(config.database_url, config.name);
  const sysDbName = getSystemDatabaseName(databaseUrl, config.database?.sys_db_name);
  logger.info(`Resetting ${sysDbName} if it exists`);
  await PostgresSystemDatabase.dropSystemDB(databaseUrl, sysDbName);
  return 0;
}
