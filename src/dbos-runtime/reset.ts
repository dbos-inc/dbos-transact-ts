import { GlobalLogger } from '../telemetry/logs';
import { confirm } from '@inquirer/prompts';
import { PostgresSystemDatabase } from '../system_database';
import { ConfigFile, getSystemDatabaseUrl } from './config';

export async function reset(config: ConfigFile, logger: GlobalLogger, cnf: boolean) {
  if (cnf) {
    const userConfirmed = await confirm({
      message:
        'This command drops your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?',
      default: false, // Default value for confirmation
    });

    if (!userConfirmed) {
      console.log('Operation cancelled.');
      process.exit(0); // Exit the process if the user cancels
    }
  }

  const sysDbUrl = getSystemDatabaseUrl(config);
  const url = new URL(sysDbUrl);
  const sysDbName = url.pathname.slice(1);
  logger.info(`Dropping '${sysDbName}' if it exists.`);
  await PostgresSystemDatabase.dropSystemDB(sysDbUrl);
  logger.info(`Dropped '${sysDbName}'.  To use DBOS in the future, you will need to create a new system database.`);
  return 0;
}
