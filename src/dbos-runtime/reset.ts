import { GlobalLogger } from '../telemetry/logs';
import { confirm } from '@inquirer/prompts';
import { PostgresSystemDatabase } from '../system_database';
import { ConfigFile, getSystemDatabaseUrl } from './config';

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

  const sysDbUrl = getSystemDatabaseUrl(config);
  const url = new URL(sysDbUrl);
  const sysDbName = url.pathname.slice(1);
  logger.info(`Resetting ${sysDbName} if it exists`);
  await PostgresSystemDatabase.dropSystemDB(sysDbUrl);
  return 0;
}
