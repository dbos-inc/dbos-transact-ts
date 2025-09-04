import { GlobalLogger } from '../telemetry/logs';
import { confirm } from '@inquirer/prompts';
import { ConfigFile, getSystemDatabaseUrl } from './config';
import { dropPGDatabase } from '../database_utils';

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

  // The basic procedure for a DROP is that you need credentials to connect to ANOTHER database,
  //  with a user that has permission to drop the one you intend to drop.
  //  (We may want more options here.)
  const res = await dropPGDatabase({
    urlToDrop: sysDbUrl,
    logger: (msg: string) => logger.info(msg),
  });

  if (res.status === 'dropped') {
    logger.info(`Dropped '${sysDbName}'.  To use DBOS in the future, you will need to create a new system database.`);
  } else if (res.status === 'did_not_exist') {
    logger.info(
      `Database '${sysDbName} was already dropped'.  To use DBOS in the future, you will need to create a new system database.`,
    );
  } else if (res.status === 'failed') {
    logger.info(
      `DROP operation for '${sysDbName} could not be attempted: \n ${res.notes.join('\n')} ${res.hint ?? ''}.`,
    );
  }
  return 0;
}
