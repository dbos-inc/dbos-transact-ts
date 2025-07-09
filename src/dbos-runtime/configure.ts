import { input, confirm } from '@inquirer/prompts';
import { readConfigFile, writeConfigFile } from './config';

export async function configure(options: { host?: string; port?: number; username?: string; appDir?: string }) {
  const config = await readConfigFile(options.appDir);
  if (config.database_url) {
    console.log(`Database is already configured as ${config.database_url}`);
    const proceed = await confirm({
      message: 'Do you want to re-configure the database connection?',
      default: false,
    });
    if (!proceed) {
      console.log('Aborting configuration.');
      return;
    }
  }

  const host =
    options.host ??
    (await input({
      message: 'What is the hostname of your Postgres server?',
      default: 'localhost',
    }));
  const port =
    options.port ??
    (await input({
      message: 'What is the port of your Postgres server?',
      default: '5432',
    }));
  const username =
    options.username ??
    (await input({
      message: 'What is your Postgres username?',
      default: 'postgres',
    }));

  config.database_url = `postgresql://${username}@${host}:${port}/${config.name}`;

  await writeConfigFile(config, options.appDir);
}
