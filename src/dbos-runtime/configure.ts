import { input } from '@inquirer/prompts';
import YAML from 'yaml';
import { readFileSync } from '../utils';
import { dbosConfigFilePath, writeConfigFile } from './config';

export async function configure(host: string | undefined, port: number | undefined, username: string | undefined) {
  const configFileContent = readFileSync(dbosConfigFilePath);
  const config = YAML.parseDocument(configFileContent);

  if (!host) {
    host = await input({
      message: 'What is the hostname of your Postgres server?',
      // Providing a default value
      default: 'localhost',
    });
  }

  if (!port) {
    const output = await input({
      message: 'What is the port of your Postgres server?',
      // Providing a default value
      default: '5432',
    });
    port = Number(output);
  }

  if (!username) {
    username = await input({
      message: 'What is your Postgres username?',
      // Providing a default value
      default: 'postgres',
    });
  }

  config.setIn(['database', 'hostname'], host);
  config.setIn(['database', 'port'], port);
  config.setIn(['database', 'username'], username);

  writeConfigFile(config, dbosConfigFilePath);
}
