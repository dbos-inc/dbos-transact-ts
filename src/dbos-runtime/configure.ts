import inquirer from "inquirer";
import { readFileSync } from "../utils";
import { ConfigFile, dbosConfigFilePath, writeConfigFile } from "./config";
import YAML from "yaml";

export async function configure(host: string | undefined, port: number | undefined, username: string | undefined) {
  const configFileContent = readFileSync(dbosConfigFilePath) as string;
  const config = YAML.parse(configFileContent) as ConfigFile;

  if (!host) {
    const output = await inquirer.prompt([
      {
        type: 'input',
        name: 'host',
        message: 'What is the hostname of your Postgres server?',
        // Providing a default value
        default: 'localhost',
      },
    ]) as { host: string };
    host = output.host;
  }

  if (!port) {
    const output = await inquirer.prompt([
      {
        type: 'input',
        name: 'port',
        message: 'What is the port of your Postgres server?',
        // Providing a default value
        default: 5432,
      },
    ]) as { port: number };
    port = Number(output.port);
  }

  if (!username) {
    const output = await inquirer.prompt([
      {
        type: 'input',
        name: 'username',
        message: 'What is your Postgres username?',
        // Providing a default value
        default: 'postgres',
      },
    ]) as { username: string };
    username = output.username;
  }

  config.database.hostname = host;
  config.database.port = port;
  config.database.username = username;

  writeConfigFile(config, dbosConfigFilePath)
}
