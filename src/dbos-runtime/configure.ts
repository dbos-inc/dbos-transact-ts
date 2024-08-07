import { input } from "@inquirer/prompts";
import { readFileSync } from "../utils";
import { ConfigFile, dbosConfigFilePath, writeConfigFile } from "./config";
import YAML from "yaml";

export async function configure(host: string | undefined, port: number | undefined, username: string | undefined) {
  const configFileContent = readFileSync(dbosConfigFilePath);
  const config = YAML.parse(configFileContent) as ConfigFile;

  if (!host) {
    host = await input(
      {
        message: 'What is the hostname of your Postgres server?',
        // Providing a default value
        default: 'localhost',
      });
  }

  if (!port) {
    const output = await input(
      {
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

  config.database.hostname = host;
  config.database.port = port;
  config.database.username = username;

  writeConfigFile(config, dbosConfigFilePath)
}
