import { readFileSync, writeFileSync } from "fs";
import YAML from "yaml";

export const dbosConfigBackupPath = "dbos-config.yaml.bak";

// A stripped-down interface containing only the fields the cloud console needs to manipulate.
export interface ConfigFile {
  database: {
    hostname: string;
    port: number;
    username: string;
    password?: string;
  };
}

export function loadConfigFile(configFilePath: string): ConfigFile {
  try {
    const configFileContent = readFileSync(configFilePath, "utf8");
    const configFile = YAML.parse(configFileContent) as ConfigFile;
    return configFile;
  } catch (e) {
    if (e instanceof Error) {
      throw new Error(`Failed to load config from ${configFilePath}: ${e.message}`);
    } else {
      throw e;
    }
  }
}

export function writeConfigFile(configFile: ConfigFile, configFilePath: string) {
  try {
    const configFileContent = YAML.stringify(configFile);
    writeFileSync(configFilePath, configFileContent);
  } catch (e) {
    if (e instanceof Error) {
      throw new Error(`Failed to write config to ${configFilePath}: ${e.message}`);
    } else {
      throw e;
    }
  }
}
