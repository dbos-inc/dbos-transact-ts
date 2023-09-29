import { OperonInitializationError } from "../error";
import { readFileSync } from "../utils";
import { OperonConfig } from "../operon";
import { PoolConfig } from "pg";
import { execSync } from "child_process";
import YAML from "yaml";
import { OperonRuntimeConfig } from "./runtime";

const operonConfigFilePath = "operon-config.yaml";

export interface ConfigFile {
  database: {
    hostname: string;
    port: number;
    username: string;
    password?: string;
    connectionTimeoutMillis: number;
    user_database: string;
    system_database: string;
    ssl_ca?: string;
    observability_database: string;
  };
  telemetryExporters?: string[];
  application: any;
  localRuntimeConfig?: OperonRuntimeConfig;
}

export function parseConfigFile(): [OperonConfig, OperonRuntimeConfig | undefined] {
  let configFile: ConfigFile;
  try {
    const configFileContent = readFileSync(operonConfigFilePath);
    const interpolatedConfig = execSync("envsubst", {
      encoding: "utf-8",
      input: configFileContent,
      env: process.env, // Jest modifies process.env, so we need to pass it explicitly for testing
    });
    configFile = YAML.parse(interpolatedConfig);
  } catch (e) {
    throw new OperonInitializationError(`Failed to load config from ${operonConfigFilePath}: ${e}`);
  }

  // Handle "Global" pool configFile
  if (!configFile.database) {
    throw new OperonInitializationError(`Operon configFileuration ${configFile} does not contain database configFile
`);
  }

  const poolConfig: PoolConfig = {
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    connectionTimeoutMillis: configFile.database.connectionTimeoutMillis,
    database: configFile.database.user_database,
  };

  if (configFile.database.ssl_ca) {
    poolConfig.ssl = { ca: [readFileSync(configFile.database.ssl_ca)], rejectUnauthorized: true };
  }

  const operonConfig: OperonConfig = {
    poolConfig: poolConfig,
    telemetryExporters: configFile.telemetryExporters || [],
    system_database: configFile.database.system_database ?? "operon_systemdb",
    observability_database: configFile.database.observability_database || undefined,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    application: configFile.application || undefined,
  };

  return [operonConfig, configFile.localRuntimeConfig];
}
