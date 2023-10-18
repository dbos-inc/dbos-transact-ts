import { OperonInitializationError } from "../error";
import { readFileSync } from "../utils";
import { OperonConfig } from "../operon";
import { PoolConfig } from "pg";
import { execSync } from "child_process";
import YAML from "yaml";
import { OperonRuntimeConfig } from "./runtime";
import { UserDatabaseName } from "../user_database";
import { OperonCLIStartOptions } from "./cli";
import { TelemetryConfig } from "../telemetry";

export const operonConfigFilePath = "operon-config.yaml";

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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    user_dbclient?: UserDatabaseName;
  };
  telemetry?: TelemetryConfig;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  application: any;
  runtimeConfig?: OperonRuntimeConfig;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dbClientMetadata?: any;
}

/*
 * Parse `operonConfigFilePath` and return OperonConfig and OperonRuntimeConfig
 * Considers OperonCLIStartOptions if provided, which takes precedence over config file
 * */
export function parseConfigFile(cliOptions?: OperonCLIStartOptions): [OperonConfig, OperonRuntimeConfig] {
  /****************************/
  /* Parse configuration file */
  /****************************/
  const configFilePath = cliOptions?.configfile ?? operonConfigFilePath;
  let configFile: ConfigFile | undefined;
  try {
    const configFileContent = readFileSync(configFilePath);
    const interpolatedConfig = execSync("envsubst", {
      encoding: "utf-8",
      input: configFileContent,
      env: process.env, // Jest modifies process.env, so we need to pass it explicitly for testing
    });
    configFile = YAML.parse(interpolatedConfig) as ConfigFile;
  } catch (e) {
    if (e instanceof Error) {
      throw new OperonInitializationError(`Failed to load config from ${configFilePath}: ${e.message}`);
    }
  }

  if (!configFile) {
    throw new OperonInitializationError(`Operon configuration file ${configFilePath} is empty`);
  }

  /*******************************/
  /* Handle user database config */
  /*******************************/
  if (!configFile.database) {
    throw new OperonInitializationError(`Operon configuration ${configFilePath} does not contain database config`);
  }

  const poolConfig: PoolConfig = {
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    connectionTimeoutMillis: configFile.database.connectionTimeoutMillis || 3000,
    database: configFile.database.user_database,
  };

  if (!poolConfig.password) {
    throw new OperonInitializationError(`Operon configuration ${configFilePath} does not contain database password`);
  }

  if (configFile.database.ssl_ca) {
    poolConfig.ssl = { ca: [readFileSync(configFile.database.ssl_ca)], rejectUnauthorized: true };
  }

  /***************************/
  /* Handle telemetry config */
  /***************************/

  // Consider CLI --loglevel flag. A bit verbose because everything is optional.
  if (cliOptions?.loglevel) {
    if (!configFile.telemetry) {
      configFile.telemetry = { logs: { logLevel: cliOptions.loglevel } };
    } else if (!configFile.telemetry.logs) {
      configFile.telemetry.logs = { logLevel: cliOptions.loglevel };
    } else {
      configFile.telemetry.logs.logLevel = cliOptions.loglevel;
    }
  }

  /************************************/
  /* Build final Operon Configuration */
  /************************************/
  const operonConfig: OperonConfig = {
    poolConfig: poolConfig,
    userDbclient: configFile.database.user_dbclient || UserDatabaseName.KNEX,
    telemetry: configFile.telemetry || undefined,
    system_database: configFile.database.system_database ?? "operon_systemdb",
    observability_database: configFile.database.observability_database || undefined,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    application: configFile.application || undefined,
    dbClientMetadata: {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      entities: configFile.dbClientMetadata?.entities,
    },
  };

  /*************************************/
  /* Build final runtime Configuration */
  /*************************************/
  const runtimeConfig: OperonRuntimeConfig = {
    entrypoint: cliOptions?.entrypoint || configFile.runtimeConfig?.entrypoint || "dist/operations.js",
    port: cliOptions?.port || configFile.runtimeConfig?.port || 3000,
  };

  return [operonConfig, runtimeConfig];
}
