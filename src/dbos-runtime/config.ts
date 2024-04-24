import { DBOSInitializationError } from "../error";
import { readFileSync } from "../utils";
import { DBOSConfig } from "../dbos-executor";
import { PoolConfig } from "pg";
import YAML from "yaml";
import { DBOSRuntimeConfig, defaultEntryPoint } from "./runtime";
import { UserDatabaseName } from "../user_database";
import { DBOSCLIStartOptions } from "./cli";
import { TelemetryConfig } from "../telemetry";
import { setApplicationVersion } from "./applicationVersion";
import { writeFileSync } from "fs";

export const dbosConfigFilePath = "dbos-config.yaml";

export interface ConfigFile {
  version: string;
  database: {
    hostname: string;
    port: number;
    username: string;
    password?: string;
    connectionTimeoutMillis?: number;
    app_db_name: string;
    sys_db_name?: string;
    ssl?: boolean;
    ssl_ca?: string;
    app_db_client?: UserDatabaseName;
    migrate?: string[];
    rollback?: string[];
  };
  http?: {
    cors_middleware?: boolean;
    credentials?: boolean;
    allowed_origins?: string[];
  };
  telemetry?: TelemetryConfig;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  application: any;
  env: Record<string, string>;
  runtimeConfig?: DBOSRuntimeConfig;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dbClientMetadata?: any;
}

/*
 * Substitute environment variables using a regex for matching.
 * Will find anything in curly braces.
 * TODO: Use a more robust solution.
 */
export function substituteEnvVars(content: string): string {
  const regex = /\${([^}]+)}/g; // Regex to match ${VAR_NAME} style placeholders
  return content.replace(regex, (_, g1: string) => {
    return process.env[g1] || ""; // If the env variable is not set, return an empty string.
  });
}

export function loadConfigFile(configFilePath: string): ConfigFile {
  try {
    const configFileContent = readFileSync(configFilePath);
    const interpolatedConfig = substituteEnvVars(configFileContent as string);
    const configFile = YAML.parse(interpolatedConfig) as ConfigFile;
    return configFile;
  } catch (e) {
    if (e instanceof Error) {
      throw new DBOSInitializationError(`Failed to load config from ${configFilePath}: ${e.message}`);
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
      throw new DBOSInitializationError(`Failed to write config to ${configFilePath}: ${e.message}`);
    } else {
      throw e;
    }
  }
}

export function constructPoolConfig(configFile: ConfigFile, useProxy: boolean = false) {
  if (!configFile.database) {
    throw new DBOSInitializationError(`DBOS configuration (dbos-config.yaml) does not contain database config`);
  }

  const poolConfig: PoolConfig = {
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    connectionTimeoutMillis: configFile.database.connectionTimeoutMillis || 3000,
    database: configFile.database.app_db_name,
  };

  if (!poolConfig.database) {
    throw new DBOSInitializationError(`DBOS configuration (dbos-config.yaml) does not contain application database name`);
  }

  if (!poolConfig.password) {
    if (useProxy) {
      poolConfig.password = "PROXY-MODE"; // Assign a password if not set. We don't need password to authenticate with the local proxy.
    } else {
      const pgPassword: string | undefined = process.env.PGPASSWORD;
      if (pgPassword) {
        poolConfig.password = pgPassword;
      } else {
        throw new DBOSInitializationError(`DBOS configuration (dbos-config.yaml) does not contain database password`);
      }
    }
  }

  // Details on Postgres SSL/TLS modes: https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION
  if (configFile.database.ssl === false) {
    // If SSL is set to false, do not use TLS
    poolConfig.ssl = false
  } else if (configFile.database.ssl_ca) {
    // If an SSL certificate is provided, connect to Postgres using TLS and verify the server certificate. (equivalent to verify-full)
    poolConfig.ssl = { ca: [readFileSync(configFile.database.ssl_ca)], rejectUnauthorized: true };
  } else if (configFile.database.ssl === undefined && (poolConfig.host === "localhost" || poolConfig.host === "127.0.0.1")) {
    // For local development only, do not use TLS unless it is specifically asked for (to support Dockerized Postgres, which does not support SSL connections)
    poolConfig.ssl = false;
  } else {
    // Otherwise, connect to Postgres using TLS but do not verify the server certificate. (equivalent to require)
    poolConfig.ssl = { rejectUnauthorized: false };
  }
  return poolConfig;
}

/*
 * Parse `dbosConfigFilePath` and return DBOSConfig and DBOSRuntimeConfig
 * Considers DBOSCLIStartOptions if provided, which takes precedence over config file
 * */
export function parseConfigFile(cliOptions?: DBOSCLIStartOptions, useProxy: boolean = false): [DBOSConfig, DBOSRuntimeConfig] {
  const configFilePath = cliOptions?.configfile ?? dbosConfigFilePath;
  const configFile: ConfigFile | undefined = loadConfigFile(configFilePath);
  if (!configFile) {
    throw new DBOSInitializationError(`DBOS configuration file ${configFilePath} is empty`);
  }

  setApplicationVersion(configFile.version);

  /*******************************/
  /* Handle user database config */
  /*******************************/

  const poolConfig = constructPoolConfig(configFile, useProxy);

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
  /* Build final DBOS configuration */
  /************************************/
  const dbosConfig: DBOSConfig = {
    poolConfig: poolConfig,
    userDbclient: configFile.database.app_db_client || UserDatabaseName.KNEX,
    telemetry: configFile.telemetry || undefined,
    system_database: configFile.database.sys_db_name ?? `${poolConfig.database}_dbos_sys`,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    application: configFile.application || undefined,
    env: configFile.env || {},
    http: configFile.http,
    dbClientMetadata: {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      entities: configFile.dbClientMetadata?.entities,
    },
  };

  /*************************************/
  /* Build final runtime Configuration */
  /*************************************/
  const entrypoints = new Set<string>();
  if (configFile.runtimeConfig?.entrypoints) {
    configFile.runtimeConfig.entrypoints.forEach((entry) => entrypoints.add(entry));
  } else {
    entrypoints.add(defaultEntryPoint);
  }
  const runtimeConfig: DBOSRuntimeConfig = {
    entrypoints: [...entrypoints],
    port: Number(cliOptions?.port) || Number(configFile.runtimeConfig?.port) || 3000,
  };

  return [dbosConfig, runtimeConfig];
}
