import { DBOSInitializationError } from "../error";
import { DBOSJSON, findPackageRoot, readFileSync } from "../utils";
import { DBOSConfig } from "../dbos-executor";
import { PoolConfig } from "pg";
import YAML from "yaml";
import { DBOSRuntimeConfig, defaultEntryPoint } from "./runtime";
import { UserDatabaseName } from "../user_database";
import { TelemetryConfig } from "../telemetry";
import { writeFileSync } from "fs";
import Ajv, { ValidateFunction } from 'ajv';
import path from "path";
import validator from "validator";

export const dbosConfigFilePath = "dbos-config.yaml";
const dbosConfigSchemaPath = path.join(findPackageRoot(__dirname), 'dbos-config.schema.json');
const dbosConfigSchema = DBOSJSON.parse(readFileSync(dbosConfigSchemaPath)) as object;
const ajv = new Ajv({allErrors: true, verbose: true});

export interface ConfigFile {
  name?: string;
  language?: string;
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
  application: object;
  env: Record<string, string>;
  runtimeConfig?: DBOSRuntimeConfig;
}

/*
 * Substitute environment variables using a regex for matching.
 * Will find anything in curly braces.
 * TODO: Use a more robust solution.
 */
export function substituteEnvVars(content: string): string {
  const regex = /\${([^}]+)}/g; // Regex to match ${VAR_NAME} style placeholders
  return content.replace(regex, (_, g1: string) => {
    return process.env[g1] || '""'; // If the env variable is not set, return an empty string.
  });
}

/**
 * Loads config file as a ConfigFile.
 * @param {string} configFilePath - The path to the config file to be loaded.
 * @returns 
 */
export function loadConfigFile(configFilePath: string): ConfigFile {
  try {
    const configFileContent = readFileSync(configFilePath);
    const interpolatedConfig = substituteEnvVars(configFileContent);
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

/**
 * Writes a YAML.Document object to configFilePath.
 * @param {YAML.Document} configFile - The config file to be written. 
 * @param {string} configFilePath - The path to the config file to be written to.
 */
export function writeConfigFile(configFile: YAML.Document, configFilePath: string) {
  try {
    const configFileContent = configFile.toString();
    writeFileSync(configFilePath, configFileContent);
  } catch (e) {
    if (e instanceof Error) {
      throw new DBOSInitializationError(`Failed to write config to ${configFilePath}: ${e.message}`);
    } else {
      throw e;
    }
  }
}

export function constructPoolConfig(configFile: ConfigFile) {
   const poolConfig: PoolConfig = {
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    connectionTimeoutMillis: configFile.database.connectionTimeoutMillis || 3000,
    database: configFile.database.app_db_name,
  };

  if (!poolConfig.database) {
    throw new DBOSInitializationError(`DBOS configuration (${dbosConfigFilePath}) does not contain application database name`);
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

function prettyPrintAjvErrors(validate: ValidateFunction<unknown>) {
  return validate.errors!.map(error => {
    let message = `Error: ${error.message}`;
    if (error.schemaPath) message += ` (schema path: ${error.schemaPath})`;
    if (error.params && error.keyword === 'additionalProperties') {
      message += `; the additional property '${error.params.additionalProperty}' is not allowed`;
    }
    if (error.data && error.keyword === 'not') {
      message += `; the value ${DBOSJSON.stringify(error.data)} is not allowed for field ${error.instancePath}`
    }
    return message;
  }).join(', ');
}

export interface ParseOptions {
  port?: number;
  loglevel?: string;
  configfile?: string;
  appDir?: string;
  appVersion?: string | boolean;
}

/*
 * Parse `dbosConfigFilePath` and return DBOSConfig and DBOSRuntimeConfig
 * Considers DBOSCLIStartOptions if provided, which takes precedence over config file
 * */
export function parseConfigFile(cliOptions?: ParseOptions, useProxy: boolean = false): [DBOSConfig, DBOSRuntimeConfig] {
  if (cliOptions?.appDir) {
    process.chdir(cliOptions.appDir)
  }
  const configFilePath = cliOptions?.configfile ?? dbosConfigFilePath;
  const configFile: ConfigFile | undefined = loadConfigFile(configFilePath);
  if (!configFile) {
    throw new DBOSInitializationError(`DBOS configuration file ${configFilePath} is empty`);
  }

  // Database field must exist
  if (!configFile.database) {
    throw new DBOSInitializationError(`DBOS configuration (${configFilePath}) does not contain database config`);
  }

  // Check for the database password
  if (!configFile.database.password) {
    if (useProxy) {
      configFile.database.password = "PROXY-MODE"; // Assign a password if not set. We don't need password to authenticate with the local proxy.
    } else {
      const pgPassword: string | undefined = process.env.PGPASSWORD;
      if (pgPassword) {
        configFile.database.password = pgPassword;
      } else {
        throw new DBOSInitializationError(`DBOS configuration (${configFilePath}) does not contain database password`);
      }
    }
  }

  const schemaValidator = ajv.compile(dbosConfigSchema);
  if (!schemaValidator(configFile)) {
    const errorMessages = prettyPrintAjvErrors(schemaValidator);
    throw new DBOSInitializationError(`${configFilePath} failed schema validation. ${errorMessages}`);
  }

  if (configFile.language && configFile.language !== "node") {
    throw new DBOSInitializationError(`${configFilePath} specifies invalid language ${configFile.language}`)
  }

  if (!isValidDBname(configFile.database.app_db_name)) {
    throw new DBOSInitializationError(`${configFilePath} specifies invalid app_db_name ${configFile.database.app_db_name}. Must be between 3 and 31 characters long and contain only lowercase letters, underscores, and digits (cannot begin with a digit).`);
  }

  /*******************************/
  /* Handle user database config */
  /*******************************/

  const poolConfig = constructPoolConfig(configFile);

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
    application: configFile.application || undefined,
    env: configFile.env || {},
    http: configFile.http,
    appVersion: getAppVersion(cliOptions?.appVersion),
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

  const appPort = Number(cliOptions?.port) || Number(configFile.runtimeConfig?.port) || 3000;
  const runtimeConfig: DBOSRuntimeConfig = {
    entrypoints: [...entrypoints],
    port: appPort,
    admin_port: Number(configFile.runtimeConfig?.admin_port) || appPort + 1,
  };

  return [dbosConfig, runtimeConfig];
}

function getAppVersion(appVersion: string | boolean | undefined) {
  if (typeof appVersion === "string") { return appVersion; }
  if (appVersion === false) { return undefined; }
  return process.env.DBOS__APPVERSION;
}

function isValidDBname(dbName: string): boolean {
  if (dbName.length < 1 || dbName.length > 63) {
    return false;
  }
  if (dbName.match(/^\d/)) {
    // Cannot start with a digit
    return false
  }
  return validator.matches(dbName, "^[a-z0-9_]+$");
}
