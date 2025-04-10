import { DBOSInitializationError } from '../error';
import { DBOSJSON, globalParams, readFileSync } from '../utils';
import { DBOSConfig, DBOSConfigInternal } from '../dbos-executor';
import { PoolConfig } from 'pg';
import YAML from 'yaml';
import { DBOSRuntimeConfig, defaultEntryPoint } from './runtime';
import { UserDatabaseName } from '../user_database';
import { TelemetryConfig } from '../telemetry';
import { writeFileSync } from 'fs';
import Ajv, { ValidateFunction } from 'ajv';
import { parse } from 'pg-connection-string';
import path from 'path';
import validator from 'validator';
import fs from 'fs';
import { GlobalLogger } from '../telemetry/logs';
import dbosConfigSchema from '../../dbos-config.schema.json';

export const dbosConfigFilePath = 'dbos-config.yaml';
const ajv = new Ajv({ allErrors: true, verbose: true });

export interface DBConfig {
  hostname?: string;
  port?: number;
  username?: string;
  password?: string;
  connectionTimeoutMillis?: number;
  app_db_name?: string;
  sys_db_name?: string;
  ssl?: boolean;
  ssl_ca?: string;
  app_db_client?: UserDatabaseName;
  migrate?: string[];
  rollback?: string[];
  local_suffix?: boolean;
}

export interface ConfigFile {
  name?: string;
  language?: string;
  database: DBConfig;
  database_url?: string;
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
    if (!configFile) {
      throw new Error(`${configFilePath} is empty `);
    }
    const schemaValidator = ajv.compile(dbosConfigSchema);
    if (!schemaValidator(configFile)) {
      const errorMessages = prettyPrintAjvErrors(schemaValidator);
      throw new Error(`${configFilePath} failed schema validation. ${errorMessages}`);
    }
    if (!configFile.database) {
      configFile.database = {}; // Create an empty database object if it doesn't exist
    }
    // Handle strings in the config file and convert them to arrays
    if (
      configFile.telemetry?.OTLPExporter?.logsEndpoint &&
      typeof configFile.telemetry.OTLPExporter.logsEndpoint === 'string'
    ) {
      configFile.telemetry.OTLPExporter.logsEndpoint = [configFile.telemetry.OTLPExporter.logsEndpoint];
    }
    if (
      configFile.telemetry?.OTLPExporter?.tracesEndpoint &&
      typeof configFile.telemetry.OTLPExporter.tracesEndpoint === 'string'
    ) {
      configFile.telemetry.OTLPExporter.tracesEndpoint = [configFile.telemetry.OTLPExporter.tracesEndpoint];
    }

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

function retrieveApplicationName(configFile: ConfigFile): string {
  let appName = configFile.name;
  if (appName !== undefined) {
    return appName;
  }
  const packageJson = JSON.parse(readFileSync(path.join(process.cwd(), 'package.json')).toString()) as {
    name: string;
  };
  appName = packageJson.name;
  if (appName === undefined) {
    throw new DBOSInitializationError(
      'Error: cannot find a valid package.json file. Please run this command in an application root directory.',
    );
  }
  return appName;
}

/**
 * Build a PoolConfig object from the config file and CLI options.
 *
 * If configFile.database_url is provided, this function expects configFile.database to be built from the database_url.
 * A connection string will be built from configFile.database, after it is fully resolved.
 * If a database_url with query parameters are provided, they will be used.
 * If no database_url is provided, or if the provided database_url does not contain query parameters, the function will build a connection string with the following query parameters:
 *  - connect_timeout
 *  - connection_limit
 *  - sslmode
 *
 * Default configuration:
 * - Hostname: localhost
 * - Port: 5432
 * - Username: postgres
 * - Password: $PGPASSWORD
 * - Database name: transformed application name. The name is either the one provided in the config file or the one found in package.json.
 *
 * @param configFile - The configuration to be used.
 * @param cliOptions - Optional CLI options.
 * @returns PoolConfig - The constructed PoolConfig object.
 */
export function constructPoolConfig(configFile: ConfigFile, cliOptions?: ParseOptions): PoolConfig {
  // Load database connection parameters. If they're not in dbos-config.yaml, load from .dbos/db_connection. Else, use defaults.
  if (!cliOptions?.silent) {
    const logger = new GlobalLogger();
    if (process.env.DBOS_DBHOST) {
      logger.info('Loading database connection parameters from debug environment variables');
    } else if (configFile.database.hostname) {
      logger.info('Loading database connection parameters from dbos-config.yaml');
    } else {
      logger.info('Using default database connection parameters');
    }
  }
  configFile.database.hostname = process.env.DBOS_DBHOST || configFile.database.hostname || 'localhost';
  const dbos_dbport = process.env.DBOS_DBPORT ? parseInt(process.env.DBOS_DBPORT) : undefined;
  configFile.database.port = dbos_dbport || configFile.database.port || 5432;
  configFile.database.username = process.env.DBOS_DBUSER || configFile.database.username || 'postgres';
  configFile.database.password =
    process.env.DBOS_DBPASSWORD || configFile.database.password || process.env.PGPASSWORD || 'dbos';

  let databaseName: string | undefined = configFile.database.app_db_name;
  const appName = retrieveApplicationName(configFile);
  if (globalParams.appName === '') {
    globalParams.appName = appName;
  }
  if (databaseName === undefined) {
    databaseName = appName.toLowerCase().replaceAll('-', '_').replaceAll(' ', '_');
    if (databaseName.match(/^\d/)) {
      databaseName = '_' + databaseName; // Append an underscore if the name starts with a digit
    }
  }
  const poolConfig: PoolConfig = {
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    connectionTimeoutMillis: configFile.database.connectionTimeoutMillis || 3000,
    database: databaseName,
    max: cliOptions?.userDbPoolSize || 20,
  };

  if (!poolConfig.database) {
    throw new DBOSInitializationError(
      `DBOS configuration (${dbosConfigFilePath}) does not contain application database name`,
    );
  }

  poolConfig.ssl = parseSSLConfig(configFile.database);

  poolConfig.connectionString = `postgresql://${String(poolConfig.user)}:${String(poolConfig.password)}@${String(poolConfig.host)}:${String(poolConfig.port)}/${String(poolConfig.database)}`;
  let hasQueryParams = false;

  // If a database_url is provided
  if (configFile.database_url) {
    const url = new URL(configFile.database_url);
    hasQueryParams = url.search.length > 1;

    if (hasQueryParams) {
      // Use provided query parameters from database_url
      poolConfig.connectionString += url.search; // url.search includes '?'
    }
  }

  // If no query parameters were provided or available, build them manually
  if (!hasQueryParams) {
    const queryParams: string[] = [];

    if (poolConfig.connectionTimeoutMillis) {
      queryParams.push(`connect_timeout=${poolConfig.connectionTimeoutMillis / 1000}`);
    }
    if (poolConfig.max) {
      queryParams.push(`connection_limit=${poolConfig.max}`);
    }
    if (poolConfig.ssl === false || poolConfig.ssl === undefined) {
      queryParams.push(`sslmode=disable`);
    } else if (poolConfig.ssl && 'ca' in poolConfig.ssl) {
      queryParams.push(`sslmode=verify-full`);
    } else {
      queryParams.push(`sslmode=require`);
    }

    if (queryParams.length > 0) {
      poolConfig.connectionString += `?${queryParams.join('&')}`;
    }
  }

  return poolConfig;
}

export function parseSSLConfig(dbConfig: DBConfig) {
  // Details on Postgres SSL/TLS modes: https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION
  if (dbConfig.ssl === false) {
    // If SSL is set to false, do not use TLS
    return false;
  }
  if (dbConfig.ssl_ca) {
    // If an SSL certificate is provided, connect to Postgres using TLS and verify the server certificate. (equivalent to verify-full)
    return { ca: [readFileSync(dbConfig.ssl_ca)], rejectUnauthorized: true };
  }
  if (dbConfig.ssl === undefined && (dbConfig.hostname === 'localhost' || dbConfig.hostname === '127.0.0.1')) {
    // For local development only, do not use TLS unless it is specifically asked for (to support Dockerized Postgres, which does not support SSL connections)
    return false;
  }
  // Otherwise, connect to Postgres using TLS but do not verify the server certificate. (equivalent to require)
  return { rejectUnauthorized: false };
}

function prettyPrintAjvErrors(validate: ValidateFunction<unknown>) {
  return validate
    .errors!.map((error) => {
      let message = `Error: ${error.message}`;
      if (error.schemaPath) message += ` (schema path: ${error.schemaPath})`;
      if (error.params && error.keyword === 'additionalProperties') {
        message += `; the additional property '${error.params.additionalProperty}' is not allowed`;
      }
      if (error.data && error.keyword === 'not') {
        message += `; the value ${DBOSJSON.stringify(error.data)} is not allowed for field ${error.instancePath}`;
      }
      return message;
    })
    .join(', ');
}

export interface ParseOptions {
  port?: number;
  loglevel?: string;
  configfile?: string;
  appDir?: string;
  appVersion?: string | boolean;
  silent?: boolean;
  forceConsole?: boolean;
  userDbPoolSize?: number;
}

/*
 * Parse `dbosConfigFilePath` and return DBOSConfig and DBOSRuntimeConfig
 * Considers DBOSCLIStartOptions if provided, which takes precedence over config file
 * */
export function parseConfigFile(cliOptions?: ParseOptions): [DBOSConfigInternal, DBOSRuntimeConfig] {
  if (cliOptions?.appDir) {
    process.chdir(cliOptions.appDir);
  }
  const configFilePath = cliOptions?.configfile ?? dbosConfigFilePath;
  const configFile: ConfigFile | undefined = loadConfigFile(configFilePath);
  if (!configFile) {
    throw new DBOSInitializationError(`DBOS configuration file ${configFilePath} is empty`);
  }

  if (configFile.database.local_suffix === true && configFile.database.hostname === 'localhost') {
    throw new DBOSInitializationError(
      `Invalid configuration (${configFilePath}): local_suffix may only be true when connecting to remote databases, not to localhost`,
    );
  }

  if (configFile.language && configFile.language !== 'node') {
    throw new DBOSInitializationError(`${configFilePath} specifies invalid language ${configFile.language}`);
  }

  /*******************************/
  /* Handle user database config */
  /*******************************/

  if (configFile.database_url) {
    configFile.database = parseDbString(configFile.database_url);
  }
  const poolConfig = constructPoolConfig(configFile, cliOptions);

  if (!isValidDBname(poolConfig.database!)) {
    throw new DBOSInitializationError(
      `${configFilePath} specifies invalid app_db_name ${poolConfig.database}. Must be between 3 and 31 characters long and contain only lowercase letters, underscores, and digits (cannot begin with a digit).`,
    );
  }

  /***************************/
  /* Handle telemetry config */
  /***************************/

  // Consider CLI --loglevel and forceConsole flags
  if (cliOptions?.loglevel) {
    configFile.telemetry = {
      ...configFile.telemetry,
      logs: { ...configFile.telemetry?.logs, logLevel: cliOptions.loglevel },
    };
  }
  if (cliOptions?.forceConsole) {
    configFile.telemetry = {
      ...configFile.telemetry,
      logs: { ...configFile.telemetry?.logs, forceConsole: cliOptions.forceConsole },
    };
  }

  /************************************/
  /* Build final DBOS configuration */
  /************************************/
  globalParams.appVersion = getAppVersion(cliOptions?.appVersion);
  const dbosConfig: DBOSConfigInternal = {
    poolConfig: poolConfig,
    userDbclient: configFile.database.app_db_client || UserDatabaseName.KNEX,
    telemetry: configFile.telemetry || { logs: { logLevel: 'info' } },
    system_database: configFile.database.sys_db_name ?? `${poolConfig.database}_dbos_sys`,
    application: configFile.application || undefined,
    env: configFile.env || {},
    http: configFile.http,
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
    runAdminServer: true,
    admin_port: Number(configFile.runtimeConfig?.admin_port) || appPort + 1,
    start: configFile.runtimeConfig?.start || [],
    setup: configFile.runtimeConfig?.setup || [],
  };

  return [dbosConfig, runtimeConfig];
}

function getAppVersion(appVersion: string | boolean | undefined) {
  if (typeof appVersion === 'string') {
    return appVersion;
  }
  if (appVersion === false) {
    return '';
  }
  return process.env.DBOS__APPVERSION || '';
}

function isValidDBname(dbName: string): boolean {
  if (dbName.length < 1 || dbName.length > 63) {
    return false;
  }
  if (dbName.match(/^\d/)) {
    // Cannot start with a digit
    return false;
  }
  return validator.matches(dbName, '^[a-z0-9_]+$');
}

/*
 This function takes a DBOSConfig and ensure that 'public' fields take precedence over 'internal' fields.
 It assumes that the DBOSConfig was passed programmatically and thus does not need to consider CLI options.

 - Application Name: check there is no inconsistency between the provided name and the one in dbos-config.yaml, if any
 - Database configuration: Ignore provided poolConfig and reconstructs it from the databaseUrl field and constructPoolConfig()
*/
export function translatePublicDBOSconfig(
  config: DBOSConfig,
  isDebugging?: boolean,
): [DBOSConfigInternal, DBOSRuntimeConfig] {
  // Check there is no discrepancy between provided name and dbos-config.yaml
  let appName = config.name;
  try {
    const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
    if (!configFile.name) {
      throw new DBOSInitializationError(`Failed to load config from ${dbosConfigFilePath}: missing name field`);
    } else {
      // Opportunistically grab the name from the config file if none was provided
      if (!appName) {
        appName = configFile.name;
        // But throw if it was provided and is different from the one in config file
      } else if (appName !== configFile.name) {
        throw new DBOSInitializationError(
          `Provided app name '${config.name}' does not match the app name '${configFile.name}' in ${dbosConfigFilePath}`,
        );
      }
    }
  } catch (e) {
    // Ignore file not found errors
    if (!(e as Error).message.includes('ENOENT: no such file or directory')) {
      throw e;
    }
  }

  // Resolve database configuration
  let dburlConfig: DBConfig = {};
  if (config.databaseUrl) {
    dburlConfig = parseDbString(config.databaseUrl);
  }

  const poolConfig = constructPoolConfig(
    {
      name: appName,
      database: dburlConfig,
      database_url: config.databaseUrl,
      application: {},
      env: {},
    },
    { silent: true, userDbPoolSize: config.userDbPoolSize },
  );

  const translatedConfig: DBOSConfigInternal = {
    name: appName,
    poolConfig: poolConfig,
    userDbclient: config.userDbclient || UserDatabaseName.KNEX,
    telemetry: {
      logs: {
        logLevel: config.logLevel || 'info',
        forceConsole: isDebugging === undefined ? false : isDebugging,
      },
      OTLPExporter: {
        tracesEndpoint: config.otlpTracesEndpoints || [],
        logsEndpoint: config.otlpLogsEndpoints || [],
      },
    },
    system_database: config.sysDbName || `${poolConfig.database}_dbos_sys`,
    sysDbPoolSize: config.sysDbPoolSize || 2,
  };

  const runtimeConfig: DBOSRuntimeConfig = {
    port: 3000,
    admin_port: config.adminPort || 3001,
    runAdminServer: config.runAdminServer === undefined ? true : config.runAdminServer,
    entrypoints: [],
    start: [],
    setup: [],
  };

  return [translatedConfig, runtimeConfig];
}

export function parseDbString(dbString: string): DBConfig {
  const parsed = parse(dbString);
  const url = new URL(dbString);
  const queryParams = Object.fromEntries(url.searchParams.entries());

  // Throw if any required field is missing
  const missingFields: string[] = [];
  if (!parsed.user) missingFields.push('username');
  if (!parsed.password) missingFields.push('password');
  if (!parsed.host) missingFields.push('hostname');
  if (!parsed.port) missingFields.push('port');
  if (!parsed.database) missingFields.push('app_db_name');

  if (missingFields.length > 0) {
    throw new Error(`Invalid database URL: missing required field(s): ${missingFields.join(', ')}`);
  }

  return {
    hostname: parsed.host as string,
    port: parseInt(parsed.port!, 10),
    username: parsed.user,
    password: parsed.password,
    app_db_name: parsed.database as string,
    ssl: 'sslmode' in parsed && (parsed.sslmode === 'require' || parsed.sslmode === 'verify-full'),
    ssl_ca: queryParams['sslrootcert'] || undefined,
    connectionTimeoutMillis: queryParams['connect_timeout']
      ? parseInt(queryParams['connect_timeout'], 10) * 1000
      : undefined,
  };
}

export function overwrite_config(
  providedDBOSConfig: DBOSConfigInternal,
  providedRuntimeConfig: DBOSRuntimeConfig,
): [DBOSConfig, DBOSRuntimeConfig] {
  // Load the DBOS configuration file and force the use of:
  // 1. Use the application name from the file. This is a defensive measure to ensure the application name is whatever it was registered with in the cloud
  // 2. The database connection parameters (sub the file data to the provided config)
  // 3. OTLP traces endpoints (add the config data to the provided config)
  // 4. Force admin_port and runAdminServer

  let configFile: ConfigFile;
  try {
    configFile = loadConfigFile(dbosConfigFilePath);
  } catch (e) {
    if ((e as Error).message.includes('ENOENT: no such file or directory')) {
      return [providedDBOSConfig, providedRuntimeConfig];
    } else {
      throw e;
    }
  }

  const appName = configFile!.name || providedDBOSConfig.name;

  configFile.database_url = undefined;
  const poolConfig = constructPoolConfig(configFile!);

  if (!providedDBOSConfig.telemetry.OTLPExporter) {
    providedDBOSConfig.telemetry.OTLPExporter = {
      tracesEndpoint: [],
      logsEndpoint: [],
    };
  }
  if (configFile!.telemetry?.OTLPExporter?.tracesEndpoint) {
    providedDBOSConfig.telemetry.OTLPExporter.tracesEndpoint =
      providedDBOSConfig.telemetry.OTLPExporter.tracesEndpoint?.concat(
        configFile!.telemetry.OTLPExporter.tracesEndpoint,
      );
  }
  if (configFile!.telemetry?.OTLPExporter?.logsEndpoint) {
    providedDBOSConfig.telemetry.OTLPExporter.logsEndpoint =
      providedDBOSConfig.telemetry.OTLPExporter.logsEndpoint?.concat(configFile!.telemetry.OTLPExporter.logsEndpoint);
  }

  const overwritenDBOSConfig = {
    ...providedDBOSConfig,
    name: appName,
    poolConfig: poolConfig,
    telemetry: providedDBOSConfig.telemetry,
    system_database: configFile!.database.sys_db_name || poolConfig.database + '_dbos_sys', // Unexepected, but possible
  };

  const overwriteDBOSRuntimeConfig = {
    admin_port: 3001,
    runAdminServer: true,
    entrypoints: providedRuntimeConfig.entrypoints,
    port: providedRuntimeConfig.port,
    start: providedRuntimeConfig.start,
    setup: providedRuntimeConfig.setup,
  };

  return [overwritenDBOSConfig, overwriteDBOSRuntimeConfig];
}
