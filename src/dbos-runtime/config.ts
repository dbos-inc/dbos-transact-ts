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
import path from 'path';
import validator from 'validator';
import { GlobalLogger } from '../telemetry/logs';
import dbosConfigSchema from '../../dbos-config.schema.json';
import { ConnectionOptions } from 'tls';

export const dbosConfigFilePath = 'dbos-config.yaml';
const ajv = new Ajv({ allErrors: true, verbose: true, allowUnionTypes: true });

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
 * Build a PoolConfig object.
 *
 * If configFile.database_url is provided, set it directly in poolConfig.connectionString and backfill the other poolConfig options.
 * Backfilling allows the rest of the code to access database parameters easily.
 * We configure the ORMs with the connection string
 * We still need to extract pool size and connectionTimeoutMillis from the config file and give them manually to the ORMs.
 *
 * If configFile.database_url is not provided, we build the connection string from the configFile.database object.
 *
 * In debug mode, apply overrides from DBOS_DBHOST, DBOS_DBPORT, DBOS_DBUSER, and DBOS_DBPASSWORD.
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
  // FIXME: this is not a good place to set the app name
  const appName = retrieveApplicationName(configFile);
  if (globalParams.appName === '') {
    globalParams.appName = appName;
  }

  const isDebugMode = process.env.DBOS_DEBUG_WORKFLOW_ID !== undefined;

  if (!cliOptions?.silent) {
    const logger = new GlobalLogger();
    if (isDebugMode) {
      logger.info('Loading database connection parameters from debug environment variables');
    } else if (configFile.database_url) {
      logger.info('Loading database connection parameters from database_url');
    } else if (configFile.database.hostname) {
      logger.info('Loading database connection parameters from dbos-config.yaml');
    } else {
      logger.info('Using default database connection parameters');
    }
  }

  let connectionString = undefined;
  let databaseName = undefined;
  let connectionTimeoutMillis = 10000;
  let ssl: boolean | ConnectionOptions = false;

  // If a database_url is found, parse it to backfill the poolConfig
  if (configFile.database_url) {
    const url = new URL(configFile.database_url);
    // If in debug mode, apply the debug overrides
    if (isDebugMode) {
      if (process.env.DBOS_DBHOST) {
        url.hostname = process.env.DBOS_DBHOST;
      }
      if (process.env.DBOS_DBPORT) {
        url.port = process.env.DBOS_DBPORT;
      }
      if (process.env.DBOS_DBUSER) {
        url.username = process.env.DBOS_DBUSER;
      }
      if (process.env.DBOS_DBPASSWORD) {
        url.password = process.env.DBOS_DBPASSWORD;
      }
      configFile.database_url = url.toString();
    }
    connectionString = configFile.database_url;
    databaseName = url.pathname.substring(1);

    const queryParams = url.searchParams;
    if (queryParams.has('connect_timeout')) {
      connectionTimeoutMillis = parseInt(queryParams.get('connect_timeout')!, 10) * 1000;
    }
    const sslMode = queryParams.get('sslmode');
    const sslRootCert = queryParams.get('sslrootcert');
    ssl = getSSLFromParams(sslMode, sslRootCert);

    // Validate required fields
    const missingFields: string[] = [];
    if (!url.username) missingFields.push('username');
    if (!url.password) missingFields.push('password');
    if (!url.hostname) missingFields.push('hostname');
    if (!databaseName) missingFields.push('database name');

    if (missingFields.length > 0) {
      throw new Error(`Invalid database URL: missing required field(s): ${missingFields.join(', ')}`);
    }

    // Backfill the poolConfig
    configFile.database = {
      ...configFile.database,
      hostname: url.hostname,
      port: url.port ? parseInt(url.port, 10) : 5432,
      username: url.username,
      password: url.password,
      app_db_name: databaseName,
    };

    if (!cliOptions?.silent) {
      const logger = new GlobalLogger();
      let logConnectionString = `postgresql://${configFile.database.username}:***@${configFile.database.hostname}:${configFile.database.port}/${databaseName}`;
      const logQueryParamsArray = Object.entries(url.searchParams.entries()).map(([key, value]) => `${key}=${value}`);
      if (logQueryParamsArray.length > 0) {
        logConnectionString += `?${logQueryParamsArray.join('&')}`;
      }
      logger.info(`Using database connection string: ${logConnectionString}`);
    }
  } else {
    // Else, build the config from configFile.database and apply overrides
    configFile.database.hostname = process.env.DBOS_DBHOST || configFile.database.hostname || 'localhost';
    const dbos_dbport = process.env.DBOS_DBPORT ? parseInt(process.env.DBOS_DBPORT) : undefined;
    configFile.database.port = dbos_dbport || configFile.database.port || 5432;
    configFile.database.username = process.env.DBOS_DBUSER || configFile.database.username || 'postgres';
    configFile.database.password =
      process.env.DBOS_DBPASSWORD || configFile.database.password || process.env.PGPASSWORD || 'dbos';
    connectionTimeoutMillis = configFile.database.connectionTimeoutMillis || 10000;

    databaseName = configFile.database.app_db_name;
    // Construct the database name from the application name, if needed
    if (databaseName === undefined || databaseName === '') {
      databaseName = appName.toLowerCase().replaceAll('-', '_').replaceAll(' ', '_');
      if (databaseName.match(/^\d/)) {
        databaseName = '_' + databaseName; // Append an underscore if the name starts with a digit
      }
    }

    connectionString = `postgresql://${configFile.database.username}:${configFile.database.password}@${configFile.database.hostname}:${configFile.database.port}/${databaseName}`;

    // Build connection string query parameters
    const queryParams: string[] = [];

    queryParams.push(`connect_timeout=${connectionTimeoutMillis / 1000}`);

    // SSL configuration
    ssl = parseSSLConfig(configFile.database);
    if (ssl === false) {
      queryParams.push(`sslmode=disable`);
    } else if (ssl && 'ca' in ssl) {
      queryParams.push(`sslmode=verify-full`);
      queryParams.push(`sslrootcert=${configFile.database.ssl_ca}`);
    } else {
      queryParams.push(`sslmode=no-verify`);
    }

    if (queryParams.length > 0) {
      connectionString += `?${queryParams.join('&')}`;
    }

    if (!cliOptions?.silent) {
      const logger = new GlobalLogger();
      let logConnectionString = `postgresql://${configFile.database.username}:***@${configFile.database.hostname}:${configFile.database.port}/${databaseName}`;
      if (queryParams.length > 0) {
        logConnectionString += `?${queryParams.join('&')}`;
      }
      logger.info(`Using database connection string: ${logConnectionString}`);
    }
  }

  // Build the final poolConfig
  const poolConfig: PoolConfig = {
    connectionString,
    connectionTimeoutMillis,
    ssl,
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    database: databaseName,
    max: cliOptions?.userDbPoolSize || 20,
  };

  if (!poolConfig.database) {
    throw new DBOSInitializationError(
      `DBOS configuration (${dbosConfigFilePath}) does not contain application database name`,
    );
  }
  return poolConfig;
}

/**
 * A helper to backfill TLS configuration from a connection string sslmode/sslrootcert query parameters
 *
 * @param sslmode
 * @param sslrootcert
 * @returns
 */
function getSSLFromParams(sslmode: string | null, sslrootcert: string | null): false | ConnectionOptions {
  if (!sslmode || sslmode === 'disable') {
    return false;
  }

  const ssl: ConnectionOptions = {};

  if (sslmode === 'require' || sslmode === 'no-verify') {
    ssl.rejectUnauthorized = false;
  } else if (sslmode === 'verify-ca' || sslmode === 'verify-full') {
    ssl.rejectUnauthorized = sslmode === 'verify-full';

    if (sslrootcert) {
      try {
        ssl.ca = readFileSync(path.resolve(sslrootcert)).toString();
      } catch (err) {
        throw new Error(`Failed to read sslrootcert from "${sslrootcert}": ${(err as Error).message}`);
      }
    } else {
      throw new Error(`sslmode=${sslmode} requires sslrootcert`);
    }
  }

  return ssl;
}

export function parseSSLConfig(dbConfig: DBConfig): false | ConnectionOptions {
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

  if (configFile.language && configFile.language !== 'node') {
    throw new DBOSInitializationError(`${configFilePath} specifies invalid language ${configFile.language}`);
  }

  /*******************************/
  /* Handle user database config */
  /*******************************/

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
  let appName = config.name;
  // Opportunistically grab the name from the config file if none was provided
  try {
    const configFile: ConfigFile | undefined = loadConfigFile(dbosConfigFilePath);
    if (configFile?.name && !appName) {
      appName = configFile.name;
    }
  } catch (e) {
    // Ignore file not found errors
    if (!(e as Error).message.includes('ENOENT: no such file or directory')) {
      throw e;
    }
  }

  // Resolve database configuration
  const poolConfig = constructPoolConfig(
    {
      name: appName,
      database: {},
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
    sysDbPoolSize: config.sysDbPoolSize || 20,
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

  if (configFile.database.ssl_ca) {
    configFile.database_url = `postgresql://${configFile.database.username}:${configFile.database.password}@${configFile.database.hostname}:${configFile.database.port}/${configFile.database.app_db_name}?connect_timeout=10&sslmode=verify-full&sslrootcert=${configFile.database.ssl_ca}`;
  } else {
    configFile.database_url = `postgresql://${configFile.database.username}:${configFile.database.password}@${configFile.database.hostname}:${configFile.database.port}/${configFile.database.app_db_name}?connect_timeout=10&sslmode=no-verify`;
  }
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
    system_database: configFile!.database.sys_db_name || poolConfig.database + '_dbos_sys', // Unexpected, but possible
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
