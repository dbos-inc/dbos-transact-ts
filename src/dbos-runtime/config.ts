import { readFileSync } from '../utils';
import { DBOSConfig, DBOSConfigInternal } from '../dbos-executor';
import YAML from 'yaml';
import { DBOSRuntimeConfig, defaultEntryPoint } from './runtime';
import { UserDatabaseName } from '../user_database';
import { writeFileSync } from 'fs';
import Ajv from 'ajv';
import path from 'path';
import dbosConfigSchema from '../../dbos-config.schema.json';
import assert from 'assert';
import validator from 'validator';

export const dbosConfigFilePath = 'dbos-config.yaml';
const ajv = new Ajv({ allErrors: true, verbose: true, allowUnionTypes: true });

export interface ConfigFile {
  name?: string;
  language?: string;
  database_url?: string;
  system_database_url?: string;
  database?: {
    app_db_client?: UserDatabaseName;
    migrate?: string[];
  };
  telemetry?: {
    logs?: {
      addContextMetadata?: boolean;
      logLevel?: string;
      silent?: boolean;
    };
    OTLPExporter?: {
      // naming nit: oltp_exporter
      logsEndpoint?: string | string[];
      tracesEndpoint?: string | string[];
    };
  };
  runtimeConfig?: Partial<DBOSRuntimeConfig>; // naming nit: runtime_config
  http?: {
    cors_middleware?: boolean;
    credentials?: boolean;
    allowed_origins?: string[];
  };
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

export function readConfigFile(dirPath?: string): ConfigFile {
  dirPath ??= process.cwd();
  const dbosConfigPath = path.join(dirPath, dbosConfigFilePath);
  const configContent = readFile(dbosConfigPath);

  const config = configContent ? (YAML.parse(substituteEnvVars(configContent)) as ConfigFile) : {};
  if (!config.name) {
    const packageJsonPath = path.join(dirPath, 'package.json');
    const packageContent = readFile(packageJsonPath);
    const $package = packageContent ? (JSON.parse(packageContent) as { name?: string }) : {};
    config.name = $package.name;
  }

  const schemaValidator = ajv.compile(dbosConfigSchema);
  if (!schemaValidator(config)) {
    throw new Error(`Config file validation failed: ${JSON.stringify(schemaValidator.errors, null, 2)}`);
  }
  return config;

  function readFile(filePath: string): string | undefined {
    try {
      return readFileSync(filePath);
    } catch (error) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        return undefined; // File does not exist
      }
      throw error; // Rethrow other errors
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

export function getSystemDatabaseUrl(
  configFileOrString: string | Pick<ConfigFile, 'name' | 'database_url' | 'system_database_url'>,
): string {
  if (typeof configFileOrString === 'string') {
    return convertUserDbUrl(configFileOrString);
  }

  if (configFileOrString.system_database_url) {
    return configFileOrString.system_database_url;
  }

  if (process.env.DBOS_SYSTEM_DATABASE_URL) {
    return process.env.DBOS_SYSTEM_DATABASE_URL;
  }

  const databaseUrl = getDatabaseUrl(configFileOrString);
  return convertUserDbUrl(databaseUrl);

  function convertUserDbUrl(databaseUrl: string) {
    const url = new URL(databaseUrl);
    const database = url.pathname.slice(1);
    url.pathname = `/${database}_dbos_sys`;
    return url.toString();
  }
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

export function getDatabaseUrl(configFile: Pick<ConfigFile, 'name' | 'database_url'>): string {
  const databaseUrl = configFile.database_url || process.env.DBOS_DATABASE_URL || defaultDatabaseUrl(configFile.name);

  const url = new URL(databaseUrl);
  const dbName = url.pathname.slice(1);

  const missingFields: string[] = [];
  if (!url.username) missingFields.push('username');
  if (!url.hostname) missingFields.push('hostname');
  if (!dbName) missingFields.push('database name');

  if (missingFields.length > 0) {
    throw new Error(`Invalid database URL: missing required field(s): ${missingFields.join(', ')}`);
  }

  assert(isValidDBname(dbName), `Database name "${dbName}" in database_url is invalid.`);

  if (process.env.DBOS_DEBUG_WORKFLOW_ID !== undefined) {
    // If in debug mode, apply the debug overrides
    url.hostname = process.env.DBOS_DBHOST || url.hostname;
    url.port = process.env.DBOS_DBPORT || url.port;
    url.username = process.env.DBOS_DBUSER || url.username;
    url.password = process.env.DBOS_DBPASSWORD || url.password;
    return url.toString();
  } else {
    return databaseUrl;
  }

  function defaultDatabaseUrl(appName?: string): string {
    assert(appName, 'Application name must be defined to construct a valid database URL.');

    const host = process.env.PGHOST || 'localhost';
    const port = process.env.PGPORT || '5432';
    const username = process.env.PGUSER || 'postgres';
    const password = process.env.PGPASSWORD || 'dbos';
    const database = toDbName(appName);
    const timeout = process.env.PGCONNECT_TIMEOUT || '10';
    const sslmode = process.env.PGSSLMODE || (host === 'localhost' ? 'disable' : 'allow');

    return `postgresql://${username}:${password}@${host}:${port}/${database}?connect_timeout=${timeout}&sslmode=${sslmode}`;
  }

  function toDbName(appName: string) {
    const dbName = appName.toLowerCase().replaceAll('-', '_').replaceAll(' ', '_');
    return dbName.match(/^\d/) ? '_' + dbName : dbName;
  }
}

export function getDbosConfig(
  config: ConfigFile,
  options: {
    logLevel?: string;
    forceConsole?: boolean;
    appVersion?: string | boolean;
  } = {},
): DBOSConfigInternal {
  assert(
    config.language === undefined || config.language === 'node',
    `Config file specifies invalid language ${config.language}`,
  );
  const userDbClient = config.database?.app_db_client ?? UserDatabaseName.KNEX;
  assert(isValidUserDbClient(userDbClient), `Invalid app_db_client ${userDbClient} in config file`);

  return translateDbosConfig(
    {
      name: config.name,
      databaseUrl: config.database_url,
      systemDatabaseUrl: config.system_database_url,
      userDatabaseClient: userDbClient,
      logLevel: options.logLevel ?? config.telemetry?.logs?.logLevel,
      addContextMetadata: config.telemetry?.logs?.addContextMetadata,
      otlpTracesEndpoints: toArray(config.telemetry?.OTLPExporter?.tracesEndpoint),
      otlpLogsEndpoints: toArray(config.telemetry?.OTLPExporter?.logsEndpoint),
      runAdminServer: config.runtimeConfig?.runAdminServer,
    },
    options,
  );
}

function toArray(endpoint: string | string[] | undefined): Array<string> {
  return endpoint ? (Array.isArray(endpoint) ? endpoint : [endpoint]) : [];
}

function isValidUserDbClient(name: string): name is UserDatabaseName {
  return Object.values(UserDatabaseName).includes(name as UserDatabaseName);
}

export function translateDbosConfig(
  config: DBOSConfig,
  options: { forceConsole?: boolean; appVersion?: string | boolean } = {},
): DBOSConfigInternal {
  const databaseUrl = getDatabaseUrl({ database_url: config.databaseUrl, name: config.name });
  const systemDatabaseUrl = getSystemDatabaseUrl({
    database_url: config.databaseUrl,
    system_database_url: config.systemDatabaseUrl,
    name: config.name,
  });
  const appVersion =
    typeof options.appVersion === 'string'
      ? options.appVersion
      : options.appVersion === false
        ? undefined
        : process.env.DBOS__APPVERSION || undefined; // convert empty string to undefined

  return {
    databaseUrl,
    userDbPoolSize: config.userDatabasePoolSize,
    systemDatabaseUrl,
    sysDbPoolSize: config.systemDatabasePoolSize,
    userDbClient: config.userDatabaseClient,
    appVersion,
    telemetry: {
      logs: {
        logLevel: config.logLevel || 'info',
        addContextMetadata: config.addContextMetadata,
        forceConsole: options.forceConsole ?? false,
      },
      OTLPExporter: {
        tracesEndpoint: config.otlpTracesEndpoints,
        logsEndpoint: config.otlpLogsEndpoints,
      },
    },
  };
}

export function getRuntimeConfig(config: ConfigFile, options: { port?: number } = {}): DBOSRuntimeConfig {
  return translateRuntimeConfig(config.runtimeConfig, options.port);
}

export function translateRuntimeConfig(config: Partial<DBOSRuntimeConfig> = {}, port?: number): DBOSRuntimeConfig {
  const entrypoints = new Set<string>();
  config.entrypoints?.forEach((entry) => entrypoints.add(entry));
  if (entrypoints.size === 0) {
    entrypoints.add(defaultEntryPoint);
  }
  port ??= config.port ?? 3000;
  return {
    entrypoints: [...entrypoints],
    port: port,
    runAdminServer: config.runAdminServer ?? true,
    admin_port: config.admin_port ?? port + 1,
    start: config.start ?? [],
    setup: config.setup ?? [],
  };
}

export function overwriteConfigForDBOSCloud(
  providedDBOSConfig: DBOSConfigInternal,
  providedRuntimeConfig: DBOSRuntimeConfig,
  configFile: ConfigFile,
): [DBOSConfigInternal, DBOSRuntimeConfig] {
  // Load the DBOS configuration file and force the use of:
  // 1. Use the application name from the file. This is a defensive measure to ensure the application name is whatever it was registered with in the cloud
  // 2. use the database URL from environment var
  // 3. OTLP traces endpoints (add the config data to the provided config)
  // 4. Force admin_port and runAdminServer

  const appName = configFile.name ?? providedDBOSConfig.name;
  let databaseUrl = process.env.DBOS_DATABASE_URL;
  if (!databaseUrl) {
    databaseUrl = configFile.database_url;
  }
  if (!databaseUrl) {
    databaseUrl = providedDBOSConfig.databaseUrl;
  }

  let systemDatabaseUrl = process.env.DBOS_SYSTEM_DATABASE_URL;
  if (!systemDatabaseUrl && process.env.DBOS_DATABASE_URL) {
    systemDatabaseUrl = getSystemDatabaseUrl(process.env.DBOS_DATABASE_URL);
  }

  if (!systemDatabaseUrl) {
    systemDatabaseUrl = configFile.system_database_url;
  }
  if (!systemDatabaseUrl) {
    systemDatabaseUrl = providedDBOSConfig.systemDatabaseUrl;
  }

  const logsSet = new Set(providedDBOSConfig.telemetry.OTLPExporter?.logsEndpoint);
  const logsEndpoint = configFile.telemetry?.OTLPExporter?.logsEndpoint;
  if (logsEndpoint) {
    if (Array.isArray(logsEndpoint)) {
      logsEndpoint.forEach((endpoint) => logsSet.add(endpoint));
    } else {
      logsSet.add(logsEndpoint);
    }
  }

  const tracesSet = new Set(providedDBOSConfig.telemetry.OTLPExporter?.tracesEndpoint);
  const tracesEndpoint = configFile.telemetry?.OTLPExporter?.tracesEndpoint;
  if (tracesEndpoint) {
    if (Array.isArray(tracesEndpoint)) {
      tracesEndpoint.forEach((endpoint) => tracesSet.add(endpoint));
    } else {
      tracesSet.add(tracesEndpoint);
    }
  }

  const overwritenDBOSConfig: DBOSConfigInternal = {
    ...providedDBOSConfig,
    name: appName,
    databaseUrl,
    systemDatabaseUrl,
    telemetry: {
      logs: providedDBOSConfig.telemetry.logs,
      OTLPExporter: {
        tracesEndpoint: Array.from(tracesSet),
        logsEndpoint: Array.from(logsSet),
      },
    },
  };

  const overwriteDBOSRuntimeConfig: DBOSRuntimeConfig = {
    admin_port: 3001,
    runAdminServer: true,
    entrypoints: providedRuntimeConfig.entrypoints,
    port: providedRuntimeConfig.port,
    start: providedRuntimeConfig.start,
    setup: providedRuntimeConfig.setup,
  };

  return [overwritenDBOSConfig, overwriteDBOSRuntimeConfig];
}
