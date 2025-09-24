import { readFileSync } from './utils';
import { DBOSConfig, DBOSRuntimeConfig, DBOSConfigInternal } from './dbos-executor';
import YAML from 'yaml';
import { writeFileSync } from 'fs';
import Ajv from 'ajv';
import path from 'path';
import dbosConfigSchema from '../dbos-config.schema.json';
import assert from 'assert';
import { maskDatabaseUrl } from './database_utils';

export const dbosConfigFilePath = 'dbos-config.yaml';
const ajv = new Ajv({ allErrors: true, verbose: true, allowUnionTypes: true });

export interface ConfigFile {
  name?: string;
  language?: string;
  system_database_url?: string;
  database?: {
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

export function isValidDatabaseName(dbName: string): boolean {
  if (dbName.length < 1 || dbName.length > 63) {
    return false;
  }
  return true;
}

export function getSystemDatabaseUrl(configFile: Pick<ConfigFile, 'name' | 'system_database_url'>): string {
  const databaseUrl = configFile.system_database_url || defaultSysDatabaseUrl(configFile.name);

  const url = new URL(databaseUrl);
  const dbName = url.pathname.slice(1);

  const missingFields: string[] = [];
  if (!url.username) missingFields.push('username');
  if (!url.hostname) missingFields.push('hostname');
  if (!dbName) missingFields.push('database name');

  if (missingFields.length > 0) {
    throw new Error(`Invalid database URL: missing required field(s): ${missingFields.join(', ')}`);
  }

  if (!isValidDatabaseName(dbName))
    throw new Error(`Database name "${dbName}" in database_url ${maskDatabaseUrl(databaseUrl)} is invalid.`);

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

  function defaultSysDatabaseUrl(appName?: string): string {
    assert(appName, 'Application name must be defined to construct a valid database URL.');

    const host = process.env.PGHOST || 'localhost';
    const port = process.env.PGPORT || '5432';
    const username = process.env.PGUSER || 'postgres';
    const password = process.env.PGPASSWORD || 'dbos';
    const database = toDbName(appName) + '_dbos_sys';
    const timeout = process.env.PGCONNECT_TIMEOUT || '10';
    const sslmode = process.env.PGSSLMODE || (host === 'localhost' ? 'disable' : 'allow');

    const dbUrl = new URL(`postgresql://host/database`);
    dbUrl.username = username;
    dbUrl.password = password;
    dbUrl.hostname = host;
    dbUrl.port = port;
    dbUrl.protocol = 'postgresql';
    dbUrl.pathname = `/${database}`;
    dbUrl.searchParams.set('connect_timeout', timeout);
    dbUrl.searchParams.set('sslmode', sslmode);
    return dbUrl.toString();
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
  } = {},
): DBOSConfigInternal {
  assert(
    config.language === undefined || config.language === 'node',
    `Config file specifies invalid language ${config.language}`,
  );

  return translateDbosConfig(
    {
      name: config.name,
      systemDatabaseUrl: config.system_database_url,
      logLevel: options.logLevel ?? config.telemetry?.logs?.logLevel,
      addContextMetadata: config.telemetry?.logs?.addContextMetadata,
      otlpTracesEndpoints: toArray(config.telemetry?.OTLPExporter?.tracesEndpoint),
      otlpLogsEndpoints: toArray(config.telemetry?.OTLPExporter?.logsEndpoint),
      runAdminServer: config.runtimeConfig?.runAdminServer,
      adminPort: config.runtimeConfig?.admin_port,
    },
    options.forceConsole,
  );
}

function toArray(endpoint: string | string[] | undefined): Array<string> {
  return endpoint ? (Array.isArray(endpoint) ? endpoint : [endpoint]) : [];
}

export function translateDbosConfig(options: DBOSConfig, forceConsole: boolean = false): DBOSConfigInternal {
  const systemDatabaseUrl = getSystemDatabaseUrl({
    system_database_url: options.systemDatabaseUrl,
    name: options.name,
  });

  return {
    name: options.name,
    systemDatabaseUrl,
    sysDbPoolSize: options.systemDatabasePoolSize,
    telemetry: {
      logs: {
        logLevel: options.logLevel || 'info',
        addContextMetadata: options.addContextMetadata,
        forceConsole,
      },
      OTLPExporter: {
        tracesEndpoint: options.otlpTracesEndpoints,
        logsEndpoint: options.otlpLogsEndpoints,
      },
    },
  };
}

export function getRuntimeConfig(config: ConfigFile): DBOSRuntimeConfig {
  return translateRuntimeConfig(config.runtimeConfig);
}

export function translateRuntimeConfig(
  config: Partial<DBOSRuntimeConfig & DBOSConfig> /*eww*/ = {},
): DBOSRuntimeConfig {
  const port = config.port ?? 3000;
  return {
    port: port,
    runAdminServer: config.runAdminServer ?? true,
    admin_port: config.admin_port ?? config.adminPort ?? port + 1,
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

  const systemDatabaseUrl = process.env.DBOS_SYSTEM_DATABASE_URL;
  assert(systemDatabaseUrl, 'DBOS_SYSTEM_DATABASE_URL must be set in DBOS Cloud environment');

  const appName = configFile.name ?? providedDBOSConfig.name;

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
    systemDatabaseUrl,
    telemetry: {
      logs: {
        ...providedDBOSConfig.telemetry.logs,
      },
      OTLPExporter: {
        logsEndpoint: Array.from(logsSet).filter((e) => !!e),
        tracesEndpoint: Array.from(tracesSet).filter((e) => !!e),
      },
    },
  };

  const overwriteDBOSRuntimeConfig: DBOSRuntimeConfig = {
    ...providedRuntimeConfig,
    admin_port: 3001,
    runAdminServer: true,
  };

  return [overwritenDBOSConfig, overwriteDBOSRuntimeConfig];
}
