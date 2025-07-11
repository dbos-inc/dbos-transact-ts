import { readFileSync } from '../utils';
import { DBOSConfig, DBOSConfigInternal } from '../dbos-executor';
import YAML from 'yaml';
import { DBOSRuntimeConfig, defaultEntryPoint } from './runtime';
import { UserDatabaseName } from '../user_database';
import { TelemetryConfig } from '../telemetry';
import { writeFileSync } from 'fs';
import Ajv from 'ajv';
import path from 'path';
import dbosConfigSchema from '../../dbos-config.schema.json';
import assert from 'node:assert';

export const dbosConfigFilePath = 'dbos-config.yaml';
const ajv = new Ajv({ allErrors: true, verbose: true, allowUnionTypes: true });

export interface DBConfig {
  sys_db_name?: string;
  app_db_client?: UserDatabaseName;
  migrate?: string[];
}

export interface ConfigFile {
  name?: string;
  language?: string;
  database?: DBConfig;
  database_url?: string;
  http?: {
    cors_middleware?: boolean;
    credentials?: boolean;
    allowed_origins?: string[];
  };
  telemetry?: TelemetryConfig;
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

export function writeConfigFile(config: ConfigFile, dirPath: string | undefined) {
  dirPath ??= process.cwd();
  const dbosConfigPath = path.join(dirPath, 'dbos-config.yaml');
  const content = YAML.stringify(config);
  writeFileSync(dbosConfigPath, content);
}

export function getDatabaseInfo(options: {
  name?: string;
  database_url?: string;
  database?: { sys_db_name?: string };
}): { databaseUrl: string; sysDbName: string } {
  const appName = options.name;
  let databaseUrl = options.database_url;
  let sysDbName = options.database?.sys_db_name;

  sysDbName ??= appName ? `${appName}_dbos_sys` : undefined;
  assert(
    sysDbName,
    'System database name could not be determined. Please provide a name in the config file or set the package name.',
  );

  databaseUrl ??= process.env['DBOS_DATABASE_URL'] ?? defaultDatabaseUrl(appName);

  const missingFields: string[] = [];
  const url = new URL(databaseUrl);
  if (!url.username) missingFields.push('username');
  if (!url.hostname) missingFields.push('hostname');
  if (!url.pathname.substring(1)) missingFields.push('database name');
  assert(missingFields.length === 0, `Invalid database URL: missing required field(s): ${missingFields.join(', ')}`);

  return { databaseUrl, sysDbName };

  function defaultDatabaseUrl(appName: string | undefined) {
    const database = appNameToDbName(appName) ?? process.env['PGDATABASE'];
    assert(
      database,
      'Database name could not be determined. Please provide a name in the config file or set the package name or PGDATABASE environment variable.',
    );

    // use standard PG environment variables from https://www.postgresql.org/docs/17/libpq-envars.html
    const host = process.env['PGHOST'] ?? 'localhost';
    const port = process.env['PGPORT'] ?? '5432';
    const user = process.env['PGUSER'] ?? 'postgres';
    const password = process.env['PGPASSWORD'] ?? 'dbos';
    const timeout = process.env['PGCONNECT_TIMEOUT'] ?? '10';
    const sslmode = process.env['PGSSLMODE'] ?? 'prefer';

    return `postgresql://${user}:${password}@${host}:${port}/${database}?connect_timeout=${timeout}&sslmode=${sslmode}`;
  }

  function appNameToDbName(appName: string | undefined) {
    const dbName = appName?.toLowerCase().replaceAll('-', '_').replaceAll(' ', '_');
    return dbName?.match(/^\d/) ? '_' + dbName : dbName;
  }
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
  const $port = port ?? config.port ?? 3000;
  return {
    entrypoints: [...entrypoints],
    port: $port,
    runAdminServer: true,
    admin_port: config.admin_port ?? $port + 1,
    start: config.start ?? [],
    setup: config.setup ?? [],
  };
}

export function getDbosConfig(
  config: ConfigFile,
  options: {
    logLevel?: string;
    forceConsole?: boolean;
  } = {},
): DBOSConfigInternal {
  assert(config.language && config.language !== 'node', `Config file specifies invalid language ${config.language}`);
  const userDbClient = config.database?.app_db_client ?? UserDatabaseName.KNEX;
  assert(isUserDatabaseName(userDbClient), `Invalid app_db_client ${userDbClient} in config file`);

  return translateDbosConfig({
    name: config.name,
    databaseUrl: config.database_url,
    sysDbName: config.database?.sys_db_name,
    userDbclient: userDbClient,
    logLevel: options.logLevel ?? config.telemetry?.logs?.logLevel,
    addContextMetadata: config.telemetry?.logs?.addContextMetadata,
    adminPort: config.runtimeConfig?.admin_port,
    runAdminServer: config.runtimeConfig?.runAdminServer,
    otlpTracesEndpoints: config.telemetry?.OTLPExporter?.tracesEndpoint,
    otlpLogsEndpoints: config.telemetry?.OTLPExporter?.logsEndpoint,
  });

  function isUserDatabaseName(name: string): name is UserDatabaseName {
    return Object.values(UserDatabaseName).includes(name as UserDatabaseName);
  }
}

export function translateDbosConfig(options: DBOSConfig, forceConsole?: boolean): DBOSConfigInternal {
  const { databaseUrl, sysDbName } = getDatabaseInfo({
    name: options.name,
    database_url: options.databaseUrl,
    database: { sys_db_name: options.sysDbName },
  });
  return {
    databaseUrl,
    userDbclient: options.userDbclient ?? UserDatabaseName.KNEX,
    system_database: sysDbName,
    telemetry: {
      logs: {
        logLevel: options.logLevel || 'info',
        forceConsole: forceConsole ?? false,
      },
      OTLPExporter: {
        tracesEndpoint: options.otlpTracesEndpoints,
        logsEndpoint: options.otlpLogsEndpoints,
      },
    },
  };
}

export function parseConfigFile(options?: { appDir?: string }): { databaseUrl: string; sysDbName: string } {
  const configFile: ConfigFile = readConfigFile(options?.appDir);
  return getDatabaseInfo(configFile);
}

export function overwrite_config(
  providedDBOSConfig: DBOSConfigInternal,
  providedRuntimeConfig: DBOSRuntimeConfig,
  configFile?: ConfigFile,
): [DBOSConfigInternal, DBOSRuntimeConfig] {
  configFile ??= readConfigFile();

  // Load the DBOS configuration file and force the use of:
  // 1. Use the application name from the file. This is a defensive measure to ensure the application name is whatever it was registered with in the cloud
  // 2. The database connection parameters (sub the file data to the provided config)
  // 3. OTLP traces endpoints (add the config data to the provided config)
  // 4. Force admin_port and runAdminServer

  const appName = configFile.name || providedDBOSConfig.name;

  if (!providedDBOSConfig.telemetry.OTLPExporter) {
    providedDBOSConfig.telemetry.OTLPExporter = {
      tracesEndpoint: [],
      logsEndpoint: [],
    };
  }
  if (configFile.telemetry?.OTLPExporter?.tracesEndpoint) {
    providedDBOSConfig.telemetry.OTLPExporter.tracesEndpoint =
      providedDBOSConfig.telemetry.OTLPExporter.tracesEndpoint?.concat(
        configFile.telemetry.OTLPExporter.tracesEndpoint,
      );
  }
  if (configFile.telemetry?.OTLPExporter?.logsEndpoint) {
    providedDBOSConfig.telemetry.OTLPExporter.logsEndpoint =
      providedDBOSConfig.telemetry.OTLPExporter.logsEndpoint?.concat(configFile.telemetry.OTLPExporter.logsEndpoint);
  }

  const overwritenDBOSConfig: DBOSConfigInternal = {
    ...providedDBOSConfig,
    name: appName,
    databaseUrl: configFile.database_url || providedDBOSConfig.databaseUrl,
    telemetry: providedDBOSConfig.telemetry,
    system_database: configFile.database?.sys_db_name || providedDBOSConfig.system_database,
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
