import { readFile } from './utils';
import { DBOSConfig, DBOSRuntimeConfig, DBOSConfigInternal } from './dbos-executor';
import YAML from 'yaml';
import path from 'path';
import assert from 'assert';
import { maskDatabaseUrl } from './database_utils';
import { DBOSJSON } from './serialization';
import { validateObservabilityQueryTimeoutMs } from './system_database';

export const dbosConfigFilePath = 'dbos-config.yaml';

export interface ConfigFile {
  name?: string;
  language?: string;
  system_database_url?: string;
  system_database_schema_name?: string;
  database?: {
    migrate?: string[];
  };
  telemetry?: {
    logs?: {
      logLevel?: string;
    };
  };
  runtimeConfig?: Partial<DBOSRuntimeConfig>; // naming nit: runtime_config
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

export async function readConfigFile(dirPath?: string): Promise<ConfigFile> {
  dirPath ??= process.cwd();
  const dbosConfigPath = path.join(dirPath, dbosConfigFilePath);
  const configContent = await readFileHelper(dbosConfigPath);

  const config = configContent ? (YAML.parse(substituteEnvVars(configContent)) as ConfigFile) : {};
  if (!config.name) {
    const packageJsonPath = path.join(dirPath, 'package.json');
    const packageContent = await readFileHelper(packageJsonPath);
    const $package = packageContent ? (JSON.parse(packageContent) as { name?: string }) : {};
    config.name = $package.name;
  }

  return config;

  async function readFileHelper(filePath: string): Promise<string | undefined> {
    try {
      return await readFile(filePath);
    } catch (error) {
      if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
        return undefined; // File does not exist
      }
      throw error; // Rethrow other errors
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

  return databaseUrl;

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
  } = {},
): DBOSConfigInternal {
  assert(
    config.language === undefined || config.language === 'node',
    `Config file specifies invalid language ${config.language}`,
  );

  let dbosConfig: DBOSConfig = {
    name: config.name,
    systemDatabaseUrl: config.system_database_url,
    systemDatabaseSchemaName: config.system_database_schema_name,
    logLevel: options.logLevel ?? config.telemetry?.logs?.logLevel,
  };
  if (process.env.DBOS__CLOUD === 'true') {
    dbosConfig = overwriteConfigForDBOSCloud(dbosConfig);
  }
  return translateDbosConfig(dbosConfig);
}

export function translateDbosConfig(options: DBOSConfig): DBOSConfigInternal {
  if (
    options.maxConcurrentQueueDispatches !== undefined &&
    (!Number.isInteger(options.maxConcurrentQueueDispatches) || options.maxConcurrentQueueDispatches <= 0)
  ) {
    throw new Error('maxConcurrentQueueDispatches must be a positive integer');
  }
  if (
    options.notificationCoalesceMs !== undefined &&
    // Reject NaN/inf too (they slip past a bare < 1 check) so the notifier's sleep can't misbehave.
    (!Number.isFinite(options.notificationCoalesceMs) || options.notificationCoalesceMs < 1)
  ) {
    throw new Error('notificationCoalesceMs must be a finite number at least 1 millisecond');
  }
  validateObservabilityQueryTimeoutMs(options.observabilityQueryTimeoutMs);
  const systemDatabaseUrl = getSystemDatabaseUrl({
    system_database_url: options.systemDatabaseUrl,
    name: options.name,
  });

  return {
    name: options.name,
    systemDatabaseUrl,
    sysDbPoolSize: options.systemDatabasePoolSize,
    systemDatabasePollingConcurrency: options.systemDatabasePollingConcurrency,
    systemDatabasePool: options.systemDatabasePool,
    systemDatabaseSchemaName: options.systemDatabaseSchemaName ?? 'dbos',
    serializer: options.serializer ?? DBOSJSON,
    telemetry: {
      logs: {
        logLevel: options.logLevel || 'info',
        addContextMetadata: options.addContextMetadata,
        logger: options.logger,
      },
      OTLPExporter: {
        tracesEndpoint: options.otlpTracesEndpoints,
        logsEndpoint: options.otlpLogsEndpoints,
      },
      otelAttributeFormat: options.otelAttributeFormat ?? 'legacy',
    },
    schedulerPollingIntervalMs: options.schedulerPollingIntervalMs,
    maxConcurrentQueueDispatches: options.maxConcurrentQueueDispatches,
    useListenNotify: options.useListenNotify ?? true,
    notificationCoalesceMs: options.notificationCoalesceMs,
    observabilityQueryTimeoutMs: options.observabilityQueryTimeoutMs,
    runMigrations: options.runMigrations ?? true,
  };
}

export function getRuntimeConfig(config: ConfigFile): DBOSRuntimeConfig {
  return {
    start: config.runtimeConfig?.start ?? [],
    setup: config.runtimeConfig?.setup ?? [],
  };
}

// DBOS Cloud supplies the registered app name, system database, and OTLP collector through environment variables.
export function overwriteConfigForDBOSCloud(config: DBOSConfig): DBOSConfig {
  const systemDatabaseUrl = process.env.DBOS_SYSTEM_DATABASE_URL;
  assert(systemDatabaseUrl, 'DBOS_SYSTEM_DATABASE_URL must be set in DBOS Cloud environment');

  return {
    ...config,
    name: process.env.DBOS_APP_NAME || config.name,
    systemDatabaseUrl,
    otlpLogsEndpoints: withEndpoint(config.otlpLogsEndpoints, process.env.DBOS__OTLP_LOGS_ENDPOINT),
    otlpTracesEndpoints: withEndpoint(config.otlpTracesEndpoints, process.env.DBOS__OTLP_TRACES_ENDPOINT),
  };
}

function withEndpoint(endpoints: string[] | undefined, endpoint: string | undefined): string[] {
  return Array.from(new Set([...(endpoints ?? []), endpoint])).filter((e): e is string => !!e);
}
