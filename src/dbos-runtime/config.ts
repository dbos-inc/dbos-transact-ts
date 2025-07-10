import YAML from 'yaml';
import { DBOSRuntimeConfig, defaultEntryPoint } from './runtime';
import { TelemetryConfig } from '../telemetry';
import Ajv from 'ajv';
import path from 'path';
import dbosConfigSchema from '../../dbos-config.schema.json';
import * as fs from 'node:fs/promises';
import { readFileSync as utilReadFileSync } from '../utils';
import assert from 'node:assert/strict';
import { DBOSConfig, DBOSConfigInternal } from '../dbos-executor';
import { UserDatabaseName } from '../user_database';

export const dbosConfigFilePath = 'dbos-config.yaml';
const ajv = new Ajv({ allErrors: true, verbose: true, allowUnionTypes: true });

export interface ConfigFile {
  name?: string;
  language?: string;
  database_url?: string;
  database?: {
    sys_db_name?: string;
    app_db_client?: string;
    migrate?: string[];
  };
  runtimeConfig?: Partial<DBOSRuntimeConfig>; // TODO: this should be runtime_config
  telemetry?: TelemetryConfig; // Peter said this should be deprecated, but Max said DBOS Cloud still uses this

  // the following properties are deprecated and will be removed in future PRs
  http?: {
    cors_middleware?: boolean;
    credentials?: boolean;
    allowed_origins?: string[];
  };
  application?: object;
  env?: Record<string, string>;
}

/*
 * Substitute environment variables using a regex for matching.
 * Will find anything in curly braces.
 * TODO: Use a more robust solution.
 */
function substituteEnvVars(content: string): string {
  const regex = /\${([^}]+)}/g; // Regex to match ${VAR_NAME} style placeholders
  return content.replace(regex, (_, g1: string) => {
    return process.env[g1] || '""'; // If the env variable is not set, return an empty string.
  });
}

async function readFile(filePath: string): Promise<string | undefined> {
  try {
    return await fs.readFile(filePath, 'utf-8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return undefined; // File does not exist
    }
    throw error; // Rethrow other errors
  }
}

function validateConfig(config: ConfigFile) {
  const schemaValidator = ajv.compile(dbosConfigSchema);
  if (!schemaValidator(config)) {
    throw new Error(`Config file validation failed: ${JSON.stringify(schemaValidator.errors, null, 2)}`);
  }
}

function parseConfigFile(content?: string): ConfigFile {
  return content ? (YAML.parse(substituteEnvVars(content)) as ConfigFile) : {};
}

function parsePackageJson(content?: string): { name?: string } {
  return content ? (JSON.parse(content) as { name?: string }) : {};
}

export async function readConfigFile(dirPath?: string): Promise<ConfigFile> {
  dirPath ??= process.cwd();
  const dbosConfigPath = path.join(dirPath, 'dbos-config.yaml');
  const configContent = await readFile(dbosConfigPath);

  const config = parseConfigFile(configContent);
  if (!config.name) {
    const packageJsonPath = path.join(dirPath, 'package.json');
    const packageContent = await readFile(packageJsonPath);
    config.name = parsePackageJson(packageContent).name;
  }

  validateConfig(config);
  return config;
}

export async function writeConfigFile(config: ConfigFile, dirPath: string | undefined) {
  dirPath ??= process.cwd();
  const dbosConfigPath = path.join(dirPath, 'dbos-config.yaml');
  const content = YAML.stringify(config);
  await fs.writeFile(dbosConfigPath, content, { encoding: 'utf8' });
}

export function getDatabaseInfo(appName?: string, databaseUrl?: string, sysDbName?: string) {
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

function isUserDatabaseName(name: string): name is UserDatabaseName {
  return Object.values(UserDatabaseName).includes(name as UserDatabaseName);
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
    userDbClient,
    logLevel: options.logLevel,
    isDebugging: options.forceConsole,
    otlpTracesEndpoints: config.telemetry?.OTLPExporter?.tracesEndpoint,
    otlpLogsEndpoints: config.telemetry?.OTLPExporter?.logsEndpoint,
    application: config.application,
    env: config.env,
  });
}

export function translateDbosConfig(options: {
  name?: string;
  databaseUrl?: string;
  sysDbName?: string;
  userDbClient?: UserDatabaseName;
  logLevel?: string;
  isDebugging?: boolean;
  otlpTracesEndpoints?: string[];
  otlpLogsEndpoints?: string[];
  application?: object;
  env?: Record<string, string>;
}): DBOSConfigInternal {
  const { databaseUrl, sysDbName } = getDatabaseInfo(options.name, options.databaseUrl, options.sysDbName);
  return {
    databaseUrl,
    poolConfig: {
      connectionString: databaseUrl,
    },
    userDbClient: options.userDbClient ?? UserDatabaseName.KNEX,
    systemDatabase: sysDbName,
    telemetry: {
      logs: {
        logLevel: options.logLevel || 'info',
        forceConsole: options.isDebugging ?? false,
      },
      OTLPExporter: {
        tracesEndpoint: options.otlpTracesEndpoints,
        logsEndpoint: options.otlpLogsEndpoints,
      },
    },
    application: options.application,
    env: options.env ?? {},
  };
}

function readFileSync(filePath: string): string | undefined {
  try {
    return utilReadFileSync(filePath, 'utf-8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return undefined; // File does not exist
    }
    throw error; // Rethrow other errors
  }
}

function readConfigFileSync(dirPath?: string): ConfigFile {
  dirPath ??= process.cwd();
  const dbosConfigPath = path.join(dirPath, 'dbos-config.yaml');
  const configContent = readFileSync(dbosConfigPath);

  const config = parseConfigFile(configContent);
  if (!config.name) {
    const packageJsonPath = path.join(dirPath, 'package.json');
    const packageContent = readFileSync(packageJsonPath);
    config.name = parsePackageJson(packageContent).name;
  }

  validateConfig(config);
  return config;
}

export function getDatabaseUrl(dirPath?: string): string {
  const config = readConfigFileSync(dirPath);
  const { databaseUrl } = getDatabaseInfo(config.name, config.database_url, config.database?.sys_db_name);
  return databaseUrl;
}
