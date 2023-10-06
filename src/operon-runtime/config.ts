import { OperonInitializationError } from "../error";
import { readFileSync } from "../utils";
import { OperonConfig } from "../operon";
import { transports, createLogger, format, Logger } from "winston";
import { PoolConfig } from "pg";
import { execSync } from "child_process";
import YAML from "yaml";
import { OperonRuntimeConfig } from "./runtime";
import { UserDatabaseName } from "../user_database";
import { OperonCLIStartOptions } from "./cli";

const operonConfigFilePath = "operon-config.yaml";

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
  telemetryExporters?: string[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  application: any;
  localRuntimeConfig?: OperonRuntimeConfig;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dbClientMetadata?: any;
}

function createGlobalLogger(logLevel: string): Logger {
  // TODO We will need to configure the formatter for "production" mode
  return createLogger({
    level: logLevel,
    format: format.combine(
      format.errors({ stack: true }),
      format.timestamp(),
      format.colorize(),
      format.printf((info) => {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const { timestamp, level, message, stack, ...args } = info;
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
        const ts = timestamp.slice(0, 19).replace("T", " ");
        // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment
        const formattedStack = stack?.split("\n").slice(1).join("\n");
        return `${ts} [${level}]: ${message} ${Object.keys(args).length ? "\n" + JSON.stringify(args, null, 2) : ""} ${stack ? "\n" + formattedStack : ""}`;
      })
    ),
    transports: [new transports.Console()],
    handleExceptions: true,
  });
}

export function parseConfigFile(cliOptions: OperonCLIStartOptions): [OperonConfig, OperonRuntimeConfig] {
  const logger = createGlobalLogger(cliOptions.loglevel);

  let configFile: ConfigFile | undefined;
  try {
    const configFileContent = readFileSync(operonConfigFilePath);
    const interpolatedConfig = execSync("envsubst", {
      encoding: "utf-8",
      input: configFileContent,
      env: process.env, // Jest modifies process.env, so we need to pass it explicitly for testing
    });
    configFile = YAML.parse(interpolatedConfig) as ConfigFile;
  } catch (e) {
    if (e instanceof Error) {
      throw new OperonInitializationError(`Failed to load config from ${operonConfigFilePath}: ${e.message}`);
    }
  }

  if (!configFile) {
    throw new OperonInitializationError(`Operon configuration file ${operonConfigFilePath} is empty`);
  }

  // Handle "Global" pool configFile
  if (!configFile.database) {
    throw new OperonInitializationError(`Operon configuration ${operonConfigFilePath} does not contain database config`);
  }

  const poolConfig: PoolConfig = {
    host: configFile.database.hostname,
    port: configFile.database.port,
    user: configFile.database.username,
    password: configFile.database.password,
    connectionTimeoutMillis: configFile.database.connectionTimeoutMillis,
    database: configFile.database.user_database,
  };

  if (!poolConfig.password) {
    throw new OperonInitializationError(`Operon configuration ${operonConfigFilePath} does not contain database password`);
  }

  if (configFile.database.ssl_ca) {
    logger.debug(`Using SSL CA ${configFile.database.ssl_ca}`);
    poolConfig.ssl = { ca: [readFileSync(configFile.database.ssl_ca)], rejectUnauthorized: true };
  }

  const operonConfig: OperonConfig = {
    poolConfig: poolConfig,
    userDbclient: configFile.database.user_dbclient || UserDatabaseName.PGNODE,
    telemetryExporters: configFile.telemetryExporters || [],
    system_database: configFile.database.system_database ?? "operon_systemdb",
    observability_database: configFile.database.observability_database || undefined,
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    application: configFile.application || undefined,
    dbClientMetadata: {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
      entities: configFile.dbClientMetadata?.entities,
    },
    logger,
  };

  // CLI takes precedence over config file, which takes precedence over default config.
  const localRuntimeConfig: OperonRuntimeConfig = {
    port: cliOptions.port || configFile.localRuntimeConfig?.port || 3000,
    logger,
  };

  return [operonConfig, localRuntimeConfig];
}
