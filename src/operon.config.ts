import { OperonError } from './error';

import { PoolConfig } from 'pg';
import YAML from 'yaml'
import fs from 'fs'
import path from 'path'

const CONFIG_FILE: string = "operon-config.yaml";
const SCHEMAS_DIR: string = "schemas";

interface OperonConfigFile {
    database: DatabaseConfig;
}

export interface DatabaseConfig {
    hostname: string;
    port: number;
    username: string;
    connectionTimeoutMillis: number;
    schemaFile: string;
}

export class OperonConfig {
  readonly poolConfig: PoolConfig;
  readonly operonDbSchema: string = '';

  constructor() {
    const currentDirectory: string = __dirname;
    const configPath: string = path.join(currentDirectory, '..', CONFIG_FILE);

    // Check if CONFIG_FILE is a valid file
    try {
      fs.stat(configPath, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
        if (error) {
          throw new OperonError(`checking on ${CONFIG_FILE}. ${error.code}: ${error.errno}`);
        } else if (!stats.isFile()) {
          throw new OperonError(`config file ${CONFIG_FILE} is not a file`);
        }
      });
    } catch (error) {
      if (error instanceof Error) {
        throw new OperonError(`calling fs.stat on ${CONFIG_FILE}: ${error.message}`);
      }
    }

    // Logic to parse CONFIG_FILE.
    // We have to initialize an empty object so TSC lets us check on it later on
    let parsedConfig: OperonConfigFile = {
      database: {
        hostname: '',
        port: 0,
        username: '',
        connectionTimeoutMillis: 0,
        schemaFile: '',
      }
    };
    try {
      const configFileContent: string = fs.readFileSync(CONFIG_FILE, 'utf8')
      parsedConfig = YAML.parse(configFileContent) as OperonConfigFile;
    } catch(error) {
      if (error instanceof Error) {
        throw new OperonError(`parsing ${CONFIG_FILE}: ${error.message}`);
      }
    }
    if (!parsedConfig) {
      throw new OperonError(`Operon configuration ${CONFIG_FILE} is empty`);
    }

    // Handle DB config
    if (!parsedConfig.database) {
      throw new OperonError(`Operon configuration ${CONFIG_FILE} does not contain database config`);
    }
    const dbConfig: DatabaseConfig = parsedConfig.database;
    const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
    if (!dbPassword) {
      throw new OperonError('DB_PASSWORD or PGPASSWORD environment variable not set');
    }

    this.poolConfig = {
      host: dbConfig.hostname,
      port: dbConfig.port,
      user: dbConfig.username,
      password: dbPassword,
      connectionTimeoutMillis: dbConfig.connectionTimeoutMillis,
      // database: 'operon',
      database: 'postgres',
    };

    // Logic to parse Operon DB schema.
    if (!dbConfig.schemaFile) {
      throw new OperonError(`Operon configuration ${CONFIG_FILE} does not contain a DB schema file`);
    }
    const schemaPath: string = path.join(currentDirectory, '..', SCHEMAS_DIR, dbConfig.schemaFile);

    // Check whether it is a valid file.
    try {
      fs.stat(schemaPath, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
        if (error) {
          throw new OperonError(`checking on ${schemaPath}. Errno: ${error.errno}`);
        } else if (!stats.isFile()) {
          throw new OperonError(`config file ${schemaPath} is not a file`);
        }
      });
    } catch (error) {
      if (error instanceof Error) {
        throw new OperonError(`calling fs.stat on ${schemaPath}: ${error.message}`);
      }
    }

    try {
      // TODO eventually we should handle all path absolute, with path.join & co
      this.operonDbSchema = fs.readFileSync(schemaPath, 'utf8')
    } catch(error) {
      if (error instanceof Error) {
        throw new OperonError(
          `parsing Operon DB schema file ${dbConfig.schemaFile}: ${error.message}`
        );
      }
    }
    if (this.operonDbSchema === '') {
      throw new OperonError(`Operon DB schema ${dbConfig.schemaFile} is empty`);
    }
  }
}
