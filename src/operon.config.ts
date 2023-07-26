import { OperonError } from './error';

import { Pool } from 'pg';
import YAML from 'yaml'
import fs from 'fs'

const configFile: string = "operon-config.yaml";

export class OperonConfig {
  readonly pool: Pool;
  // We will add operonRoles: Role[] here in a next PR
  // We will add a "debug" flag here to be used in other parts of the codebase

  constructor() {
    // Check if configFile is a valid file
    fs.stat(configFile, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
      if (error) {
        throw new OperonError(`Config file ${configFile} does not exist`);
      } else if (!stats.isFile()) {
        throw new OperonError(`Config file ${configFile} is not a valid file`);
      }
    });

    // Logic to parse configFile. XXX We don't have a typed schema for the config file. Should we?
    const configFileContent: string = fs.readFileSync(configFile, 'utf8')
    const parsedConfig = YAML.parse(configFileContent) // eslint-disable-line @typescript-eslint/no-unsafe-assignment

    // Handle DB config
    const dbConfig = parsedConfig.database; // eslint-disable-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
    const dbPassword: string = // eslint-disable-line @typescript-eslint/no-unsafe-assignment
            dbConfig.password || // eslint-disable-line @typescript-eslint/no-unsafe-member-access
            process.env.DB_PASSWORD ||
            process.env.PGPASSWORD;
    this.pool = new Pool({
      host: dbConfig.hostname, // eslint-disable-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
      port: dbConfig.port, // eslint-disable-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
      user: dbConfig.username, // eslint-disable-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
      password: dbPassword,
      connectionTimeoutMillis: dbConfig.connectionTimeoutMillis, // eslint-disable-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
      database: 'postgres', // For now we use the default postgres database
    });
  }
}
