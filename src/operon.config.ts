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

        // Logic to parse configFile
        const configFileContent: string = fs.readFileSync(configFile, 'utf8')
        const parsedConfig: any = YAML.parse(configFileContent) // XXX We could maybe have a typed format for the config...
        const dbConfig: any = parsedConfig.database;
        // Use config provided password first then attempt to parse env vars
        const dbPassword: string =
            dbConfig.password ||
            process.env.DB_PASSWORD ||
            process.env.PGPASSWORD;
        this.pool = new Pool({
            host: dbConfig.hostname,
            port: dbConfig.port,
            user: dbConfig.username,
            password: dbPassword,
            connectionTimeoutMillis: dbConfig.connectionTimeoutMillis,
            database: 'postgres', // For now we use the default postgres database
        });
    }
}
