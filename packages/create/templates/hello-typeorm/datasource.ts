import { parseConfigFile } from '@dbos-inc/dbos-sdk/dist/src/dbos-runtime/config';
import { DataSource } from "typeorm";

const [dbosConfig, ] = parseConfigFile();

const AppDataSource = new DataSource({
    type: 'postgres',
    host: dbosConfig.poolConfig.host,
    port: dbosConfig.poolConfig.port,
    username: dbosConfig.poolConfig.user,
    password: dbosConfig.poolConfig.password,
    database: dbosConfig.poolConfig.database,
    entities: ['dist/entities/*.js'],
    migrations: ['dist/migrations/*.js'],
});

AppDataSource.initialize()
    .then(() => {
        console.log("Data Source has been initialized!");
    })
    .catch((err) => {
        console.error("Error during Data Source initialization", err);
    });

export default AppDataSource;