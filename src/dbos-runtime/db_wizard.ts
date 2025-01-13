import { PoolConfig } from "pg";

export async function db_wizard(poolConfig: PoolConfig): Promise<PoolConfig> {

    console.log("WIZARD");
    // 1. Check the connectivity to the database. Return if successful. If cannot connect, continue to the following steps.


    // 2. If the error is due to password authentication or the configuration is non-default, surface the error and exit.


    // 3. If the database config is the default one, check if the user has Docker properly installed.

    // 4. If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.

    // 5. If no Docker, then prompt the user to log in to DBOS Cloud and provision a DB there. Wait for the remote DB to be ready, and then create a copy of the original config file, and then load the remote connection string to the local config file.

    // 6. Save the config to the config file and return the updated config.
    // TODO: make the config file prettier

    return poolConfig
}

// async function checkDbConnectivity(config: PoolConfig): Promise<Error | null> {
//     // Add a default connection timeout if not specified
//     const configWithTimeout: PoolConfig = {
//         ...config,
//         connectionTimeoutMillis: config.connectionTimeoutMillis ?? 2000 // 2 second timeout
//     };

//     // Create a new pool with the configuration
//     const pool = new Pool(configWithTimeout);

//     try {
//         // Attempt to connect and execute a simple query
//         const client = await pool.connect();
//         try {
//             const result = await client.query('SELECT 1 as value');
            
//             // Verify the returned value
//             if (result.rows[0]?.value !== 1) {
//                 console.error(
//                     `Unexpected value returned from database: expected 1, received ${result.rows[0]?.value}`
//                 );
//                 return new Error('Unexpected database response');
//             }
            
//             return null;
//         } catch (error) {
//             return error instanceof Error ? error : new Error('Unknown database error');
//         } finally {
//             // Release the client back to the pool
//             client.release();
//         }
//     } catch (error) {
//         return error instanceof Error ? error : new Error('Failed to connect to database');
//     } finally {
//         // End the pool
//         await pool.end();
//     }
// }