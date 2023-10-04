const { Client } = require('pg');

const POSTGRES_HOST = process.env.POSTGRES_HOST || "localhost";

const executeQuery = async (client, query) => {
    console.log(query);
    return await client.query(query);
};

const createUserAndDb = async () => {
    const adminClient = new Client({
        host: POSTGRES_HOST,
        user: 'postgres',
        password: process.env.PGPASSWORD
    });

    await adminClient.connect();

    // Attempt to create the new user 'hello'
    try {
        await executeQuery(adminClient, "CREATE USER hello WITH PASSWORD 'hello';");
    } catch (error) {
        if (!error.message.includes('already exists')) {
            throw error;
        }
        console.log("User 'hello' already exists. Continuing...");
    }

    await executeQuery(adminClient, "ALTER USER hello CREATEDB;");

    // Check if the database 'hello' exists before trying to drop it
    const dbExistsResult = await executeQuery(adminClient, "SELECT 1 FROM pg_database WHERE datname='hello';");

    if (dbExistsResult.rowCount > 0) {
        await executeQuery(adminClient, "DROP DATABASE hello;");
    }

    // Connect as hello user to create the DB
    const helloClient = new Client({
        host: POSTGRES_HOST,
        user: 'hello',
        password: 'hello',
        database: 'postgres',
    });

    await helloClient.connect();
    await executeQuery(helloClient, "CREATE DATABASE hello;");

    // Grant permissions
    await executeQuery(adminClient, "GRANT CREATE, USAGE ON SCHEMA public TO hello;");

    await adminClient.end();
    await helloClient.end();
};

const createTables = async () => {
    const helloClient = new Client({
        host: POSTGRES_HOST,
        user: 'hello',
        password: 'hello',
        database: 'hello'
    });

    await helloClient.connect();

    await executeQuery(helloClient, "CREATE TABLE IF NOT EXISTS OperonHello (greeting_id SERIAL PRIMARY KEY, greeting TEXT);");

    await helloClient.end();
};

(async () => {
    await createUserAndDb();
    await createTables();
})();
