# Operon Hello

This is an [Operon app](https://dbos-inc.github.io/operon-docs/) bootstrapped with `operon init`.

## Getting Started

First, start the database.
Operon works with any Postgres database, but to make things easier, we've provided a nifty script that starts Postgres locally in a Docker container and creates a database:

```bash
export PGPASSWORD=dbos
./start_postgres_docker.sh
```

Then, create some database tables.
In this quickstart, we use [knex.js](https://knexjs.org/) to manage database migrations.
Run our provided migration to create a database table:

```bash
npx knex migrate:latest
```

Next, build and run the app:

```bash
npm run build
npx operon start
```

Finally, curl the server to see that it's working!

```bash
 curl http://localhost:3000/greeting/operon
```

You can add more functionality to the app by modifying `src/userFunctions.ts`, then re-building and re-starting it.

## Learn More

To learn more about Operon, take a look at [our documentation](https://dbos-inc.github.io/operon-docs/) or our [source code](https://github.com/dbos-inc/operon).