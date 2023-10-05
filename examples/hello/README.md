# Operon Hello

This is an [Operon app](https://dbos-inc.github.io/operon-docs/) bootstrapped with `operon init`.

## Getting Started

First, set up the database.
Operon works with any Postgres database, but to make things easier, we've provided nifty scripts that start Postgres locally in a Docker container and set up some tables:

```bash
./start_postgres_docker.sh
export PGPASSWORD=dbos
node init_database.js
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