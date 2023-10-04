# Operon Hello

This is an [Operon app](https://dbos-inc.github.io/operon-docs/) boostrapped with `operon init`.

## Getting Started

First, set up the database.
Operon works with any Postgres database, but to make things easier, we've provided a nifty script that starts Postgres locally in a Docker container:

```bash
./start_postgres_docker.sh
```

Then, set up some tables:

```bash
npm i
node init_database.js
```

Next, build and run the app:

```bash
npm run build
npx operon start
```

Open [http://localhost:3000/greeting/operon](http://localhost:3000/greeting/operon) to see the result.

Alternatively, use curl:

```bash
 curl -i http://localhost:3000/greeting/operon
```

You can add more functionality to the app by modifying `src/userFunctions.ts`, then re-building and re-starting it.

## Learn More

To learn more about Operon, take a look at [our documentation](https://dbos-inc.github.io/operon-docs/) or our [source code](https://github.com/dbos-inc/operon).