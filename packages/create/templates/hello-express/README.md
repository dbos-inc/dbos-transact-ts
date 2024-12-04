# DBOS Hello

This is a [DBOS app](https://docs.dbos.dev/) bootstrapped with `npx @dbos-inc/create`, using [Express.js](https://expressjs.com/) and [Knex](https://docs.dbos.dev/tutorials/using-knex) to interact with postgres.

## Getting Started

Before you can launch your app, you need a database.
DBOS works with any Postgres database, but to make things easier, we've provided a script that starts a Docker Postgres container and creates a database.
Run:

```bash
node start_postgres_docker.js
```

If successful, the script should print `Database started successfully!`.

Next, you can build and run the app in one step under `nodemon`:

```bash
npm run dev
```

To see that it's working, visit this URL in your browser: [`http://localhost:3000/greeting/dbos`](http://localhost:3000/greeting/dbos).
You should get this message: `Hello, dbos! You have been greeted 1 times.`
Each time you refresh the page, the counter should go up by one!

Congratulations! You just launched a DBOS application.

## Production build

In production, instead of using `nodemon`, the following separate steps should be used to build, run database setup, and start the app.

```bash
npm run build
```

Then, run a schema migration to create some tables:

```bash
npx dbos-sdk migrate
```

If successful, the migration should print `Migration successful!`.

Finally, run the app:

```bash
npx dbos-sdk start
```

## The application

- In `src/operations.ts`, the Express app object is created and configured to serve an "hello world" DBOS workflow on `/greetings/:user`. This file also hosts the code of said DBOS workflow: an `Hello` class with a single `helloTransaction` method.
- `src/main.ts` declares the code to start a DBOS instance and an Express application. When you pass the Express app object as parameter to `DBOS.launch()`, DBOS will wrap all routes with an [OpenTelemetry](https://opentelemetry.io/) tracing middleware and tie HTTP traces to DBOS workflow traces.

To add more functionality to this application, modify `src/operations.ts`. If you used `npm run dev`, it will automatically rebuild and restart.

## Running in DBOS Cloud

To deploy this app to DBOS Cloud, first install the DBOS Cloud CLI (example with [npm](https://www.npmjs.com/)):

```shell
npm i -g @dbos-inc/dbos-cloud
```

Then, run this command to deploy your app:

```shell
dbos-cloud app deploy
```

## Next Steps

- For a detailed tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
