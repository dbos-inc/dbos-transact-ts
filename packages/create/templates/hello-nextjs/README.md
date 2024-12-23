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

```

## Production build

In production, instead of using `nodemon`, the following separate steps should be used to build, run database setup, and start the app.

```bash
npm run build
```

Then, run a schema migration to create some tables:

```bash
npx dbos migrate
```

If successful, the migration should print `Migration successful!`.

Finally, run the app:

```bash
npm run start
```

To see that it's working, visit this URL in your browser: [`http://localhost:3000/`](http://localhost:3000/).

Click on the "Run DBOS Workflow" button.

You should get this message: `Hello, dbos! You have been greeted 1 times.`
Each time you refresh the page, the counter should go up by one!

Congratulations! You just launched a DBOS application.

## The application

- In `src/action/dbosWorkflow.tsx`, the DBOS workflow is created. When called, it increments a counter in the database and returns a greeting.

- The code below in the same file, launches the dbos runtime. This is required for DBOS runtime to start. Do not remove the code
```
if (process.env.NEXT_PHASE !== "phase-production-build") {
    await DBOS.launch();
}
```
- The workflow is called by the POST method in app/greetings/route.ts.

- The POST is called by the component in src/components/callDBOSWorkflow.tsx. It calls the route /greetings.

- The component is called from the main UI page.tsx.

- This is how the DBOS workflow is wired to the NextJs application.


 DBOS will wrap all routes with an [OpenTelemetry](https://opentelemetry.io/) tracing middleware and tie HTTP traces to DBOS workflow traces.

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

