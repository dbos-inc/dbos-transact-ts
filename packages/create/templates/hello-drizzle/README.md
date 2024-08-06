# DBOS Hello

This is a [DBOS app](https://docs.dbos.dev/) bootstrapped with `npx @dbos-inc/dbos-sdk init`.

## Getting Started

Before you can launch your app, you need a database.
DBOS works with any Postgres database, but to make things easier, we've provided a script that starts a Docker Postgres container and creates a database.
Run:

```bash
node start_postgres_docker.js
```

If successful, the script should print `Database started successfully!`.

Next, build the app:

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

To see that it's working, visit this URL in your browser: [`http://localhost:3000/greeting/dbos`](http://localhost:3000/greeting/dbos).
You should get this message: `Hello, dbos! You have been greeted 1 times.`
Each time you refresh the page, the counter should go up by one!

Congratulations! You just launched a DBOS application.

## Next Steps

- To add more functionality to this application, modify `src/operations.ts`, then rebuild and restart it.  Alternatively, `npm run dev` uses `nodemon` to automatically rebuild and restart the app when source files change, using instructions specified in `nodemon.json`.
- For a detailed tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
