# DBOS Hello

This is a [DBOS app](https://docs.dbos.dev/) bootstrapped with `npx @dbos-inc/create` and using [Knex](https://docs.dbos.dev/typescript/tutorials/orms/using-knex).

## Getting Started

Before you can launch your app, you need a database.
DBOS works with any Postgres database, but to make things easier, we've provided a script that starts a Docker Postgres container and creates a database.
Run:

```bash
node start_postgres_docker.js
```

If successful, the script should print `Database started successfully!`.

To build the app, set up the database, and run, in one step:
```bash
npm run dev
```
This uses `nodemon`, so if you change your app it will automatically restart with changes.

To see that it's working, visit this URL in your browser: [`http://localhost:3000/greeting/dbos`](http://localhost:3000/greeting/dbos).
You should get this message: `Hello, dbos! You have been greeted 1 times.`
Each time you refresh the page, the counter should go up by one!

Congratulations! You just launched a DBOS application.

### Separate Build and Run Steps

To build the app:
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
npx dbos start
```

## Next Steps

- To add more functionality to this application, modify `src/operations.ts`, and save it.  If you are using, `npm run dev`, `nodemon` will rebuild and restart the app automatically.
- For a detailed tutorial, check out our [programming quickstart](https://docs.dbos.dev/typescript/programming-guide).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/quickstart)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc).
