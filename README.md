# Operon

 Operon requires Node.js version 18. You may want to use [nvm](https://github.com/nvm-sh/nvm) to manage Node.js versions.
```shell
nvm install 18
```

Use `npm` to install dependencies, build, and test Operon:
```shell
npm install
npm run build
npm test
```

To run the Operon tests, you must have a Postgres database accessible on `localhost`.
This database must have the default `postgres` user with password `dbos` (or alternatively, set the password in `jest.setup.ts`).
Additionally, [`wal_level`](https://www.postgresql.org/docs/current/runtime-config-wal.html) must be set to `logical` in `postgresql.conf` and the [`wal2json`](https://github.com/eulerto/wal2json) Postgres plugin must be installed.

# Deploying a Serverless Operon Application

1.  First, build the app you want to deploy:

```shell
cd <app root folder>
npm link <operon repo path>
npm run build
```

We assume the app writes all Operon functions in decorated classes (with endpoint decorators) exported from src/userFunctions.ts.

2.  Then, start your app using the `operon` CLI:

```shell
npx operon start -p <port>
```

It should print the output:
```shell
[server]: Server is running at http://localhost:<port>
```

3.  Now your app is deployed and you can send HTTP requests to it.
For example, in the `hello` sample app (examples/hello), open your browser to  `http://localhost:3000/greeting/{name}` and see the output!