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