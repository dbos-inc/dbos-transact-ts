# Contributing to DBOS Transact

Thank you for considering contributing to DBOS Transact. We welcome contributions from everyone, including bug fixes, feature enhancements, documentation improvements, or any other form of contribution.

## How to Contribute

To get started with DBOS Transact, please read the [README](README.md).

You can contribute in many ways. Some simple ways are:

- Use the SDK and open issues to report bugs, questions, or concerns with the SDK or its documentation.
- Respond to issues with advice or suggestions.
- Participate in discussions in our [Discord](https://discord.gg/fMwQjeW5zg) channel.
- Contribute fixes and improvements to the code. The documentation lives in [dbos-inc/dbos-docs](https://github.com/dbos-inc/dbos-docs).

### To contribute code, please follow these steps:

1. Fork this GitHub repository to your own account.

2. Clone the forked repository to your local machine.

3. Create a branch.

4. Make your change, following [Development](#development) below.

5. Write tests.

6. Commit the changes to your forked repository.

7. Submit a pull request to this repository.
   In the PR description please include:

- Description of the fix/feature.
- Brief description of implementation.
- Description of how you tested the fix.

## Development

You need Node.js 20 or later and a Postgres server. The tests expect Postgres on `localhost:5432` with user `postgres` and the password in `PGPASSWORD` (default `dbos`). One way to run it:

```shell
docker run -d --name dbos-postgres -p 5432:5432 -e POSTGRES_PASSWORD=dbos postgres:16
```

Install, build, and lint:

```shell
npm ci         # also installs a pre-commit hook that formats staged files with Prettier
npm run build  # compiles the SDK and the packages in packages/
npm run lint
```

Build before testing: some tests run the compiled CLI from `dist/`.

Run a single test file, or the whole suite:

```shell
npx jest tests/dbos.test.ts
npm run test:unit
```

The tests share one database, so run them one at a time; when passing several files to `jest` yourself, add `--runInBand`. To use a different server, set `DBOS_TEST_DB_URL` (for example `postgresql://postgres:dbos@localhost:5432/dbostest`); the tests put their system database in `<database name>_dbos_sys`. The CockroachDB tests run only when `DBOS_COCKROACHDB_URL` is set.

`npm run test:packages` runs the tests for each package in `packages/`. The Kafka receiver packages also need a broker on `localhost:9092` (or set `KAFKA_BROKER`):

```shell
docker run -d --name dbos-kafka -p 9092:9092 apache/kafka
```

## Requesting features

If you have a feature request or an idea for an enhancement, feel free to open an issue on GitHub. Describe the feature or enhancement you'd like to see and why it would be valuable. Discuss it with the community on the [Discord](https://discord.gg/fMwQjeW5zg) channel.

## Discuss with the community

If you are stuck, need help, or wondering if a certain contribution will be welcome, please ask! You can reach out to us on [Discord](https://discord.gg/fMwQjeW5zg) or open an issue on GitHub.

## Code of conduct

It is important to us that contributing to DBOS will be a pleasant experience, if necessary, please refer to our [code of conduct](CODE_OF_CONDUCT.md) for participation guidelines.
