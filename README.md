# DBOS Transact

DBOS Transact is a **modern TypeScript framework** for backend applications.

You want to build your next application with DBOS Transact because you need:

- **Durable execution**.  If your app is interrupted for any reason, it [automatically resumes from where it left off](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial).  Reliable message delivery is [built in](https://docs.dbos.dev/typescript/tutorials/workflow-communication-tutorial). Idempotency is [built in](https://docs.dbos.dev/typescript/tutorials/idempotency-tutorial).
- **Built-in observability**. Automatically emit [OpenTelemetry](https://opentelemetry.io/)-compatible [logs and traces](https://docs.dbos.dev/typescript/tutorials/logging) from any application. Query your app's history from the [command line](https://docs.dbos.dev/typescript/reference/cli#workflow-management-commands) or [with SQL](https://docs.dbos.dev/explanations/system-tables).
- **A framework built for the tools you love**. Build with TypeScript and **any** PostgreSQL-compatible database. Use raw SQL or your favorite query builder or ORM&mdash;we support [Drizzle](https://docs.dbos.dev/typescript/tutorials/using-drizzle), [Knex](https://docs.dbos.dev/typescript/tutorials/using-knex), [TypeORM](https://docs.dbos.dev/typescript/tutorials/using-typeorm), and [Prisma](https://docs.dbos.dev/typescript/tutorials/using-prisma) out of the box.
- **Blazing-fast, developer-friendly serverless**.  Develop your project locally and run it anywhere. When you're ready, [deploy it for free to DBOS Cloud](https://docs.dbos.dev/quickstart) and we'll host it for you, [25x faster](https://www.dbos.dev/blog/dbos-vs-aws-step-functions-benchmark) and [15x cheaper](https://www.dbos.dev/blog/dbos-vs-lambda-cost) than AWS Lambda.

## Getting Started

The fastest way to get started is by following the [quickstart](https://docs.dbos.dev/getting-started/quickstart), where you'll learn how to get a DBOS Transact application running in less than five minutes.

## Documentation

Check out the full documentation at [https://docs.dbos.dev/](https://docs.dbos.dev/).

## Main Features

Here are some of the core features of DBOS Transact:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Transactions](https://docs.dbos.dev/typescript/tutorials/transaction-tutorial)                              | Easily and safely query your application database using [Drizzle](https://docs.dbos.dev/typescript/tutorials/using-drizzle), [Knex](https://docs.dbos.dev/typescript/tutorials/using-knex), [TypeORM](https://docs.dbos.dev/typescript/tutorials/using-typeorm), [Prisma](https://docs.dbos.dev/typescript/tutorials/using-prisma), or raw SQL.
| [Workflows](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial)                                    | Reliable workflow orchestration&#8212;resume your program after any failure.
| [HTTP Serving](https://docs.dbos.dev/typescript/tutorials/http-serving-tutorial)                             | Set up endpoints to serve requests from your application.
| [Idempotency](https://docs.dbos.dev/typescript/tutorials/idempotency-tutorial)                               | Automatically make any request idempotent, so your requests happen exactly once.
| [Authentication and Authorization](https://docs.dbos.dev/typescript/tutorials/authentication-authorization)  | Secure your HTTP endpoints so only authorized users can access them.
| [Kafka Integration](https://docs.dbos.dev/typescript/tutorials/kafka-integration)                            | Consume Kafka messages exactly-once with transactions or workflows.
| [Scheduled Workflows](https://docs.dbos.dev/typescript/tutorials/scheduled-workflows)                        | Schedule your workflows to run exactly-once per time interval with cron-like syntax.
| [Testing and Debugging](https://docs.dbos.dev/typescript/tutorials/testing-tutorial)                         | Easily write unit tests for your applications, compatible with Jest and other popular testing frameworks.
| [Self-Hosting](https://docs.dbos.dev/typescript/tutorials/self-hosting)                                      | Host your applications anywhere, as long as they have a Postgres database to connect to.

And DBOS Cloud:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Serverless App Deployment](https://docs.dbos.dev/cloud-tutorials/application-management)      | Deploy apps to DBOS Cloud in minutes.
| [Interactive Time Travel](https://docs.dbos.dev/cloud-tutorials/interactive-timetravel)        | Query your application database as of any past point in time.
| [Time Travel Debugging](https://docs.dbos.dev/cloud-tutorials/timetravel-debugging)            | Replay any DBOS Cloud trace locally on your computer.
| [Cloud Database Management](https://docs.dbos.dev/cloud-tutorials/database-management)         | Provision cloud Postgres instances for your applications. Alternatively, [bring your own database](https://docs.dbos.dev/cloud-tutorials/byod-management).
| [Built-in Observability](https://docs.dbos.dev/cloud-tutorials/monitoring-dashboard)           | Built-in log capture, request tracing, and dashboards.

## Community

If you're interested in building with us, please star our repository and join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
