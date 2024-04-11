# DBOS Transact

DBOS Transact is a **transactional TypeScript framework** for developing database-backed applications with built-in once-and-only-once code execution.

You want to build your next application with DBOS Transact because:

- **It's reliable by default**.  If your workflows are interrupted for any reason, they [will always resume from where they left off](https://docs.dbos.dev/tutorials/workflow-tutorial#reliability-guarantees).  Reliable message delivery is [built in](https://docs.dbos.dev/tutorials/workflow-communication-tutorial#reliability-guarantees-1). Idempotency is [built in](https://docs.dbos.dev/tutorials/idempotency-tutorial).
- **It's simple**.  Write your business logic with serverless functions and either [self-host them](https://docs.dbos.dev/tutorials/self-hosting) or [deploy them to DBOS cloud](https://docs.dbos.dev/getting-started/quickstart#deploying-to-dbos-cloud) in minutes. Store your data in any PostgreSQL-compatible database&mdash;we'll manage the transactions for you.
- **It makes debugging easy**.  With our [time travel debugger](https://docs.dbos.dev/cloud-tutorials/timetravel-debugging), you can "rewind time" and replay any DBOS Cloud trace locally on your computer, exactly as it originally happened.

## Getting Started

The fastest way to get started is by following the [quickstart](https://docs.dbos.dev/getting-started/quickstart), where you'll learn how to get a DBOS Transact application running in less than five minutes.

## Main Features

Here are some of the core features of DBOS Transact:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Transactions](https://docs.dbos.dev/tutorials/transaction-tutorial)                              | Easily and safely query your application database
| [Workflows](https://docs.dbos.dev/tutorials/workflow-tutorial)                                    | Reliable workflow orchestration&#8212;resume your program after any failure.
| [HTTP Serving](https://docs.dbos.dev/tutorials/http-serving-tutorial)                             | Set up endpoints to serve requests from your application.
| [Idempotency](https://docs.dbos.dev/tutorials/idempotency-tutorial)                               | Automatically make any request idempotent, so your requests happen exactly once.
| [Authentication and Authorization](https://docs.dbos.dev/tutorials/authentication-authorization)  | Secure your HTTP endpoints so only authorized users can access them.
| [Kafka Integration](https://docs.dbos.dev/tutorials/kafka-integration)                            | Consume Kafka messages exactly-once with transactions or workflows.
| [Testing and Debugging](https://docs.dbos.dev/tutorials/testing-tutorial)                         | Easily write unit tests for your applications, compatible with Jest and other popular testing frameworks.
| [Self-Hosting](https://docs.dbos.dev/tutorials/self-hosting)                                      | Host your applications anywhere, as long as they have a Postgres database to connect to.

And DBOS Cloud:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Serverless App Deployment](https://docs.dbos.dev/cloud-tutorials/application-management)      | Deploy apps to DBOS Cloud in minutes.
| [Time Travel Debugging](https://docs.dbos.dev/cloud-tutorials/timetravel-debugging)            | Replay any DBOS Cloud trace locally on your computer.
| [Cloud Database Management](https://docs.dbos.dev/cloud-tutorials/database-management)         | Provision cloud Postgres instances for your applications.
| [Built-in Observability](https://docs.dbos.dev/cloud-tutorials/monitoring-dashboard)           | Built-in log capture, request tracing, and dashboards.

## Documentation

You can find our full documentation at [https://docs.dbos.dev/](https://docs.dbos.dev/).

Check out our [Getting Started](https://docs.dbos.dev/getting-started/) for an overview of how to start with DBOS Transact.

Our documentation has the following sections:

- [Getting Started](https://docs.dbos.dev/getting-started)
- [DBOS Transact Tutorials](https://docs.dbos.dev/category/dbos-transact-tutorials)
- [DBOS Cloud Tutorials](https://docs.dbos.dev/category/dbos-cloud-tutorials)
- [API Reference](https://docs.dbos.dev/category/reference)
- [Concepts and Explanations](https://docs.dbos.dev/category/concepts-and-explanations)

## Community

Please join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
