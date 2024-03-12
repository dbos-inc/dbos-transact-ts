# DBOS TypeScript SDK

DBOS is a **transactional serverless** SDK and platform that helps you develop and deploy database-backed TypeScript applications.
You develop your applications in TypeScript and PostgreSQL with this SDK, test them locally, then deploy them to DBOS Cloud in minutes.

You want to build your next database-backed application with DBOS because:

- **It's simple**.  Write your business logic using serverless functions, test them locally, and deploy them to [DBOS Cloud](https://docs.dbos.dev/getting-started/quickstart-cloud) in minutes.  Store all your data in Postgres&#8212;we'll manage the connections and transactions for you.
- **It's reliable by default**.  If your workflows are interrupted for any reason, they [will always resume from where they left off](https://docs.dbos.dev/tutorials/workflow-tutorial#reliability-guarantees).  Reliable message delivery is [built in](https://docs.dbos.dev//tutorials/workflow-communication-tutorial#reliability-guarantees-1). Idempotency is [built in](https://docs.dbos.dev/tutorials/idempotency-tutorial).
- **It makes debugging easy**.  With our [time travel debugger](https://docs.dbos.dev/cloud-tutorials/timetravel-debugging), you can "rewind time" and replay any DBOS Cloud trace locally on your computer, exactly as it originally happened.

## Getting Started

The fastest way to get started with DBOS is by following the [quickstart](https://docs.dbos.dev/getting-started/quickstart), where you'll learn how to get a DBOS application running in less than five minutes.

## Main Features

Here are some of the core features of the DBOS TypeScript SDK:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Transactions](https://docs.dbos.dev/tutorials/transaction-tutorial)                              | Easily and safely query your application database
| [Workflows](https://docs.dbos.dev/tutorials/workflow-tutorial)                                    | Reliable workflow orchestration&#8212;resume your program after any failure.
| [HTTP Serving](https://docs.dbos.dev/tutorials/http-serving-tutorial)                             | Set up endpoints to serve requests from your application.
| [Idempotency](https://docs.dbos.dev/tutorials/idempotency-tutorial)                               | Automatically make any request idempotent, so your requests happen exactly once.
| [Authentication and Authorization](https://docs.dbos.dev/tutorials/authentication-authorization)  | Secure your HTTP endpoints so only authorized users can access them.
| [Testing and Debugging](https://docs.dbos.dev/tutorials/testing-tutorial)                         | Easily write unit tests for your applications, compatible with Jest and other popular testing frameworks.

And DBOS Cloud:

| Feature                                                                       | Description
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| [Serverless App Deployment](https://docs.dbos.dev/cloud-tutorials/application-management)      | Deploy apps to DBOS Cloud in minutes.
| [Time Travel Debugging](https://docs.dbos.dev/cloud-tutorials/timetravel-debugging)            | Replay any DBOS Cloud trace locally on your computer.
| [Cloud Database Management](https://docs.dbos.dev/cloud-tutorials/database-management)         | Provision cloud Postgres instances for your applications.
| [Built-in Observability](https://docs.dbos.dev/cloud-tutorials/monitoring-dashboard)           | Built-in log capture, request tracing, and dashboards.

## Documentation

You can find our full documentation at [https://docs.dbos.dev/](https://docs.dbos.dev/).

Check out our [Getting Started](https://docs.dbos.dev/getting-started/) for an overview of how to start with DBOS.

Our documentation has the following sections:

- [Getting Started](https://docs.dbos.dev/getting-started/)
- [DBOS SDK Tutorials](https://docs.dbos.dev/category/dbos-sdk-tutorials)
- [DBOS Cloud Tutorials](https://docs.dbos.dev/category/dbos-cloud-tutorials)
- [API Reference](https://docs.dbos.dev/category/reference)
- [Concepts and Explanations](https://docs.dbos.dev/category/concepts-and-explanations)

## Community

Please join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).
