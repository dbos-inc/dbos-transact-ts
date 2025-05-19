<div align="center">

# DBOS Transact: A Lightweight Durable Execution Library Built on Postgres

#### [Documentation](https://docs.dbos.dev/) &nbsp;&nbsp;•&nbsp;&nbsp; [Examples](https://docs.dbos.dev/examples) &nbsp;&nbsp;•&nbsp;&nbsp; [Github](https://github.com/dbos-inc) &nbsp;&nbsp;•&nbsp;&nbsp; [Discord](https://discord.com/invite/jsmC6pXGgX)

</div>

---

DBOS Transact is a TypeScript library for **ultra-lightweight durable execution**.
For example:

```javascript
class Example {
  @DBOS.step()
  static async step_one() {
    ...
  }

  @DBOS.step()
  static async step_two() {
    ...
  }

  @DBOS.workflow()
  static async workflow() {
    await Example.step_one()
    await Example.step_two()
  }
}
```

Durable execution means persisting the execution state of your program while it runs, so if it is ever interrupted or crashes, it automatically resumes from where it left off.
Durable execution helps solve many common problems:

- Orchestrating long-running or business-critical workflows so they seamlessly recover from any failure.
- Running reliable background jobs with no timeouts.
- Processing incoming events (e.g. from Kafka) exactly once.
- Running a fault-tolerant distributed task queue.
- Running a reliable cron scheduler.
- Operating an AI agent, or anything that connects to an unreliable or non-deterministic API.

What’s unique about DBOS's implementation of durable execution is that it’s implemented in a **lightweight library** that’s **totally backed by Postgres**.
To use DBOS, just `npm install` it and annotate your program with DBOS decorators.
Under the hood, those decorators store your program's execution state (which workflows are currently executing and which steps they've completed) in a Postgres database.
If your program crashes or is interrupted, they automatically recover its workflows from their stored state.
So all you need to use DBOS is Postgres&mdash;there are no other dependencies you have to manage, no separate workflow server.

One big advantage of this approach is that you can add DBOS to **any** TypeScript application&mdash;**it’s just a library**.
For example, you can use DBOS to add reliable background jobs or cron scheduling or queues to your Next.js app with no external dependencies except Postgres.

## Getting Started

Initialize a starter app with:

```shell
npx @dbos-inc/create -t dbos-node-starter
```

Or, if you want to use the integration with Next.js:

```shell
npx @dbos-inc/create -t dbos-nextjs-starter
```

Then build and run your app with:

```shell
npm install
npm run build
npm run start
```

Visit your app in your browser at [`localhost:3000`](http://localhost:3000) to see durable execution in action!

To learn how to build more complex workflows, check out the [programming guide](https://docs.dbos.dev/typescript/programming-guide) or [docs](https://docs.dbos.dev/).

## Documentation

[https://docs.dbos.dev](https://docs.dbos.dev)

## Community

If you're interested in building with us, please star our repository and join our community on [Discord](https://discord.gg/fMwQjeW5zg)!
If you see a bug or have a feature request, don't hesitate to open an issue here on GitHub.
If you're interested in contributing, check out our [contributions guide](./CONTRIBUTING.md).

Fake change to trigger build
