# DBOS Hello with TypeORM

This is a [DBOS app](https://docs.dbos.dev/) bootstrapped with `npx @dbos-inc/create` and using [TypeORM](https://docs.dbos.dev/typescript/tutorials/orms/using-typeorm).

## Getting Started

Run these commands to build your app, set up its database, then launch it:

```bash
npm run build
npx dbos migrate
npm run start
```

To see that it's working, visit this URL in your browser: [`http://localhost:3000/`](http://localhost:3000/).

Congratulations! You just launched a DBOS application.

## Next Steps

- To add more functionality to this application, modify `src/main.ts`, then rebuild and restart it.  Alternatively, `npm run dev` uses `nodemon` to automatically rebuild and restart the app when source files change, using instructions specified in `nodemon.json`.
- For a detailed tutorial, check out the [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [the documentation](https://docs.dbos.dev/) or [source code](https://github.com/dbos-inc/dbos-transact-ts).
