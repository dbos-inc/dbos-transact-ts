# DBOS Date/Time Steps

This is a [DBOS](https://docs.dbos.dev/) [step](https://docs.dbos.dev/typescript/tutorials/step-tutorial) for getting the current date / time.

The reason that date retrieval should be wrapped in a `@DBOS.step` is so that replayed workflows get the recorded value and therefore have the same behavior as the original run.

## Available Functions

### `DBOSDateTime.getCurrentDate()`

This function returns a `Date` object representing the current clock time.

### `DBOSDateTime.getCurrentTime()`

This function returns a `number` of milliseconds since January 1, 1970, UTC, in the same manner as `Date.now()`.

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS Transact programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
