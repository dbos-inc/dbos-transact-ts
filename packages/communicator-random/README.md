# DBOS Random Step

This is a [DBOS](https://docs.dbos.dev/) [step](https://docs.dbos.dev/typescript/tutorials/step-tutorial) for generating random numbers.

The reason that random number generation should be wrapped in a `@DBOS.step` is so that replayed workflows get the recorded value and therefore have the same behavior as the original.

## Available Functions

### `DBOSRandom.random()`

`DBOSRandom.random` is a wrapper for `Math.random()` and similarly produces a `number` in the range from 0 to 1.

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact-ts).
