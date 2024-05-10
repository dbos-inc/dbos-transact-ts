# DBOS Random Communicator

This is a [DBOS](https://docs.dbos.dev/) [communicator](https://docs.dbos.dev/tutorials/communicator-tutorial) for generating random numbers.

The reason that random number generation should be wrapped in a communicator is so that replayed workflows get the recorded value and therefore have the same behavior as the original.

## Available Functions

### `random()`
`random` is a wrapper for `Math.random()` and similarly produces a `number` in the range from 0 to 1.

## Next Steps
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
