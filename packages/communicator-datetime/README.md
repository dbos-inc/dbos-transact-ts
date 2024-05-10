# DBOS Date/Time Communicator

This is a [DBOS](https://docs.dbos.dev/) [communicator](https://docs.dbos.dev/tutorials/communicator-tutorial) for getting the current date / time.

The reason that date retrieval should be wrapped in a communicator is so that replayed workflows get the recorded value and therefore have the same behavior as the original.

## Available Functions

### `getCurrentDate()`

This function returns a `Date` object representing the current clock time.

### `getCurrentTime()`
This function returns a `number` of milliseconds since January 1, 1970, UTC, in the same manner as `new Date().getTime()`.


## Next Steps
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
