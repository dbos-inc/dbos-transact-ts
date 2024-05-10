# DBOS `bcrypt` Communicator

This is a [DBOS](https://docs.dbos.dev/) [communicator](https://docs.dbos.dev/tutorials/communicator-tutorial) for generating bcrypt hashes.

The reason that some `bcrypt` operations should be wrapped in a communicator is that they generate random numbers.  By using a communicator, replayed or restarted workflows will get the recorded value and therefore have the same behavior as the original.

## Available Functions

### `bcryptGenSalt(saltRounds?:number)`
`bcryptGenSalt` produces a random salt.  Optional parameter is the number of rounds.

### `bcryptHash(txt: string, saltRounds?:number)`
`bcryptHash` generates a random salt and uses it to create a hash of `txt`.

## Next Steps
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
