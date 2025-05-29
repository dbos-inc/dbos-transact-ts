# DBOS `bcrypt` Steps

This is a [DBOS](https://docs.dbos.dev/) [step](https://docs.dbos.dev/typescript/tutorials/step-tutorial) for generating bcrypt hashes.

The reason that some `bcrypt` operations should be wrapped in a `@DBOS.step` is that they generate random numbers. By using a step, replayed or restarted workflows will get the recorded value and therefore have the same behavior as the original.

## Available Functions

### `bcryptGenSalt(saltRounds?:number)`

`bcryptGenSalt` produces a random salt. Optional parameter is the number of rounds.

### `bcryptHash(txt: string, saltRounds?:number)`

`bcryptHash` generates a random salt and uses it to create a hash of `txt`.

## Examples

It is suggested to use the `BcryptStepV2` from this library. The functionality is the same, but invocation syntax is simpler.

```typescript
import { BcryptStep } from '@dbos-inc/dbos-bcrypt';
//...
const hashedPassword = await BcryptStep.bcryptHash(password);
//...
const isValid = await BcryptStep.bcryptCompare(password, hashedPassword);
```

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact-ts).
