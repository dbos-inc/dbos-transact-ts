import { TransactionContext, OperonTransaction, GetApi, HandlerContext } from '@dbos-inc/operon'
import { Knex } from 'knex';

// The schema of the database table used in this example.
interface operon_hello {
  name: string;
  greet_count: number;
}

export class Hello {

  @GetApi('/greeting/:user') // Serve this function from the /greeting endpoint with 'user' as a path parameter
  static async helloHandler(handlerCtxt: HandlerContext, user: string) {
    // Invoke helloTransaction to greet the user and track how many times they've been greeted.
    return handlerCtxt.invoke(Hello).helloTransaction(user);
  }

  @OperonTransaction()  // Declare this function to be a transaction.
  static async helloTransaction(txnCtxt: TransactionContext<Knex>, user: string) {
    // Retrieve and increment the number of times this user has been greeted.
    const rows = await txnCtxt.client<operon_hello>("operon_hello")
      // Insert greet_count for this user.
      .insert({ name: user, greet_count: 1 })
      // If already present, increment it instead.
      .onConflict("name").merge({ greet_count: txnCtxt.client.raw('operon_hello.greet_count + 1') })
      // Return the inserted or incremented value.
      .returning("greet_count");               
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }
}
