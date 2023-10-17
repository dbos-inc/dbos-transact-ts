import { TransactionContext, OperonTransaction, GetApi, HandlerContext } from '@dbos-inc/operon'
import { Knex } from 'knex';

// The schema of the database table used in this example.
export interface operon_hello {
  name: string;
  greet_count: number;
}

export class Hello {

  @GetApi('/greeting/:user') // Serve this function from the /greeting endpoint with 'user' as a path parameter
  static async helloHandler(ctxt: HandlerContext, user: string) {
    return ctxt.invoke(Hello).helloTransaction(user);
  }

  @OperonTransaction()  // Declare this function to be a transaction.
  static async helloTransaction(ctxt: TransactionContext<Knex>, user: string) {
    // Retrieve and increment the number of times this user has been greeted.
    const rows = await ctxt.client<operon_hello>("operon_hello")
      .insert({ name: user, greet_count: 1 })
      .onConflict("name") // If user is already present, increment greet_count.
        .merge({ greet_count: ctxt.client.raw('operon_hello.greet_count + 1') })
      .returning("greet_count");
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }
}
