import { TransactionContext, OperonTransaction, GetApi, ArgSource, ArgSources } from '@dbos-inc/operon'
import { Knex } from 'knex';

// The schema of the database table used in this example.
export interface operon_hello {
  name: string;
  greet_count: number;
}

export class Hello {

  @GetApi('/greeting/:user') // Serve this function from HTTP GET requests to the /greeting endpoint with 'user' as a path parameter
  @OperonTransaction()  // Run this function as a database transaction
  static async helloTransaction(ctxt: TransactionContext<Knex>, @ArgSource(ArgSources.URL) user: string) {
    // Retrieve and increment the number of times this user has been greeted.
    const query = "INSERT INTO operon_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = operon_hello.greet_count + 1 RETURNING greet_count;"
    const { rows } = await ctxt.client.raw(query, [user]) as { rows: operon_hello[] };
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }
}
