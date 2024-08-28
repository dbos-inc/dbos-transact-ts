import { HandlerContext, TransactionContext, Transaction, GetApi, ArgSource, ArgSources } from '@dbos-inc/dbos-sdk';
import { Knex } from 'knex';

const app_notes = `
To learn how to run this app on your computer, visit the
<a href="https://docs.dbos.dev/getting-started/quickstart" >DBOS Quickstart</a>.<br>
After that, to learn how to build apps, visit the
<a href="https://docs.dbos.dev/getting-started/quickstart-programming" >DBOS Programming Guide</a>.`;

// The schema of the database table used in this example.
export interface dbos_hello {
  name: string;
  greet_count: number;
}

export class Hello {

  @GetApi('/') // Serve a quick readme for the app
  static async readme(_ctxt: HandlerContext) {
    const readme = `<html><body><p>
           Welcome to the DBOS Hello App!<br><br>
           Visit the route /greeting/:name to be greeted!<br>
           For example, visit <a href="/greeting/dbos">/greeting/dbos</a>.<br>
           The counter increments with each page visit.<br>
           If you visit a new name like <a href="/greeting/alice">/greeting/alice</a>, the counter starts at 1.<br><br>
           ${app_notes}
           </p></body></html>`;
    return Promise.resolve(readme);
  }

  @GetApi('/greeting/:user') // Serve this function from HTTP GET requests to the /greeting endpoint with 'user' as a path parameter
  @Transaction()  // Run this function as a database transaction
  static async helloTransaction(ctxt: TransactionContext<Knex>, @ArgSource(ArgSources.URL) user: string) {
    // Retrieve and increment the number of times this user has been greeted.
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    const { rows } = await ctxt.client.raw(query, [user]) as { rows: dbos_hello[] };
    const greet_count = rows[0].greet_count;
    const greeting = `Hello, ${user}! You have been greeted ${greet_count} times.`;
    const page = `<html><body><p>${greeting}<br><br>${app_notes}</p></body></html>`;
    return page;
  }
}
