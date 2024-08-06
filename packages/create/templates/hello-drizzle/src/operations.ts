import { HandlerContext, TransactionContext, Transaction, GetApi, ArgSource, ArgSources } from '@dbos-inc/dbos-sdk';
import { DBOSHello } from './schema';
import { sql } from 'drizzle-orm';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';

export class Hello {

  @GetApi('/') // Serve a quick readme for the app
  static async readme(_ctxt: HandlerContext) {
    const readme = '<html><body><p>' +
           'Welcome to the DBOS Hello App!<br><br>' +
           'Visit the route /greeting/:name to be greeted!<br>' +
           'For example, visit <a href="/greeting/dbos">/greeting/dbos</a>.<br>' +
           'The counter increments with each page visit.<br>' +
           'If you visit a new name like <a href="/greeting/alice">/greeting/alice</a>, the counter starts at 1.' +
           '</p></body></html>';
    return Promise.resolve(readme);
  }

  @GetApi('/greeting/:user') // Serve this function from HTTP GET requests to the /greeting endpoint with 'user' as a path parameter
  @Transaction()  // Run this function as a database transaction
  static async helloTransaction(ctxt: TransactionContext<NodePgDatabase>, @ArgSource(ArgSources.URL) user: string) {
    const output = await ctxt.client.execute(sql`
      INSERT INTO ${DBOSHello} (name, greet_count)
      VALUES (${user}, 1)
      ON CONFLICT (name) DO UPDATE
      SET greet_count = ${DBOSHello.greet_count} + 1
      RETURNING greet_count
    `);
    const greet_count = output.rows[0].greet_count as number;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }
}
