import { HandlerContext, TransactionContext, Transaction, GetApi } from '@dbos-inc/dbos-sdk';
import { dbosHello } from './schema';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';

const app_notes = `
To learn how to run this app on your computer, visit the
<a href="https://docs.dbos.dev/getting-started/quickstart" >DBOS Quickstart</a>.<br>
After that, to learn how to build apps, visit the
<a href="https://docs.dbos.dev/getting-started/quickstart-programming" >DBOS Programming Guide</a>.`;

export class Hello {

  @GetApi('/') // Serve a quick readme for the app
  static async readme(_ctxt: HandlerContext) {
    const readme = `<html><body><p>
           Welcome to the DBOS Hello App!<br><br>
           Visit the route /greeting/:name to be greeted!<br>
           For example, visit <a href="/greeting/dbos">/greeting/dbos</a>.<br>
           The counter increments with each page visit.<br><br>
           ${app_notes}
           </p></body></html>`;
    return Promise.resolve(readme);
  }

  @GetApi('/greeting/:user')
  @Transaction()
  static async helloTransaction(ctxt: TransactionContext<NodePgDatabase>, user: string) {
    const greeting = `Hello, ${user}!`;
    const greetings_output = await ctxt.client.insert(dbosHello).values({greeting}).returning({greet_count: dbosHello.greet_count});
    const greeting_message = `${greeting} We have made ${greetings_output[0].greet_count} greetings.`;
    const page = `<html><body><p>${greeting_message}<br><br>${app_notes}</p></body></html>`;
    return page;
  }
}
