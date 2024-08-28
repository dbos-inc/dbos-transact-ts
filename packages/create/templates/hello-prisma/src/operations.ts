import { HandlerContext, TransactionContext, Transaction, GetApi } from '@dbos-inc/dbos-sdk';
import { PrismaClient } from "@prisma/client";

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

  @GetApi('/greeting/:name')
  @Transaction()
  static async helloTransaction(txnCtxt: TransactionContext<PrismaClient>, name: string)  {
    const greeting = `Hello, ${name}!`;
    const res = await txnCtxt.client.dbosHello.create({
      data: {
        greeting: greeting,
      },
    });
    const greeting_note = `Greeting ${res.greeting_id}: ${greeting}`;
    const page = `<html><body><p>${greeting_note}<br><br>${app_notes}</p></body></html>`;
    return page;
  }
}
