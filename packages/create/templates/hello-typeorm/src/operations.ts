// Welcome to DBOS!

// This is the Quickstart TypeORM template app. It greets visitors, counting how many total greetings were made.
// To learn how to run this app, visit the TypeORM tutorial: https://docs.dbos.dev/tutorials/using-typeorm

import { HandlerContext, TransactionContext, Transaction, GetApi, OrmEntities } from '@dbos-inc/dbos-sdk';
import { EntityManager } from "typeorm";
import { DBOSHello } from '../entities/DBOSHello';

@OrmEntities([DBOSHello])
export class Hello {

  @GetApi('/greeting/:name')
  @Transaction()
  static async helloTransaction(txnCtxt: TransactionContext<EntityManager>, name: string) {
    const greeting = `Hello, ${name}!`;
    let entity = new DBOSHello();
    entity.greeting = greeting;
    entity = await txnCtxt.client.save(entity);
    const greeting_note =  `Greeting ${entity.greeting_id}: ${greeting}`;
    return Hello.makeHTML(greeting_note);
  }

  // Serve a quick readme for the app at the / endpoint
  @GetApi('/')
  static async readme(_ctxt: HandlerContext) {
    const message = Hello.makeHTML(
      `Visit the route <code class="bg-gray-100 px-1 rounded">/greeting/{name}</code> to be greeted!<br>
      For example, visit <code class="bg-gray-100 px-1 rounded"><a href="/greeting/Mike" class="text-blue-600 hover:underline">/greeting/Mike</a></code><br>
      The counter increments with each page visit.`
    );
    return Promise.resolve(message);
  }

  // A helper function to create HTML pages with some styling
  static makeHTML(message: string) {
    const page = `
      <!DOCTYPE html>
      <html lang="en">
      <head>
          <title>DBOS Template App</title>
          <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="font-sans text-gray-800 p-6 max-w-2xl mx-auto">
          <h1 class="text-3xl font-semibold mb-4">Welcome to DBOS!</h1>
          <p class="mt-8 mb-8">` + message + `</p>
          <p class="mb-2">
              This is the TypeORM quickstart template app. Read the documentation for it <a href="https://docs.dbos.dev/tutorials/using-typeorm" class="text-blue-600 hover:underline">here</a>.
          </p>
      </body>
      </html>`;
    return page;
  }
}
