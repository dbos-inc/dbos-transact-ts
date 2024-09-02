// Welcome to DBOS!

// This is the Quickstart Drizzle template app. It greets visitors, counting how many total greetings were made.
// To learn how to run this app, visit the Drizzle tutorial: https://docs.dbos.dev/tutorials/using-drizzle

import { HandlerContext, TransactionContext, Transaction, GetApi } from '@dbos-inc/dbos-sdk';
import { dbosHello } from './schema';
import { NodePgDatabase } from 'drizzle-orm/node-postgres';

export class Hello {

  // Serve this function from HTTP GET requests at the /greeting endpoint with 'user' as a path parameter
  @GetApi('/greeting/:user')
  @Transaction()
  static async helloTransaction(ctxt: TransactionContext<NodePgDatabase>, user: string) {
    const greeting = `Hello, ${user}!`;
    const greetings_output = await ctxt.client.insert(dbosHello).values({greeting}).returning({greet_count: dbosHello.greet_count});
    const greeting_message = `${greeting} We have made ${greetings_output[0].greet_count} greetings.`;
    return Hello.makeHTML(greeting_message);
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
              This is the Drizzle quickstart template app. Read the documentation for it <a href="https://docs.dbos.dev/tutorials/using-drizzle" class="text-blue-600 hover:underline">here</a>.
          </p>
      </body>
      </html>`;
    return page;
  }
}
