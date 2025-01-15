// Welcome to DBOS!

// This is a sample "Hello" app built with DBOS.
// It greets visitors and keeps track of how many times each visitor has been greeted.

// First, let's import DBOS
import { ArgSource, ArgSources, DBOS } from "@dbos-inc/dbos-sdk";

// Then, let's declare a type representing the "dbos_hello" database table
export interface dbos_hello {
  name: string;
  greet_count: number;
}

// Now let's define a class with some static functions.
// DBOS uses TypeScript decorators to automatically make your functions reliable, so they need to be static.
export class Hello {
  // Serve this function from HTTP GET requests at the /greeting endpoint with 'user' as a path parameter
  @DBOS.getApi("/greeting/:user")
  @DBOS.transaction() // Run this function as a database transaction
  static async helloTransaction(@ArgSource(ArgSources.URL) user: string) {
    // Retrieve and increment the number of times this user has been greeted.
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const { rows } = (await DBOS.knexClient.raw(query, [user])) as { rows: dbos_hello[] };
    const greet_count = rows[0].greet_count;
    const greeting = `Hello, ${user}! You have been greeted ${greet_count} times.`;
    return Hello.makeHTML(greeting);
  }

  // Serve a quick readme for the app at the / endpoint
  @DBOS.getApi("/")
  static async readme() {
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
              To learn how to run this app yourself, visit our
              <a href="https://docs.dbos.dev/quickstart?language=typescript" class="text-blue-600 hover:underline">Quickstart</a>.
          </p><p class="mb-2">
              Then, to learn how to build crashproof apps, continue to our
              <a href="https://docs.dbos.dev/typescript/programming-guide" class="text-blue-600 hover:underline">Programming Guide</a>.<br>
          </p>
      </body>
      </html>`;
    return page;
  }
}
