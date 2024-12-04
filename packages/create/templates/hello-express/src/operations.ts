// Welcome to DBOS!

// This is a sample "Hello" app built with DBOS, Express.js, and Knex.
// It greets visitors and keeps track of how many times each visitor has been greeted.

// First let's import express and DBOS
import express from "express";
import { DBOS } from "@dbos-inc/dbos-sdk";

// Then, let's declare a type representing the "dbos_hello" database table
export interface dbos_hello {
  name: string;
  greet_count: number;
}

// Now let's define a class with some static functions.
// DBOS uses TypeScript decorators to automatically make your functions reliable, so they need to be static.
export class Hello {
  // This function greets a user and increments the greet count in the database.
  // The @DBOS.transaction() decorator ensures that this function runs as a database transaction.
  @DBOS.transaction()
  static async helloTransaction(user: string) {
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    const { rows } = (await DBOS.knexClient.raw(query, [user])) as { rows: dbos_hello[] };
    const greet_count = rows[0].greet_count;
    const greeting = `Hello, ${user}! You have been greeted ${greet_count} times.`;
    return Hello.makeHTML(greeting);
  }

  // Finally, we will declare helper functions to serve static HTML to user.s

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

// Now, let's create an Express app and define some routes.
export const app = express();
// Parse JSON payloads and make it available to req.body
app.use(express.json());

// We'll serve the README at the root of the app
app.get("/", async (_, res) => {
  res.send(await Hello.readme());
});

// Serve this function from HTTP GET requests at the /greeting endpoint with 'user' as a path parameter
// The handler will in turn call a reliable DBOS operation (helloTransaction) to greet the user
app.get("/greeting/:user", async (req, res) => {
  const { user } = req.params;
  res.send(await Hello.helloTransaction(user));
});
