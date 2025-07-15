// Welcome to DBOS!

// This is the Quickstart Drizzle template app. It greets visitors, counting how many total greetings were made.
// To learn how to run this app, visit the Drizzle tutorial: https://docs.dbos.dev/tutorials/using-drizzle

import express, { Request, Response } from 'express';
import { DBOS } from '@dbos-inc/dbos-sdk';
import { dbosHello } from './schema';

import { DrizzleDataSource } from '@dbos-inc/drizzle-datasource';

const config = {
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432'),
  database: process.env.PGDATABASE || 'dbos_drizzle',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || 'dbos',
};

const drizzleds = new DrizzleDataSource('app-db', config);

export class Hello {
  // This transaction uses DBOS and drizzle to perform database operations.
  @drizzleds.transaction()
  static async helloTransaction(user: string) {
    const greeting = `Hello, ${user}!`;
    const greetings_output = await drizzleds.client
      .insert(dbosHello)
      .values({ greeting })
      .returning({ greet_count: dbosHello.greet_count });
    const greeting_message = `${greeting} We have made ${greetings_output[0].greet_count} greetings.`;
    return makeHTML(greeting_message);
  }
}

// Let's create an HTML + CSS Readme for our app.
function readme() {
  return makeHTML(
    `Visit the route <code class="bg-gray-100 px-1 rounded">/greeting/{name}</code> to be greeted!<br>
    For example, visit <code class="bg-gray-100 px-1 rounded"><a href="/greeting/Mike" class="text-blue-600 hover:underline">/greeting/Mike</a></code><br>
    The counter increments with each page visit.`,
  );
}

function makeHTML(message: string) {
  const page =
    `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <title>DBOS Template App</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="font-sans text-gray-800 p-6 max-w-2xl mx-auto">
        <h1 class="text-3xl font-semibold mb-4">Welcome to DBOS!</h1>
        <p class="mt-8 mb-8">` +
    message +
    `</p>
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

// Let's create an HTTP server using Express.js
export const app = express();
app.use(express.json());

// Serve the Readme from the root path
app.get('/', (req: Request, res: Response) => {
  res.send(readme());
});

// Serve the transaction from the /greeting/:name path
app.get('/greeting/:name', (req: Request, res: Response) => {
  const { name } = req.params;
  Hello.helloTransaction(name)
    .then((result) => res.send(result))
    .catch((error) => {
      console.error(error);
      res.status(500).send('Internal Server Error');
    });
});

// Finally, launch DBOS and start the server
async function main() {
  DBOS.setConfig({
    name: 'dbos-drizzle',
    databaseUrl: process.env.DBOS_DATABASE_URL,
  });
  await DBOS.launch({ expressApp: app });
  const PORT = parseInt(process.env.NODE_PORT || '3000');
  const ENV = process.env.NODE_ENV || 'development';

  app.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
    console.log(`🌟 Environment: ${ENV}`);
  });
}

// Only start the server when this file is run directly from Node
if (require.main === module) {
  main().catch(console.log);
}
