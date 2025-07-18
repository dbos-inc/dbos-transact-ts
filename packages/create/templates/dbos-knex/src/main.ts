// Welcome to DBOS!

// This is a sample "Hello" app built with DBOS and Knex.
// It greets visitors and keeps track of how many times each visitor has been greeted.

import express, { Request, Response } from 'express';
import { DBOS } from '@dbos-inc/dbos-sdk';
import { KnexDataSource } from '@dbos-inc/knex-datasource';

const config = {
  client: 'pg',
  connection: {
    host: process.env.PGHOST || 'localhost',
    port: parseInt(process.env.PGPORT || '5432'),
    database: process.env.PGDATABASE || 'dbos_knex',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'dbos',
  },
};

const knexds = new KnexDataSource('app-db', config);

export interface dbos_hello {
  name: string;
  greet_count: number;
}

export class Hello {
  // This transaction uses DBOS and Knex to perform database operations.
  // It retrieves and increments the number of times a user has been greeted.
  @knexds.transaction()
  static async helloTransaction(user: string) {
    const query =
      'INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;';
    const { rows } = (await knexds.client.raw(query, [user])) as { rows: dbos_hello[] };
    const greet_count = rows[0].greet_count;
    const greeting = `Hello, ${user}! You have been greeted ${greet_count} times.`;
    return makeHTML(greeting);
  }

  @knexds.transaction({ readOnly: true })
  static async getCount(user: string) {
    return await knexds.client<dbos_hello>('dbos_hello').where({ name: user }).select('*');
  }

  @knexds.transaction()
  static async deleteUser(user: string) {
    await knexds.client<dbos_hello>('dbos_hello').where({ name: user }).delete();
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
    name: 'dbos-knex',
    databaseUrl: process.env.DBOS_DATABASE_URL,
  });
  await DBOS.launch();
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
