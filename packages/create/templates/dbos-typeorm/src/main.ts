// Welcome to DBOS!

// This is the Quickstart TypeORM template app. It greets visitors, counting how many total greetings were made.
// To learn how to run this app, visit the TypeORM tutorial: https://docs.dbos.dev/tutorials/using-typeorm

import express, { Request, Response } from 'express';
import { DBOS } from '@dbos-inc/dbos-sdk';
import { DBOSHello } from '../entities/DBOSHello';

import { TypeOrmDataSource } from '@dbos-inc/typeorm-datasource';

const config = {
  connectionString: process.env.DBOS_DATABASE_URL ?? 'postgres://postgres:dbos@localhost:5432/dbos_typeorm',
};

const dataSource = new TypeOrmDataSource('app-db', config, [DBOSHello]);

export class Hello {
  // This transaction uses DBOS and TypeORM to perform database operations.
  @dataSource.transaction()
  static async helloTransaction(name: string) {
    const greeting = `Hello, ${name}!`;
    let entity = new DBOSHello();
    entity.greeting = greeting;
    entity = await dataSource.entityManager.save(entity);
    const greeting_note = `Greeting ${entity.greeting_id}: ${greeting}`;
    return makeHTML(greeting_note);
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
    name: 'dbos-typeorm',
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
