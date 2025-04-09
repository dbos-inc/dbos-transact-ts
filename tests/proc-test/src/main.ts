import express, { Request, Response } from 'express';
import { DBOS } from '@dbos-inc/dbos-sdk';

export interface dbos_hello {
  name: string;
  greet_count: number;
}

export class Hello {
  @DBOS.workflow()
  static async helloWorkflow(user: string) {
    const greeting = await Hello.helloProcedure(user);
    return makeHTML(greeting);
  }

  @DBOS.storedProcedure()
  static async helloProcedure(user: string) {
    const query =
      'INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;';
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.`;
  }
}

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

export const app = express();
app.use(express.json());

app.get('/', (req: Request, res: Response) => {
  res.send(readme());
});

app.get('/greeting/:name', (req: Request, res: Response) => {
  const { name } = req.params;
  Hello.helloWorkflow(name)
    .then((result) => res.send(result))
    .catch((error) => {
      console.error(error);
      res.status(500).send('Internal Server Error');
    });
});

async function main() {
  DBOS.setConfig({
    name: 'dbos-proc',
    databaseUrl: process.env.DBOS_DATABASE_URL,
  });
  await DBOS.launch({ expressApp: app });
  const PORT = 3000;
  const ENV = process.env.NODE_ENV || 'development';

  app.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
    console.log(`🌟 Environment: ${ENV}`);
  });
}

if (require.main === module) {
  main().catch(console.log);
}
