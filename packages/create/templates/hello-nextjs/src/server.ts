const next = require('next');
const http = require('http');

const { DBOS } = require('@dbos-inc/dbos-sdk');

const { DbosWorkflowClass } = require("./operations");
export { DbosWorkflowClass };

const app = next({ dev: process.env.NODE_ENV !== 'production' });
const handle = app.getRequestHandler();

async function main() {
  await DBOS.launch();

  await app.prepare();

  const PORT = DBOS.runtimeConfig?.port ?? 3000;
  const ENV = process.env.NODE_ENV || 'development';

  http.createServer((req: unknown, res: unknown) => {
    handle(req, res);
  }).listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
    console.log(`🌟 Environment: ${ENV}`);
  });
}

main().catch((err) => {
  console.error('Error starting server:', err);
});

/*
// Only start the server when this file is run directly from Node
if (require.main === module) {
  main().catch(console.log);
}
*/

