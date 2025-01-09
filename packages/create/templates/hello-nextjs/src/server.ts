import next from 'next';
import http from 'http';

import { DBOS } from '@dbos-inc/dbos-sdk';

import { DbosWorkflowClass } from "./operations";
export { DbosWorkflowClass };

const app = next({ dev: process.env.NODE_ENV !== 'production' });
const handle = app.getRequestHandler();

async function main() {
  await DBOS.launch();

  await app.prepare();

  const PORT = DBOS.runtimeConfig?.port ?? 3000;
  const ENV = process.env.NODE_ENV || 'development';

  http.createServer((req, res) => {
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

