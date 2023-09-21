import { Operon } from '../operon';
import { OperonHttpServer } from '../httpServer/server';
import * as fs from 'fs';

/**
 * Load an application's Operon functions, assumed to be in src/userFunctions.ts (which is compiled to dist/userFunction.js).
 */
function loadFunctions() {
  const workingDirectory = process.cwd();
  console.log("Current directory:", workingDirectory);
  const userFunctions = workingDirectory + "/dist/userFunctions.js";
  if (fs.existsSync(userFunctions)) {
    return require(userFunctions);
  } else {
    return null;
  }
}

/**
 * Start an HTTP server hosting an application's Operon functions.
 */
export async function startServer(port: number) {
  const exports = loadFunctions();
  if (exports === null) {
    console.log("userFunctions not found");
    return;
  }

  const classes: any[] = [];
  for (let key in exports) {
    classes.push(exports[key]);
  }
  // Initialize Operon.
  const operon: Operon = new Operon();
  operon.useNodePostgres();
  await operon.init(...classes);

  const server = new OperonHttpServer(operon)
  server.listen(port);
}
