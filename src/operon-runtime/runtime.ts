/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon } from '../operon';
import { OperonHttpServer } from '../httpServer/server';
import * as fs from 'fs';
import { isObject } from 'lodash';

interface ModuleExports {
  [key: string]: any;
}

/**
 * Load an application's Operon functions, assumed to be in src/userFunctions.ts (which is compiled to dist/userFunction.js).
 */
function loadFunctions() : ModuleExports | null {
  const workingDirectory = process.cwd();
  console.log("Current directory:", workingDirectory);
  const userFunctions = workingDirectory + "/dist/userFunctions.js";
  if (fs.existsSync(userFunctions)) {
    /* eslint-disable-next-line @typescript-eslint/no-var-requires */
    return require(userFunctions) as ModuleExports;
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

  const classes: object[] = [];
  for (const key in exports) {
    if (isObject(exports[key])) {
      classes.push(exports[key] as object);
    }
  }
  // Initialize Operon.
  const operon: Operon = new Operon();
  operon.useNodePostgres();
  await operon.init(...classes);

  const server = new OperonHttpServer(operon)
  server.listen(port);
}
