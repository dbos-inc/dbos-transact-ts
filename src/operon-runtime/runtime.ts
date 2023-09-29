/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon, OperonConfig } from '../operon';
import { OperonHttpServer } from '../httpServer/server';
import * as fs from 'fs';
import { isObject } from 'lodash';
import { Server } from 'http';
import { OperonError } from '../error';

interface ModuleExports {
  [key: string]: any;
}

export interface OperonRuntimeConfig {
  port: number;
}

const defaultConfig: OperonRuntimeConfig = {
  port: 3000,
}

export class OperonRuntime {
  private operon: Operon;
  private server: Server | null = null;

  constructor(operonConfig: OperonConfig, readonly runtimeConfig: OperonRuntimeConfig = defaultConfig) {
    // Initialize Operon.
    this.operon = new Operon(operonConfig);
    this.operon.useNodePostgres();
  }

  /**
   * Initialize the runtime by loading user functions and initiatilizing the Operon object
   */
  async init() {
    const exports = await this.loadFunctions();
    if (exports === null) {
      throw new OperonError("userFunctions not found");
    }

    const classes: object[] = [];
    for (const key in exports) {
      if (isObject(exports[key])) {
        classes.push(exports[key] as object);
      }
    }
    await this.operon.init(...classes);
  }

  /**
   * Load an application's Operon functions, assumed to be in src/userFunctions.ts (which is compiled to dist/userFunction.js).
   */
  private loadFunctions(): Promise<ModuleExports> | null {
    const workingDirectory = process.cwd();
    const userFunctions = workingDirectory + "/dist/userFunctions.js";
    if (fs.existsSync(userFunctions)) {
      /* eslint-disable-next-line @typescript-eslint/no-var-requires */
      return import(userFunctions) as Promise<ModuleExports>;
    } else {
      return null;
    }
  }

  /**
   * Start an HTTP server hosting an application's Operon functions.
   */
  startServer(inputConfig: OperonRuntimeConfig = defaultConfig) {
    // CLI takes precedence over config file, which takes precedence over default config.
    const config: OperonRuntimeConfig = {
      port: inputConfig.port || this.runtimeConfig.port,
    }

    const server = new OperonHttpServer(this.operon)

    this.server = server.listen(config.port);
    console.log(`Starting server on port: ${config.port}`);
  }

  /**
   * Shut down the HTTP server and destroy Operon.
   */
  async destroy() {
    this.server?.close();
    await this.operon?.destroy();
  }
}
