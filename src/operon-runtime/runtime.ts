import { Operon, OperonConfig } from '../operon';
import { OperonHttpServer } from '../httpServer/server';
import * as fs from 'fs';
import { isObject } from 'lodash';
import { Server } from 'http';
import { OperonError } from '../error';

interface ModuleExports {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

export interface OperonRuntimeConfig {
  port: number;
}

export class OperonRuntime {
  private operon: Operon;
  private server: Server | null = null;

  constructor(operonConfig: OperonConfig, private readonly runtimeConfig: OperonRuntimeConfig) {
    // Initialize Operon.
    this.operon = new Operon(operonConfig);
  }

  /**
   * Initialize the runtime by loading user functions and initiatilizing the Operon object
   */
  async init() {
    const exports = await this.loadFunctions();
    if (exports === null) {
      this.operon.logger.error("operations not found");
      throw new OperonError("operations not found");
    }

    const classes: object[] = [];
    var initFunction = null;
    for (const key in exports) {
      this.operon.logger.info("found key " + key);
      if (isObject(exports[key])) {
        classes.push(exports[key] as object);
        this.operon.logger.debug(`Loaded class: ${key}`);
      }
      if (key === "initializeApp") {
        initFunction = exports[key] as Function ;
      }
    }

    if (initFunction != null) {
      initFunction();
    } 
    
    await this.operon.init(...classes);
  
  }

  /**
   * Load an application's Operon functions, assumed to be in src/operations.ts (which is compiled to dist/operations.js).
   */
  private loadFunctions(): Promise<ModuleExports> | null {
    const workingDirectory = process.cwd();
    const operations = workingDirectory + "/dist/operations.js";
    if (fs.existsSync(operations)) {
      /* eslint-disable-next-line @typescript-eslint/no-var-requires */
      return import(operations) as Promise<ModuleExports>;
    } else {
      this.operon.logger.warn("operations not found");
      return null;
    }
  }

  /**
   * Start an HTTP server hosting an application's Operon functions.
   */
  startServer() {
    // CLI takes precedence over config file, which takes precedence over default config.

    const server: OperonHttpServer = new OperonHttpServer(this.operon)

    this.server = server.listen(this.runtimeConfig.port);
    this.operon.logRegisteredHTTPUrls();
  }

  /**
   * Shut down the HTTP server and destroy Operon.
   */
  async destroy() {
    this.server?.close();
    await this.operon?.destroy();
  }
}
