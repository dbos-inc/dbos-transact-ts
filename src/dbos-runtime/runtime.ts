import { DBOSWFE, DBOSConfig } from '../dbos-workflow';
import { DBOSHttpServer } from '../httpServer/server';
import * as fs from 'fs';
import { isObject } from 'lodash';
import { Server } from 'http';
import { DBOSError } from '../error';
import path from 'node:path';

interface ModuleExports {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

export interface DBOSRuntimeConfig {
  entrypoint: string;
  port: number;
}

export class DBOSRuntime {
  private wfe: DBOSWFE;
  private server: Server | null = null;

  constructor(dbosConfig: DBOSConfig, private readonly runtimeConfig: DBOSRuntimeConfig) {
    // Initialize workflow executor.
    this.wfe = new DBOSWFE(dbosConfig);
  }

  /**
   * Initialize the runtime by loading user functions and initializing the workflow executor object
   */
  async init() {
    const exports = await this.loadFunctions();
    if (exports === null) {
      this.wfe.logger.error("operations not found");
      throw new DBOSError("operations not found");
    }

    const classes: object[] = [];
    for (const key in exports) {
      if (isObject(exports[key])) {
        classes.push(exports[key] as object);
        this.wfe.logger.debug(`Loaded class: ${key}`);
      }
    }

    await this.wfe.init(...classes);
  }

  /**
   * Load an application's workflow functions, assumed to be in src/operations.ts (which is compiled to dist/operations.js).
   */
  private loadFunctions(): Promise<ModuleExports> | null {
    const entrypoint = this.runtimeConfig.entrypoint;
    const operations = path.isAbsolute(entrypoint) ? entrypoint : path.join(process.cwd(), entrypoint);
    if (fs.existsSync(operations)) {
      /* eslint-disable-next-line @typescript-eslint/no-var-requires */
      return import(operations) as Promise<ModuleExports>;
    } else {
      this.wfe.logger.warn(`${entrypoint} not found`);
      return null;
    }
  }

  /**
   * Start an HTTP server hosting an application's functions.
   */
  startServer() {
    // CLI takes precedence over config file, which takes precedence over default config.

    const server: DBOSHttpServer = new DBOSHttpServer(this.wfe)

    this.server = server.listen(this.runtimeConfig.port);
    this.wfe.logRegisteredHTTPUrls();
  }

  /**
   * Shut down the HTTP server and destroy workflow executor.
   */
  async destroy() {
    this.server?.close();
    await this.wfe?.destroy();
  }
}
