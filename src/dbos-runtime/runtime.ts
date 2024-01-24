import { DBOSExecutor, DBOSConfig } from '../dbos-executor';
import { DBOSHttpServer } from '../httpServer/server';
import * as fs from 'fs';
import { isObject } from 'lodash';
import { Server } from 'http';
import { DBOSError } from '../error';
import path from 'node:path';
import { exit } from 'process';


interface ModuleExports {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

export interface DBOSRuntimeConfig {
  entrypoint: string;
  port: number;
}

export class DBOSRuntime {
  private dbosConfig: DBOSConfig;
  private dbosExec: DBOSExecutor | null = null;
  private server: Server | null = null;

  constructor(dbosConfig: DBOSConfig, private readonly runtimeConfig: DBOSRuntimeConfig) {
    // Initialize workflow executor.
    this.dbosConfig = dbosConfig;
  }

  /**
   * Initialize the runtime and start the server
   */
  async initAndStart() {
    try {
      this.dbosExec = new DBOSExecutor(this.dbosConfig);
      const classes = await DBOSRuntime.loadClasses(this.runtimeConfig.entrypoint);
      if (classes.length === 0) {
        throw new DBOSError("operations not found");
      }
      await this.dbosExec.init(...classes);    
      const server: DBOSHttpServer = new DBOSHttpServer(this.dbosExec)
      this.server = server.listen(this.runtimeConfig.port);
      this.dbosExec.logRegisteredHTTPUrls();
    } catch (error) {
      this.dbosExec?.logger.error(error);
      await this.destroy(); //ensures log contents get flushed to OpenTelemetry exporters
      throw error;
    }
    this.onSigterm = this.onSigterm.bind(this);
    process.on('SIGTERM', this.onSigterm);
    process.on('SIGQUIT', this.onSigterm);
  }

  /**
   * Load an application's workflow functions, assumed to be in src/operations.ts (which is compiled to dist/operations.js).
   */
  static async loadClasses(entrypoint: string): Promise<object[]> {
    const operations = path.isAbsolute(entrypoint) ? entrypoint : path.join(process.cwd(), entrypoint);
    let exports: ModuleExports;
    if (fs.existsSync(operations)) {
      /* eslint-disable-next-line @typescript-eslint/no-var-requires */
      exports = (await import(operations)) as Promise<ModuleExports>;
    } else {
      throw new DBOSError(`Failed to load operations from the entrypoint ${entrypoint}`);
    }
    const classes: object[] = [];
    for (const key in exports) {
      if (isObject(exports[key])) {
        classes.push(exports[key] as object);
      }
    }
    return classes;
  }

  onSigterm(): void {
    let err = new DBOSError("Received a termination signal; exiting.");
    this.dbosExec?.logger.error(err); 
    this.destroy().finally(() => {
      process.exit(1);
    })
  }

  /**
   * Shut down the HTTP server and destroy workflow executor.
   */
  async destroy() {
    this.server?.close();
    await this.dbosExec?.destroy();
  }
}
