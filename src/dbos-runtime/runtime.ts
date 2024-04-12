import { DBOSExecutor, DBOSConfig } from '../dbos-executor';
import { DBOSHttpServer } from '../httpServer/server';
import * as fs from 'fs';
import { isObject } from 'lodash';
import { DBOSFailLoadOperationsError } from '../error';
import path from 'node:path';
import { Server } from 'http';
import { pathToFileURL } from 'url';
import { DBOSKafka } from '../kafka/kafka';

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
  private servers: { appServer: Server, adminServer: Server } | undefined
  private kafka: DBOSKafka | null = null;

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
        throw new DBOSFailLoadOperationsError("operations not found");
      }
      await this.dbosExec.init(...classes);
      const server = new DBOSHttpServer(this.dbosExec)
      this.servers = await server.listen(this.runtimeConfig.port);
      this.dbosExec.logRegisteredHTTPUrls();
      this.kafka = new DBOSKafka(this.dbosExec);
      await this.kafka.initKafka();
    } catch (error) {
      this.dbosExec?.logger.error(error);
      if (error instanceof DBOSFailLoadOperationsError) {
        console.error('\x1b[31m%s\x1b[0m', "Did you compile this application? Hint: run `npm run build` and try again");
        process.exit(1);
      }
      await this.destroy(); //wrap up, i.e. flush log contents to OpenTelemetry exporters
      process.exit(1);
    }
    const onSigterm = this.onSigterm.bind(this);
    process.on('SIGTERM', onSigterm);
    process.on('SIGQUIT', onSigterm);
  }

  /**
   * Load an application's workflow functions, assumed to be in src/operations.ts (which is compiled to dist/operations.js).
   */
  static async loadClasses(entrypoint: string): Promise<object[]> {
    const operations = path.isAbsolute(entrypoint) ? entrypoint : path.join(process.cwd(), entrypoint);
    let exports: ModuleExports;
    if (fs.existsSync(operations)) {
      const operationsURL = pathToFileURL(operations).href;
      exports = (await import(operationsURL)) as Promise<ModuleExports>;
    } else {
      throw new DBOSFailLoadOperationsError(`Failed to load operations from the entrypoint ${entrypoint}`);
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
    this.dbosExec?.logger.info("Stopping application: received a termination signal");
    void this.destroy().finally(() => {
      process.exit(1);
    })
  }

  /**
    * Shut down the HTTP server and destroy workflow executor.
    */
  async destroy() {
    await this.kafka?.destroyKafka();
    if (this.servers) {
      this.servers.appServer.close()
      this.servers.adminServer.close()
    }
    await this.dbosExec?.destroy();
  }
}
