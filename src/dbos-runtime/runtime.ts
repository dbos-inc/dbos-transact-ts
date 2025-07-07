import { DBOSExecutor, DBOSConfigInternal } from '../dbos-executor';
import { DBOSHttpServer } from '../httpServer/server';
import * as fs from 'fs';
import { isObject } from 'lodash';
import { DBOSFailLoadOperationsError } from '../error';
import path from 'node:path';
import { Server } from 'http';
import { pathToFileURL } from 'url';
import { DBOSScheduler } from '../scheduler/scheduler';
import { wfQueueRunner } from '../wfqueue';
import { DBOS } from '../dbos';

interface ModuleExports {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [key: string]: any;
}

export interface DBOSRuntimeConfig {
  entrypoints: string[];
  port: number;
  admin_port: number;
  runAdminServer: boolean;
  start: string[];
  setup: string[];
}
export const defaultEntryPoint = 'dist/operations.js';

export class DBOSRuntime {
  private dbosConfig: DBOSConfigInternal;
  private dbosExec?: DBOSExecutor = undefined;
  private servers: { appServer?: Server; adminServer: Server } | undefined;
  private scheduler?: DBOSScheduler = undefined;
  private wfQueueRunner?: Promise<void> = undefined;

  constructor(
    dbosConfig: DBOSConfigInternal,
    private readonly runtimeConfig: DBOSRuntimeConfig,
  ) {
    // Initialize workflow executor.
    this.dbosConfig = dbosConfig;
    DBOS.setConfig(dbosConfig);
  }

  /**
   * Initialize the runtime and start the server
   */
  async initAndStart() {
    try {
      if (!this.dbosConfig.poolConfig) {
        throw new Error('DBOS pool configuration is not initialized');
      }
      this.dbosExec = new DBOSExecutor(this.dbosConfig);
      this.dbosExec.logger.debug(`Loading classes from entrypoints ${JSON.stringify(this.runtimeConfig.entrypoints)}`);
      await DBOSRuntime.loadClasses(this.runtimeConfig.entrypoints);
      await this.dbosExec.init();
      const server = new DBOSHttpServer(this.dbosExec);
      this.servers = await server.listen(this.runtimeConfig.port, this.runtimeConfig.admin_port);
      this.dbosExec.logRegisteredHTTPUrls();

      this.scheduler = new DBOSScheduler(this.dbosExec);
      this.scheduler.initScheduler();
      this.scheduler.logRegisteredSchedulerEndpoints();

      wfQueueRunner.logRegisteredEndpoints(this.dbosExec);
      this.wfQueueRunner = wfQueueRunner.dispatchLoop(this.dbosExec);
    } catch (error) {
      if (!this.dbosExec) {
        throw error;
      }
      this.dbosExec.logger.error(error);
      if (error instanceof DBOSFailLoadOperationsError) {
        console.error('\x1b[31m%s\x1b[0m', 'Did you compile this application? Hint: run `npm run build` and try again');
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
  static async loadClasses(entrypoints: string[]): Promise<object[]> {
    const allClasses: object[] = [];
    for (const entrypoint of entrypoints) {
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
      allClasses.push(...classes);
    }
    if (allClasses.length === 0) {
      throw new DBOSFailLoadOperationsError('operations not found');
    }
    return allClasses;
  }

  onSigterm(): void {
    this.dbosExec?.logger.info('Stopping application: received a termination signal');
    void this.destroy().finally(() => {
      process.exit(1);
    });
  }

  /**
   * Shut down the HTTP and other services and destroy workflow executor.
   */
  async destroy() {
    await this.scheduler?.destroyScheduler();
    try {
      wfQueueRunner.stop();
      await this.wfQueueRunner;
    } catch (err) {
      const e = err as Error;
      this.dbosExec?.logger.warn(`Error destroying workflow queue runner: ${e.message}`);
    }
    if (this.servers) {
      this.servers.appServer?.close();
      this.servers.adminServer.close();
    }
    await this.dbosExec?.destroy();
  }
}
