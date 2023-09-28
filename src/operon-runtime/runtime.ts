/* eslint-disable @typescript-eslint/no-explicit-any */
import { Operon } from '../operon';
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

// Configuration file config (this.operon.config.runtimeConfig) overwrites CLI input configs, which overwrites default configs.
// XXX: let's agree on the precedence of configs.
function generateRuntimeConfig(
  inputConfig: OperonRuntimeConfig,
  fileConfig: OperonRuntimeConfig | undefined): OperonRuntimeConfig
  {
    return {
      port: inputConfig.port || fileConfig?.port || defaultConfig.port,
    }
}

export class OperonRuntime {
  private operon: Operon | null = null;
  private server: Server | null = null;

  /**
   * Load an application's Operon functions, assumed to be in src/userFunctions.ts (which is compiled to dist/userFunction.js).
   */
  private async loadFunctions(): Promise<ModuleExports | null> {
    const workingDirectory = process.cwd();
    const userFunctions = workingDirectory + "/dist/userFunctions.js";
    if (fs.existsSync(userFunctions)) {
      /* eslint-disable-next-line @typescript-eslint/no-var-requires */
      return await import(userFunctions) as ModuleExports;
    } else {
      return null;
    }
  }

  /**
   * Start an HTTP server hosting an application's Operon functions.
   */
  async startServer(inputConfig: OperonRuntimeConfig = defaultConfig) {
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
    // Initialize Operon.
    this.operon = new Operon();
    this.operon.useNodePostgres();
    await this.operon.init(...classes);

    const config: OperonRuntimeConfig = generateRuntimeConfig(inputConfig, this.operon.config.localRuntimeConfig);

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
