import * as fs from 'fs';
import { isObject } from 'lodash';
import { DBOSFailLoadOperationsError } from '../error';
import path from 'node:path';
import { pathToFileURL } from 'url';

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
  /**
   * Load an application's workflow functions from the compiled JS files.
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
}
