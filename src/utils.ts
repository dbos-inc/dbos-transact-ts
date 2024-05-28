import fs from "fs";
import path from "path";
import vercelms from 'ms';

//Definitions copied from ms@3.0.0-canary.1
declare type Unit = 'Years' | 'Year' | 'Yrs' | 'Yr' | 'Y' | 'Weeks' | 'Week' | 'W' | 'Days' | 'Day' | 'D' | 'Hours' | 'Hour' | 'Hrs' | 'Hr' | 'H' | 'Minutes' | 'Minute' | 'Mins' | 'Min' | 'M' | 'Seconds' | 'Second' | 'Secs' | 'Sec' | 's' | 'Milliseconds' | 'Millisecond' | 'Msecs' | 'Msec' | 'Ms';
declare type UnitAnyCase = Unit | Uppercase<Unit> | Lowercase<Unit>;
export declare type StringValue = `${number}` | `${number}${UnitAnyCase}` | `${number} ${UnitAnyCase}`;

export function ms (value: StringValue) {
  return vercelms(value)
}

/*
 * Use the node.js `fs` module to read the content of a file
 * Handles cases where:
 * - the file does not exist
 * - the file is not a valid file
 **/
export function readFileSync(path: string, encoding: BufferEncoding = "utf8"): string {
  // First, check the file
  fs.stat(path, (error: NodeJS.ErrnoException | null, stats: fs.Stats) => {
    if (error) {
      throw new Error(`checking on ${path}. ${error.code}: ${error.errno}`);
    } else if (!stats.isFile()) {
      throw new Error(`config file ${path} is not a file`);
    }
  });

  // Then, read its content
  return fs.readFileSync(path, { encoding } );
}

export const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

export type ValuesOf<T> = T[keyof T];

// Adapated and translated from from: https://github.com/junosuarez/find-root
export function findPackageRoot(start: string | string[]): string {
    if (typeof start === 'string') {
        if (!start.endsWith(path.sep)) {
            start += path.sep;
        }
        start = start.split(path.sep);
    }

    if (start.length === 0) {
        throw new Error('package.json not found in path');
    }

    start.pop();
    const dir = start.join(path.sep);

    if (fs.existsSync(path.join(dir, 'package.json'))) {
        return dir;
    }

    return findPackageRoot(start);
}

/**
 * Reviver and Replacer
 * --------------------
 * These can be passed to JSON.stringify and JSON.parse, respectively, to support more types.
 *
 * Additional types supported:
 * - Buffer
 *
 * Currently, these are only used for operation inputs.
 * TODO: Use in other contexts where we perform serialization and deserialization.
 */

export function DBOSReplacer(_key: string, value: unknown) {
  return value;
}

interface SerializedBuffer {
  type: 'Buffer';
  data: number[];
}

export function DBOSReviver(_key: string, value: unknown): unknown {
  const candidate = value as SerializedBuffer;
  if (candidate && candidate.type === 'Buffer' && Array.isArray(candidate.data)) {
    return Buffer.from(candidate.data);
  }
  return value;
}

