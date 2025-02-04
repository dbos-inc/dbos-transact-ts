import fs from "fs";
import path from "path";

/*
 * A wrapper of readFileSync used for mocking in tests
 **/
export function readFileSync(path: string, encoding: BufferEncoding = "utf8"): string {
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
 * - Dates
 *
 * Currently, these are only used for operation inputs.
 * TODO: Use in other contexts where we perform serialization and deserialization.
 */

interface SerializedBuffer {
  type: 'Buffer';
  data: number[];
}

type DBOSSerializeType = 'dbos_Date' | 'dbos_BigInt';

interface DBOSSerialized {
  dbos_type: DBOSSerializeType;
}

interface DBOSSerializedDate extends DBOSSerialized {
  dbos_type: 'dbos_Date';
  dbos_data: string;
}

interface DBOSSerializedBigInt extends DBOSSerialized {
  dbos_type: 'dbos_BigInt';
  dbos_data: string;
}

//https://www.typescriptlang.org/docs/handbook/2/functions.html#declaring-this-in-a-function
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function DBOSReplacer(this: any, key: string, value: unknown) {
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment
  const actualValue = this[key];
  if (actualValue instanceof Date) {
    const res: DBOSSerializedDate = {
        dbos_type: 'dbos_Date',
        dbos_data: actualValue.toISOString()
    }
    return res;
  }

  if (typeof actualValue === 'bigint') {
    const res: DBOSSerializedBigInt = {
      dbos_type: 'dbos_BigInt',
      dbos_data: actualValue.toString(),
    };
    return res;
  }
  return value;
}

function isSerializedBuffer(value: unknown): value is SerializedBuffer {
  return typeof value === 'object'
    && value !== null
    && (value as Record<string, unknown>).type === 'Buffer';
}

function isSerializedDate(value: unknown): value is DBOSSerializedDate {
  return typeof value === 'object'
    && value !== null
    && (value as Record<string, unknown>).dbos_type === 'dbos_Date';
}

function isSerializedBigInt(value: unknown): value is DBOSSerializedBigInt {
  return typeof value === 'object'
    && value !== null
    && (value as Record<string, unknown>).dbos_type === 'dbos_BigInt';
}

export function DBOSReviver(_key: string, value: unknown): unknown {
  switch (true) {
    case isSerializedBuffer(value):
      return Buffer.from(value.data);
    case isSerializedDate(value):
      return new Date(Date.parse(value.dbos_data));
    case isSerializedBigInt(value):
      return BigInt(value.dbos_data);
    default:
      return value;
  }
}

export const DBOSJSON = {
  parse: (text: string) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return JSON.parse(text, DBOSReviver)
  },
  stringify: (value: unknown) => {
    return JSON.stringify(value, DBOSReplacer)
  }
}

export function exhaustiveCheckGuard(_: never): never {
  throw new Error('Exaustive matching is not applied');
}
