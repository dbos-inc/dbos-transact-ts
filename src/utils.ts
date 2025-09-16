import fs from 'node:fs';
import path from 'node:path';
import superjson from 'superjson';
import type { SuperJSONResult } from 'superjson/dist/types';

// Register Buffer transformer for SuperJSON
superjson.registerCustom<Buffer, number[]>(
  {
    isApplicable: (v): v is Buffer => Buffer.isBuffer(v),
    serialize: (v) => Array.from(v),
    deserialize: (v) => Buffer.from(v),
  },
  'Buffer',
);

/*
 * A wrapper of readFileSync used for mocking in tests
 **/
export function readFileSync(path: string, encoding: BufferEncoding = 'utf8'): string {
  return fs.readFileSync(path, { encoding });
}

function loadDbosVersion(): string {
  try {
    const packageJsonPath = path.join(findPackageRoot(__dirname), 'package.json');
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const packageJson = require(packageJsonPath) as { version: string };
    return packageJson.version;
  } catch (error) {
    // Return "unknown" if package.json cannot be found or loaded
    // This can happen in bundled environments where the file system structure is different
    return 'unknown';
  }
}

export const globalParams = {
  appVersion: process.env.DBOS__APPVERSION || '', // The one true source of appVersion
  wasComputed: false, // Was app version set or computed? Stored procs don't support computed versions.
  executorID: process.env.DBOS__VMID || 'local', // The one true source of executorID
  appID: process.env.DBOS__APPID || '', // The one true source of appID
  dbosVersion: loadDbosVersion(), // The version of the DBOS library
};
export const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

// The name of the internal queue used by DBOS
export const INTERNAL_QUEUE_NAME = '_dbos_internal_queue';
export const DEBOUNCER_WORKLOW_NAME = '_dbos_debouncer_workflow';

/*
A cancellable sleep function that returns a promise and a callback
The promise can be awaited for and will automatically resolve after the given time
When cancel is called, not only it clears the timeout, but also resolves the promise
So any waiters on the cancelable sleep will be resolved
*/
export function cancellableSleep(ms: number) {
  let timeoutId: ReturnType<typeof setTimeout> | undefined = undefined;
  let resolvePromise: () => void;
  let resolved = false;

  const promise = new Promise<void>((resolve) => {
    resolvePromise = () => {
      if (resolved) return;
      resolved = true;
      resolve();
      timeoutId = undefined;
    };
    timeoutId = setTimeout(resolvePromise, ms);
  });

  const cancel = () => {
    if (timeoutId) {
      clearTimeout(timeoutId);
      timeoutId = undefined;
      resolvePromise();
    }
  };

  return { promise, cancel };
}

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
      dbos_data: actualValue.toISOString(),
    };
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
  return typeof value === 'object' && value !== null && (value as Record<string, unknown>).type === 'Buffer';
}

function isSerializedDate(value: unknown): value is DBOSSerializedDate {
  return typeof value === 'object' && value !== null && (value as Record<string, unknown>).dbos_type === 'dbos_Date';
}

function isSerializedBigInt(value: unknown): value is DBOSSerializedBigInt {
  return typeof value === 'object' && value !== null && (value as Record<string, unknown>).dbos_type === 'dbos_BigInt';
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

// Keep the old DBOSJSON implementation for reference/testing
export const DBOSJSONLegacy = {
  parse: (text: string | null) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return text === null ? null : JSON.parse(text, DBOSReviver);
  },
  stringify: (value: unknown): string | undefined => {
    return JSON.stringify(value, DBOSReplacer);
  },
};

// Constants for SuperJSON serialization marker
export const SERIALIZER_MARKER_KEY = '__dbos_serializer';
export const SERIALIZER_MARKER_VALUE = 'superjson';
const SERIALIZER_MARKER_STRING = `"${SERIALIZER_MARKER_KEY}":"${SERIALIZER_MARKER_VALUE}"`;

// Type for our branded SuperJSON record with the marker
type DBOSBrandedSuperjsonRecord = SuperJSONResult & {
  [SERIALIZER_MARKER_KEY]: typeof SERIALIZER_MARKER_VALUE;
};

/**
 * Detects if a parsed object was serialized by our DBOSJSON with SuperJSON.
 * We check for our explicit marker to avoid ANY ambiguity with user data.
 * Also validates the object has the shape expected by superjson.deserialize().
 */
function isDBOSBrandedSuperjsonRecord(obj: unknown): obj is DBOSBrandedSuperjsonRecord {
  return (
    typeof obj === 'object' &&
    obj !== null &&
    SERIALIZER_MARKER_KEY in obj &&
    obj[SERIALIZER_MARKER_KEY] === SERIALIZER_MARKER_VALUE &&
    'json' in obj
  );
}

function sjstringify(value: unknown) {
  // Use SuperJSON for all new serialization
  const serialized = superjson.serialize(value);

  // Add our explicit marker to make detection unambiguous
  return JSON.stringify({
    ...serialized,
    [SERIALIZER_MARKER_KEY]: SERIALIZER_MARKER_VALUE,
  });
}

/**
 * DBOSJSON with SuperJSON support for richer type serialization.
 *
 * Backwards compatible - can deserialize both old DBOSJSON format and new SuperJSON format.
 * New serialization uses SuperJSON to handle Sets, Maps, undefined, RegExp, circular refs, etc.
 */
export const DBOSJSON = {
  parse: (text: string | null | undefined): unknown => {
    if (text === null || text === undefined) return null; // This is from legacy; SuperJSON can do it.

    /**
     * Performance optimization: String check before JSON parsing.
     *
     * Why not just parse once and check the resulting object?
     * - Legacy DBOSJSON data needs the DBOSReviver function during parsing
     * - SuperJSON data must be parsed WITHOUT the reviver (it would corrupt the structure)
     * - We can't know which parser to use without inspecting the data first
     *
     * This string check lets us:
     * 1. Parse legacy data correctly with DBOSReviver in one pass (99% of cases)
     * 2. Only double-parse when we detect new SuperJSON format (rare for now)
     * 3. Avoid corrupting SuperJSON's meta structure with the wrong reviver
     */
    const hasSuperJSONMarker = text.includes(SERIALIZER_MARKER_STRING);

    if (hasSuperJSONMarker) {
      // Parse without reviver first to check if it's really our SuperJSON format
      const vanillaParsed: unknown = JSON.parse(text);
      if (isDBOSBrandedSuperjsonRecord(vanillaParsed)) {
        return superjson.deserialize(vanillaParsed);
      }
      // False positive - user data happened to contain our marker string
      // Fall through to parse with reviver
    }

    // Legacy DBOSJSON format
    return DBOSJSONLegacy.parse(text);
  },
  stringify: sjstringify,
};

//#region Serialization Protection
// Serialization protection - for a serialized object, provide a replacement that gives clear errors
//  from called functions that will not be there after deserialization.

type PathToMember = Array<string | number | symbol>;
type AnyObject = { [key: string | symbol]: unknown };

/**
 * Roundtrips `value` and then attaches throwing stubs for any
 * functions present on the original (own props + prototype methods) that
 * aren’t present as functions on the deserialized.
 */
export function serializeFunctionInputOutput<T>(
  value: T,
  path: PathToMember = [],
): { deserialized: T; stringified: string } {
  const stringified = DBOSJSON.stringify(value);
  const deserialized = DBOSJSON.parse(stringified) as T;
  if (isObjectish(deserialized)) {
    attachFunctionStubs(value as unknown as AnyObject, deserialized as unknown as AnyObject, path);
  }
  return { deserialized, stringified };
}

// Walks original & deserialized in lockstep and attaches stubs for missing functions.
function attachFunctionStubs(original: AnyObject, deserialized: AnyObject, path: PathToMember = []): void {
  // Avoid infinite cycles
  const seen = new WeakSet<object>();
  const pairQueue: Array<{ o: AnyObject; d: AnyObject; p: PathToMember }> = [{ o: original, d: deserialized, p: path }];

  while (pairQueue.length) {
    const { o, d, p } = pairQueue.pop()!;
    if (seen.has(o)) continue;
    seen.add(o);

    // Collect function keys from the original
    for (const key of collectFunctionKeys(o)) {
      const hasInDeser = key in d;

      if (!hasInDeser) {
        defineThrowingStub(d, key, p);
      }
    }

    // Recurse into child properties (plain objects & arrays, but not maps/sets)
    for (const key of getAllKeys(o)) {
      try {
        const childO = o[key];
        const childD = d[key];

        if (!shouldRecurse(childO, childD)) continue;

        pairQueue.push({ o: childO as AnyObject, d: childD as AnyObject, p: [...p, key] });
      } catch {
        // Ignore property accessors that throw
      }
    }

    // Map/Set values
    if (o instanceof Map && d instanceof Map) {
      for (const [k, vO] of o as Map<unknown, unknown>) {
        const vD = (d as Map<unknown, unknown>).get(k);
        if (shouldRecurse(vO, vD)) {
          const step = isIndexableKey(k) ? String(k) : '[MapValue]';
          pairQueue.push({ o: vO as AnyObject, d: vD as AnyObject, p: [...p, step] });
        }
      }
    }
    if (o instanceof Set && d instanceof Set) {
      const arrO = Array.from(o as Set<unknown>);
      const arrD = Array.from(d as Set<unknown>);
      for (let i = 0; i < Math.min(arrO.length, arrD.length); i++) {
        const vO = arrO[i];
        const vD = arrD[i];
        if (shouldRecurse(vO, vD)) {
          pairQueue.push({ o: vO as AnyObject, d: vD as AnyObject, p: [...p, i] });
        }
      }
    }
  }
}

function isObjectish(v: unknown): v is object {
  return (typeof v === 'object' && v !== null) || typeof v === 'function';
}

function defineThrowingStub(target: AnyObject, key: string | symbol, path: PathToMember) {
  const stub = function (this: unknown, ..._args: unknown[]) {
    throw new Error(
      `Attempted to call '${String(
        key,
      )}' at path ${formatPath(path)} on an object that is a serialized function input our output value. ` +
        `Functions are not preserved through serialization. `,
    );
  };

  try {
    Object.defineProperty(target, key, {
      value: stub,
      configurable: true,
      writable: false,
      enumerable: false,
    });
  } catch {
    // Fall back to assignment
    target[key] = stub;
  }
}

function shouldRecurse(a: unknown, b: unknown): a is object & NonNullable<AnyObject> {
  if (!a || !b) return false;
  if (typeof a !== 'object' || typeof b !== 'object') return false;
  // Avoid recursing into special non-plain objects (Date, RegExp, etc.)
  const bad = [Date, RegExp, WeakMap, WeakSet, ArrayBuffer, DataView];
  if (bad.some((t) => a instanceof t)) return false;
  return true;
}

function getAllKeys(obj: object): Array<string | symbol> {
  const names = Object.getOwnPropertyNames(obj);
  const syms = Object.getOwnPropertySymbols(obj);
  return [...names, ...syms];
}

function collectFunctionKeys(obj: object): Array<string | symbol> {
  const keys = new Set<string | symbol>();
  // Own props
  for (const k of getAllKeys(obj)) {
    const d = Object.getOwnPropertyDescriptor(obj, k);
    if (d && 'value' in d && typeof d.value === 'function') keys.add(k);
  }
  // Prototype chain methods (so we also stub class methods lost after deserialization)
  let proto = Object.getPrototypeOf(obj) as unknown;
  while (proto && proto !== Object.prototype) {
    for (const k of Object.getOwnPropertyNames(proto)) {
      if (k === 'constructor') continue;
      const d = Object.getOwnPropertyDescriptor(proto, k);
      if (d && 'value' in d && typeof d.value === 'function') keys.add(k);
    }
    proto = Object.getPrototypeOf(proto);
  }
  return Array.from(keys);
}

function formatPath(path: PathToMember): string {
  if (path.length === 0) return '(root)';
  return path
    .map((seg) =>
      typeof seg === 'number'
        ? `[${seg}]`
        : typeof seg === 'symbol'
          ? `[${String(seg)}]`
          : /^<?[A-Za-z_$][A-Za-z0-9_$]*>?$/.test(seg)
            ? `.${seg}`
            : `[${JSON.stringify(seg)}]`,
    )
    .join('')
    .replace(/^\./, '');
}

function isIndexableKey(k: unknown): k is string | number {
  return typeof k === 'string' || typeof k === 'number';
}

//#endregion

export function exhaustiveCheckGuard(_: never): never {
  throw new Error('Exaustive matching is not applied');
}

// Capture original functions
const originalStdoutWrite = process.stdout.write.bind(process.stdout);
const originalStderrWrite = process.stderr.write.bind(process.stderr);

export function interceptStreams(onMessage: (msg: string, stream: 'stdout' | 'stderr') => void) {
  const intercept = (stream: 'stdout' | 'stderr', originalWrite: typeof process.stdout.write) => {
    return (
      chunk: Uint8Array | string,
      encodingOrCb?: BufferEncoding | ((err?: Error | null) => void),
      cb?: (err?: Error | null) => void,
    ): boolean => {
      const message = chunk.toString();
      onMessage(message, stream);
      if (typeof encodingOrCb === 'function') {
        return originalWrite(chunk, encodingOrCb); // Handle case where encodingOrCb is a callback
      }
      return originalWrite(chunk, encodingOrCb as BufferEncoding, cb); // Handle case where encodingOrCb is a BufferEncoding
    };
  };

  process.stdout.write = intercept('stdout', originalStdoutWrite);
  process.stderr.write = intercept('stderr', originalStderrWrite);
}

// The `pg` package we use does not parse the connect_timeout parameter, so we need to handle it ourselves.
export function getClientConfig(databaseUrl: string | URL) {
  const connectionString = typeof databaseUrl === 'string' ? databaseUrl : databaseUrl.toString();
  const timeout = getTimeout(typeof databaseUrl === 'string' ? new URL(databaseUrl) : databaseUrl);
  return {
    connectionString,
    connectionTimeoutMillis: timeout ? timeout * 1000 : 10000,
  };

  function getTimeout(url: URL) {
    try {
      const $timeout = url.searchParams.get('connect_timeout');
      return $timeout ? Number.parseInt($timeout, 10) : undefined;
    } catch {
      // Ignore errors in parsing the connect_timeout parameter
      return undefined;
    }
  }
}
