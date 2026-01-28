import { deserializeError, serializeError } from 'serialize-error';
import superjson from 'superjson';
import type { SuperJSONResult, JSONValue } from 'superjson/dist/types';
import { JsonValue, JsonWorkflowArgs, JsonWorkflowErrorData, PortableWorkflowError } from '../schemas/system_db_schema';
import { WorkflowSerializationFormat } from './workflow';
export { type JSONValue };

/**
 * Generic serializer interface for DBOS.
 * Implementations must be able to serialize any value to a string and deserialize it back.
 */
export interface DBOSSerializer {
  /**
   * Return a name for the serialization format
   */
  name: () => string;

  /**
   * Serialize a value to a string.
   * @param value - The value to serialize
   * @returns The serialized string representation
   */
  stringify(value: unknown): string;

  /**
   * Deserialize a string back to a value.
   * @param text - A serialized string (potentially null or undefined)
   * @returns The deserialized value, or null if the input was null/undefined
   */
  parse(text: string | null | undefined): unknown;
}

export type SerializationRecipe<T, S extends JSONValue> = {
  name: string;
  isApplicable: (v: unknown) => v is T;
  serialize: (v: T) => S;
  deserialize: (s: S) => T;
};

export function registerSerializationRecipe<T, S extends JSONValue>(r: SerializationRecipe<T, S>) {
  superjson.registerCustom(r, r.name);
}

// Register Buffer transformer for SuperJSON
registerSerializationRecipe<Buffer, number[]>({
  isApplicable: (v): v is Buffer => Buffer.isBuffer(v),
  serialize: (v) => Array.from(v),
  deserialize: (v) => Buffer.from(v),
  name: 'Buffer',
});

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
  name: () => 'js_legacy',
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
export const DBOSJSON: DBOSSerializer = {
  name: () => 'js_superjson',

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

function portableJsonReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();

  if (typeof value === 'bigint') return value.toString(10);

  if (value instanceof Map) {
    // If keys are strings, represent as a plain JSON object.
    let allStringKeys = true;
    for (const k of value.keys()) {
      if (typeof k !== 'string') {
        allStringKeys = false;
        break;
      }
    }
    if (!allStringKeys) {
      throw new TypeError(`Attempt to do portable JSON serialization of a map with non-string keys`);
      // Other option: list of [key,value] pairs (portable, but needs schema/consumer intent)
      // return Array.from(value.entries());
    }

    const obj: Record<string, unknown> = {};
    for (const [k, v] of value.entries()) obj[k as string] = v;
    return obj;
  }

  if (value instanceof Set) return Array.from(value.values());

  if (value instanceof Error) {
    return { name: value.name, message: value.message };
    // If you want stack too:
    // return { name: value.name, message: value.message, stack: value.stack };
  }

  return value;
}

/**
 * DBOS Portable JSON serializer,
 *   should be something that can be implemented in any language
 */
export const DBOSPortableJSON: DBOSSerializer = {
  name: () => 'portable_json',
  parse: (text: string | null) => {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return text === null ? null : JSON.parse(text);
  },
  stringify: (value: unknown): string => {
    return JSON.stringify(value, portableJsonReplacer);
  },
};

// Serialization protection - for a serialized object, provide a replacement that gives clear errors
//  from called functions that will not be there after deserialization.

type PathToMember = Array<string | number | symbol>;
type AnyObject = { [key: string | symbol]: unknown };

/**
 * Roundtrips `value` through serialization.  This doesn't preserve functions by default.
 *   So then, we recursively attach function stubs that throw clear errors, for any
 *   functions present on the original (own props + prototype methods) that
 *   aren't present as functions on the deserialized object.
 * The return is both the deserialized object and its serialized string.
 */
export function serializeFunctionInputOutput<T>(
  value: T,
  path: PathToMember = [],
  serializer: DBOSSerializer,
  serializationType?: WorkflowSerializationFormat,
): { deserialized: T; stringified: string; sername: string } {
  const serialization =
    serializationType === 'portable'
      ? DBOSPortableJSON.name()
      : serializationType === 'native'
        ? DBOSJSON.name()
        : serializer.name();

  return serializeFunctionInputOutputWithSerializer(value, path, serializer, serialization);
}

export function serializeFunctionInputOutputWithSerializer<T>(
  value: T,
  path: PathToMember = [],
  serializer: DBOSSerializer,
  serialization: string | null,
): { deserialized: T; stringified: string; sername: string } {
  for (const ser of [DBOSPortableJSON, DBOSJSON]) {
    if (serialization === ser.name()) {
      const stringified = ser.stringify(value);
      const deserialized = ser.parse(stringified) as T;
      if (isObjectish(deserialized)) {
        attachFunctionStubs(value as unknown as AnyObject, deserialized as unknown as AnyObject, path);
      }
      return { deserialized, stringified, sername: ser.name() };
    }
  }

  const sername = serializer.name();
  if (serialization && serialization !== sername) {
    throw new TypeError(
      `Serializer provided (${sername}) is not compatible with the required serialization (${serialization})`,
    );
  }
  const stringified = serializer.stringify(value);
  const deserialized = serializer.parse(stringified) as T;
  if (serializer.name() === DBOSJSON.name() && isObjectish(deserialized)) {
    attachFunctionStubs(value as unknown as AnyObject, deserialized as unknown as AnyObject, path);
  }
  return { deserialized, stringified, sername };
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
      if (!(key in d)) {
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
        `Functions are not preserved through serialization; see 'DBOS.registerSerialization'. `,
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

// Deserialize a plain value (not function inputs) using specified serialization,
//   or the provided default
export function deserializeValue(
  serializedValue: string | null,
  serialization: string | null,
  serializer: DBOSSerializer,
): unknown {
  if (serialization === DBOSPortableJSON.name()) {
    return DBOSPortableJSON.parse(serializedValue);
  }
  if (serialization === DBOSJSON.name()) {
    return DBOSJSON.parse(serializedValue);
  }
  if (!serialization || serialization === serializer.name()) {
    return serializer.parse(serializedValue);
  }
  throw new TypeError(`Value deserialization type ${serialization} is not available`);
}

// Deserialize a plain value (not function inputs) using specified serialization,
//   or the provided default
export function deserializePositionalArgs(
  serializedValue: string | null,
  serialization: string | null,
  serializer: DBOSSerializer,
): unknown[] {
  if (serialization === DBOSPortableJSON.name()) {
    return (DBOSPortableJSON.parse(serializedValue) as JsonWorkflowArgs).positionalArgs ?? [];
  }
  if (serialization === DBOSJSON.name()) {
    return DBOSJSON.parse(serializedValue) as unknown[];
  }
  if (!serialization || serialization === serializer.name()) {
    return serializer.parse(serializedValue) as unknown[];
  }
  throw new TypeError(`Value deserialization type ${serialization} is not available`);
}

export function deserializeResError(
  serializedValue: string | null,
  serialization: string | null,
  serializer: DBOSSerializer,
): Error {
  if (serialization === DBOSPortableJSON.name()) {
    const errdata = DBOSPortableJSON.parse(serializedValue) as JsonWorkflowErrorData;
    throw new PortableWorkflowError(errdata.message, errdata.name, errdata.code, errdata.data);
  }
  if (serialization === DBOSJSON.name()) {
    return deserializeError(DBOSJSON.parse(serializedValue));
  }
  if (!serialization || serialization === serializer.name()) {
    return deserializeError(serializer.parse(serializedValue));
  }
  throw new TypeError(`Value deserialization type ${serialization} is not available`);
}

// Attempt to deserialize a value, but if it fails, retun the raw string.
// Used for "best-effort" in introspection methods which may encounter
// old undeserializable data.
export function safeParse(serializer: DBOSSerializer, val: string, serialization: string | null) {
  try {
    return deserializeValue(val, serialization, serializer);
  } catch (e) {
    return val;
  }
}

export function safeParsePositionalArgs(serializer: DBOSSerializer, val: string, serialization: string | null) {
  try {
    return deserializePositionalArgs(val, serialization, serializer);
  } catch (e) {
    return val;
  }
}

export function safeParseError(serializer: DBOSSerializer, val: string, serialization: string | null) {
  try {
    return deserializeResError(val, serialization, serializer);
  } catch (e) {
    return new Error(val);
  }
}

export function serializeValue(
  value: unknown,
  serializer: DBOSSerializer,
  serializationFormat: WorkflowSerializationFormat,
): { serializedValue: string | null; serialization: string | null } {
  if (serializationFormat === 'portable') {
    return {
      serializedValue: DBOSPortableJSON.stringify(value),
      serialization: DBOSPortableJSON.name(),
    };
  }
  if (serializationFormat === 'native') {
    return {
      serializedValue: DBOSJSON.stringify(value),
      serialization: DBOSJSON.name(),
    };
  }
  return {
    serializedValue: serializer.stringify(value),
    serialization: serializer.name(),
  };
}

export function serializeArgs(
  positionalArgs: unknown[] | undefined,
  namedArgs: { [key: string]: unknown } | undefined,
  serializer: DBOSSerializer,
  serializationFormat: WorkflowSerializationFormat,
): { serializedValue: string | null; serialization: string | null } {
  if (serializationFormat === 'portable') {
    return {
      serializedValue: DBOSPortableJSON.stringify({ positionalArgs, namedArgs } as JsonWorkflowArgs),
      serialization: DBOSPortableJSON.name(),
    };
  }
  if (namedArgs) {
    throw new TypeError(`Serialization format '${serializationFormat}' does not currently support named args.`);
  }
  if (serializationFormat === 'native') {
    return {
      serializedValue: DBOSJSON.stringify(positionalArgs),
      serialization: DBOSJSON.name(),
    };
  }
  return {
    serializedValue: serializer.stringify(positionalArgs),
    serialization: serializer.name(),
  };
}

export function serializeResError(
  err: Error,
  serializer: DBOSSerializer,
  serializationType: WorkflowSerializationFormat,
): { serializedValue: string | null; serialization: string | null } {
  const serialization =
    serializationType === 'portable'
      ? DBOSPortableJSON.name()
      : serializationType === 'native'
        ? DBOSJSON.name()
        : serializer.name();
  return serializeResErrorWithSerializer(err, serializer, serialization);
}

export function serializeResErrorWithSerializer(
  err: Error,
  serializer: DBOSSerializer,
  serialization: string | null,
): { serializedValue: string | null; serialization: string | null } {
  if (serialization === DBOSPortableJSON.name()) {
    return {
      serializedValue: DBOSPortableJSON.stringify({
        name: err.name,
        message: err.message,
        code: (err as { code?: unknown }).code,
        data: (err as { data?: JsonValue }).data,
      } as JsonWorkflowErrorData),
      serialization: DBOSPortableJSON.name(),
    };
  }
  if (serialization === DBOSJSON.name()) {
    return {
      serializedValue: DBOSJSON.stringify(serializeError(err)),
      serialization: DBOSJSON.name(),
    };
  }
  return {
    serializedValue: serializer.stringify(serializeError(err)),
    serialization: serializer.name(),
  };
}
