/**
 * Types that can be serialized by SuperJSON.
 * Based on SuperJSON v1.13.3 supported types.
 */
export type SuperJSONPrimitive =
  | string
  | number
  | boolean
  | null
  | undefined
  | bigint;

/**
 * Complex types that SuperJSON can serialize.
 */
export type SuperJSONComplex =
  | Date
  | RegExp
  | Error
  | URL
  | Map<SuperJSONSerializable, SuperJSONSerializable>
  | Set<SuperJSONSerializable>
  | Array<SuperJSONSerializable>
  | { [key: string]: SuperJSONSerializable };

/**
 * All types that can be serialized by SuperJSON.
 */
export type SuperJSONSerializable = SuperJSONPrimitive | SuperJSONComplex;

/**
 * Branded error type for non-serializable values.
 * 
 * Uses TypeScript's structural typing to embed error messages directly in the type.
 * When type checking fails, the error message appears in the TypeScript output.
 * 
 * @example
 * ```typescript
 * async function myWorkflow() {
 *   return { callback: () => {} };
 * }
 * // Error: Type '() => void' is not assignable to type
 * // 'NotSerializable<"Functions cannot be serialized. Remove function properties or convert to data.">'
 * ```
 */
export type NotSerializable<Context extends string = ""> = {
  readonly __error: "This type cannot be serialized and persisted by DBOS";
  readonly __hint: "Functions, Symbols, and class instances (except Date, Error, RegExp, URL) cannot cross serialization boundaries";
  readonly __context: Context;
};

/**
 * Helper type to format property names in error messages.
 */
type PropertyName<K> = K extends string ? K : K extends number ? `[${K}]` : K extends symbol ? "[symbol]" : "unknown";

/**
 * Constrains a type to only include SuperJSON serializable values.
 * Non-serializable types are replaced with branded error types that provide clear error messages.
 * 
 * This ensures compile-time type safety for workflow and step return types.
 */
export type SerializableOnly<T> = T extends SuperJSONSerializable
  ? T
  : T extends (...args: any[]) => any
    ? NotSerializable<"Functions cannot be serialized. Remove function properties or convert to data.">
    : T extends symbol
      ? NotSerializable<"Symbols cannot be serialized. Use strings or numbers instead.">
      : T extends object
        ? T extends Error | Date | RegExp | URL // These are allowed objects
          ? T
          : T extends Map<infer K, infer V>
            ? Map<SerializableOnly<K>, SerializableOnly<V>>
            : T extends Set<infer V>
              ? Set<SerializableOnly<V>>
              : T extends Array<infer V>
                ? Array<SerializableOnly<V>>
                : T extends Buffer // Buffer is supported via custom transformer
                  ? Buffer
                  : {
                      [K in keyof T]: T[K] extends (...args: any[]) => any
                        ? NotSerializable<`Function property '${PropertyName<K>}' cannot be serialized. Remove or convert to data.`>
                        : T[K] extends symbol
                          ? NotSerializable<`Symbol property '${PropertyName<K>}' cannot be serialized. Use string or number instead.`>
                          : SerializableOnly<T[K]>;
                    }
        : T;

/**
 * Validates that all elements in a tuple/array are serializable.
 * Used for constraining function arguments that cross serialization boundaries.
 */
export type SerializableArgs<T extends readonly unknown[]> = {
  [K in keyof T]: SerializableOnly<T[K]>;
};