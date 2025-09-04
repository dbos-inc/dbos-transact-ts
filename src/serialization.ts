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
 * Constrains a type to only include SuperJSON serializable values.
 * Non-serializable properties (functions, symbols, classes, etc.) are converted to never,
 * effectively removing them from the type.
 * 
 * This ensures compile-time type safety for workflow and step return types.
 */
export type SerializableOnly<T> = T extends SuperJSONSerializable
  ? T
  : T extends (...args: any[]) => any
    ? never
    : T extends symbol
      ? never
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
                      [K in keyof T as T[K] extends (...args: any[]) => any
                        ? never
                        : T[K] extends symbol
                          ? never
                          : K]: SerializableOnly<T[K]>;
                    }
        : T;

/**
 * Validates that all elements in a tuple/array are serializable.
 * Used for constraining function arguments that cross serialization boundaries.
 */
export type SerializableArgs<T extends readonly unknown[]> = {
  [K in keyof T]: SerializableOnly<T[K]>;
};