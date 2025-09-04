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
 * Branded error type that provides clear, actionable error messages when non-serializable types are used.
 * 
 * Instead of TypeScript's default "Type 'X' is not assignable to type 'never'" error,
 * developers see helpful messages explaining what went wrong and how to fix it.
 * 
 * @example
 * When a developer tries to return a function from a workflow:
 * ```typescript
 * async function myWorkflow() {
 *   return { callback: () => {} };  // ❌ Error shows: "Functions cannot be serialized"
 * }
 * ```
 * 
 * The error message will include:
 * - The specific problem (functions can't be serialized)
 * - Actionable guidance (remove function properties or convert to data)
 * - General context about serialization boundaries
 * 
 * This technique uses TypeScript's structural typing to create an impossible-to-satisfy
 * type that carries error information in its structure, making errors self-documenting.
 */
export type NotSerializable<Context extends string = ""> = {
  readonly __error: "This type cannot be serialized and persisted by DBOS";
  readonly __hint: "Functions, Symbols, and class instances (except Date, Error, RegExp, URL) cannot cross serialization boundaries";
  readonly __context: Context;
};

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