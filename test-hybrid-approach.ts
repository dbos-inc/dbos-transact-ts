// Hybrid approach - use overloads for better UX

type SerializableOnly<T> = T extends string | number | boolean | null | undefined | bigint
  ? T
  : T extends (...args: any[]) => any
    ? never
    : T extends symbol
      ? never
      : T extends Date | RegExp | Error | URL | Buffer
        ? T
        : T extends Map<infer K, infer V>
          ? Map<SerializableOnly<K>, SerializableOnly<V>>
          : T extends Set<infer V>
            ? Set<SerializableOnly<V>>
            : T extends Array<infer V>
              ? Array<SerializableOnly<V>>
              : T extends object
                ? {
                    [K in keyof T as T[K] extends (...args: any[]) => any 
                      ? never 
                      : T[K] extends symbol 
                        ? never 
                        : K]: SerializableOnly<T[K]>
                  }
                : T;

// Helper type to check if a type is fully serializable
type IsFullySerializable<T> = SerializableOnly<T> extends T ? true : false;

// Error type for better messages
type NonSerializableError = "Error: Return type contains non-serializable values (functions or symbols)";

interface RegisterWorkflow {
  // Overload 1: If the return type is fully serializable, accept it
  <This, Args extends unknown[], Return>(
    func: IsFullySerializable<Return> extends true 
      ? (this: This, ...args: Args) => Promise<Return>
      : NonSerializableError
  ): (this: This, ...args: Args) => Promise<Return>;
  
  // Overload 2: Generic case with SerializableOnly transformation
  <This, Args extends unknown[], Return>(
    func: (this: This, ...args: Args) => Promise<SerializableOnly<Return>>
  ): (this: This, ...args: Args) => Promise<SerializableOnly<Return>>;
}

declare const registerWorkflow: RegisterWorkflow;

// Test cases
async function workflowBad(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

async function workflowGood(): Promise<{ data: string; date: Date }> {
  return { data: "hello", date: new Date() };
}

const bad = registerWorkflow(workflowBad);   // Should error
const good = registerWorkflow(workflowGood);  // Should work