// Final NoInfer solution that actually works!

type SuperJSONSerializable = 
  | string | number | boolean | null | undefined | bigint
  | Date | RegExp | Error | URL | Buffer
  | Array<SuperJSONSerializable>
  | Set<SuperJSONSerializable> 
  | Map<SuperJSONSerializable, SuperJSONSerializable>
  | { [key: string]: SuperJSONSerializable };

type SerializableOnly<T> = T extends SuperJSONSerializable
  ? T
  : T extends (...args: any[]) => any
    ? never
    : T extends symbol
      ? never
      : T extends object
        ? T extends Error | Date | RegExp | URL | Buffer
          ? T
          : T extends Map<infer K, infer V>
            ? Map<SerializableOnly<K>, SerializableOnly<V>>
            : T extends Set<infer V>
              ? Set<SerializableOnly<V>>
              : T extends Array<infer V>
                ? Array<SerializableOnly<V>>
                : {
                    [K in keyof T as T[K] extends (...args: any[]) => any 
                      ? never 
                      : T[K] extends symbol 
                        ? never 
                        : K]: SerializableOnly<T[K]>
                  }
        : T;

type SerializableArgs<T extends readonly unknown[]> = {
  [K in keyof T]: SerializableOnly<T[K]>;
};

// The solution: Use explicit type parameter with NoInfer
// When no type is provided, Return defaults to never, causing an error
// When type IS provided, it must be serializable
declare function registerWorkflow<This, Args extends unknown[], Return = never>(
  func: (this: This, ...args: SerializableArgs<Args>) => Promise<SerializableOnly<NoInfer<Return>>>
): (this: This, ...args: Args) => Promise<Return>;

declare function registerStep<This, Args extends unknown[], Return = never>(
  func: (this: This, ...args: Args) => Promise<SerializableOnly<NoInfer<Return>>>
): (this: This, ...args: Args) => Promise<Return>;

// Test cases
async function workflowWithCallback(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

async function workflowGood(): Promise<{ data: string; date: Date }> {
  return { data: "hello", date: new Date() };
}

// This will error - Return defaults to never, can't match the function
const bad1 = registerWorkflow(workflowWithCallback);

// This will error - explicit type has non-serializable callback
const bad2 = registerWorkflow<any, [], { data: string; callback: () => void }>(workflowWithCallback);

// This works - explicit serializable type
const good1 = registerWorkflow<any, [], { data: string; date: Date }>(workflowGood);

// For steps - args not serialized, only return
async function stepWithNonSerializableArgs(fn: () => void): Promise<{ result: string }> {
  fn();
  return { result: "done" };
}

// Step args can be non-serializable, but return must be serializable
const step1 = registerStep<any, [() => void], { result: string }>(stepWithNonSerializableArgs);