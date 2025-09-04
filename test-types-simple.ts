// Simple test to see how SerializableOnly behaves

type SuperJSONPrimitive =
  | string
  | number
  | boolean
  | null
  | undefined
  | bigint;

type SuperJSONComplex =
  | Date
  | RegExp
  | Error
  | URL
  | Map<SuperJSONSerializable, SuperJSONSerializable>
  | Set<SuperJSONSerializable>
  | Array<SuperJSONSerializable>
  | { [key: string]: SuperJSONSerializable };

type SuperJSONSerializable = SuperJSONPrimitive | SuperJSONComplex;

type SerializableOnly<T> = T extends SuperJSONSerializable
  ? T
  : T extends (...args: any[]) => any
    ? never
    : T extends symbol
      ? never
      : T extends object
        ? T extends Error | Date | RegExp | URL
          ? T
          : T extends Map<infer K, infer V>
            ? Map<SerializableOnly<K>, SerializableOnly<V>>
            : T extends Set<infer V>
              ? Set<SerializableOnly<V>>
              : T extends Array<infer V>
                ? Array<SerializableOnly<V>>
                : T extends Buffer
                  ? Buffer
                  : {
                      [K in keyof T as T[K] extends (...args: any[]) => any
                        ? never
                        : T[K] extends symbol
                          ? never
                          : K]: SerializableOnly<T[K]>;
                    }
        : T;

// Test 1: Function returns object with function - should error
function test1(): SerializableOnly<{ callback: () => void; data: string }> {
  // The return type becomes { data: string } because callback is removed
  // So returning an object WITH callback should error
  return { 
    callback: () => {}, // This line should cause error
    data: "hello" 
  };
}

// Test 2: Function directly returns function - should error
function test2(): SerializableOnly<() => void> {
  // The return type becomes never
  return () => {}; // This should error - can't return value of type never
}

// Test 3: Valid serializable object - should work
function test3(): SerializableOnly<{ data: string; date: Date }> {
  return { 
    data: "hello",
    date: new Date()
  }; // This should work fine
}

// Now simulate the actual DBOS signature
declare function registerWorkflow<Args extends unknown[], Return>(
  func: (...args: Args) => Promise<SerializableOnly<Return>>
): void;

// Test with bad workflow
async function badWorkflow(): Promise<{ callback: () => void }> {
  return { callback: () => {} };
}

// This line should produce type error
// @ts-expect-error: Testing that this produces an error
const result = registerWorkflow(badWorkflow);

// Let's also test inline to see the exact error
registerWorkflow(async () => {
  return { callback: () => console.log("oops") };
});