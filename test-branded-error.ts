// Using branded error types for clear error messages

type SuperJSONSerializable = 
  | string | number | boolean | null | undefined | bigint
  | Date | RegExp | Error | URL | Buffer
  | Array<SuperJSONSerializable>
  | Set<SuperJSONSerializable> 
  | Map<SuperJSONSerializable, SuperJSONSerializable>
  | { [key: string]: SuperJSONSerializable };

// Branded error types with descriptive messages
type NotSerializable<T = unknown> = {
  readonly __error: "This type cannot be serialized and persisted by DBOS";
  readonly __hint: "Functions, Symbols, and class instances (except Date, Error, RegExp, URL) cannot cross serialization boundaries";
  readonly __type: T;
};

// More specific error messages
type FunctionNotSerializable = {
  readonly __error: "Functions cannot be serialized";
  readonly __hint: "Remove function properties or convert to serializable data";
};

type SymbolNotSerializable = {
  readonly __error: "Symbols cannot be serialized";
  readonly __hint: "Use strings or numbers instead of symbols";
};

type SerializableOnly<T> = T extends SuperJSONSerializable
  ? T
  : T extends (...args: any[]) => any
    ? FunctionNotSerializable
    : T extends symbol
      ? SymbolNotSerializable
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

// Test the function signature
declare function registerWorkflow<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => Promise<SerializableOnly<Return>>
): (this: This, ...args: Args) => Promise<Return>;

// Test case 1: Function with callback property
async function workflowWithCallback(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

// This should show error about functions not being serializable
const test1 = registerWorkflow(workflowWithCallback);

// Test case 2: Direct function return
async function workflowReturnsFunction(): Promise<() => void> {
  return () => console.log("oops");
}

// This should show FunctionNotSerializable error
const test2 = registerWorkflow(workflowReturnsFunction);

// Test case 3: Symbol in object
async function workflowWithSymbol(): Promise<{ id: symbol }> {
  return { id: Symbol("test") };
}

// This should show SymbolNotSerializable error  
const test3 = registerWorkflow(workflowWithSymbol);

// Test case 4: Good workflow
async function goodWorkflow(): Promise<{ data: string; error: Error; date: Date }> {
  return { data: "hello", error: new Error("test"), date: new Date() };
}

// This should work fine
const test4 = registerWorkflow(goodWorkflow);