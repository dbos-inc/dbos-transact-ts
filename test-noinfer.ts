// Test using NoInfer to prevent type inference

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
        ? {
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

// Original approach - TypeScript infers Return from the function
declare function registerWorkflowOld<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => Promise<SerializableOnly<Return>>
): (this: This, ...args: Args) => Promise<Return>;

// NEW: Use NoInfer to prevent TypeScript from inferring Return from the function body
declare function registerWorkflowNew<This, Args extends unknown[], Return>(
  func: (this: This, ...args: SerializableArgs<Args>) => Promise<NoInfer<SerializableOnly<Return>>>
): (this: This, ...args: Args) => Promise<Return>;

// Test cases
async function workflowWithCallback(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

async function workflowWithSymbol(): Promise<{ data: string; sym: symbol }> {
  return { data: "hello", sym: Symbol("test") };
}

async function goodWorkflow(): Promise<{ data: string; date: Date; map: Map<string, number> }> {
  return { data: "hello", date: new Date(), map: new Map([["key", 42]]) };
}

// Test with old approach (no errors expected - inference bypasses constraint)
const old1 = registerWorkflowOld(workflowWithCallback);
const old2 = registerWorkflowOld(workflowWithSymbol);
const old3 = registerWorkflowOld(goodWorkflow);

// Test with new NoInfer approach (should error on non-serializable)
const new1 = registerWorkflowNew(workflowWithCallback); // Should error!
const new2 = registerWorkflowNew(workflowWithSymbol);   // Should error!
const new3 = registerWorkflowNew(goodWorkflow);         // Should work

// Also test with inline functions to see the errors
registerWorkflowNew(async function() {
  return { callback: () => console.log("oops") };
});

registerWorkflowNew(async function(): Promise<{ data: string }> {
  return { data: "hello" }; // This should work
});