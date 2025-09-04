// Different NoInfer approach - prevent inference of Return entirely

type SerializableOnly<T> = T extends string | number | boolean | null | undefined
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

// Approach 1: NoInfer on the entire return type
declare function registerWorkflow1<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => NoInfer<Promise<SerializableOnly<Return>>>
): (this: This, ...args: Args) => Promise<Return>;

// Approach 2: Force the function to match a specific shape
declare function registerWorkflow2<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => Promise<Return> extends Promise<SerializableOnly<Return>> 
    ? Promise<Return>
    : never
): (this: This, ...args: Args) => Promise<Return>;

// Approach 3: Use NoInfer differently - on the Return generic itself
declare function registerWorkflow3<This, Args extends unknown[], Return = never>(
  func: (this: This, ...args: Args) => Promise<SerializableOnly<NoInfer<Return>>>
): (this: This, ...args: Args) => Promise<Return>;

// Test function with non-serializable return
async function badWorkflow(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

// Test each approach
const test1 = registerWorkflow1(badWorkflow);
const test2 = registerWorkflow2(badWorkflow); 
const test3 = registerWorkflow3(badWorkflow);

// Try with explicit type argument
const test4 = registerWorkflow3<any, [], { data: string; callback: () => void }>(badWorkflow);