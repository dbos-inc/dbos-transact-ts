// Final approach - use overloads for better error messages

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

// Check if a type equals never
type IsNever<T> = [T] extends [never] ? true : false;

// Check if types are equal
type Equals<X, Y> = 
  (<T>() => T extends X ? 1 : 2) extends 
  (<T>() => T extends Y ? 1 : 2) ? true : false;

// The actual approach used in the PR already works for getResult()!
// The issue is with registerWorkflow signature

// Let's use a different strategy - make the func parameter accept SerializableOnly
declare function registerWorkflow<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => Promise<Return>,
  config?: any
): Return extends SuperJSONSerializable 
  ? (this: This, ...args: Args) => Promise<Return>
  : SerializableOnly<Return> extends never
    ? ["Error: Return type contains non-serializable values (functions or symbols)"]
    : (this: This, ...args: Args) => Promise<SerializableOnly<Return>>;

// Test
async function badWorkflow(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

async function goodWorkflow(): Promise<{ data: string; date: Date }> {
  return { data: "hello", date: new Date() };
}

const bad = registerWorkflow(badWorkflow);  // Should have error in return type
const good = registerWorkflow(goodWorkflow); // Should work fine