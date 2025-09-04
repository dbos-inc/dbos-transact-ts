// Test to understand the registerWorkflow behavior

type SerializableOnly<T> = T extends string | number | boolean | null | undefined
  ? T
  : T extends (...args: any[]) => any
    ? never
    : T extends object
      ? {
          [K in keyof T as T[K] extends (...args: any[]) => any ? never : K]: SerializableOnly<T[K]>
        }
      : T;

// Simulate DBOS.registerWorkflow signature
declare function registerWorkflow<Args extends unknown[], Return>(
  func: (...args: Args) => Promise<SerializableOnly<Return>>
): (...args: Args) => Promise<Return>;

// Test case 1: Function that returns object with callback
async function workflowWithCallback(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

// What happens when we register it?
const registered1 = registerWorkflow(workflowWithCallback);
// The PROBLEM: TypeScript infers Return = { data: string; callback: () => void }
// The function signature expects Promise<SerializableOnly<Return>>
// But our function returns Promise<Return> directly
// TypeScript seems to be matching these, but they shouldn't match!

// Let's try to understand the type inference
type InferredReturn = { data: string; callback: () => void };
type ExpectedParam = Promise<SerializableOnly<InferredReturn>>;
type ActualReturn = Promise<InferredReturn>;

// These should NOT be assignable, but let's check
declare let expected: ExpectedParam;
declare let actual: ActualReturn;

// This should error but might not due to structural typing
expected = actual; // Does this error?