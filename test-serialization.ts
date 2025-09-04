import { SerializableOnly } from './src/serialization';

// Test 1: Function in object
type Test1 = SerializableOnly<{ data: string; callback: () => void }>;
// Result: { data: string } - callback is omitted

// Test 2: Direct function
type Test2 = SerializableOnly<() => void>;
// Result: never

// Test 3: Array with functions
type Test3 = SerializableOnly<Array<string | (() => void)>>;
// Result: Array<string | never> which simplifies to Array<string>

// Let's test with actual usage:
declare function registerWorkflow<Args extends unknown[], Return>(
  func: (...args: Args) => Promise<SerializableOnly<Return>>
): void;

// This should error because the function signature expects SerializableOnly<Return>
// but we're returning something that includes a function
async function badWorkflow(): Promise<{ callback: () => void }> {
  return { callback: () => console.log('oops') };
}

// Try to register it
registerWorkflow(badWorkflow); // Should this error?
