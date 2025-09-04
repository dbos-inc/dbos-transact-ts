// Test that branded error types produce clear error messages
import { SerializableOnly } from './src/serialization';

// Simulate the DBOS signatures
declare function registerWorkflow<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => Promise<SerializableOnly<Return>>
): (this: This, ...args: Args) => Promise<Return>;

declare function registerStep<This, Args extends unknown[], Return>(
  func: (this: This, ...args: Args) => Promise<SerializableOnly<Return>>
): (this: This, ...args: Args) => Promise<Return>;

// Test 1: Function property in object
async function workflowWithCallback() {
  return { 
    data: "hello", 
    processData: () => console.log("processing") 
  };
}

const test1 = registerWorkflow(workflowWithCallback);
// Expected error: Should mention "Functions cannot be serialized"

// Test 2: Symbol in return type
async function workflowWithSymbol() {
  return {
    id: Symbol("unique"),
    name: "test"
  };
}

const test2 = registerWorkflow(workflowWithSymbol);
// Expected error: Should mention "Symbols cannot be serialized"

// Test 3: Direct function return
async function workflowReturnsFunction() {
  return () => "hello world";
}

const test3 = registerStep(workflowReturnsFunction);
// Expected error: Should show NotSerializable with function context

// Test 4: Valid serializable types - should work
async function validWorkflow() {
  return {
    string: "hello",
    number: 42,
    boolean: true,
    date: new Date(),
    error: new Error("test"),
    map: new Map([["key", "value"]]),
    set: new Set([1, 2, 3]),
    array: [1, 2, 3],
    nested: {
      data: "nested"
    }
  };
}

const test4 = registerWorkflow(validWorkflow);
// Should work without errors