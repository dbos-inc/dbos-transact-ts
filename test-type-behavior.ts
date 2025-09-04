import { SerializableOnly } from './src/serialization';

// Test what SerializableOnly does to different types
type TestFunction = SerializableOnly<() => void>;
// Result: never

type TestObjectWithFunction = SerializableOnly<{ 
  data: string; 
  callback: () => void;
  nested: {
    value: number;
    fn: () => string;
  }
}>;
// Result: { data: string; nested: { value: number } }

// Now test with the actual function signature
import { DBOS } from './src/dbos';

// This should produce a type error
async function workflowWithFunction() {
  return {
    data: "hello",
    callback: () => console.log("oops")
  };
}

// Try to register it - this should fail type checking
DBOS.registerWorkflow(workflowWithFunction);

// This should also produce a type error
async function stepWithSymbol() {
  return {
    data: "hello",
    sym: Symbol("test")
  };
}

// Try to register it - this should fail type checking  
DBOS.registerStep(stepWithSymbol);

// This SHOULD work fine
async function goodWorkflow() {
  return {
    data: "hello",
    map: new Map([["key", "value"]]),
    error: new Error("test"),
    date: new Date()
  };
}

DBOS.registerWorkflow(goodWorkflow); // Should be OK
