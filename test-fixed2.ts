// Alternative approach using conditional types

type Primitive = string | number | boolean | null | undefined | bigint;
type SerializableObject = {
  [key: string]: Primitive | SerializableObject | Array<Primitive | SerializableObject>;
};
type Serializable = Primitive | SerializableObject | Array<Primitive | SerializableObject>;

// Helper to check if a type is serializable
type IsSerializable<T> = T extends Serializable ? T : never;

// The constraint approach
type ValidateSerializable<T> = 
  T extends (...args: any[]) => any ? ["Error: Functions cannot be serialized"] :
  T extends symbol ? ["Error: Symbols cannot be serialized"] :
  T extends object ? {
    [K in keyof T]: ValidateSerializable<T[K]>
  } :
  T;

// New signature using conditional type for error
declare function registerWorkflow<Args extends unknown[], Return>(
  func: (...args: Args) => Promise<Return> & 
    (ValidateSerializable<Return> extends Return ? {} : never)
): (...args: Args) => Promise<Return>;

// Test
async function badWorkflow(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

const registered = registerWorkflow(badWorkflow);