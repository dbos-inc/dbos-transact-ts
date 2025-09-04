// Fixed version that actually enforces constraints

type SerializableOnly<T> = T extends string | number | boolean | null | undefined
  ? T
  : T extends (...args: any[]) => any
    ? never
    : T extends object
      ? {
          [K in keyof T as T[K] extends (...args: any[]) => any ? never : K]: SerializableOnly<T[K]>
        }
      : T;

// WRONG - TypeScript infers Return from the passed function
declare function registerWorkflowWrong<Args extends unknown[], Return>(
  func: (...args: Args) => Promise<SerializableOnly<Return>>
): (...args: Args) => Promise<Return>;

// CORRECT - Force the function to return SerializableOnly<Return>, then extract Return
declare function registerWorkflowCorrect<Args extends unknown[], Return>(
  func: (...args: Args) => Promise<Return>
): (...args: Args) => Promise<SerializableOnly<Return>>;

// But wait, that changes the return type of the registered function...
// We need a different approach

// ACTUALLY CORRECT - Constrain Return itself
declare function registerWorkflow<
  Args extends unknown[], 
  Return extends SerializableOnly<Return>
>(
  func: (...args: Args) => Promise<Return>
): (...args: Args) => Promise<Return>;

// Test it
async function badWorkflow(): Promise<{ data: string; callback: () => void }> {
  return { data: "hello", callback: () => {} };
}

// This should now error!
const registered = registerWorkflow(badWorkflow);