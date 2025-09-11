import { DBOS } from '.';
import { StartWorkflowParams } from './dbos';
import { getFunctionRegistrationByName, UntypedAsyncFunction } from './decorators';

// Parameters for the debouncer workflow
interface DebouncerOptions {
  workflowClassName: string;
  workflowName: string;
  startWorkflowParams: StartWorkflowParams;
  debounceTimeoutMs?: number;
}

// The message sent from a debounce to the debouncer workflow
interface DebouncerMessage {
  args: unknown[];
  messageID: string;
  debouncePeriodMs: number;
}

export async function debouncerWorkflow(
  initialDebouncePeriodMs: number,
  options: DebouncerOptions,
  ...args: unknown[]
) {
  const methReg = getFunctionRegistrationByName(options.workflowClassName, options.workflowName);
  const func = methReg?.registeredFunction as UntypedAsyncFunction;
  await DBOS.startWorkflow(func, options.startWorkflowParams)(...args);
}
