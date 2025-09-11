import { DBOS } from '.';
import { getFunctionRegistrationByName, UntypedAsyncFunction } from './decorators';

interface ContextOptions {
  workflow_id: string;
  deduplication_id?: string;
  priority?: number;
  app_version?: string;
  workflow_timeout_sec?: number;
}

// Parameters for the debouncer workflow
interface DebouncerOptions {
  workflow_name: string;
  workflow_class_name: string;
  debounce_timeout_sec?: number;
  queue_name?: string;
}

// The message sent from a debounce to the debouncer workflow
interface DebouncerMessage {
  args: unknown[];
  message_id: string;
  debounce_period_sec: number;
}

export async function debouncerWorkflow(
  initialDebouncePeriodMs: number,
  ctx: ContextOptions,
  options: DebouncerOptions,
  ...args: unknown[]
) {
  const methReg = getFunctionRegistrationByName(options.workflow_class_name, options.workflow_name);
  const func = methReg?.registeredFunction as UntypedAsyncFunction;
  await DBOS.startWorkflow(func, { workflowID: ctx.workflow_id })(...args);
}
