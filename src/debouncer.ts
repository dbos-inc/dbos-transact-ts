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

const _DEBOUNCER_TOPIC = 'DEBOUNCER_TOPIC';

export const debouncerWorkflow = DBOS.registerWorkflow(
  async (initialDebouncePeriodMs: number, options: DebouncerOptions, ...args: unknown[]) => {
    let workflowInputs = args;
    const debounceDeadlineEpochMs = options.debounceTimeoutMs
      ? Date.now() + options.debounceTimeoutMs
      : Number.MAX_VALUE;
    let debouncePeriodMs = initialDebouncePeriodMs;
    while (Date.now() < debounceDeadlineEpochMs) {
      const timeUntilDeadline = Math.max(debounceDeadlineEpochMs - Date.now(), 0);
      const timeoutMs = Math.min(debouncePeriodMs, timeUntilDeadline);
      const message = await DBOS.recv<DebouncerMessage>(_DEBOUNCER_TOPIC, timeoutMs / 1000);
      if (message === null) {
        break;
      } else {
        workflowInputs = message['args'];
        debouncePeriodMs = message['debouncePeriodMs'];
        await DBOS.setEvent(message.messageID, message.messageID);
      }
    }
    const methReg = getFunctionRegistrationByName(options.workflowClassName, options.workflowName);
    if (!methReg || !methReg.registeredFunction) {
      throw Error(`Invalid workflow name provided to debouncer: ${options.workflowName}`);
    }
    const func = methReg?.registeredFunction as UntypedAsyncFunction;
    await DBOS.startWorkflow(func, options.startWorkflowParams)(...workflowInputs);
  },
);
