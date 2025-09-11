import { DBOS } from '.';
import { StartWorkflowParams } from './dbos';
import { getFunctionRegistrationByName, UntypedAsyncFunction } from './decorators';

// Parameters for the debouncer workflow
interface DebouncerWorkflowParams {
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

interface DebouncerConfig {
  workflowClassName: string;
  workflowName: string;
  startWorkflowParams: StartWorkflowParams;
  debounceTimeoutMs?: number;
  debouncerKey: string;
}

const _DEBOUNCER_TOPIC = 'DEBOUNCER_TOPIC';

const debouncerWorkflow = DBOS.registerWorkflow(
  async (initialDebouncePeriodMs: number, cfg: DebouncerWorkflowParams, ...args: unknown[]) => {
    let workflowInputs = args;
    const debounceDeadlineEpochMs = cfg.debounceTimeoutMs ? Date.now() + cfg.debounceTimeoutMs : Number.MAX_VALUE;
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
    const methReg = getFunctionRegistrationByName(cfg.workflowClassName, cfg.workflowName);
    if (!methReg || !methReg.registeredFunction) {
      throw Error(`Invalid workflow name provided to debouncer: ${cfg.workflowName}`);
    }
    const func = methReg?.registeredFunction as UntypedAsyncFunction;
    await DBOS.startWorkflow(func, cfg.startWorkflowParams)(...workflowInputs);
  },
);

export class Debouncer {
  private readonly cfg: DebouncerWorkflowParams;
  private readonly debouncerKey: string;
  constructor(params: DebouncerConfig) {
    this.cfg = {
      workflowClassName: params.workflowClassName,
      workflowName: params.workflowName,
      startWorkflowParams: params.startWorkflowParams,
      debounceTimeoutMs: params.debounceTimeoutMs,
    };
    this.debouncerKey = params.debouncerKey;
  }

  async debounce(debouncePeriodMs: number, ...args: unknown[]) {
    await DBOS.startWorkflow(debouncerWorkflow)(debouncePeriodMs, this.cfg, ...args);
    return DBOS.retrieveWorkflow(this.cfg.startWorkflowParams.workflowID!);
  }
}
