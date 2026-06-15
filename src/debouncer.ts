import { randomUUID } from 'node:crypto';
import { DBOS, DBOSClient } from '.';
import { StartWorkflowParams } from './dbos';
import { DBOSExecutor } from './dbos-executor';
import {
  getFunctionRegistration,
  getFunctionRegistrationByName,
  getRegisteredFunctionClassName,
  UntypedAsyncFunction,
} from './decorators';
import { getDBOSErrorCode, QueueDedupIDDuplicated } from './error';
import { DEBOUNCER_WORKLOW_NAME, INTERNAL_QUEUE_NAME } from './utils';

// Parameters for the debouncer workflow
interface DebouncerWorkflowParams {
  workflowName: string;
  workflowClassName: string;
  startWorkflowParams?: StartWorkflowParams;
  debounceTimeoutMs?: number;
}

// The message sent from a debounce to the debouncer workflow
interface DebouncerMessage {
  args: unknown[];
  messageID: string;
  debouncePeriodMs: number;
}

interface DebouncerConfig<Args extends unknown[], Return> {
  workflow: (...args: Args) => Promise<Return>;
  startWorkflowParams?: StartWorkflowParams;
  debounceTimeoutMs?: number;
}

interface DebouncerClientConfig {
  workflowName: string;
  workflowClassName?: string;
  startWorkflowParams?: StartWorkflowParams;
  debounceTimeoutMs?: number;
}

const _DEBOUNCER_TOPIC = 'DEBOUNCER_TOPIC';

export const debouncerWorkflowFunction = async (
  initialDebouncePeriodMs: number,
  cfg: DebouncerWorkflowParams,
  ...args: unknown[]
) => {
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
};

export class Debouncer<Args extends unknown[], Return> {
  private readonly cfg: DebouncerWorkflowParams;
  constructor(params: DebouncerConfig<Args, Return>) {
    const wInfo = getFunctionRegistration(params.workflow);
    this.cfg = {
      workflowName: wInfo?.name ?? params.workflow.name,
      workflowClassName: getRegisteredFunctionClassName(params.workflow),
      startWorkflowParams: params.startWorkflowParams,
      debounceTimeoutMs: params.debounceTimeoutMs,
    };
  }

  async debounce(debounceKey: string, debouncePeriodMs: number, ...args: Args) {
    if (debouncePeriodMs <= 0) {
      throw Error(`debouncePeriodMs must be positive, not ${debouncePeriodMs}`);
    }
    const cfg = { ...this.cfg };
    cfg.startWorkflowParams = this.cfg.startWorkflowParams ? { ...this.cfg.startWorkflowParams } : {};
    cfg.startWorkflowParams.workflowID = cfg.startWorkflowParams.workflowID ?? (await DBOS.randomUUID());
    while (true) {
      const deduplicationID = `${cfg.workflowClassName}.${cfg.workflowName}-${debounceKey}`;
      try {
        // Attempt to enqueue a debouncer for this workflow
        await DBOS.startWorkflow(DBOSExecutor.debouncerWorkflow!, {
          queueName: INTERNAL_QUEUE_NAME,
          enqueueOptions: { deduplicationID },
        })(debouncePeriodMs, cfg, ...args);
        return DBOS.retrieveWorkflow<Return>(cfg.startWorkflowParams.workflowID);
      } catch (e) {
        // If there is already a debouncer, send a message to it.
        if (e instanceof Error && getDBOSErrorCode(e) === QueueDedupIDDuplicated) {
          const dedupWorkflowID = await DBOS.runStep(async () => {
            return await DBOSExecutor.globalInstance?.systemDatabase.getDeduplicatedWorkflow(
              INTERNAL_QUEUE_NAME,
              deduplicationID,
            );
          });
          if (!dedupWorkflowID) {
            continue;
          } else {
            const messageID = await DBOS.randomUUID();
            const message: DebouncerMessage = {
              messageID,
              args,
              debouncePeriodMs,
            };
            await DBOS.send(dedupWorkflowID, message, _DEBOUNCER_TOPIC);
            // Wait for the debouncer to acknowledge receipt of the message.
            // If the message is not acknowledged, this likely means the debouncer started its workflow
            // and exited without processing this message, so try again.
            const event = await DBOS.getEvent(dedupWorkflowID, messageID, 1000);
            if (!event) {
              continue;
            }
            // Retrieve the user workflow ID from the input to the debouncer
            // and return a handle to it.
            const dedupWorkflowInput = await DBOS.retrieveWorkflow(dedupWorkflowID).getWorkflowInputs();
            const typedInput = dedupWorkflowInput as Parameters<typeof debouncerWorkflowFunction>;
            const userWorkflowID = typedInput[1].startWorkflowParams!.workflowID!;
            return DBOS.retrieveWorkflow<Return>(userWorkflowID);
          }
        } else {
          throw e;
        }
      }
    }
  }
}

export class DebouncerClient {
  private readonly cfg: DebouncerWorkflowParams;
  constructor(
    readonly client: DBOSClient,
    params: DebouncerClientConfig,
  ) {
    this.cfg = {
      workflowName: params.workflowName,
      workflowClassName: params.workflowClassName || '',
      startWorkflowParams: params.startWorkflowParams,
      debounceTimeoutMs: params.debounceTimeoutMs,
    };
  }

  async debounce(debounceKey: string, debouncePeriodMs: number, ...args: unknown[]) {
    if (debouncePeriodMs <= 0) {
      throw Error(`debouncePeriodMs must be positive, not ${debouncePeriodMs}`);
    }
    const cfg = { ...this.cfg };
    cfg.startWorkflowParams = this.cfg.startWorkflowParams ? { ...this.cfg.startWorkflowParams } : {};
    cfg.startWorkflowParams.workflowID = cfg.startWorkflowParams.workflowID ?? String(randomUUID());
    while (true) {
      const deduplicationID = `${cfg.workflowClassName}.${cfg.workflowName}-${debounceKey}`;
      try {
        // Attempt to enqueue a debouncer for this workflow
        await this.client.enqueue(
          { workflowName: DEBOUNCER_WORKLOW_NAME, queueName: INTERNAL_QUEUE_NAME, deduplicationID },
          debouncePeriodMs,
          cfg,
          ...args,
        );
        return this.client.retrieveWorkflow(cfg.startWorkflowParams.workflowID);
      } catch (e) {
        // If there is already a debouncer, send a message to it.
        if (e instanceof Error && getDBOSErrorCode(e) === QueueDedupIDDuplicated) {
          // Access the private client system database
          // eslint-disable-next-line @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-explicit-any
          const dedupWorkflowID = (await (this.client as any).systemDatabase.getDeduplicatedWorkflow(
            INTERNAL_QUEUE_NAME,
            deduplicationID,
          )) as string;
          if (!dedupWorkflowID) {
            continue;
          } else {
            const messageID = String(randomUUID());
            const message: DebouncerMessage = {
              messageID,
              args,
              debouncePeriodMs,
            };
            await this.client.send(dedupWorkflowID, message, _DEBOUNCER_TOPIC);
            // Wait for the debouncer to acknowledge receipt of the message.
            // If the message is not acknowledged, this likely means the debouncer started its workflow
            // and exited without processing this message, so try again.
            const event = await this.client.getEvent(dedupWorkflowID, messageID, 1000);
            if (!event) {
              continue;
            }
            // Retrieve the user workflow ID from the input to the debouncer
            // and return a handle to it.
            const dedupWorkflowInput = await this.client.retrieveWorkflow(dedupWorkflowID).getWorkflowInputs();
            const typedInput = dedupWorkflowInput as Parameters<typeof debouncerWorkflowFunction>;
            const userWorkflowID = typedInput[1].startWorkflowParams!.workflowID!;
            return this.client.retrieveWorkflow(userWorkflowID);
          }
        } else {
          throw e;
        }
      }
    }
  }
}
