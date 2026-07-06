import { DBOS, DBOSClient } from '.';
import { runTransactionalInternalStep, StartWorkflowParams } from './dbos';
import { DBOSExecutor } from './dbos-executor';
import { getNextWFID } from './context';
import {
  ensureDBOSIsLaunched,
  getFunctionRegistration,
  getFunctionRegistrationByName,
  getRegisteredFunctionClassName,
  UntypedAsyncFunction,
} from './decorators';
import { DBOSError, DBOSQueueDuplicatedError, getDBOSErrorCode, QueueDedupIDDuplicated } from './error';
import { serializeArgs, serializeFunctionInputOutput } from './serialization';
import { DebounceResult } from './system_database';
import { INTERNAL_QUEUE_NAME } from './utils';
import { WorkflowHandle, WorkflowSerializationFormat } from './workflow';
import { PortableWorkflowError } from '../schemas/system_db_schema';

// Parameters for the debouncer
interface DebouncerParams {
  workflowName: string;
  workflowClassName: string;
  startWorkflowParams?: StartWorkflowParams;
  debounceTimeoutMs?: number;
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
  // Serialization format for the workflow's inputs, e.g. 'portable' for a workflow registered with portable serialization.
  serializationType?: WorkflowSerializationFormat;
}

/**
 * True if `e` is a queue-deduplication error, including the portable-serialization
 * replay form, which carries only the original type name.
 */
function isQueueDeduplicatedError(e: unknown): boolean {
  if (e instanceof Error && getDBOSErrorCode(e) === QueueDedupIDDuplicated) {
    return true;
  }
  return e instanceof PortableWorkflowError && e.name === DBOSQueueDuplicatedError.name;
}

/**
 * A debounce owns the workflow's deduplication ID (the debounce key) and its delay
 * (the debounce period), so a caller must not also set them. Priority and partition
 * keys are rejected because they cannot apply to a debounced enqueue.
 */
function rejectConflictingOptions(params: StartWorkflowParams | undefined): void {
  const enqueueOptions = params?.enqueueOptions;
  if (enqueueOptions?.deduplicationID !== undefined) {
    throw new DBOSError(
      'Cannot debounce a workflow with a deduplicationID set: the debounce key is used as the workflow deduplication ID.',
    );
  }
  if (enqueueOptions?.delaySeconds !== undefined) {
    throw new DBOSError('Cannot debounce a workflow with a delay set: the debounce period controls the delay.');
  }
  if (enqueueOptions?.priority !== undefined) {
    throw new DBOSError(
      'Cannot debounce a workflow with a priority set: priority is not supported for debounced workflows.',
    );
  }
  if (enqueueOptions?.queuePartitionKey !== undefined) {
    throw new DBOSError(
      'Cannot debounce a workflow with a queue partition key set: partitioned queues do not support deduplication, which debouncing requires.',
    );
  }
  if (params?.duplicationPolicy === 'return-existing') {
    throw new DBOSError(
      "Cannot debounce a workflow with duplicationPolicy 'return-existing': a debounce owns the deduplication behavior.",
    );
  }
}

// The action a debounce caller should take after a bounce attempt.
type BounceAction = 'return' | 'enqueue' | 'raise' | 'retry';

/**
 * Decide what a debounce caller should do after a bounce attempt.
 * - 'return': an existing debounced workflow was extended; return a handle to it.
 * - 'enqueue': the key is unheld; enqueue a fresh debounced workflow.
 * - 'raise': the key is held by a non-debounced workflow or by a different workflow
 *   whose debounce key collides; surface the deduplication conflict.
 * - 'retry': a same-name debounced holder flipped out of DELAYED mid-bounce (a rare race); retry.
 */
function classifyBounce(result: DebounceResult, workflowName: string, workflowClassName: string): BounceAction {
  if (result.bouncedWorkflowID !== null) {
    return 'return';
  }
  if (result.holderWorkflowID === null) {
    return 'enqueue';
  }
  if (!result.holderIsDebounced) {
    return 'raise';
  }
  if (result.holderWorkflowName !== workflowName || (result.holderWorkflowClassName ?? '') !== workflowClassName) {
    return 'raise';
  }
  return 'retry';
}

export class Debouncer<Args extends unknown[], Return> {
  private readonly cfg: DebouncerParams;
  constructor(params: DebouncerConfig<Args, Return>) {
    const wInfo = getFunctionRegistration(params.workflow);
    this.cfg = {
      workflowName: wInfo?.name ?? params.workflow.name,
      workflowClassName: getRegisteredFunctionClassName(params.workflow),
      startWorkflowParams: params.startWorkflowParams,
      debounceTimeoutMs: params.debounceTimeoutMs,
    };
  }

  async debounce(debounceKey: string, debouncePeriodMs: number, ...args: Args): Promise<WorkflowHandle<Return>> {
    if (debouncePeriodMs <= 0) {
      throw Error(`debouncePeriodMs must be positive, not ${debouncePeriodMs}`);
    }
    ensureDBOSIsLaunched('debounce');
    rejectConflictingOptions(this.cfg.startWorkflowParams);

    const exec = DBOSExecutor.globalInstance!;
    const queueName = this.cfg.startWorkflowParams?.queueName ?? INTERNAL_QUEUE_NAME;
    const deduplicationID = `${this.cfg.workflowClassName}.${this.cfg.workflowName}-${debounceKey}`;

    // Capture a pinned workflow ID once: it is re-applied on every enqueue attempt (a lost
    // dedup race consumes it before throwing) and goes unused when a bounce coalesces.
    const pinnedWorkflowID = getNextWFID(this.cfg.startWorkflowParams?.workflowID);

    // Resolve the workflow function so bounced inputs serialize identically to enqueued ones.
    const methReg = getFunctionRegistrationByName(this.cfg.workflowClassName, this.cfg.workflowName);
    if (!methReg || !methReg.registeredFunction) {
      throw new DBOSError(`Invalid workflow name provided to debouncer: ${this.cfg.workflowName}`);
    }
    const func = methReg.registeredFunction as UntypedAsyncFunction;
    const serializationType = methReg.workflowConfig?.serialization;

    while (true) {
      // Try to extend an existing debounced workflow for this key first (the sole coalescing mechanism).
      // In a workflow, the bounce commits atomically with its step checkpoint so a crash can never
      // commit one without the other, which on recovery would re-bounce work that already ran.
      const funcArgs = await serializeFunctionInputOutput(
        serializationType === 'portable' ? { positionalArgs: args } : args,
        [this.cfg.workflowName, '<arguments>'],
        exec.serializer,
        serializationType,
      );
      const bounceParams = {
        workflowName: this.cfg.workflowName,
        workflowClassName: this.cfg.workflowClassName,
        queueName,
        deduplicationID,
        delayUntilEpochMS: Date.now() + debouncePeriodMs,
        input: funcArgs.stringified,
        serialization: funcArgs.sername,
      };
      const result = await runTransactionalInternalStep<DebounceResult>(
        (client) => exec.systemDatabase.debounceDelayedWorkflow(bounceParams, client),
        'DBOS.debounceDelayedWorkflow',
      );

      const action = classifyBounce(result, this.cfg.workflowName, this.cfg.workflowClassName);
      if (action === 'return') {
        return DBOS.retrieveWorkflow<Return>(result.bouncedWorkflowID!);
      }
      if (action === 'raise') {
        throw new DBOSQueueDuplicatedError(result.holderWorkflowID!, queueName, deduplicationID);
      }
      if (action === 'retry') {
        continue;
      }

      // action === 'enqueue': the key is free, create a fresh debounced workflow.
      const debounceDeadlineEpochMS = this.cfg.debounceTimeoutMs ? Date.now() + this.cfg.debounceTimeoutMs : undefined;
      try {
        // A null timeout detaches any propagated workflow deadline: a debounce delay can be long,
        // so an inherited absolute deadline could expire before the debounced workflow ever runs.
        const handle = await DBOS.startWorkflow(func, {
          workflowID: pinnedWorkflowID,
          queueName,
          timeoutMS: this.cfg.startWorkflowParams?.timeoutMS ?? null,
          workflowAttributes: this.cfg.startWorkflowParams?.workflowAttributes,
          enqueueOptions: {
            applicationVersion: this.cfg.startWorkflowParams?.enqueueOptions?.applicationVersion,
            deduplicationID,
            delaySeconds: debouncePeriodMs / 1000,
            debounceDeadlineEpochMS,
            isDebounced: true,
          },
        })(...args);
        return handle as WorkflowHandle<Return>;
      } catch (e) {
        // A concurrent debounce grabbed the key between bounce and enqueue; loop to bounce that workflow instead.
        if (!isQueueDeduplicatedError(e)) {
          throw e;
        }
        continue;
      }
    }
  }
}

export class DebouncerClient {
  private readonly cfg: DebouncerParams;
  private readonly serializationType: WorkflowSerializationFormat;
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
    this.serializationType = params.serializationType;
  }

  async debounce(debounceKey: string, debouncePeriodMs: number, ...args: unknown[]): Promise<WorkflowHandle<unknown>> {
    if (debouncePeriodMs <= 0) {
      throw Error(`debouncePeriodMs must be positive, not ${debouncePeriodMs}`);
    }
    rejectConflictingOptions(this.cfg.startWorkflowParams);

    const queueName = this.cfg.startWorkflowParams?.queueName ?? INTERNAL_QUEUE_NAME;
    const deduplicationID = `${this.cfg.workflowClassName}.${this.cfg.workflowName}-${debounceKey}`;

    while (true) {
      // Try to extend an existing debounced workflow for this key first (the sole coalescing mechanism).
      const serparam = await serializeArgs(args, undefined, this.client.serializer, this.serializationType);
      const result = await this.client.debounceDelayedWorkflow({
        workflowName: this.cfg.workflowName,
        workflowClassName: this.cfg.workflowClassName,
        queueName,
        deduplicationID,
        delayUntilEpochMS: Date.now() + debouncePeriodMs,
        input: serparam.serializedValue,
        serialization: serparam.serialization,
      });

      const action = classifyBounce(result, this.cfg.workflowName, this.cfg.workflowClassName);
      if (action === 'return') {
        return this.client.retrieveWorkflow(result.bouncedWorkflowID!);
      }
      if (action === 'raise') {
        throw new DBOSQueueDuplicatedError(result.holderWorkflowID!, queueName, deduplicationID);
      }
      if (action === 'retry') {
        continue;
      }

      // action === 'enqueue': the key is free, create a fresh debounced workflow.
      const debounceDeadlineEpochMS = this.cfg.debounceTimeoutMs ? Date.now() + this.cfg.debounceTimeoutMs : undefined;
      try {
        const workflowID = await this.client.enqueueDebounced(
          {
            workflowName: this.cfg.workflowName,
            workflowClassName: this.cfg.workflowClassName || undefined,
            queueName,
            workflowID: this.cfg.startWorkflowParams?.workflowID,
            workflowTimeoutMS: this.cfg.startWorkflowParams?.timeoutMS ?? undefined,
            appVersion: this.cfg.startWorkflowParams?.enqueueOptions?.applicationVersion,
            attributes: this.cfg.startWorkflowParams?.workflowAttributes,
            deduplicationID,
            delaySeconds: debouncePeriodMs / 1000,
            serializationType: this.serializationType,
          },
          debounceDeadlineEpochMS,
          args,
        );
        return this.client.retrieveWorkflow(workflowID);
      } catch (e) {
        // A concurrent debounce grabbed the key between bounce and enqueue; loop to bounce that workflow instead.
        if (!isQueueDeduplicatedError(e)) {
          throw e;
        }
        continue;
      }
    }
  }
}
