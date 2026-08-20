import { DBOSLocalCtx, functionIDGetIncrementForCtx, getCurrentContextStore, isInWorkflowCtx } from './context';
import {
  DBOSNonExistentWorkflowError,
  DBOSStreamNondeterminismError,
  DBOSStreamTimeoutError,
  DBOSUnexpectedStepError,
} from './error';
import {
  deserializeResError,
  deserializeValue,
  DBOSSerializer,
  serializeResError,
  serializeValue,
} from './serialization';
import {
  DBOS_STREAM_CLOSED_SENTINEL,
  isLegacyClosedSentinel,
  isStreamClosedSentinel,
  SystemDatabase,
} from './system_database';
import { cancellableSleep } from './utils';
import { isWorkflowActive } from './workflow';

/** Everything a stream read needs, once its caller has resolved its options. */
export interface StreamReadParams {
  offset: number;
  pollingIntervalMs: number;
  /** How long to wait for each value; the clock restarts every time one is delivered. */
  timeoutMS?: number;
  functionName: string;
  /** Whether a read from a workflow is recorded as a step. Client reads never are. */
  checkpoint: boolean;
}

// Returned by StreamReadCheckpointer.replay when a step has no recorded result and must be read live.
const noRecordedValue = Symbol('noRecordedValue');

// How often a reader re-checks its own workflow for cancellation while it reads.
const STREAM_CANCEL_CHECK_INTERVAL_MS = 1000;

/**
 * Checkpoints the values a workflow reads from a stream, one step per value.
 *
 * A replayed reader re-yields its recorded values instead of re-reading a stream that may have
 * advanced since, so it observes the sequence it originally did. Reads from a step, from a client,
 * or from outside a workflow are not recorded.
 */
class StreamReadCheckpointer {
  #ctx: DBOSLocalCtx | undefined = undefined;
  #bound = false;
  // Function IDs are allocated in order, so the first miss proves every later one misses too.
  #replaying = true;
  #startedAtEpochMs = 0;
  #lastCancelCheckMs = Date.now();
  // Whether this reader currently holds a reserved step that is not yet recorded.
  #inFlight = false;

  constructor(
    private readonly sysdb: SystemDatabase,
    private readonly serializer: DBOSSerializer,
    private readonly key: string,
    private readonly functionName: string,
  ) {}

  /** Reserve the step that records the next value, or undefined if this read is not checkpointed. */
  begin(): number | undefined {
    if (!this.#bound) {
      this.#bound = true;
      const ctx = getCurrentContextStore();
      // Bind once and hold the context: a nested call swaps the ambient one out from under us.
      if (ctx !== undefined && isInWorkflowCtx(ctx)) {
        this.#ctx = ctx;
      }
    }
    if (this.#ctx === undefined) {
      return undefined;
    }
    // Sequential reads record before they yield and so never overlap; an overlap means the reads
    // are concurrent and their step IDs depend on scheduling. Every read shares one function name,
    // so the step-name guard would not catch the resulting swap on replay.
    if ((this.#ctx.activeStreamReads ?? 0) > 0) {
      throw new DBOSStreamNondeterminismError(this.#ctx.workflowId!, this.key);
    }
    this.#ctx.activeStreamReads = (this.#ctx.activeStreamReads ?? 0) + 1;
    this.#inFlight = true;
    // Started when the read began, so the recorded step spans the wait for the value.
    this.#startedAtEpochMs = Date.now();
    return functionIDGetIncrementForCtx(this.#ctx);
  }

  /** Release the reserved step once its outcome is settled. Idempotent. */
  end(): void {
    if (!this.#inFlight) return;
    this.#inFlight = false;
    if (this.#ctx !== undefined) {
      this.#ctx.activeStreamReads = (this.#ctx.activeStreamReads ?? 1) - 1;
    }
  }

  /** Return the value recorded for this step, or noRecordedValue to read it live. */
  async replay(functionID: number | undefined): Promise<unknown> {
    if (functionID === undefined || !this.#replaying) {
      return noRecordedValue;
    }
    const workflowID = this.#ctx!.workflowId!;
    const recorded = await this.sysdb.getOperationResultAndThrowIfCancelled(workflowID, functionID);
    if (recorded === undefined) {
      this.#replaying = false;
      return noRecordedValue;
    }
    if (recorded.functionName !== this.functionName) {
      throw new DBOSUnexpectedStepError(workflowID, functionID, this.functionName, recorded.functionName!);
    }
    if (recorded.error !== null && recorded.error !== undefined) {
      // A timeout is the only failure recorded here; a replay re-raises it without waiting.
      throw await deserializeResError(recorded.error, recorded.serialization ?? null, this.serializer);
    }
    return await deserializeValue(recorded.output ?? null, recorded.serialization ?? null, this.serializer);
  }

  /**
   * Throw if the reading workflow has been cancelled, probing at most once an interval.
   *
   * The replay probe stops querying once past the frontier, so nothing else would notice.
   */
  async checkCancelled(): Promise<void> {
    if (this.#ctx === undefined) return;
    const now = Date.now();
    if (now - this.#lastCancelCheckMs < STREAM_CANCEL_CHECK_INTERVAL_MS) return;
    this.#lastCancelCheckMs = now;
    // Through the limiter, as the stream read itself is: a fan-out of readers must not check out
    // every pool client and starve control-plane operations.
    await this.sysdb.checkIfCanceledLimited(this.#ctx.workflowId!);
  }

  /**
   * Record a value delivered to the workflow, in the app's serializer as step outputs are.
   *
   * Checkpoints are read back only by this runtime replaying this workflow, never across languages,
   * so they do not follow the workflow's declared interop format.
   */
  async record(functionID: number | undefined, value: unknown): Promise<void> {
    if (functionID === undefined) return;
    const serval = await serializeValue(value, this.serializer, undefined);
    await this.sysdb.recordOperationResult(
      this.#ctx!.workflowId!,
      functionID,
      this.functionName,
      true,
      this.#startedAtEpochMs,
      Date.now(),
      { output: serval.serializedValue, serialization: serval.serialization },
    );
    this.end();
  }

  /**
   * Record that the wait for this value timed out, so a replay raises it rather than waiting.
   *
   * A timeout is an outcome rather than a failure of the read, the same way getEvent records the
   * null it returns, and it is the failure a workflow is most likely to catch and act on.
   */
  async recordTimeout(functionID: number | undefined, error: Error): Promise<void> {
    if (functionID === undefined) return;
    const sererr = await serializeResError(error, this.serializer, undefined);
    await this.sysdb.recordOperationResult(
      this.#ctx!.workflowId!,
      functionID,
      this.functionName,
      true,
      this.#startedAtEpochMs,
      Date.now(),
      { error: sererr.serializedValue, serialization: sererr.serialization },
    );
    this.end();
  }
}

/** Yield a stream's values in order, checkpointing each one when read from a workflow. */
export async function* readStreamCore<T>(
  sysdb: SystemDatabase,
  serializer: DBOSSerializer,
  workflowID: string,
  key: string,
  params: StreamReadParams,
): AsyncGenerator<T, void, unknown> {
  const { pollingIntervalMs, timeoutMS, functionName } = params;
  let offset = params.offset;
  const checkpointer = params.checkpoint ? new StreamReadCheckpointer(sysdb, serializer, key, functionName) : undefined;
  const payload = `${workflowID}::${key}`;
  let finalRead = false;

  try {
    while (true) {
      // One step per delivered value, reserved before the read so it spans the wait.
      const functionID = checkpointer?.begin();
      await checkpointer?.checkCancelled();
      // The timeout is per value: the clock restarts every time one is delivered.
      const deadline = timeoutMS !== undefined ? Date.now() + timeoutMS : undefined;
      const recorded = checkpointer ? await checkpointer.replay(functionID) : noRecordedValue;
      if (recorded !== noRecordedValue) {
        // Settled from history, so the step is no longer in flight.
        checkpointer?.end();
        if (isStreamClosedSentinel(recorded)) {
          return;
        }
        yield recorded as T;
        offset += 1;
        continue;
      }

      let value: { serializedValue: string; serialization: string | null } | undefined;
      while (true) {
        // Register a listener before reading so a notification arriving between the
        // read and the wait below is not lost; a fresh promise per iteration gives
        // the "clear before reading" semantics.
        let resolveNotification: () => void;
        const messagePromise = new Promise<void>((resolve) => {
          resolveNotification = resolve;
        });
        const cbr = sysdb.streamsMap.registerCallback(payload, resolveNotification!);
        try {
          // One round trip for both the value and the workflow's status.
          const read = await sysdb.readStreamValue(workflowID, key, offset);
          if (read.status === null) {
            throw new DBOSNonExistentWorkflowError(`Workflow ${workflowID} does not exist`);
          }
          value = read.value;
          if (value !== undefined || finalRead) {
            break;
          }
          // No value yet: stop if the workflow is done, else wait for a notification (bounded by the poll interval so termination is noticed).
          if (!isWorkflowActive(read.status)) {
            // Cancel/timeout set a terminal status while the workflow may still be writing, so drain to the first empty offset before stopping.
            finalRead = true;
            continue;
          }
          let waitMs = pollingIntervalMs;
          if (deadline !== undefined) {
            const remaining = deadline - Date.now();
            if (remaining <= 0) {
              const error = new DBOSStreamTimeoutError(workflowID, key, timeoutMS);
              await checkpointer?.recordTimeout(functionID, error);
              throw error;
            }
            waitMs = Math.min(remaining, pollingIntervalMs);
          }
          await checkpointer?.checkCancelled();
          const { promise, cancel } = cancellableSleep(waitMs);
          try {
            await Promise.race([messagePromise, promise]);
          } finally {
            cancel();
          }
        } finally {
          sysdb.streamsMap.deregisterCallback(cbr);
        }
      }

      if (value === undefined) {
        // The end is recorded too, so a replay stops exactly where this read did.
        await checkpointer?.record(functionID, DBOS_STREAM_CLOSED_SENTINEL);
        return;
      }
      // Tested after deserializing, as the replay branch above must, so the two never disagree about
      // where the stream ends. The legacy marker is short-circuited because it does not parse.
      const deserialized = isLegacyClosedSentinel(value.serializedValue)
        ? DBOS_STREAM_CLOSED_SENTINEL
        : await deserializeValue(value.serializedValue, value.serialization, serializer);
      if (isStreamClosedSentinel(deserialized)) {
        await checkpointer?.record(functionID, DBOS_STREAM_CLOSED_SENTINEL);
        return;
      }
      // Recorded before the yield, so a crash after the workflow acts on the value still replays it.
      await checkpointer?.record(functionID, deserialized);
      yield deserialized as T;
      offset += 1;
    }
  } finally {
    // Release the reserved step if the read was abandoned or threw mid-flight.
    checkpointer?.end();
  }
}

/** Return the single value at one offset of a stream, waiting for it to be written. */
export async function readStreamOffsetCore<T>(
  sysdb: SystemDatabase,
  serializer: DBOSSerializer,
  workflowID: string,
  key: string,
  params: StreamReadParams,
): Promise<T> {
  // A `for await` closes the generator on return, so the listener is deregistered here.
  for await (const value of readStreamCore<T>(sysdb, serializer, workflowID, key, params)) {
    return value;
  }
  // The stream ended before reaching this offset, so no value will ever arrive.
  throw new DBOSStreamTimeoutError(workflowID, key);
}
