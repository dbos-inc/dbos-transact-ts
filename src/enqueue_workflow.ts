/**
 * Enqueue a workflow from inside an application using options alone, without a
 * reference to its function. The target may live in another process or another
 * language, so nothing here is validated against the local registry.
 */

import { DBOSExecutor } from './dbos-executor';
import { getCurrentContextStore, getNextWFID, functionIDGetIncrement, isInWorkflowCtx } from './context';
import { buildEnqueueStatus, type EnqueueWorkflowOptions } from './enqueue_options';
import { RetrievedHandle } from './workflow';
import type { WorkflowHandle } from './workflow';
import type { WorkflowStatusInternal } from './system_database';
import { DBOSQueueDuplicatedError } from './error';
import { deserializeResError, serializeResError } from './serialization';
import { globalParams } from './utils';

/**
 * Resolve the options against the ambient DBOS context, so an enqueue from inside a
 * workflow honours `withNextWorkflowID`, `withAuthedContext` and friends. `appVersion`
 * is the deliberate exception: the target may belong to another executor, so stamping
 * the caller's version would strand the row.
 */
function resolveOptions(options: EnqueueWorkflowOptions): EnqueueWorkflowOptions {
  const pctx = getCurrentContextStore();
  return {
    ...options,
    workflowID: getNextWFID(options.workflowID),
    workflowTimeoutMS: options.workflowTimeoutMS ?? pctx?.workflowTimeoutMS ?? undefined,
    // No explicit target, so this application owns it.
    applicationName: options.applicationName ?? globalParams.appName,
  };
}

/**
 * Enqueue the row, recording it as a child of the calling workflow when there is one.
 * Replay-safe: a recorded enqueue returns its original child ID rather than enqueueing
 * a second workflow, and rethrows a recorded failure.
 */
export async function enqueueWorkflowWithOptions<T = unknown>(
  options: EnqueueWorkflowOptions,
  positionalArgs: unknown[],
  namedArgs?: Record<string, unknown>,
): Promise<WorkflowHandle<T>> {
  const exec = DBOSExecutor.globalInstance!;
  const sysdb = exec.systemDatabase;

  const pctx = getCurrentContextStore();
  const inWorkflow = pctx !== undefined && isInWorkflowCtx(pctx);
  const callerID = inWorkflow ? pctx.workflowId : undefined;
  const callerFunctionID = inWorkflow ? functionIDGetIncrement() : undefined;

  if (callerID !== undefined && callerFunctionID !== undefined) {
    const recorded = await sysdb.getOperationResultAndThrowIfCancelled(callerID, callerFunctionID);
    if (recorded) {
      if (recorded.error) {
        throw await deserializeResError(recorded.error, recorded.serialization ?? null, exec.serializer);
      }
      return new RetrievedHandle<T>(sysdb, recorded.childWorkflowID!);
    }
  }

  const resolved = resolveOptions(options);
  const internalStatus: WorkflowStatusInternal = await buildEnqueueStatus(
    resolved,
    exec.serializer,
    positionalArgs,
    namedArgs,
  );

  const childStartTime = Date.now();
  try {
    await sysdb.initWorkflowStatus(internalStatus, null);
  } catch (e) {
    if (e instanceof DBOSQueueDuplicatedError && callerID !== undefined && callerFunctionID !== undefined) {
      const sererr = await serializeResError(e, exec.serializer, undefined);
      await sysdb.recordOperationResult(
        callerID,
        callerFunctionID,
        internalStatus.workflowName,
        true,
        childStartTime,
        Date.now(),
        { error: sererr.serializedValue, serialization: sererr.serialization },
      );
    }
    throw e;
  }

  if (callerID !== undefined && callerFunctionID !== undefined) {
    await sysdb.recordOperationResult(
      callerID,
      callerFunctionID,
      internalStatus.workflowName,
      true,
      childStartTime,
      Date.now(),
      { childWorkflowID: internalStatus.workflowUUID },
    );
  }

  return new RetrievedHandle<T>(sysdb, internalStatus.workflowUUID);
}
