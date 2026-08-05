/**
 * Enqueue a workflow from inside an application using options alone, without a
 * reference to its function. The target may live in another process or another
 * language, so nothing here is validated against the local registry.
 */

import { DBOSExecutor } from './dbos-executor';
import {
  getCurrentContextStore,
  getNextWFID,
  functionIDGetIncrement,
  isInWorkflowCtx,
  isWithinWorkflowCtx,
} from './context';
import { buildEnqueueStatus, type EnqueueWorkflowOptions } from './enqueue_options';
import { RetrievedHandle } from './workflow';
import type { WorkflowHandle } from './workflow';
import type { WorkflowStatusInternal } from './system_database';
import { DBOSError, DBOSInvalidWorkflowTransitionError, DBOSQueueDuplicatedError } from './error';
import { deserializeResError, serializeResError } from './serialization';
import { globalParams } from './utils';

/**
 * Resolve the options against the ambient DBOS context, so an enqueue from inside a
 * workflow honours `withNextWorkflowID`, `withAuthedContext` and friends. `appVersion`
 * is the deliberate exception: the target may belong to another executor, so stamping
 * the caller's version would strand the row.
 *
 * Inside a workflow the ID derives from the caller and its function ID, so a replay
 * after a crash between the enqueue and its checkpoint rebuilds the same ID and
 * collides on workflow_uuid instead of enqueueing the workflow a second time.
 */
function resolveOptions(
  options: EnqueueWorkflowOptions,
  assignedID: string | undefined,
  callerID: string | undefined,
  callerFunctionID: number | undefined,
): EnqueueWorkflowOptions {
  const pctx = getCurrentContextStore();
  const derivedID =
    callerID !== undefined && callerFunctionID !== undefined ? `${callerID}-${callerFunctionID}` : undefined;
  return {
    ...options,
    workflowID: assignedID ?? derivedID,
    authenticatedUser: options.authenticatedUser ?? pctx?.authenticatedUser,
    authenticatedRoles: options.authenticatedRoles ?? pctx?.authenticatedRoles,
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
  if (pctx !== undefined && isWithinWorkflowCtx(pctx) && !inWorkflow) {
    throw new DBOSInvalidWorkflowTransitionError(
      'Invalid call to `enqueueWorkflowWithOptions` from within a `step` or `transaction`',
    );
  }
  const callerID = inWorkflow ? pctx.workflowId : undefined;
  const callerFunctionID = inWorkflow ? functionIDGetIncrement() : undefined;
  // Consume an ambient withNextWorkflowID assignment even on a replay, so it cannot leak to a later workflow start.
  const assignedID = getNextWFID(options.workflowID);

  if (callerID !== undefined && callerFunctionID !== undefined) {
    const recorded = await sysdb.getOperationResultAndThrowIfCancelled(callerID, callerFunctionID);
    if (recorded) {
      if (recorded.error) {
        throw await deserializeResError(recorded.error, recorded.serialization ?? null, exec.serializer);
      }
      return new RetrievedHandle<T>(sysdb, recorded.childWorkflowID!);
    }
  }

  const resolved = resolveOptions(options, assignedID, callerID, callerFunctionID);
  if (resolved.queuePartitionKey !== undefined && resolved.deduplicationID !== undefined) {
    throw new DBOSError('Deduplication is not supported for partitioned queues');
  }
  const internalStatus: WorkflowStatusInternal = await buildEnqueueStatus(
    resolved,
    exec.serializer,
    positionalArgs,
    namedArgs,
  );
  // Fields DBOS internals own, which buildEnqueueStatus leaves for its caller to stamp.
  internalStatus.parentWorkflowID = callerID;
  internalStatus.applicationID = globalParams.appID;
  // Without an explicit timeout, inherit an ambient withWorkflowTimeout, else the parent's propagated deadline.
  if (resolved.workflowTimeoutMS === undefined) {
    if (pctx?.workflowTimeoutMS) {
      internalStatus.timeoutMS = pctx.workflowTimeoutMS;
    } else if (pctx?.workflowTimeoutMS !== null) {
      // A null ambient timeout explicitly detaches the parent's deadline.
      internalStatus.deadlineEpochMS = pctx?.deadlineEpochMS;
    }
  }

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
