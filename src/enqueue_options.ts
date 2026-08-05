/**
 * Status-row construction for enqueueing a workflow from options alone, backing
 * `DBOS.enqueueWorkflowWithOptions`. It builds the same ENQUEUED row a
 * `DBOSClient.enqueue` would, so a workflow enqueued either way is indistinguishable
 * to the executor that eventually runs it.
 */

import { randomUUID } from 'node:crypto';
import type { WorkflowStatusInternal } from './system_database';
import { StatusString, validateWorkflowAttributes, type WorkflowSerializationFormat } from './workflow';
import { type DBOSSerializer, serializeArgs } from './serialization';
import { DBOSError, DBOSInvalidQueuePriorityError } from './error';
import { DBOS_QUEUE_MIN_PRIORITY, DBOS_QUEUE_MAX_PRIORITY } from './dbos-executor';

/**
 * Options describing a workflow to enqueue by name, without a reference to its
 * function. The workflow may be implemented by another process, or in another
 * language, as long as it shares this system database.
 */
export interface EnqueueWorkflowOptions {
  /** The name of the queue to which the workflow will be enqueued. */
  queueName: string;
  /** The name of the method that will be invoked when the workflow runs. */
  workflowName: string;
  /** The class containing the method to invoke, if any. */
  workflowClassName?: string;
  /** The ConfiguredInstance containing the method to invoke, if any. */
  workflowConfigName?: string;
  /** An identifier for the workflow, for idempotency. A new UUID if unset. */
  workflowID?: string;
  /**
   * The application version this workflow requires. If unset, only an executor
   * running the latest registered application version dequeues it.
   */
  appVersion?: string;
  /** Timeout in milliseconds, measured from the moment the workflow is dequeued. */
  workflowTimeoutMS?: number;
  /** Deduplication ID on the queue; no deduplication is performed if unset. */
  deduplicationID?: string;
  /** Serialization for the enqueued arguments. */
  serializationType?: WorkflowSerializationFormat;
  /** Priority on the queue, 1 ~ 2,147,483,647. Default 0 (highest priority). */
  priority?: number;
  /** Partition key for partitioned queues. */
  queuePartitionKey?: string;
  /** Seconds to delay before the workflow becomes eligible to run. */
  delaySeconds?: number;
  /** Custom key-value attributes to attach to the workflow at creation. */
  attributes?: Record<string, unknown>;
  /** The authenticated user recorded on the workflow. Defaults to the caller's ambient authenticated user, if any. */
  authenticatedUser?: string;
  /** The authenticated roles recorded on the workflow. Defaults to the caller's ambient authenticated roles, if any. */
  authenticatedRoles?: string[];
  /**
   * The application that owns and runs this workflow. Defaults to the enqueuer's own
   * application. Leaving both unset enqueues an unclaimed workflow, which any
   * application sharing the system database may run.
   */
  applicationName?: string;
}

/**
 * Build (without persisting) the ENQUEUED row these options describe. Fields DBOS
 * internals own (parent linkage, executor, application ID) are left unset here and
 * stamped by the caller.
 */
export async function buildEnqueueStatus(
  options: EnqueueWorkflowOptions,
  serializer: DBOSSerializer,
  positionalArgs: unknown[],
  namedArgs?: Record<string, unknown>,
  defaultSerialization?: WorkflowSerializationFormat,
): Promise<WorkflowStatusInternal> {
  validateWorkflowAttributes(options.attributes);
  if (
    options.priority !== undefined &&
    (options.priority < DBOS_QUEUE_MIN_PRIORITY || options.priority > DBOS_QUEUE_MAX_PRIORITY)
  ) {
    throw new DBOSInvalidQueuePriorityError(options.priority, DBOS_QUEUE_MIN_PRIORITY, DBOS_QUEUE_MAX_PRIORITY);
  }
  if (options.workflowID !== undefined && options.workflowID.trim() === '') {
    throw new DBOSError(
      `Invalid workflow ID '${options.workflowID}': workflow IDs must be non-empty and cannot be only whitespace.`,
    );
  }
  const workflowUUID = options.workflowID ?? randomUUID();
  const serparam = await serializeArgs(
    positionalArgs,
    namedArgs,
    serializer,
    options.serializationType ?? defaultSerialization,
  );
  const delayUntilEpochMS =
    options.delaySeconds !== undefined && options.delaySeconds > 0
      ? Date.now() + options.delaySeconds * 1000
      : undefined;

  return {
    workflowUUID,
    status: delayUntilEpochMS !== undefined ? StatusString.DELAYED : StatusString.ENQUEUED,
    workflowName: options.workflowName,
    workflowClassName: options.workflowClassName ?? '',
    workflowConfigName: options.workflowConfigName ?? '',
    queueName: options.queueName,
    authenticatedUser: options.authenticatedUser ?? '',
    output: null,
    error: null,
    assumedRole: '',
    authenticatedRoles: options.authenticatedRoles ?? [],
    request: {},
    executorId: '',
    applicationVersion: options.appVersion,
    applicationID: '',
    createdAt: Date.now(),
    timeoutMS: options.workflowTimeoutMS,
    deadlineEpochMS: undefined,
    input: serparam.serializedValue,
    deduplicationID: options.deduplicationID,
    priority: options.priority ?? 0,
    queuePartitionKey: options.queuePartitionKey,
    serialization: serparam.serialization,
    delayUntilEpochMS,
    attributes: options.attributes,
    applicationName: options.applicationName,
  };
}
