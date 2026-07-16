/**
 * Entry point for event receivers (Kafka, SQS, ...) that live in their own packages.
 *
 * This is not part of the user-facing API: it exists so a receiver can reach the internals it
 * needs — chiefly durable batch enqueue — without those internals landing on the `DBOS` class.
 */
import { DBOSExecutor, PrepareEnqueuedWorkflowOptions } from './dbos-executor';
import { ensureDBOSIsLaunched, TypedAsyncFunction } from './decorators';
import { WorkflowStatusInternal } from './system_database';
import { QueueParameters, wfQueueRunner, WorkflowQueue } from './wfqueue';

export type { PrepareEnqueuedWorkflowOptions };

/**
 * An assembled, not-yet-persisted ENQUEUED workflow row. Produced by {@link prepareEnqueuedWorkflow}
 * and durably enqueued by {@link enqueueWorkflows}.
 */
export type PreparedWorkflow = WorkflowStatusInternal;

/**
 * Build, without persisting, an ENQUEUED row for `workflow`, to be durably enqueued in bulk by
 * {@link enqueueWorkflows}. Together they let a receiver enqueue a batch of workflows in one
 * transaction instead of one transaction per workflow.
 *
 * Any ambient DBOS context is ignored: the row inherits no parent, auth, or attributes.
 */
export async function prepareEnqueuedWorkflow<T extends unknown[], R>(
  workflow: TypedAsyncFunction<T, R>,
  args: T,
  options: PrepareEnqueuedWorkflowOptions,
): Promise<PreparedWorkflow> {
  ensureDBOSIsLaunched('prepareEnqueuedWorkflow');
  return await DBOSExecutor.globalInstance!.prepareEnqueuedWorkflow(workflow, args, options);
}

/**
 * Durably enqueue a batch of workflows built by {@link prepareEnqueuedWorkflow}, in a single
 * transaction. Workflows whose ID already exists are skipped rather than updated, so redelivering
 * the same batch is a no-op and each workflow runs exactly once.
 *
 * Throws rather than retrying if the database is unreachable, so the caller keeps control: retry
 * the same batch until it succeeds, and commit nothing to the source until it does.
 *
 * @returns The IDs of the workflows actually enqueued by this call.
 */
export async function enqueueWorkflows(workflows: PreparedWorkflow[]): Promise<Set<string>> {
  ensureDBOSIsLaunched('enqueueWorkflows');
  return await DBOSExecutor.globalInstance!.systemDatabase.enqueueWorkflows(workflows);
}

/**
 * Mark a queue as fed by this process's own poller (e.g. a Kafka consumer), so it is always
 * dispatched even when a `listenQueues` filter names only other queues. Without this, workflows
 * the poller enqueues would sit ENQUEUED forever.
 *
 * Must be called before `DBOS.launch`, when the queue dispatcher takes its snapshot.
 */
export function registerPollerQueue(name: string): void {
  wfQueueRunner.pollerQueueNames.add(name);
}

/**
 * Get the in-memory queue registered under `name` in this process, creating it if there is none.
 *
 * A receiver must resolve its internal queues through this rather than caching them: a registry
 * clear (`DBOS.shutdown({ deregister: true })`) drops the registration, and a cached queue would
 * silently stop being dispatched, leaving its workflows ENQUEUED forever.
 */
export function getOrCreateQueue(name: string, params: QueueParameters = {}): WorkflowQueue {
  return wfQueueRunner.wfQueuesByName.get(name) ?? new WorkflowQueue(name, params);
}

/**
 * Look up a queue by name: an in-memory queue registered in this process if there is one,
 * otherwise a database-backed queue. Returns `null` if neither exists.
 */
export async function getQueue(name: string): Promise<WorkflowQueue | null> {
  const inMemory = wfQueueRunner.wfQueuesByName.get(name);
  if (inMemory) return inMemory;
  ensureDBOSIsLaunched('getQueue');
  const record = await DBOSExecutor.globalInstance!.systemDatabase.getQueue(name);
  return record === null ? null : WorkflowQueue._fromRecord(record);
}
