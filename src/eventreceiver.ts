/**
 * Entry point for event receivers (Kafka, SQS, ...) that live in their own packages.
 *
 * This is not part of the user-facing API: it exists so a receiver can reach the internals it
 * needs — chiefly durable batch enqueue — without those internals landing on the `DBOS` class.
 */
import { DBOSExecutor, PrepareEnqueuedWorkflowOptions } from './dbos-executor';
import { ensureDBOSIsLaunched, TypedAsyncFunction } from './decorators';
import { WorkflowStatusInternal } from './system_database';
import { wfQueueRunner, WorkflowQueue } from './wfqueue';

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

/** How a receiver declares the process-local queues it enqueues onto. */
export { registerInternalQueue } from './wfqueue';

/**
 * Look up a queue by name: one of DBOS's own process-local queues if there is one under that
 * name, otherwise a database-backed queue. Returns `null` if neither exists.
 */
export async function getQueue(name: string): Promise<WorkflowQueue | null> {
  const internal = wfQueueRunner.getInternalQueue(name);
  if (internal) return internal;
  ensureDBOSIsLaunched('getQueue');
  const record = await DBOSExecutor.globalInstance!.systemDatabase.getQueue(name);
  return record === null ? null : new WorkflowQueue(record);
}
