// Workflow archiving (Transact Enterprise): completed workflows are copied out of the
// system database, and retention only deletes those already copied.

import * as enterprise from './enterprise';
import { DBOSError, DBOSWorkflowArchiveNotConfiguredError } from './error';
import type { SystemDatabase } from './system_database';

/**
 * Where completed workflows are archived, such as the enterprise package's Parquet archive.
 * Its `archiveID` names the location, so it must not change once workflows are in it.
 */
export interface WorkflowArchive {
  readonly archiveID: string;
}

/** Copy the archived snapshot (workflowID, completedAt) back into `sysdb`. */
export async function importArchivedWorkflow(
  sysdb: SystemDatabase,
  archive: WorkflowArchive | undefined,
  workflowID: string,
  completedAt: number,
): Promise<void> {
  if (archive === undefined) {
    throw new DBOSWorkflowArchiveNotConfiguredError();
  }
  await enterprise
    .load(enterprise.WORKFLOW_ARCHIVE.description)
    .importArchivedWorkflow(sysdb, archive, workflowID, completedAt);
}

export const ARCHIVE_SHARDED_REFUSED = 'Workflow archiving is not supported on a sharded system database';

/** Refuse a workflow archive on a system database its archiver cannot run on. */
export async function checkArchiveSupported(sysdb: SystemDatabase): Promise<void> {
  if (sysdb.sharded) {
    throw new DBOSError(ARCHIVE_SHARDED_REFUSED);
  }
  // Without advisory locks there, every process would archive every workflow.
  if (await sysdb.isCockroach()) {
    throw new DBOSError('Workflow archiving is not supported on CockroachDB');
  }
}
