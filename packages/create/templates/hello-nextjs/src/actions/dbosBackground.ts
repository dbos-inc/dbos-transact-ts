"use server";

import { DBOS } from '@dbos-inc/dbos-sdk';
import { DbosBackgroundWorkflow } from '@/dbos/DbosBackgroundWorkflow';

// The exported function is the entry point for the workflow
export async function dbosBackgroundTask(workflowID: string) {
  DBOS.logger.info("Hello from DBOS!");
  return DBOS.startWorkflow(DbosBackgroundWorkflow, {workflowID: workflowID}).backgroundTask(10);
}