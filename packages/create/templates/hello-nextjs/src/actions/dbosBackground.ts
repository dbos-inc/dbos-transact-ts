"use server";

import { DBOS } from '@dbos-inc/dbos-sdk';

// The schema of the database table used in this example.
export interface dbos_hello {
  name: string;
  greet_count: number;
}

class DbosBackgroundWorkflow {
  @DBOS.transaction()
  static async backgroundTaskStep(i : number) {
      DBOS.logger.info(`Completed step ${i}`);
  }

  @DBOS.workflow()
  static async backgroundTask(i: number) {
      DBOS.logger.info("Hello from background task!");
      for (let j = 1; j <= i; j++) {
          await DbosBackgroundWorkflow.backgroundTaskStep(j);
          DBOS.logger.info("Sleeping for 2 seconds");
          await DBOS.sleepSeconds(2);
          await DBOS.setEvent("steps_event", j)
      }
      DBOS.logger.info("Background task complete!");
  }
}

// The exported function is the entry point for the workflow
// The function is exported and not the class because Next does not support exporting classes
export async function dbosBackgroundTask(workflowID: string) {
  DBOS.logger.info("Hello from DBOS!");
  return DBOS.startWorkflow(DbosBackgroundWorkflow, {workflowID: workflowID}).backgroundTask(10);
}