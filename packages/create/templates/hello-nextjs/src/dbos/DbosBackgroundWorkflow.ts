"use server";
import { DBOS } from '@dbos-inc/dbos-sdk';

// The schema of the database table used in this example.

export interface dbos_hello {
  name: string;
  greet_count: number;
}
export class DbosBackgroundWorkflow {
  @DBOS.transaction()
  static async backgroundTaskStep(i: number) {
    DBOS.logger.info(`Completed step ${i}`);
  }

  @DBOS.workflow()
  static async backgroundTask(i: number) {
    DBOS.logger.info("Hello from background task!");
    for (let j = 1; j <= i; j++) {
      await DbosBackgroundWorkflow.backgroundTaskStep(j);
      DBOS.logger.info("Sleeping for 2 seconds");
      await DBOS.sleepSeconds(2);
      await DBOS.setEvent("steps_event", j);
    }
    DBOS.logger.info("Background task complete!");
  }
}
