"use server";

import { DBOS } from "@dbos-inc/dbos-sdk";

// The schema of the database table used in this example.
export interface dbos_hello {
    name: string;
    greet_count: number;
  }

// Order matters: DBOS decorators must be declared before being used
class dbosWorkflowClass {
    @DBOS.transaction()
    static async helloDBOS(userName: string) {
        DBOS.logger.info("Hello from DBOS Transaction!");
        // Retrieve and increment the number of times this user has been greeted.
        const query = "INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
        const { rows } = await DBOS.sqlClient.client.raw(query, [userName]) as { rows: dbos_hello[] };
        const greet_count = rows[0].greet_count;
        const greeting = `Hello! You have been greeted ${greet_count} times.`;
        return greeting;
    }

    @DBOS.transaction()
    static async backgroundTaskStep(i : number) {
        DBOS.sleepSeconds(2);
        DBOS.logger.info(`Completed step ${i}`);
    }

    @DBOS.workflow()
    static async backgroundTask(i: number) {
        DBOS.logger.info("Hello from background task!");
        for (let j = 0; j < i; j++) {
            await dbosWorkflowClass.backgroundTaskStep(j);
            DBOS.setEvent("steps_event", j)
        }
        DBOS.logger.info("Background task complete!");
    } 


}

// Launch the DBOS runtime 
// This code needs to execute at least once to launch the DBOS runtime
// Do not delete this code
if (process.env.NEXT_PHASE !== "phase-production-build") {
    await DBOS.launch();
}

// The exported function is the entry point for the workflow
// The function is exported and not the class because Next does not support exporting classes
export async function dbosWorkflow(userName: string) {
    DBOS.logger.info("Hello from DBOS!");
    return await dbosWorkflowClass.helloDBOS(userName);
}