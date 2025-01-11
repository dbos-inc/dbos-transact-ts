
import { DBOS } from "@dbos-inc/dbos-sdk";
import { Knex } from "knex";

export interface dbos_hello {
    name: string;
    greet_count: number;
  }

function getClient() {return DBOS.knexClient as Knex;}

// Order matters: DBOS decorators must be declared before being used
export class dbosWorkflowClass {

    static mjclassName = "dbosWorkflowClass";
    @DBOS.transaction()
    static async helloDBOS(userName: string) {
        DBOS.logger.info("Hello from DBOS Transaction!");
        // Retrieve and increment the number of times this user has been greeted.
        const query = "INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
        const { rows } = await getClient().raw(query, [userName]) as { rows: dbos_hello[] };
        const greet_count = rows[0].greet_count;
        const greeting = `Hello! You have been greeted ${greet_count} times.`;
        return greeting;
    }

    @DBOS.transaction()
    static async backgroundTaskStep(i : number) {
        DBOS.logger.info(`Completed step ${i}`);
    }

    @DBOS.workflow()
    static async backgroundTask(i: number) {
        DBOS.logger.info("Hello from background task!");
        for (let j = 1; j <= i; j++) {
            await dbosWorkflowClass.backgroundTaskStep(j);
            DBOS.logger.info("Sleeping for 2 seconds");
            await DBOS.sleepSeconds(2);
            await DBOS.setEvent("steps_event", j)
        }
        DBOS.logger.info("Background task complete!");
    } 

}
