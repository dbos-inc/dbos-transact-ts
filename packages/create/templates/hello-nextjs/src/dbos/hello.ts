import { DBOS } from "@dbos-inc/dbos-sdk";
import { Knex } from "knex";

export interface dbos_hello {
    name: string;
    greet_count: number;
  }

function getClient() {return DBOS.knexClient as Knex;}

export class helloWorkflowClass {

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

}