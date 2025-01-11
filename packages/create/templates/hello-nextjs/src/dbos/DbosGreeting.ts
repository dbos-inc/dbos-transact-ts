"use server";
import { DBOS } from '@dbos-inc/dbos-sdk';

// The schema of the database table used in this example.
export interface dbos_hello {
  name: string;
  greet_count: number;
}

export class DbosGreeting {
  @DBOS.transaction()
  static async helloDBOS(userName: string) {
    DBOS.logger.info("Hello from DBOS Transaction!");
    // Retrieve and increment the number of times this user has been greeted.
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    const { rows } = await DBOS.knexClient.raw(query, [userName]) as { rows: dbos_hello[]; };
    const greet_count = rows[0].greet_count;
    const greeting = `Hello! You have been greeted ${greet_count} times.`;
    return greeting;
  }
}
