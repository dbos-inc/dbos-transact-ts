import { DBOS, StoredProcedure, StoredProcedureContext, Transaction, TransactionContext, Workflow, WorkflowContext } from '@dbos-inc/dbos-sdk';
import { Knex } from 'knex';

// The schema of the database table used in this example.
export interface dbos_hello {
  name: string;
  greet_count: number;
}

export class StoredProcTest {

  @StoredProcedure({ readOnly: true })
  static async getGreetCount(ctxt: StoredProcedureContext, user: string): Promise<number> {
    const query = "SELECT greet_count FROM dbos_hello WHERE name = $1;";
    const { rows } = await ctxt.query<dbos_hello>(query, [user]);
    return rows.length === 0 ? 0 : rows[0].greet_count;
  }

  @StoredProcedure() // not marking this read only so the tx output table write is not buffered
  static async getHelloRowCount(ctxt: StoredProcedureContext): Promise<number> {
    const query = "SELECT COUNT(*) FROM dbos_hello;";
    const { rows } = await ctxt.query<{count: string}>(query);
    return parseInt(rows[0].count);
  }

  @StoredProcedure()  // Run this function as a database transaction
  static async helloProcedure(ctxt: StoredProcedureContext, user: string): Promise<string> {
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    const { rows } = await ctxt.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @Workflow()
  static async procGreetingWorkflow(ctxt: WorkflowContext, user: string): Promise<{ count: number; greeting: string; rowCount: number }> {
    const count = await ctxt.invoke(StoredProcTest).getGreetCount(user);
    const greeting = await ctxt.invoke(StoredProcTest).helloProcedure(user);
    const rowCount = await ctxt.invoke(StoredProcTest).getHelloRowCount();
    return { count, greeting, rowCount };
  }

  @StoredProcedure()
  static async procError(_ctxt: StoredProcedureContext): Promise<void> {
    await Promise.resolve();
    throw new Error("This is a test error");
  }

  @Workflow()
  static async procErrorWorkflow(ctxt: WorkflowContext, user: string): Promise<string> {
    const greeting = await ctxt.invoke(StoredProcTest).helloProcedure(user);
    const _count = await ctxt.invoke(StoredProcTest).getGreetCount(user);
    await ctxt.invoke(StoredProcTest).procError();
    return greeting;
  }

  @StoredProcedure({ readOnly: true, executeLocally: true })
  static async getGreetCountLocal(ctxt: StoredProcedureContext, user: string): Promise<number> {
    const query = "SELECT greet_count FROM dbos_hello WHERE name = $1;";
    const { rows } = await ctxt.query<dbos_hello>(query, [user]);
    return rows.length === 0 ? 0 : rows[0].greet_count;
  }

  @StoredProcedure({ executeLocally: true })  // Run this function as a database transaction
  static async helloProcedureLocal(ctxt: StoredProcedureContext, user: string): Promise<string> {
    // Retrieve and increment the number of times this user has been greeted.
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    const { rows } = await ctxt.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @StoredProcedure({executeLocally: true}) // like getHelloRowCount, not marked as read only so the tx output does not get buffered
  static async getHelloRowCountLocal(ctxt: StoredProcedureContext): Promise<number> {
    const query = "SELECT COUNT(*) FROM dbos_hello;";
    const { rows } = await ctxt.query<{count: string}>(query);
    return parseInt(rows[0].count);
  }

  @Workflow()
  static async procLocalGreetingWorkflow(ctxt: WorkflowContext, user: string): Promise<{ count: number; greeting: string; rowCount: number }> {
    const count = await ctxt.invoke(StoredProcTest).getGreetCountLocal(user);
    const greeting = await ctxt.invoke(StoredProcTest).helloProcedureLocal(user);
    const rowCount = await ctxt.invoke(StoredProcTest).getHelloRowCountLocal();
    return { count, greeting, rowCount };
  }

  @Transaction({ readOnly: true })
  static async getGreetCountTx(ctxt: TransactionContext<Knex>, user: string): Promise<number> {
    const query = "SELECT greet_count FROM dbos_hello WHERE name = ?;";
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const result = await ctxt.client.raw(query, user) as { rows: dbos_hello[] } | undefined;
    if (result && result.rows.length > 0) { return result.rows[0].greet_count; }
    return 0;
  }

  @Transaction()  // Run this function as a database transaction
  static async helloTransaction(ctxt: TransactionContext<Knex>, user: string) {
    // Retrieve and increment the number of times this user has been greeted.
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const { rows } = await ctxt.client.raw(query, [user]) as { rows: dbos_hello[] };
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @Workflow()
  static async txAndProcGreetingWorkflow(ctxt: WorkflowContext, user: string): Promise<{ count: number; greeting: string; }> {
    // Retrieve the number of times this user has been greeted.
    const count = await ctxt.invoke(StoredProcTest).getGreetCountTx(user);
    const greeting = await ctxt.invoke(StoredProcTest).helloProcedure(user);

    return { count, greeting };
  }

  @DBOS.workflow()
  static async txAndProcGreetingWorkflow_v2(user: string): Promise<{ count: number; greeting: string; local: string }> {
    // Retrieve the number of times this user has been greeted.
    const count = await StoredProcTest.getGreetCountTx_v2(user);
    const greeting = await StoredProcTest.helloProcedure_v2(user);
    const local = await StoredProcTest.helloProcedure_v2_local(`${user}_local`);
    
    return { count, greeting, local };
  }

  @DBOS.transaction({ readOnly: true })
  static async getGreetCountTx_v2(user: string): Promise<number> {
    const query = "SELECT greet_count FROM dbos_hello WHERE name = ?;";
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const result = await DBOS.knexClient.raw(query, user) as { rows: dbos_hello[] } | undefined;
    if (result && result.rows.length > 0) { return result.rows[0].greet_count; }
    return 0;
  }

  @DBOS.storedProcedure({ executeLocally: true})
  static async helloProcedure_v2_local(user: string): Promise<string> {
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @DBOS.storedProcedure()
  static async helloProcedure_v2(user: string): Promise<string> {
    const query = "INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;";
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @DBOS.workflow()
  static async wf_GetWorkflowID() {
    return StoredProcTest.sp_GetWorkflowID();
  }

  /* eslint-disable @typescript-eslint/require-await */

  @DBOS.storedProcedure()
  static async sp_GetWorkflowID() {
    return DBOS.workflowID;
  }

  @DBOS.storedProcedure()
  static async sp_GetAuth() {
    return {
      user: DBOS.authenticatedUser,
      roles: DBOS.authenticatedRoles,
    };
  }

  @DBOS.storedProcedure()
  static async sp_GetRequest() {
    return DBOS.request;
  }

  /* eslint-enable @typescript-eslint/require-await */
}
