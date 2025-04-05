import { DBOS } from '@dbos-inc/dbos-sdk';

// The schema of the database table used in this example.
export interface dbos_hello {
  name: string;
  greet_count: number;
}

export class StoredProcTest {
  @DBOS.storedProcedure()
  static async getGreetCount(user: string): Promise<number> {
    const query = 'SELECT greet_count FROM dbos_hello WHERE name = $1;';
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    return rows.length === 0 ? 0 : rows[0].greet_count;
  }

  @DBOS.storedProcedure()
  static async getHelloRowCount(): Promise<number> {
    const query = 'SELECT COUNT(*) FROM dbos_hello;';
    const { rows } = await DBOS.pgClient.query<{ count: string }>(query);
    return parseInt(rows[0].count);
  }

  @DBOS.storedProcedure() // Run this function as a database transaction
  static async helloProcedure(user: string): Promise<string> {
    const query =
      'INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;';
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @DBOS.workflow()
  static async procGreetingWorkflow(user: string): Promise<{ count: number; greeting: string; rowCount: number }> {
    const count = await StoredProcTest.getGreetCount(user);
    const greeting = await StoredProcTest.helloProcedure(user);
    const rowCount = await StoredProcTest.getHelloRowCount();
    return { count, greeting, rowCount };
  }

  @DBOS.storedProcedure()
  static async procError(): Promise<void> {
    await Promise.resolve();
    throw new Error('This is a test error');
  }

  @DBOS.workflow()
  static async procErrorWorkflow(user: string): Promise<string> {
    const greeting = await StoredProcTest.helloProcedure(user);
    const _count = await StoredProcTest.getGreetCount(user);
    await StoredProcTest.procError();
    return greeting;
  }

  @DBOS.storedProcedure({ executeLocally: true })
  static async getGreetCountLocal(user: string): Promise<number> {
    const query = 'SELECT greet_count FROM dbos_hello WHERE name = $1;';
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    return rows.length === 0 ? 0 : rows[0].greet_count;
  }

  @DBOS.storedProcedure({ executeLocally: true }) // Run this function as a database transaction
  static async helloProcedureLocal(user: string): Promise<string> {
    // Retrieve and increment the number of times this user has been greeted.
    const query =
      'INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;';
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @DBOS.storedProcedure({ executeLocally: true })
  static async getHelloRowCountLocal(): Promise<number> {
    const query = 'SELECT COUNT(*) FROM dbos_hello;';
    const { rows } = await DBOS.pgClient.query<{ count: string }>(query);
    return parseInt(rows[0].count);
  }

  @DBOS.workflow()
  static async procLocalGreetingWorkflow(user: string): Promise<{ count: number; greeting: string; rowCount: number }> {
    const count = await StoredProcTest.getGreetCountLocal(user);
    const greeting = await StoredProcTest.helloProcedureLocal(user);
    const rowCount = await StoredProcTest.getHelloRowCountLocal();
    return { count, greeting, rowCount };
  }

  @DBOS.transaction()
  static async getGreetCountTx(user: string): Promise<number> {
    const query = 'SELECT greet_count FROM dbos_hello WHERE name = ?;';
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const result = (await DBOS.knexClient.raw(query, user)) as { rows: dbos_hello[] } | undefined;
    if (result && result.rows.length > 0) {
      return result.rows[0].greet_count;
    }
    return 0;
  }

  @DBOS.transaction() // Run this function as a database transaction
  static async helloTransaction(user: string) {
    // Retrieve and increment the number of times this user has been greeted.
    const query =
      'INSERT INTO dbos_hello (name, greet_count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;';
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const { rows } = (await DBOS.knexClient.raw(query, [user])) as { rows: dbos_hello[] };
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @DBOS.workflow()
  static async txAndProcGreetingWorkflow(user: string): Promise<{ count: number; greeting: string }> {
    // Retrieve the number of times this user has been greeted.
    const count = await StoredProcTest.getGreetCountTx(user);
    const greeting = await StoredProcTest.helloProcedure(user);

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

  @DBOS.transaction()
  static async getGreetCountTx_v2(user: string): Promise<number> {
    const query = 'SELECT greet_count FROM dbos_hello WHERE name = ?;';
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
    const result = (await DBOS.knexClient.raw(query, user)) as { rows: dbos_hello[] } | undefined;
    if (result && result.rows.length > 0) {
      return result.rows[0].greet_count;
    }
    return 0;
  }

  @DBOS.storedProcedure({ executeLocally: true })
  static async helloProcedure_v2_local(user: string): Promise<string> {
    const query =
      'INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;';
    const { rows } = await DBOS.pgClient.query<dbos_hello>(query, [user]);
    const greet_count = rows[0].greet_count;
    return `Hello, ${user}! You have been greeted ${greet_count} times.\n`;
  }

  @DBOS.storedProcedure()
  static async helloProcedure_v2(user: string): Promise<string> {
    const query =
      'INSERT INTO dbos_hello (name, greet_count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET greet_count = dbos_hello.greet_count + 1 RETURNING greet_count;';
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
