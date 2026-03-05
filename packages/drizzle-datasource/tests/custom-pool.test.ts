import { DBOS } from '@dbos-inc/dbos-sdk';
import { Client, Pool } from 'pg';
import { DrizzleDataSource } from '..';
import { dropDB, ensureDB } from './test-helpers';
import { randomUUID } from 'crypto';
import SuperJSON from 'superjson';
import { pgTable, text, integer } from 'drizzle-orm/pg-core';
import { drizzle } from 'drizzle-orm/node-postgres';
import { pushSchema } from 'drizzle-kit/api';
import { sql } from 'drizzle-orm';

const config = { user: 'postgres', database: 'drizzle_pool_test_userdb' };

const greetingsTable = pgTable('greetings', {
  name: text('name').primaryKey().notNull(),
  greet_count: integer('greet_count').default(0),
});

const customPool = new Pool(config);
const poolDataSource = new DrizzleDataSource('pool-db', customPool);

async function insertFunction(user: string) {
  const result = await poolDataSource.client
    .insert(greetingsTable)
    .values({ name: user, greet_count: 1 })
    .onConflictDoUpdate({
      target: greetingsTable.name,
      set: {
        greet_count: sql`${greetingsTable.greet_count} + 1`,
      },
    })
    .returning({ greet_count: greetingsTable.greet_count });
  const row = result.length > 0 ? result[0] : undefined;
  return { user, greet_count: row?.greet_count };
}

const regInsertFunction = poolDataSource.registerTransaction(insertFunction);

async function insertWorkflow(user: string) {
  return await regInsertFunction(user);
}

const regInsertWorkflow = DBOS.registerWorkflow(insertWorkflow);

interface transaction_completion {
  workflow_id: string;
  function_num: number;
  output: string | null;
  error: string | null;
}

describe('DrizzleDataSource with custom Pool', () => {
  beforeAll(async () => {
    const client = new Client({ ...config, database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, 'drizzle_pool_test', true);
      await dropDB(client, 'drizzle_pool_test_dbos_sys', true);
      await dropDB(client, config.database, true);
      await ensureDB(client, config.database);
    } finally {
      await client.end();
    }

    await DrizzleDataSource.initializeDBOSSchema(config);

    const drizzlePool = new Pool(config);
    const db = drizzle(drizzlePool);
    try {
      const res = await pushSchema({ greetingsTable }, db);
      await res.apply();
    } finally {
      await drizzlePool.end();
    }
  });

  afterAll(async () => {
    await customPool.end();
  });

  beforeEach(async () => {
    DBOS.setConfig({ name: 'drizzle-pool-test' });
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('insert with custom pool', async () => {
    const user = 'poolTest1';
    await customPool.query('DELETE FROM greetings WHERE name = $1', [user]);
    const workflowID = randomUUID();

    await expect(DBOS.withNextWorkflowID(workflowID, () => regInsertWorkflow(user))).resolves.toMatchObject({
      user,
      greet_count: 1,
    });

    const { rows } = await customPool.query<transaction_completion>(
      'SELECT * FROM dbos.transaction_completion WHERE workflow_id = $1',
      [workflowID],
    );
    expect(rows.length).toBe(1);
    expect(rows[0].output).not.toBeNull();
    expect(SuperJSON.parse(rows[0].output!)).toMatchObject({ user, greet_count: 1 });
  });

  test('custom pool is not closed after DBOS shutdown', async () => {
    // Explicitly shut down DBOS to trigger destroy() on all datasources
    await DBOS.shutdown();
    // The custom pool should still be usable after DBOS.shutdown()
    // because DrizzleDataSource should not close a user-provided pool
    const { rows } = await customPool.query<{ val: number }>('SELECT 1 as val');
    expect(rows[0].val).toBe(1);
    // Re-launch so the afterEach shutdown doesn't fail
    DBOS.setConfig({ name: 'drizzle-pool-test' });
    await DBOS.launch();
  });
});
