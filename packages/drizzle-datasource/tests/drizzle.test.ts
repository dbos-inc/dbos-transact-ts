/* eslint-disable */
import { DBOS } from '@dbos-inc/dbos-sdk';
import { DrizzleDataSource } from '..';
import { randomUUID } from 'node:crypto';
import { setUpDBOSTestDb } from './testutils';
import { pgTable, text } from 'drizzle-orm/pg-core';
import { eq } from 'drizzle-orm/expressions';
import { Pool, PoolConfig } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { pushSchema } from 'drizzle-kit/api';

const kv = pgTable('kv', {
  id: text('id').primaryKey().default('t'),
  value: text('value').default('v'),
});

async function createSchema(config: PoolConfig, entities: { [key: string]: object }) {
  const drizzlePool = new Pool(config);
  const db = drizzle(drizzlePool);
  try {
    const res = await pushSchema(entities, db);
    await res.apply();
  } finally {
    await drizzlePool.end();
  }
}

const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
if (!dbPassword) {
  throw new Error('DB_PASSWORD or PGPASSWORD environment variable not set');
}

const databaseUrl = `postgresql://postgres:${dbPassword}@localhost:5432/drizzle_testdb?sslmode=disable`;

const poolconfig = {
  connectionString: databaseUrl,
  user: 'postgres',
  password: dbPassword,
  database: 'drizzle_testdb',

  host: 'localhost',
  port: 5432,
};

const drizzleDS = new DrizzleDataSource('app-db', poolconfig, { kv });

const dbosConfig = {
  name: 'dbos_drizzle_test',
  databaseUrl: databaseUrl,
  poolConfig: poolconfig,
  system_database: 'drizzle_testdb_dbos_sys',
  telemetry: {
    logs: {
      silent: true,
    },
  },
};

async function txFunctionGuts() {
  expect(DBOS.isInTransaction()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(true);
  const res = await DrizzleDataSource.client.execute("SELECT 'Tx2 result' as a");
  return res.rows[0].a as string;
}

const txFunc = drizzleDS.registerTransaction(txFunctionGuts, 'MySecondTx', {});

async function wfFunctionGuts() {
  // Transaction variant 2: Let DBOS run a code snippet as a step
  const p1 = await drizzleDS.runTransaction(
    async () => {
      return (await DrizzleDataSource.client.execute("SELECT 'My first tx result' as a")).rows[0].a;
    },
    'MyFirstTx',
    { accessMode: 'read only' },
  );

  // Transaction variant 1: Use a registered DBOS transaction function
  const p2 = await txFunc();

  return p1 + '|' + p2;
}

// Workflow functions must always be registered before launch; this
//  allows recovery to occur.
const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, 'workflow');

class DBWFI {
  @drizzleDS.transaction({ accessMode: 'read only' })
  static async tx(): Promise<string> {
    let res = await DrizzleDataSource.client.execute("SELECT 'My decorated tx result' as a");
    return res.rows[0].a as string;
  }

  @DBOS.workflow()
  static async wf(): Promise<string> {
    return await DBWFI.tx();
  }
}

describe('decoratorless-api-tests', () => {
  beforeAll(async () => {
    DBOS.setConfig(dbosConfig);
    await setUpDBOSTestDb(dbosConfig);
    await DrizzleDataSource.initializeInternalSchema(dbosConfig.poolConfig);
    await createSchema(dbosConfig.poolConfig, { kv });
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('bare-tx-wf-functions', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await wfFunction();
      expect(res).toBe('My first tx result|Tx2 result');
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('MyFirstTx');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('MySecondTx');
  });

  test('decorated-tx-wf-functions', async () => {
    const wfid = randomUUID();

    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DBWFI.wf();
      expect(res).toBe('My decorated tx result');
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('tx');
  });
});

class KVController {
  @drizzleDS.transaction()
  static async testTxn(id: string, value: string) {
    await DrizzleDataSource.client.insert(kv).values({ id: id, value: value }).onConflictDoNothing().execute();

    return id;
  }

  static async readTxn(id: string): Promise<string> {
    const kvp = await DrizzleDataSource.client.select().from(kv).where(eq(kv.id, id)).limit(1).execute();

    return kvp?.[0]?.value ?? '<Not Found>';
  }

  @DBOS.workflow()
  static async wf(id: string, value: string) {
    return await KVController.testTxn(id, value);
  }
}

const txFunc2 = drizzleDS.registerTransaction(KVController.readTxn, 'explicitRegister', {});
async function explicitWf(id: string): Promise<string> {
  return await txFunc2(id);
}
const wfFunction2 = DBOS.registerWorkflow(explicitWf, 'explicitworkflow');

describe('drizzle-tests', () => {
  const userDB = new Pool(dbosConfig.poolConfig);
  const userDrizzle = drizzle(userDB);

  beforeAll(async () => {
    DBOS.setConfig(dbosConfig);
    await setUpDBOSTestDb(dbosConfig);
    await DrizzleDataSource.initializeInternalSchema(dbosConfig.poolConfig);
    await createSchema(dbosConfig.poolConfig, { kv });
  });

  afterAll(async () => {
    await userDB.end();
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-drizzle', async () => {
    const user = `test-${Date.now()}`;

    await KVController.wf(user, 'value');

    const kvp = await userDrizzle.select().from(kv).where(eq(kv.id, user)).limit(1).execute();
    expect(kvp?.[0]?.value).toBe('value');
  });

  test('drizzle-register', async () => {
    const user = `test-${Date.now()}`;

    await expect(wfFunction2(user)).resolves.toBe('<Not Found>');
    await KVController.wf(user, 'value');
    await expect(wfFunction2(user)).resolves.toBe('value');
  });
});
