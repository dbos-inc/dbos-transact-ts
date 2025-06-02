/* eslint-disable */
import { DBOS, OrmEntities } from '@dbos-inc/dbos-sdk';
import { DrizzleDS } from '../src/drizzle_datasource';
import { randomUUID } from 'node:crypto';
import { setUpDBOSTestDb } from './testutils';
import { pgTable, text } from 'drizzle-orm/pg-core';

export const kv = pgTable('kv', {
  id: text('id').primaryKey().default('t'),
  value: text('value').default('v'),
});

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

const drizzleDS = new DrizzleDS('app-db', poolconfig, { kv });
DBOS.registerDataSource(drizzleDS);

const dbosConfig = {
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
  const res = await DrizzleDS.drizzleClient.execute("SELECT 'Tx2 result' as a");
  return res.rows[0].a as string;
}

const txFunc = DBOS.registerTransaction('app-db', txFunctionGuts, { name: 'MySecondTx' }, {});

async function wfFunctionGuts() {
  // Transaction variant 2: Let DBOS run a code snippet as a step
  const p1 = await drizzleDS.runTransactionStep(
    async () => {
      return (await DrizzleDS.drizzleClient.execute("SELECT 'My first tx result' as a")).rows[0].a;
    },
    'MyFirstTx',
    { readOnly: true },
  );

  // Transaction variant 1: Use a registered DBOS transaction function
  const p2 = await txFunc();

  return p1 + '|' + p2;
}

// Workflow functions must always be registered before launch; this
//  allows recovery to occur.
const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, {
  name: 'workflow',
});

class DBWFI {
  @drizzleDS.transaction({ readOnly: true })
  static async tx(): Promise<string> {
    let res = await DrizzleDS.drizzleClient.execute("SELECT 'My decorated tx result' as a");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    return res.rows[0].a as string;
  }

  @DBOS.workflow()
  static async wf(): Promise<string> {
    return await DBWFI.tx();
  }
}

describe('decoratorless-api-tests', () => {
  beforeAll(() => {
    DBOS.setConfig(dbosConfig);
  });

  beforeEach(async () => {
    await setUpDBOSTestDb(dbosConfig);
    await drizzleDS.initializeSchema();
    await DBOS.launch();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS kv;`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS kv (id SERIAL PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  afterAll(() => {
    console.log('afterAll cleanup done');
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

    console.log('Running decorated tx wf function');
    await DBOS.withNextWorkflowID(wfid, async () => {
      const res = await DBWFI.wf();
      expect(res).toBe('My decorated tx result');
    });
    console.log('Done running decorated tx wf function');

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('tx');
    console.log('Done checking wf steps', wfsteps);
  });

  test('do-nothing', async () => {
    console.log('Running do-nothing test');
  });
});

/* class KVController {
  @typeOrmDS.transaction()
  static async testTxn(id: string, value: string) {
    const kv: KV = new KV();
    kv.id = id;
    kv.value = value;
    const res = await TypeOrmDS.entityManager.save(kv);

    return res.id;
  }

  static async readTxn(id: string): Promise<string> {
    const kvp = await TypeOrmDS.entityManager.findOneBy(KV, { id: id });
    return Promise.resolve(kvp?.value || '<Not Found>');
  }

  @DBOS.workflow()
  static async wf(id: string, value: string) {
    return await KVController.testTxn(id, value);
  }
}

const txFunc2 = DBOS.registerTransaction('app-db', KVController.readTxn, { name: 'explicitRegister' }, {});
async function explicitWf(id: string): Promise<string> {
  return await txFunc2(id);
}
const wfFunction2 = DBOS.registerWorkflow(explicitWf, {
  name: 'explicitworkflow',
});

describe('typeorm-tests', () => {
  beforeAll(() => {
    DBOS.setConfig(dbosConfig);
  });

  beforeEach(async () => {
    await setUpDBOSTestDb(dbosConfig);
    await typeOrmDS.initializeSchema();
    await DBOS.launch();
    await typeOrmDS.createSchema();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-typeorm', async () => {
    await expect(KVController.wf('test', 'value')).resolves.toBe('test');
  });

  test('typeorm-register', async () => {
    await expect(wfFunction2('test')).resolves.toBe('<Not Found>');
    await KVController.wf('test', 'value');
    await expect(wfFunction2('test')).resolves.toBe('value');
  });
});
*/
