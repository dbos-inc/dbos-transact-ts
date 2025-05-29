import { DBOS, OrmEntities } from '@dbos-inc/dbos-sdk';
import { TypeOrmDS } from './TypeOrmDataSource';
import { Entity, Column, PrimaryColumn, EntityManager } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { setUpDBOSTestDb } from './testutils';

@Entity()
export class KV {
  @PrimaryColumn()
  id: string = 't';

  @Column()
  value: string = 'v';
}
const dbPassword: string | undefined = process.env.DB_PASSWORD || process.env.PGPASSWORD;
if (!dbPassword) {
  throw new Error('DB_PASSWORD or PGPASSWORD environment variable not set');
}

const databaseUrl = `postgresql://postgres:${dbPassword}@localhost:5432/typeorm_testdb?sslmode=disable`;

const poolconfig = {
  connectionString: databaseUrl,
  user: 'postgres',
  password: dbPassword,
  database: 'typeorm_testdb',

  host: 'localhost',
  port: 5432,
};

const typeOrmDS = new TypeOrmDS('app-db', poolconfig, [KV]);
DBOS.registerDataSource(typeOrmDS);

const dbosConfig = {
  name: 'dbostest',
  databaseUrl: databaseUrl,
  database: {
    app_db_client: 'typeorm',
  },
  poolConfig: poolconfig,
  system_database: 'typeorm_testdb_dbos_sys',
  application: {
    counter: 3,
    shouldExist: 'exists',
  },
  telemetry: {
    logs: {
      silent: true,
    },
  },
};

async function txFunctionGuts() {
  expect(DBOS.isInTransaction()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(true);
  const res = await TypeOrmDS.entityManager.query("SELECT 'Tx2 result' as a");
  // return res.rows[0].a;
  return res[0].a;
}

const txFunc = DBOS.registerTransaction('app-db', txFunctionGuts, { name: 'MySecondTx' }, {});

async function wfFunctionGuts() {
  // Transaction variant 2: Let DBOS run a code snippet as a step
  const p1 = await typeOrmDS.runTransactionStep(
    async () => {
      return (await TypeOrmDS.entityManager.query("SELECT 'My first tx result' as a"))[0].a;
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
  @typeOrmDS.transaction({ readOnly: true })
  static async tx() {
    let res = await TypeOrmDS.entityManager.query("SELECT 'My decorated tx result' as a");
    return res[0].a;
  }

  @DBOS.workflow()
  static async wf() {
    return await DBWFI.tx();
  }
}

describe('decoratorless-api-tests', () => {
  beforeAll(async () => {
    DBOS.setConfig(dbosConfig);
  });

  beforeEach(async () => {
    await setUpDBOSTestDb(dbosConfig);
    await typeOrmDS.initializeSchema();
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

let globalCnt = 0;
@OrmEntities([KV])
class KVController {
  @typeOrmDS.transaction()
  static async testTxn(id: string, value: string) {
    const kv: KV = new KV();
    kv.id = id;
    kv.value = value;
    const res = await (TypeOrmDS.entityManager as EntityManager).save(kv);
    globalCnt += 1;
    return res.id;
  }

  @typeOrmDS.transaction({ readOnly: true })
  static async readTxn(id: string) {
    globalCnt += 1;
    const kvp = await (TypeOrmDS.entityManager as EntityManager).findOneBy(KV, { id: id });
    return Promise.resolve(kvp?.value || '<Not Found>');
  }

  @DBOS.workflow()
  static async wf(id: string, value: string) {
    return await KVController.testTxn(id, value);
  }
}

describe('typeorm-tests', () => {
  beforeAll(async () => {
    DBOS.setConfig(dbosConfig);
  });

  beforeEach(async () => {
    globalCnt = 0;
    await setUpDBOSTestDb(dbosConfig);
    await typeOrmDS.initializeSchema();
    await DBOS.launch();
    typeOrmDS.createSchema();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-typeorm', async () => {
    await expect(KVController.wf('test', 'value')).resolves.toBe('test');
  });
});
