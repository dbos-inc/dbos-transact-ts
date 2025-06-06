/* eslint-disable */
import { DBOS } from '@dbos-inc/dbos-sdk';
import { TypeOrmDataSource } from '../src';
import { Entity, Column, PrimaryColumn, PrimaryGeneratedColumn } from 'typeorm';
import { randomUUID } from 'node:crypto';
import { setUpDBOSTestDb } from './testutils';

@Entity()
class KV {
  @PrimaryColumn()
  id: string = 't';

  @Column()
  value: string = 'v';
}

@Entity()
class User {
  @PrimaryGeneratedColumn('uuid')
  id: string = '';

  @Column()
  birthday: Date = new Date();

  @Column('varchar')
  name: string = '';

  @Column('money')
  salary: number = 0;
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

const typeOrmDS = new TypeOrmDataSource('app-db', poolconfig, [KV, User]);

const dbosConfig = {
  databaseUrl: databaseUrl,
  poolConfig: poolconfig,
  system_database: 'typeorm_testdb_dbos_sys',
  telemetry: {
    logs: {
      silent: true,
    },
  },
};

async function txFunctionGuts() {
  expect(DBOS.isInTransaction()).toBe(true);
  expect(DBOS.isWithinWorkflow()).toBe(true);
  const res = await TypeOrmDataSource.entityManager.query("SELECT 'Tx2 result' as a");
  return res[0].a;
}

const txFunc = typeOrmDS.registerTransaction(txFunctionGuts, 'MySecondTx', { readOnly: true });

async function wfFunctionGuts() {
  // Transaction variant 2: Let DBOS run a code snippet as a step
  const p1 = await typeOrmDS.runTransaction(
    async () => {
      return (await TypeOrmDataSource.entityManager.query("SELECT 'My first tx result' as a"))[0].a;
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
const wfFunction = DBOS.registerWorkflow(wfFunctionGuts, 'workflow');

class DBWFI {
  @typeOrmDS.transaction({ readOnly: true })
  static async tx(): Promise<number> {
    return await TypeOrmDataSource.entityManager.count(User);
  }

  @DBOS.workflow()
  static async wf(): Promise<string> {
    return `${await DBWFI.tx()}`;
  }
}

describe('decoratorless-api-tests', () => {
  beforeAll(() => {
    DBOS.setConfig(dbosConfig);
  });

  beforeEach(async () => {
    await setUpDBOSTestDb(dbosConfig);
    await typeOrmDS.initializeInternalSchema();
    await typeOrmDS.createSchema();
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
      expect(res).toBe('0');
    });

    const wfsteps = (await DBOS.listWorkflowSteps(wfid))!;
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('tx');
  });
});

class KVController {
  @typeOrmDS.transaction()
  static async testTxn(id: string, value: string) {
    const kv: KV = new KV();
    kv.id = id;
    kv.value = value;
    const res = await TypeOrmDataSource.entityManager.save(kv);

    return res.id;
  }

  static async readTxn(id: string): Promise<string> {
    const kvp = await TypeOrmDataSource.entityManager.findOneBy(KV, { id: id });
    return Promise.resolve(kvp?.value || '<Not Found>');
  }

  @DBOS.workflow()
  static async wf(id: string, value: string) {
    return await KVController.testTxn(id, value);
  }
}

const txFunc2 = typeOrmDS.registerTransaction(KVController.readTxn, 'explicitRegister', {});
async function explicitWf(id: string): Promise<string> {
  return await txFunc2(id);
}
const wfFunction2 = DBOS.registerWorkflow(explicitWf, 'explicitworkflow');

describe('typeorm-tests', () => {
  beforeAll(() => {
    DBOS.setConfig(dbosConfig);
  });

  beforeEach(async () => {
    await setUpDBOSTestDb(dbosConfig);
    await typeOrmDS.initializeInternalSchema();
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
