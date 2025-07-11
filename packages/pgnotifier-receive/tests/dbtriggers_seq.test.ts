import { DBOS, DBOSConfig } from '@dbos-inc/dbos-sdk';

import { DBTrigger, TriggerOperation } from '../src';
import { ClientBase, Pool, PoolClient } from 'pg';
import { KnexDataSource } from '@dbos-inc/knex-datasource';

const config = {
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432'),
  database: process.env.PGDATABASE || 'postgres',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || 'dbos',
};
const pool = new Pool(config);

const kconfig = { client: 'pg', connection: config };

const knexds = new KnexDataSource('app', kconfig);

const trig = new DBTrigger({
  connect: async () => {
    const conn = pool.connect();
    return conn;
  },
  disconnect: async (c: ClientBase) => {
    (c as PoolClient).release();
    return Promise.resolve();
  },
  query: async <R>(sql: string, params?: unknown[]) => {
    return (await pool.query(sql, params)).rows as R[];
  },
});

function sleepms(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

const testTableName = 'dbos_test_trig_seq';

class DBOSTriggerTestClassSN {
  static nTSUpdates = 0;
  static tsRecordMap: Map<number, TestTable> = new Map();

  static nSNUpdates = 0;
  static snRecordMap: Map<number, TestTable> = new Map();

  static reset() {
    DBOSTriggerTestClassSN.nTSUpdates = 0;
    DBOSTriggerTestClassSN.tsRecordMap = new Map();

    DBOSTriggerTestClassSN.nSNUpdates = 0;
    DBOSTriggerTestClassSN.snRecordMap = new Map();
  }

  @trig.triggerWorkflow({
    tableName: testTableName,
    recordIDColumns: ['order_id'],
    sequenceNumColumn: 'seqnum',
    sequenceNumJitter: 2,
    installDBTrigger: true,
  })
  @DBOS.workflow()
  static async triggerWFBySeq(op: TriggerOperation, key: number[], rec: unknown) {
    DBOS.logger.debug(`WFSN ${op} - ${JSON.stringify(key)} / ${JSON.stringify(rec)}`);
    expect(op).toBe(TriggerOperation.RecordUpserted);
    if (op === TriggerOperation.RecordUpserted) {
      DBOSTriggerTestClassSN.snRecordMap.set(key[0], rec as TestTable);
      ++DBOSTriggerTestClassSN.nSNUpdates;
    }
    return Promise.resolve();
  }

  @trig.triggerWorkflow({
    tableName: testTableName,
    recordIDColumns: ['order_id'],
    timestampColumn: 'update_date',
    timestampSkewMS: 60000,
    installDBTrigger: true,
  })
  @DBOS.workflow()
  static async triggerWFByTS(op: TriggerOperation, key: number[], rec: unknown) {
    DBOS.logger.debug(`WFTS ${op} - ${JSON.stringify(key)} / ${JSON.stringify(rec)}`);
    expect(op).toBe(TriggerOperation.RecordUpserted);
    if (op === TriggerOperation.RecordUpserted) {
      DBOSTriggerTestClassSN.tsRecordMap.set(key[0], rec as TestTable);
      ++DBOSTriggerTestClassSN.nTSUpdates;
    }
    return Promise.resolve();
  }

  @knexds.transaction()
  static async insertRecord(rec: TestTable) {
    await knexds.client<TestTable>(testTableName).insert(rec);
  }

  @knexds.transaction()
  static async deleteRecord(order_id: number) {
    await knexds.client<TestTable>(testTableName).where({ order_id }).delete();
  }

  @knexds.transaction()
  static async updateRecordStatus(order_id: number, status: string, seqnum: number, update_date: Date) {
    await knexds.client<TestTable>(testTableName).where({ order_id }).update({ status, seqnum, update_date });
  }
}

interface TestTable {
  order_id: number;
  seqnum: number;
  update_date: Date;
  price: number;
  item: string;
  status: string;
}

describe('test-db-triggers', () => {
  beforeAll(async () => {});

  beforeEach(async () => {
    await KnexDataSource.initializeDBOSSchema(kconfig);
    const config: DBOSConfig = {
      name: 'dbtrig_seq',
    };
    DBOS.setConfig(config);
    await trig.db.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await trig.db.query(`
            CREATE TABLE IF NOT EXISTS ${testTableName}(
              order_id SERIAL PRIMARY KEY,
              seqnum INTEGER,
              update_date TIMESTAMP,
              price DECIMAL(10,2),
              item TEXT,
              status VARCHAR(10)
            );`);
    DBOSTriggerTestClassSN.reset();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
    await trig.db.query(`DROP TABLE IF EXISTS ${testTableName};`);
  });

  test('trigger-seqnum', async () => {
    await DBOSTriggerTestClassSN.insertRecord({
      order_id: 1,
      seqnum: 1,
      update_date: new Date('2024-01-01 11:11:11'),
      price: 10,
      item: 'Spacely Sprocket',
      status: 'Ordered',
    });
    await DBOSTriggerTestClassSN.updateRecordStatus(1, 'Packed', 2, new Date('2024-01-01 11:11:12'));
    while (DBOSTriggerTestClassSN.nSNUpdates < 2 || DBOSTriggerTestClassSN.nTSUpdates < 2) await sleepms(10);
    expect(DBOSTriggerTestClassSN.nSNUpdates).toBe(2);
    expect(DBOSTriggerTestClassSN.nTSUpdates).toBe(2);
    expect(DBOSTriggerTestClassSN.snRecordMap.get(1)?.status).toBe('Packed');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status).toBe('Packed');

    await DBOSTriggerTestClassSN.insertRecord({
      order_id: 2,
      seqnum: 3,
      update_date: new Date('2024-01-01 11:11:13'),
      price: 10,
      item: 'Cogswell Cog',
      status: 'Ordered',
    });
    await DBOSTriggerTestClassSN.updateRecordStatus(1, 'Shipped', 5, new Date('2024-01-01 11:11:15'));
    while (DBOSTriggerTestClassSN.nSNUpdates < 4 || DBOSTriggerTestClassSN.nTSUpdates < 4) await sleepms(10);
    expect(DBOSTriggerTestClassSN.nSNUpdates).toBe(4);
    expect(DBOSTriggerTestClassSN.nTSUpdates).toBe(4);
    expect(DBOSTriggerTestClassSN.snRecordMap.get(1)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(2)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(2)?.status).toBe('Ordered');

    // Take down
    await DBOS.deactivateEventReceivers();

    // Do more stuff
    // Invalid record, won't show up because it is well out of sequence
    await DBOSTriggerTestClassSN.insertRecord({
      order_id: 999,
      seqnum: -999,
      update_date: new Date('1900-01-01 11:11:13'),
      price: 10,
      item: 'Cogswell Cog',
      status: 'Ordered',
    });

    // A few more valid records, back in time a little
    await DBOSTriggerTestClassSN.insertRecord({
      order_id: 3,
      seqnum: 4,
      update_date: new Date('2024-01-01 11:11:14'),
      price: 10,
      item: 'Griswold Gear',
      status: 'Ordered',
    });
    await DBOSTriggerTestClassSN.insertRecord({
      order_id: 4,
      seqnum: 6,
      update_date: new Date('2024-01-01 11:11:16'),
      price: 10,
      item: 'Wallace Wheel',
      status: 'Ordered',
    });
    await DBOSTriggerTestClassSN.updateRecordStatus(4, 'Shipped', 7, new Date('2024-01-01 11:11:17'));

    // Test restore
    console.log(
      '************************************************** Restart *****************************************************',
    );
    DBOSTriggerTestClassSN.reset();

    await DBOS.initEventReceivers();

    console.log(
      '************************************************** Restarted *****************************************************',
    );
    DBOSTriggerTestClassSN.reset();
    // We had processed up to 5 before,
    // The count of 7 is a bit confusing because we're sharing a table.  We expect all 4 orders to be sent based on time, and 3 based on SN
    while (DBOSTriggerTestClassSN.nSNUpdates < 7 || DBOSTriggerTestClassSN.nTSUpdates < 7) await sleepms(10);
    await sleepms(100);

    console.log(
      '************************************************** Catchup Complete *****************************************************',
    );

    expect(DBOSTriggerTestClassSN.nSNUpdates).toBe(7);
    // With 60 seconds, we will see all records again
    expect(DBOSTriggerTestClassSN.nTSUpdates).toBe(7);

    expect(DBOSTriggerTestClassSN.snRecordMap.get(1)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(2)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(2)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(3)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(3)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(4)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(4)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(999)?.status).toBeUndefined();
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(999)?.status).toBeUndefined();
  }, 20000);
});
