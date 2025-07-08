import { DBOS, createTestingRuntime, TestingRuntime, WorkflowQueue } from '@dbos-inc/dbos-sdk';

import { DBTriggerWorkflow, TriggerOperation } from '../dbtrigger/dbtrigger';

function sleepms(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

const testTableName = 'dbos_test_trig_seq';

class DBOSTestNoClass {}

const q = new WorkflowQueue('schedQ');

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

  @DBTriggerWorkflow({
    tableName: testTableName,
    recordIDColumns: ['order_id'],
    sequenceNumColumn: 'seqnum',
    sequenceNumJitter: 2,
    queueName: q.name,
    dbPollingInterval: 1000,
  })
  @DBOS.workflow()
  static async pollWFBySeq(op: TriggerOperation, key: number[], rec: unknown) {
    DBOS.logger.debug(`WFSN Poll ${op} - ${JSON.stringify(key)} / ${JSON.stringify(rec)}`);
    expect(op).toBe(TriggerOperation.RecordUpserted);
    const trec = rec as TestTable;
    if (
      !DBOSTriggerTestClassSN.snRecordMap.has(key[0]) ||
      trec.seqnum > DBOSTriggerTestClassSN.snRecordMap.get(key[0])!.seqnum
    ) {
      DBOSTriggerTestClassSN.snRecordMap.set(key[0], trec);
      await DBOSTriggerTestClassSN.snUpdate();
    }
    return Promise.resolve();
  }

  @DBTriggerWorkflow({
    tableName: testTableName,
    recordIDColumns: ['order_id'],
    timestampColumn: 'update_date',
    timestampSkewMS: 60000,
    dbPollingInterval: 1000,
  })
  @DBOS.workflow()
  static async pollWFByTS(op: TriggerOperation, key: number[], rec: unknown) {
    DBOS.logger.debug(`WFTS Poll ${op} - ${JSON.stringify(key)} / ${JSON.stringify(rec)}`);
    expect(op).toBe(TriggerOperation.RecordUpserted);
    if (op === TriggerOperation.RecordUpserted) {
      const trec = rec as TestTable;
      if (
        !DBOSTriggerTestClassSN.tsRecordMap.has(key[0]) ||
        trec.update_date > DBOSTriggerTestClassSN.tsRecordMap.get(key[0])!.update_date
      ) {
        DBOSTriggerTestClassSN.tsRecordMap.set(key[0], trec);
        await DBOSTriggerTestClassSN.tsUpdate();
      }
    }
    return Promise.resolve();
  }

  @DBOS.step()
  static async snUpdate() {
    ++DBOSTriggerTestClassSN.nSNUpdates;
    return Promise.resolve();
  }

  @DBOS.step()
  static async tsUpdate() {
    ++DBOSTriggerTestClassSN.nTSUpdates;
    return Promise.resolve();
  }

  @DBOS.transaction()
  static async insertRecord(rec: TestTable) {
    await DBOS.knexClient<TestTable>(testTableName).insert(rec);
  }

  @DBOS.transaction()
  static async deleteRecord(order_id: number) {
    await DBOS.knexClient<TestTable>(testTableName).where({ order_id }).delete();
  }

  @DBOS.transaction()
  static async updateRecordStatus(order_id: number, status: string, seqnum: number, update_date: Date) {
    await DBOS.knexClient<TestTable>(testTableName).where({ order_id }).update({ status, seqnum, update_date });
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

describe('test-db-trigger-polling', () => {
  let testRuntime: TestingRuntime;

  beforeAll(async () => {});

  beforeEach(async () => {
    testRuntime = await createTestingRuntime([DBOSTestNoClass], 'dbtriggers-test-dbos-config.yaml');
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`
            CREATE TABLE IF NOT EXISTS ${testTableName}(
              order_id SERIAL PRIMARY KEY,
              seqnum INTEGER,
              update_date TIMESTAMP,
              price DECIMAL(10,2),
              item TEXT,
              status VARCHAR(10)
            );`);
    await testRuntime.destroy();
    testRuntime = await createTestingRuntime(undefined, 'dbtriggers-test-dbos-config.yaml');
    DBOSTriggerTestClassSN.reset();
  });

  afterEach(async () => {
    await testRuntime.deactivateEventReceivers();
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.destroy();
  });

  test('dbpoll-seqnum', async () => {
    await DBOSTriggerTestClassSN.insertRecord({
      order_id: 1,
      seqnum: 1,
      update_date: new Date('2024-01-01 11:11:11'),
      price: 10,
      item: 'Spacely Sprocket',
      status: 'Ordered',
    });
    await DBOSTriggerTestClassSN.updateRecordStatus(1, 'Packed', 2, new Date('2024-01-01 11:11:12'));
    while (DBOSTriggerTestClassSN.snRecordMap.get(1)?.status !== 'Packed') await sleepms(10);
    while (DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status !== 'Packed') await sleepms(10);
    while (DBOSTriggerTestClassSN.nSNUpdates < 1 || DBOSTriggerTestClassSN.nTSUpdates < 1) await sleepms(10);

    // If these occurred close together, we would not see the insert+update separately...
    expect(DBOSTriggerTestClassSN.nSNUpdates).toBeGreaterThanOrEqual(1);
    expect(DBOSTriggerTestClassSN.nSNUpdates).toBeLessThanOrEqual(2);
    expect(DBOSTriggerTestClassSN.nTSUpdates).toBeGreaterThanOrEqual(1);
    expect(DBOSTriggerTestClassSN.nTSUpdates).toBeLessThanOrEqual(2);
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
    while (DBOSTriggerTestClassSN.snRecordMap.get(1)?.status !== 'Shipped') await sleepms(10);
    while (DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status !== 'Shipped') await sleepms(10);
    while (DBOSTriggerTestClassSN.snRecordMap.get(2)?.status !== 'Ordered') await sleepms(10);
    while (DBOSTriggerTestClassSN.tsRecordMap.get(2)?.status !== 'Ordered') await sleepms(10);

    expect(DBOSTriggerTestClassSN.snRecordMap.get(1)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(1)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(2)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(2)?.status).toBe('Ordered');

    // Take down
    await testRuntime.deactivateEventReceivers();

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

    await testRuntime.initEventReceivers();

    console.log(
      '************************************************** Restarted *****************************************************',
    );
    DBOSTriggerTestClassSN.reset();

    // Catchup may not be sequential, wait for the last update to get processed...
    while (DBOSTriggerTestClassSN.snRecordMap.get(3)?.status !== 'Ordered') await sleepms(10);
    while (DBOSTriggerTestClassSN.tsRecordMap.get(3)?.status !== 'Ordered') await sleepms(10);
    while (DBOSTriggerTestClassSN.snRecordMap.get(4)?.status !== 'Shipped') await sleepms(10);
    while (DBOSTriggerTestClassSN.tsRecordMap.get(4)?.status !== 'Shipped') await sleepms(10);
    await sleepms(100);

    console.log(
      '************************************************** Catchup Complete *****************************************************',
    );

    expect(DBOSTriggerTestClassSN.snRecordMap.get(3)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(3)?.status).toBe('Ordered');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(4)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(4)?.status).toBe('Shipped');
    expect(DBOSTriggerTestClassSN.snRecordMap.get(999)?.status).toBeUndefined();
    expect(DBOSTriggerTestClassSN.tsRecordMap.get(999)?.status).toBeUndefined();

    const wfs = await testRuntime.getWorkflows({
      workflowName: 'pollWFBySeq',
    });

    let foundQ = false;
    for (const wfid of wfs.workflowUUIDs) {
      const stat = await testRuntime.retrieveWorkflow(wfid).getStatus();
      if (stat?.queueName === q.name) foundQ = true;
    }
    expect(foundQ).toBeTruthy();
  }, 15000);
});
