import { createTestingRuntime, TestingRuntime, DBOS } from '@dbos-inc/dbos-sdk';
import { DBTrigger, DBTriggerWorkflow, TriggerOperation } from '../dbtrigger/dbtrigger';

const testTableName = 'dbos_test_orders';

function sleepms(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

class DBOSTestNoClass {}

class DBOSTriggerTestClass {
  static nInserts = 0;
  static nDeletes = 0;
  static nUpdates = 0;
  static recordMap: Map<number, TestTable> = new Map();

  static nWFUpdates = 0;
  static wfRecordMap: Map<number, TestTable> = new Map();

  static reset() {
    DBOSTriggerTestClass.nInserts = 0;
    DBOSTriggerTestClass.nDeletes = 0;
    DBOSTriggerTestClass.nUpdates = 0;
    DBOSTriggerTestClass.recordMap = new Map();

    DBOSTriggerTestClass.nWFUpdates = 0;
    DBOSTriggerTestClass.wfRecordMap = new Map();
  }

  @DBTrigger({ tableName: testTableName, recordIDColumns: ['order_id'], installDBTrigger: true })
  static async triggerNonWF(op: TriggerOperation, key: number[], rec: unknown) {
    if (op === TriggerOperation.RecordDeleted) {
      ++DBOSTriggerTestClass.nDeletes;
      DBOSTriggerTestClass.recordMap.delete(key[0]);
    }
    if (op === TriggerOperation.RecordInserted) {
      DBOSTriggerTestClass.recordMap.set(key[0], rec as TestTable);
      ++DBOSTriggerTestClass.nInserts;
    }
    if (op === TriggerOperation.RecordUpdated) {
      DBOSTriggerTestClass.recordMap.set(key[0], rec as TestTable);
      ++DBOSTriggerTestClass.nUpdates;
    }
    return Promise.resolve();
  }

  @DBTriggerWorkflow({ tableName: testTableName, recordIDColumns: ['order_id'], installDBTrigger: true })
  @DBOS.workflow()
  static async triggerWF(op: TriggerOperation, key: number[], rec: unknown) {
    DBOS.logger.debug(`WF ${op} - ${JSON.stringify(key)} / ${JSON.stringify(rec)}`);
    expect(op).toBe(TriggerOperation.RecordUpserted);
    if (op === TriggerOperation.RecordUpserted) {
      DBOSTriggerTestClass.wfRecordMap.set(key[0], rec as TestTable);
      ++DBOSTriggerTestClass.nWFUpdates;
    }
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
  static async updateRecordStatus(order_id: number, status: string) {
    await DBOS.knexClient<TestTable>(testTableName).where({ order_id }).update({ status });
  }
}

interface TestTable {
  order_id: number;
  order_date: Date;
  price: number;
  item: string;
  status: string;
}

describe('test-db-triggers', () => {
  let testRuntime: TestingRuntime;

  beforeAll(async () => {});

  beforeEach(async () => {
    testRuntime = await createTestingRuntime([DBOSTestNoClass], 'dbtriggers-test-dbos-config.yaml');
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`
            CREATE TABLE IF NOT EXISTS ${testTableName}(
              order_id SERIAL PRIMARY KEY,
              order_date TIMESTAMP,
              price DECIMAL(10,2),
              item TEXT,
              status VARCHAR(10)
            );`);
    await testRuntime.destroy();
    testRuntime = await createTestingRuntime(undefined, 'dbtriggers-test-dbos-config.yaml');
    DBOSTriggerTestClass.reset();
  });

  afterEach(async () => {
    await testRuntime.deactivateEventReceivers();
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.destroy();
  });

  test('trigger-nonwf', async () => {
    await DBOSTriggerTestClass.insertRecord({
      order_id: 1,
      order_date: new Date(),
      price: 10,
      item: 'Spacely Sprocket',
      status: 'Ordered',
    });
    while (DBOSTriggerTestClass.nInserts < 1) await sleepms(10);
    expect(DBOSTriggerTestClass.nInserts).toBe(1);
    expect(DBOSTriggerTestClass.recordMap.get(1)?.status).toBe('Ordered');
    while (DBOSTriggerTestClass.nWFUpdates < 1) await sleepms(10);
    expect(DBOSTriggerTestClass.nWFUpdates).toBe(1);
    expect(DBOSTriggerTestClass.wfRecordMap.get(1)?.status).toBe('Ordered');

    await DBOSTriggerTestClass.insertRecord({
      order_id: 2,
      order_date: new Date(),
      price: 10,
      item: 'Cogswell Cog',
      status: 'Ordered',
    });
    while (DBOSTriggerTestClass.nInserts < 2) await sleepms(10);
    expect(DBOSTriggerTestClass.nInserts).toBe(2);
    expect(DBOSTriggerTestClass.nDeletes).toBe(0);
    expect(DBOSTriggerTestClass.nUpdates).toBe(0);
    expect(DBOSTriggerTestClass.recordMap.get(2)?.status).toBe('Ordered');
    while (DBOSTriggerTestClass.nWFUpdates < 2) await sleepms(10);
    expect(DBOSTriggerTestClass.nWFUpdates).toBe(2);
    expect(DBOSTriggerTestClass.wfRecordMap.get(2)?.status).toBe('Ordered');

    await DBOSTriggerTestClass.deleteRecord(2);
    while (DBOSTriggerTestClass.nDeletes < 1) await sleepms(10);
    expect(DBOSTriggerTestClass.nInserts).toBe(2);
    expect(DBOSTriggerTestClass.nDeletes).toBe(1);
    expect(DBOSTriggerTestClass.nUpdates).toBe(0);
    expect(DBOSTriggerTestClass.recordMap.get(2)?.status).toBeUndefined();
    expect(DBOSTriggerTestClass.nWFUpdates).toBe(2); // Workflow does not trigger on delete

    await DBOSTriggerTestClass.updateRecordStatus(1, 'Shipped');
    while (DBOSTriggerTestClass.nUpdates < 1) await sleepms(10);
    expect(DBOSTriggerTestClass.nInserts).toBe(2);
    expect(DBOSTriggerTestClass.nDeletes).toBe(1);
    expect(DBOSTriggerTestClass.nUpdates).toBe(1);
    expect(DBOSTriggerTestClass.recordMap.get(1)?.status).toBe('Shipped');
    await sleepms(100);
    // This update does not start a workflow as there is no update marker column.
    expect(DBOSTriggerTestClass.nWFUpdates).toBe(2);
  }, 15000);
});
