import { Operon, TransactionContext } from "src/";
import { v1 as uuidv1 } from 'uuid';
import { sleep } from "./helper";

describe('concurrency-tests', () => {
  let operon: Operon;
  const testTableName = 'OperonConcurrencyTestKv';

  beforeEach(async () => {
    operon = new Operon();
    await operon.resetOperonTables();
    await operon.pool.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.pool.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test('duplicate-workflow',async () => {
    // Run two workflows concurrently with the same UUID.
    // We should handle this properly without failures.
    const testFunction = async (txnCtxt: TransactionContext, id: number, sleepMs: number=0) => {
      await sleep(sleepMs);
      return id;
    };

    operon.registerTransaction(testFunction, {});

    const workflowUUID = uuidv1();
    const futRes1 = operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 10);
    const futRes2 = operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 10);
  
    await expect(futRes1).resolves.toBe(10);
    await expect(futRes2).resolves.toBe(10);
  });

});