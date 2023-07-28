import { Operon, OperonError, TransactionContext } from "src/";
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
    let res1: number | undefined;
    let res2: number | undefined;
    try {
      const futRes1 = operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 10);
      const futRes2 = operon.transaction(testFunction, {workflowUUID: workflowUUID}, 10, 10);
    
      res1 = await futRes1;
      res2 = await futRes2;
    } catch (error) {
      expect(error).toBeInstanceOf(OperonError);
      const err: OperonError = error as OperonError;
      expect(err.message).toBe('Conflicting UUIDs');
    }

    if (res1 === undefined) {
      expect(res2).toBe(10);
    } else {
      expect(res2).toBeUndefined();
      expect(res1).toBe(10);
    }
  });

});