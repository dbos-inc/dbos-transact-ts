import { Operon, WorkflowContext, TransactionContext } from "src/";
import { v1 as uuidv1 } from 'uuid';

interface KvTable {
  id?: number,
  value?: string,
}

// Sleep for specified milliseconds.
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

describe('concurrency-tests', () => {
  let operon: Operon;
  const testTableName = 'OperonConcurrentKv';

  beforeEach(async () => {
    operon = new Operon({
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'dbos',
      connectionTimeoutMillis:  3000
    });
    await operon.resetOperonTables();
    await operon.pool.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.pool.query(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await operon.pool.end();
  });

  test('simple-function', async() => {
    const testFunction = async (txnCtxt: TransactionContext, id: number, name: string) => {
      const { rows } = await txnCtxt.client.query<KvTable>(`INSERT INTO ${testTableName} VALUES ($1, $2) RETURNING id`, [id, name]);
      await sleep(10);
      return JSON.stringify(rows[0]);
    };

    let counter: number = 0;
    try {
      // Start two concurrent transactions.
      const futRes1 = operon.transaction(testFunction, {}, 10, "hello");
      const futRes2 = operon.transaction(testFunction, {}, 10, "world");
      const res1 = await futRes1;
      if (res1 !== null) {
        counter += 1;
      }
      const res2 = await futRes2;
      if (res2 != null) {
        counter += 1;
      }
    } catch (err) {
      /* handling? */
      console.error("Captured: ", err);
    }

    expect(counter).toBe(1);
  });
});