import { Knex } from "knex";
import { DBOSConfig, TestingRuntime, Transaction, TransactionContext, Workflow, WorkflowContext } from "../../src";
import { DBTrigger, DBTriggerWorkflow, TriggerOperation } from "../../src/dbtrigger/dbtrigger";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { UserDatabaseName } from "../../src/user_database";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { sleepms } from "../../src/utils";

const testTableName = "dbos_test_orders";

type KnexTransactionContext = TransactionContext<Knex>;

class DBOSTestNoClass {

}

class DBOSTriggerTestClass {
    static nInserts = 0;
    static nDeletes = 0;
    static nUpdates = 0;
    static recordMap: Map<number, TestTable> = new Map();

    static reset() {
        DBOSTriggerTestClass.nInserts = 0;
        DBOSTriggerTestClass.nDeletes = 0;
        DBOSTriggerTestClass.nUpdates = 0;
        DBOSTriggerTestClass.recordMap = new Map();
    }

    @DBTrigger({tableName: testTableName, recordIDColumns: ['order_id']})
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

    @DBTriggerWorkflow({tableName: testTableName})
    @Workflow()
    static async triggerWF(_ctxt: WorkflowContext, op: TriggerOperation, key: number[], rec: unknown) {
        if (op === TriggerOperation.RecordDeleted) {
            DBOSTriggerTestClass.recordMap.delete(key[0]);
            ++DBOSTriggerTestClass.nDeletes;
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

    @Transaction()
    static async insertRecord(ctx: KnexTransactionContext, rec: TestTable) {
        await ctx.client<TestTable>(testTableName).insert(rec);
    }

    @Transaction()
    static async deleteRecord(ctx: KnexTransactionContext, order_id: number) {
        await ctx.client<TestTable>(testTableName).where({order_id}).delete();
    }

    @Transaction()
    static async updateRecordStatus(ctx: KnexTransactionContext, order_id: number, status: string) {
        await ctx.client<TestTable>(testTableName).where({order_id}).update({status});
    }
}

interface TestTable {
    order_id: number,
    order_date: Date,
    price: number,
    item: string,
    status: string,
}

describe("test-db-triggers", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        config = generateDBOSTestConfig(UserDatabaseName.KNEX);
        await setUpDBOSTestDb(config);  
    });

    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime([DBOSTestNoClass], config);
        await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
        await testRuntime.queryUserDB(`
            CREATE TABLE IF NOT EXISTS ${testTableName}(
              order_id SERIAL PRIMARY KEY,
              order_date TIMESTAMP,
              price DECIMAL(10,2),
              item TEXT,
              status VARCHAR(10)
            );`
        );
        await testRuntime.destroy();
        testRuntime = await createInternalTestRuntime(undefined, config);
        DBOSTriggerTestClass.reset()
    });
    
    afterEach(async () => {
        // Don't.  Listeners will block this.
        //await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
        await testRuntime.destroy();
    });
  
    test("trigger-nonwf", async () => {
        await testRuntime.invoke(DBOSTriggerTestClass).insertRecord({order_id: 1, order_date: new Date(), price: 10, item: "Spacely Sprocket", status:"Ordered"});
        while (DBOSTriggerTestClass.nInserts < 1) {
            await sleepms(10);
        }
        expect(DBOSTriggerTestClass.nInserts).toBe(1);
        expect(DBOSTriggerTestClass.nDeletes).toBe(0);
        expect(DBOSTriggerTestClass.nUpdates).toBe(0);
        expect(DBOSTriggerTestClass.recordMap.get(1)?.status).toBe("Ordered");
        await testRuntime.invoke(DBOSTriggerTestClass).insertRecord({order_id: 2, order_date: new Date(), price: 10, item: "Cogswell Cog", status:"Ordered"});
        while (DBOSTriggerTestClass.nInserts < 2) await sleepms(10);
        expect(DBOSTriggerTestClass.nInserts).toBe(2);
        expect(DBOSTriggerTestClass.nDeletes).toBe(0);
        expect(DBOSTriggerTestClass.nUpdates).toBe(0);
        expect(DBOSTriggerTestClass.recordMap.get(2)?.status).toBe("Ordered");
        await testRuntime.invoke(DBOSTriggerTestClass).deleteRecord(2);
        while (DBOSTriggerTestClass.nDeletes < 1) await sleepms(10);
        expect(DBOSTriggerTestClass.nInserts).toBe(2);
        expect(DBOSTriggerTestClass.nDeletes).toBe(1);
        expect(DBOSTriggerTestClass.nUpdates).toBe(0);
        expect(DBOSTriggerTestClass.recordMap.get(2)?.status).toBeUndefined();
        await testRuntime.invoke(DBOSTriggerTestClass).updateRecordStatus(1, "Shipped");
        while (DBOSTriggerTestClass.nUpdates < 1) await sleepms(10);
        expect(DBOSTriggerTestClass.nInserts).toBe(2);
        expect(DBOSTriggerTestClass.nDeletes).toBe(1);
        expect(DBOSTriggerTestClass.nUpdates).toBe(1);
        expect(DBOSTriggerTestClass.recordMap.get(1)?.status).toBe("Shipped");
    }, 15000);

    test("trigger-wf", async () => {
        return Promise.resolve();
    }, 15000);
});
