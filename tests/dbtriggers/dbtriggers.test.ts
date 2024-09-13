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

    static reset() {
        DBOSTriggerTestClass.nInserts = 0;
        DBOSTriggerTestClass.nDeletes = 0;
        DBOSTriggerTestClass.nUpdates = 0;
    }

    @DBTrigger({tableName: testTableName})
    static async triggerNonWF(op: TriggerOperation, key: string[], rec: unknown) {
        console.log(`Triggered: ${op} / ${JSON.stringify(key)} / ${JSON.stringify(rec)}`)
        if (op === TriggerOperation.RecordDeleted)  ++DBOSTriggerTestClass.nDeletes;
        if (op === TriggerOperation.RecordInserted) ++DBOSTriggerTestClass.nInserts;
        if (op === TriggerOperation.RecordUpdated)  ++DBOSTriggerTestClass.nUpdates;
        return Promise.resolve();
    }

    @DBTriggerWorkflow({tableName: testTableName})
    @Workflow()
    static async triggerWF(ctxt: WorkflowContext, op: TriggerOperation, key: string[], rec: unknown) {
        console.log(`Triggered: ${op} / ${JSON.stringify(key)} / ${JSON.stringify(rec)}`)
        if (op === TriggerOperation.RecordDeleted)  ++DBOSTriggerTestClass.nDeletes;
        if (op === TriggerOperation.RecordInserted) ++DBOSTriggerTestClass.nInserts;
        if (op === TriggerOperation.RecordUpdated)  ++DBOSTriggerTestClass.nUpdates;
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
        console.log("Drop/create");
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
        console.log("Destroy");
        await testRuntime.destroy();
        console.log("Create again");
        testRuntime = await createInternalTestRuntime(undefined, config);
        DBOSTriggerTestClass.reset()
    });
    
    afterEach(async () => {
        // Don't.  Listeners will block this.
        //await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
        console.log("Destroy starts");
        await testRuntime.destroy();
        console.log("Destroy ends");
    });
  
    test("trigger-nonwf", async () => {
        console.log("Started test")
        await testRuntime.invoke(DBOSTriggerTestClass).insertRecord({order_id: 1, order_date: new Date(), price: 10, item: "Spacely Sprocket", status:"Ordered"});
        console.log("After insert")
        while (DBOSTriggerTestClass.nInserts < 1) {
            await sleepms(10);
        }
        console.log("After wait")
        expect(DBOSTriggerTestClass.nInserts).toBe(1);
        expect(DBOSTriggerTestClass.nDeletes).toBe(0);
        expect(DBOSTriggerTestClass.nUpdates).toBe(0);
        await testRuntime.invoke(DBOSTriggerTestClass).insertRecord({order_id: 2, order_date: new Date(), price: 10, item: "Cogswell Cog", status:"Ordered"});
        while (DBOSTriggerTestClass.nInserts < 2) await sleepms(10);
        expect(DBOSTriggerTestClass.nInserts).toBe(2);
        expect(DBOSTriggerTestClass.nDeletes).toBe(0);
        expect(DBOSTriggerTestClass.nUpdates).toBe(0);
        await testRuntime.invoke(DBOSTriggerTestClass).deleteRecord(2);
        while (DBOSTriggerTestClass.nDeletes < 1) await sleepms(10);
        expect(DBOSTriggerTestClass.nInserts).toBe(2);
        expect(DBOSTriggerTestClass.nDeletes).toBe(1);
        expect(DBOSTriggerTestClass.nUpdates).toBe(0);
        await testRuntime.invoke(DBOSTriggerTestClass).updateRecordStatus(1, "Shipped");
        while (DBOSTriggerTestClass.nUpdates < 1) await sleepms(10);
        expect(DBOSTriggerTestClass.nInserts).toBe(2);
        expect(DBOSTriggerTestClass.nDeletes).toBe(1);
        expect(DBOSTriggerTestClass.nUpdates).toBe(1);
        console.log("Test done")
    }, 15000);

    test("trigger-wf", async () => {
        console.log("Started WF test")
    }, 15000);
});
