import { Knex } from "knex";
import { DBOSConfig, TestingRuntime, Transaction, TransactionContext, Workflow, WorkflowContext } from "../../src";
import { DBTrigger, DBTriggerWorkflow, TriggerOperation } from "../../src/dbtrigger/dbtrigger";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { UserDatabaseName } from "../../src/user_database";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";

const testTableName = "dbos_test_orders";

type KnexTransactionContext = TransactionContext<Knex>;


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
    static async triggerNonWF(op: TriggerOperation, _key: string[], _rec: unknown) {
        if (op === TriggerOperation.RecordDeleted)  ++DBOSTriggerTestClass.nDeletes;
        if (op === TriggerOperation.RecordInserted) ++DBOSTriggerTestClass.nInserts;
        if (op === TriggerOperation.RecordUpdated)  ++DBOSTriggerTestClass.nUpdates;
        return Promise.resolve();
    }

    @DBTriggerWorkflow({tableName: testTableName})
    @Workflow()
    static async triggerWF(ctxt: WorkflowContext, op: TriggerOperation, _key: string[], _rec: unknown) {
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
        testRuntime = await createInternalTestRuntime(undefined, config);
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
        DBOSTriggerTestClass.reset()
    });
    
    afterEach(async () => {
        await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
        await testRuntime.destroy();
    });
  
    test("trigger-nonwf", async () => {
        await testRuntime.invoke(DBOSTriggerTestClass).insertRecord({order_id: 1, order_date: new Date(), price: 10, item: "Spacely Sprocket", status:"Ordered"});
        await testRuntime.invoke(DBOSTriggerTestClass).insertRecord({order_id: 2, order_date: new Date(), price: 10, item: "Cogswell Cog", status:"Ordered"});
        await testRuntime.invoke(DBOSTriggerTestClass).deleteRecord(2);
        await testRuntime.invoke(DBOSTriggerTestClass).updateRecordStatus(1, "Shipped");
    }, 15000);

    test("trigger-wf", async () => {
    }, 15000);
});
