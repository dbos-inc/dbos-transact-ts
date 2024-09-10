import { DBOSConfig, TestingRuntime, Workflow, WorkflowContext } from "../../src";
import { DBTrigger, DBTriggerWorkflow, TriggerOperation } from "../../src/dbtrigger/dbtrigger";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";

const testTableName = "dbos_test_orders";

class DBOSTriggerTestClass {
    static nCalls = 0;
    static nInserts = 0;
    static nDeletes = 0;
    static nUpdates = 0;

    static reset() {
        DBOSTriggerTestClass.nCalls = 0;
        DBOSTriggerTestClass.nInserts = 0;
        DBOSTriggerTestClass.nDeletes = 0;
        DBOSTriggerTestClass.nUpdates = 0;
    }

    @DBTrigger({tableName: testTableName})
    static async triggerNonWF(op: TriggerOperation, _key: string[], _rec: unknown) {
        if (op === TriggerOperation.RecordDeleted)  ++DBOSTriggerTestClass.nDeletes;
        if (op === TriggerOperation.RecordInserted) ++DBOSTriggerTestClass.nInserts;
        if (op === TriggerOperation.RecordUpdated)  ++DBOSTriggerTestClass.nUpdates;
    }

    @DBTriggerWorkflow({tableName: testTableName})
    @Workflow()
    static async triggerWF(ctxt: WorkflowContext, op: TriggerOperation, _key: string[], _rec: unknown) {
        if (op === TriggerOperation.RecordDeleted)  ++DBOSTriggerTestClass.nDeletes;
        if (op === TriggerOperation.RecordInserted) ++DBOSTriggerTestClass.nInserts;
        if (op === TriggerOperation.RecordUpdated)  ++DBOSTriggerTestClass.nUpdates;
    }
}

describe("test-db-triggers", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });

    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime(undefined, config);
        await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
        await testRuntime.queryUserDB(`
            CREATE TABLE IF NOT EXISTS ${testTableName}(
              order_id SERIAL PRIMARY KEY,
              order_date TIMESTAMP,
              price: DECIMAL(10,2),
              item TEXT,
              status: VARCHAR(10)
            );`
        );
    });
    
    afterEach(async () => {
        await testRuntime.destroy();
    });
  
    test("trigger-nonwf", async () => {
    }, 15000);
});
