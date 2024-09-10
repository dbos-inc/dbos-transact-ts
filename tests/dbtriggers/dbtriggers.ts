import { DBOSConfig, TestingRuntime, Workflow, WorkflowContext } from "../../src";
import { DBTrigger, DBTriggerWorkflow, TriggerOperation } from "../../src/dbtrigger/dbtrigger";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";

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

    @DBTrigger({tableName: 'test_kv'})
    static async triggerNonWF(op: TriggerOperation, _key: string[], _rec: unknown) {
        if (op === TriggerOperation.RecordDeleted)  ++DBOSTriggerTestClass.nDeletes;
        if (op === TriggerOperation.RecordInserted) ++DBOSTriggerTestClass.nInserts;
        if (op === TriggerOperation.RecordUpdated)  ++DBOSTriggerTestClass.nUpdates;
    }

    @DBTriggerWorkflow({tableName: 'test_kv'})
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
    });
  
    afterEach(async () => {
    }, 10000);
  
    test("trigger-nonwf", async () => {
    }, 15000);
});
