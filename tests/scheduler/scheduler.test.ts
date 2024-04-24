import { Scheduled, TestingRuntime, Workflow, WorkflowContext } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";

describe("kafka-tests", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });
  
    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime([DBOSSchedTestClass], config);
    });
  
    afterEach(async () => {
        await testRuntime.destroy();
    }, 10000);
  
    test("wf-scheduled", async () => {
    });
});

class DBOSSchedTestClass {
    // eslint-disable-next-line @typescript-eslint/require-await
    @Scheduled({})
    @Workflow()
    static async scheduledDefault(_ctxt: WorkflowContext, _schedTime: Date, _startTime: Date, _nRunning: number, _nRunningHere: number) {
    }
}
  