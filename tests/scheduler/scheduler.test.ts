import { Scheduled, SchedulerMode, TestingRuntime, Workflow, WorkflowContext } from "../../src";
import { DBOSConfig } from "../../src/dbos-executor";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { sleep } from "../../src/utils";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";

describe("scheduled-wf-tests", () => {
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
        await sleep(3000);
        expect(DBOSSchedTestClass.nCalls).toBeGreaterThanOrEqual(2);
        expect(DBOSSchedTestClass.nTooEarly).toBe(0);
        expect(DBOSSchedTestClass.nTooLate).toBe(0);
    });
});

class DBOSSchedTestClass {
    static nCalls = 0;
    static nTooEarly = 0;
    static nTooLate = 0;

    @Scheduled({crontab: '* * * * * *', mode: SchedulerMode.ExactlyOncePerIntervalWhenActive})
    @Workflow()
    static async scheduledDefault(ctxt: WorkflowContext, schedTime: Date, startTime: Date) {
        DBOSSchedTestClass.nCalls++;

        if (schedTime.getTime() > startTime.getTime()) DBOSSchedTestClass.nTooEarly++;
        if (startTime.getTime() - schedTime.getTime() > 1500) DBOSSchedTestClass.nTooLate++;

        await ctxt.sleep(2);
    }
}
