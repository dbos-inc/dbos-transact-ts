import { Communicator, CommunicatorContext, TestingRuntime, Workflow, WorkflowContext } from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { WorkflowQueue } from "../src";
import { v4 as uuidv4 } from "uuid";


const queue = new WorkflowQueue("testQ");

describe("scheduled-wf-tests-simple", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        TestWFs.reset();
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });

    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime(undefined, config);
    });

    afterEach(async () => {
        await testRuntime.destroy();
    }, 10000);
  
    test("simple-queue", async () => {
        const wfid = uuidv4();
        TestWFs.wfid = wfid;

        const wfh = await testRuntime.startWorkflow(TestWFs, wfid, {}, queue).testWorkflow('abc', '123');
        expect(await wfh.getResult()).toBe('abcd123');
        expect(await testRuntime.invokeWorkflow(TestWFs, wfid).testWorkflow('abc', '123')).toBe('abcd123');
        expect(TestWFs.wfCounter).toBe(2);
        expect(TestWFs.stepCounter).toBe(1);
    });
});


class TestWFs
{
    static wfCounter = 0;
    static stepCounter = 0;
    static wfid: string;

    static reset() {
        TestWFs.wfCounter = 0;
        TestWFs.stepCounter = 0;
    }

    @Workflow()
    static async testWorkflow(ctx: WorkflowContext, var1: string, var2: string) {
        expect(ctx.workflowUUID).toBe(TestWFs.wfid);
        ++TestWFs.wfCounter;
        var1 = await ctx.invoke(TestWFs).testStep(var1);
        return Promise.resolve(var1 + var2);
    }

    @Communicator()
    static async testStep(ctx: CommunicatorContext, str: string) {
        ++TestWFs.stepCounter;
        return Promise.resolve(str + 'd');
    }
}

/*
+def test_simple_queue(dbos: DBOS) -> None:
+    wf_counter: int = 0
+    step_counter: int = 0
+
+    wfid = str(uuid.uuid4())
+
+    with SetWorkflowID(wfid):
+        handle = queue.enqueue(test_workflow, "abc", "123")
+    assert handle.get_result() == "abcd123"
+    with SetWorkflowID(wfid):
+        assert test_workflow("abc", "123") == "abcd123"
+    assert wf_counter == 2
+    assert step_counter == 1
+
+
+def test_one_at_a_time(dbos: DBOS) -> None:
+    wf_counter = 0
+    flag = False
+    workflow_event = threading.Event()
+    main_thread_event = threading.Event()
+
+    @DBOS.workflow()
+    def workflow_one() -> None:
+        nonlocal wf_counter
+        wf_counter += 1
+        main_thread_event.set()
+        workflow_event.wait()
+
+    @DBOS.workflow()
+    def workflow_two() -> None:
+        nonlocal flag
+        flag = True
+
+    queue = Queue("test_queue", 1)
+    handle1 = queue.enqueue(workflow_one)
+    handle2 = queue.enqueue(workflow_two)
+
+    main_thread_event.wait()
+    time.sleep(2)  # Verify the other task isn't scheduled on subsequent poller ticks.
+    assert not flag
+    workflow_event.set()
+    assert handle1.get_result() == None
+    assert handle2.get_result() == None
+    assert flag
+    assert wf_counter == 1
+
*/