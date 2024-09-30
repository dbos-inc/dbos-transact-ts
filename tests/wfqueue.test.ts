import { Communicator, CommunicatorContext, StatusString, TestingRuntime, Workflow, WorkflowContext } from "../src";
import { DBOSConfig, DBOSExecutor } from "../src/dbos-executor";
import { createInternalTestRuntime, TestingRuntimeImpl } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { WorkflowQueue } from "../src";
import { v4 as uuidv4 } from "uuid";
import { sleepms } from "../src/utils";
import { PostgresSystemDatabase } from "../src/system_database";
import { workflow_queue } from "../schemas/system_db_schema";


const queue = new WorkflowQueue("testQ");
const serialqueue = new WorkflowQueue("serialQ", 1);
const serialqueueLimited = new WorkflowQueue("serialQL", 1, {limitPerPeriod: 10, periodSec: 1});
const childqueue = new WorkflowQueue("childQ", 3);

async function queueEntriesAreCleanedUp(dbos: TestingRuntimeImpl) {
    let maxTries = 10;
    let success = false;
    while (maxTries > 0) {
        const r = await (dbos.getDBOSExec().systemDatabase as PostgresSystemDatabase)
           .knexDB<workflow_queue>(`${DBOSExecutor.systemDBSchemaName}.workflow_queue`)
           .count()
           .first();
        if (`${r!.count}` === '0') {
            success = true;
            break;
        }
        await sleepms(1000);
        --maxTries;
    }
    return success;
}

describe("queued-wf-tests-simple", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);  
    });

    beforeEach(async () => {
        TestWFs.reset();
        TestWFs2.reset();
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
        expect((await wfh.getStatus())?.queueName).toBe('testQ');

        expect(await testRuntime.invokeWorkflow(TestWFs, wfid).testWorkflow('abc', '123')).toBe('abcd123');
        expect(TestWFs.wfCounter).toBe(2);
        expect(TestWFs.stepCounter).toBe(1);

        expect((await wfh.getStatus())?.queueName).toBe('testQ');
    });

    test("one-at-a-time", async() => {
        await runOneAtATime(testRuntime, serialqueue);
    }, 10000);

    test("child-wfs-queue", async() => {
        expect (await testRuntime.invokeWorkflow(TestChildWFs).testWorkflow('a','b')).toBe('adbdadbd');
    }, 10000);

    test("test_one_at_a_time_with_limiter", async() => {
        await runOneAtATime(testRuntime, serialqueueLimited);
    }, 10000);
});

/*
def test_one_at_a_time_with_limiter(dbos: DBOS) -> None:
    wf_counter = 0
    flag = False
    workflow_event = threading.Event()
    main_thread_event = threading.Event()

    @DBOS.workflow()
    def workflow_one() -> None:
        nonlocal wf_counter
        wf_counter += 1
        main_thread_event.set()
        workflow_event.wait()

    @DBOS.workflow()
    def workflow_two() -> None:
        nonlocal flag
        flag = True

    queue = Queue("test_queue", concurrency=1, limiter={"limit": 10, "period": 1})
    handle1 = queue.enqueue(workflow_one)
    handle2 = queue.enqueue(workflow_two)

    main_thread_event.wait()
    time.sleep(2)  # Verify the other task isn't scheduled on subsequent poller ticks.
    assert not flag
    workflow_event.set()
    assert handle1.get_result() == None
    assert handle2.get_result() == None
    assert flag
    assert wf_counter == 1
*/

/*
def test_limiter(dbos: DBOS) -> None:

    @DBOS.workflow()
    def test_workflow(var1: str, var2: str) -> float:
        assert var1 == "abc" and var2 == "123"
        return time.time()

    limit = 5
    period = 2
    queue = Queue("test_queue", limiter={"limit": limit, "period": period})

    handles: list[WorkflowHandle[float]] = []
    times: list[float] = []

    # Launch a number of tasks equal to three times the limit.
    # This should lead to three "waves" of the limit tasks being
    # executed simultaneously, followed by a wait of the period,
    # followed by the next wave.
    num_waves = 3
    for _ in range(limit * num_waves):
        h = queue.enqueue(test_workflow, "abc", "123")
        handles.append(h)
    for h in handles:
        times.append(h.get_result())

    # Verify that each "wave" of tasks started at the ~same time.
    for wave in range(num_waves):
        for i in range(wave * limit, (wave + 1) * limit - 1):
            assert times[i + 1] - times[i] < 0.1

    # Verify that the gap between "waves" is ~equal to the period
    for wave in range(num_waves - 1):
        assert times[limit * wave] - times[limit * wave - 1] < period + 0.1

    # Verify all workflows get the SUCCESS status eventually
    dbos._sys_db.wait_for_buffer_flush()
    for h in handles:
        assert h.get_status().status == WorkflowStatusString.SUCCESS.value

    # Verify all queue entries eventually get cleaned up.
    assert queue_entries_are_cleaned_up(dbos)
*/

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

class TestWFs2
{
    static wfCounter = 0;
    static flag = false;
    static wfid: string;
    static mainResolve?: () => void;
    static wfPromise?: Promise<void>;

    static reset() {
        TestWFs2.wfCounter = 0;
        TestWFs2.flag = false;
    }

    @Workflow()
    static async workflowOne(_ctx: WorkflowContext) {
        ++TestWFs2.wfCounter;
        TestWFs2.mainResolve?.();
        await TestWFs2.wfPromise;
        return Promise.resolve();
    }

    @Workflow()
    static async workflowTwo(_ctx: WorkflowContext) {
        TestWFs2.flag = true; // Tell if this ran yet
        return Promise.resolve();
    }
}

class TestChildWFs
{
    @Workflow()
    static async testWorkflow(ctx: WorkflowContext, var1: string, var2: string) {
        const wfh1 = await ctx.startWorkflow(TestChildWFs, undefined, childqueue).testChildWF(var1);
        const wfh2 = await ctx.startWorkflow(TestChildWFs, undefined, childqueue).testChildWF(var2);
        const wfh3 = await ctx.startWorkflow(TestChildWFs, undefined, childqueue).testChildWF(var1);
        const wfh4 = await ctx.startWorkflow(TestChildWFs, undefined, childqueue).testChildWF(var2);

        await ctx.sleepms(1000);
        expect((await wfh4.getStatus())?.status).toBe(StatusString.ENQUEUED);

        await ctx.send(wfh1.getWorkflowUUID(), 'go', 'release');
        await ctx.send(wfh2.getWorkflowUUID(), 'go', 'release');
        await ctx.send(wfh3.getWorkflowUUID(), 'go', 'release');
        await ctx.send(wfh4.getWorkflowUUID(), 'go', 'release');

        return (await wfh1.getResult() + await wfh2.getResult() +
           await wfh3.getResult() + await wfh4.getResult())
    }

    @Workflow()
    static async testChildWF(ctx: WorkflowContext, str: string) {
        await ctx.recv('release', 30);
        return Promise.resolve(str + 'd');
    }
}

async function runOneAtATime(testRuntime: TestingRuntime, queue: WorkflowQueue) {
    let wfRes: () => void = () => { };
    TestWFs2.wfPromise = new Promise<void>((resolve, _rj) => { wfRes = resolve; });
    const mainPromise = new Promise<void>((resolve, _rj) => { TestWFs2.mainResolve = resolve; });
    const wfh1 = await testRuntime.startWorkflow(TestWFs2, undefined, undefined, queue).workflowOne();
    expect((await wfh1.getStatus())?.queueName).toBe(queue.name);
    const wfh2 = await testRuntime.startWorkflow(TestWFs2, undefined, undefined, queue).workflowTwo();
    expect((await wfh2.getStatus())?.queueName).toBe(queue.name);
    await mainPromise;
    await sleepms(2000);
    expect(TestWFs2.flag).toBeFalsy();
    wfRes?.();
    await wfh1.getResult();
    await wfh2.getResult();
    expect(TestWFs2.flag).toBeTruthy();
    expect(TestWFs2.wfCounter).toBe(1);
    expect(await queueEntriesAreCleanedUp(testRuntime as TestingRuntimeImpl)).toBe(true);
}

