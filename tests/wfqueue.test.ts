import { Communicator, CommunicatorContext, StatusString, TestingRuntime, Workflow, WorkflowContext, WorkflowHandle } from "../src";
import { DBOSConfig, DBOSExecutor } from "../src/dbos-executor";
import { createInternalTestRuntime, TestingRuntimeImpl } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { WorkflowQueue } from "../src";
import { v4 as uuidv4 } from "uuid";
import { sleepms } from "../src/utils";
import { PostgresSystemDatabase } from "../src/system_database";
import { workflow_queue } from "../schemas/system_db_schema";
import { DEBUG_TRIGGER_WORKFLOW_QUEUE_START, setDebugTrigger } from "../src/debugpoint";


const queue = new WorkflowQueue("testQ");
const serialqueue = new WorkflowQueue("serialQ", 1);
const serialqueueLimited = new WorkflowQueue("serialQL", 1, {limitPerPeriod: 10, periodSec: 1});
const childqueue = new WorkflowQueue("childQ", 3);

const qlimit = 5;
const qperiod = 2
const rlqueue = new WorkflowQueue("limited_queue", undefined, {limitPerPeriod: qlimit, periodSec: qperiod});

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

    test("test-queue_rate_limit", async() => {
        const handles: WorkflowHandle<number>[] = [];
        const times: number[] = [];

        // Launch a number of tasks equal to three times the limit.
        // This should lead to three "waves" of the limit tasks being
        //   executed simultaneously, followed by a wait of the period,
        //   followed by the next wave.
        const numWaves = 3;

        for (let i = 0; i< qlimit * numWaves; ++i) {
            const h = await testRuntime.startWorkflow(TestWFs, undefined, undefined, rlqueue).testWorkflowTime("abc", "123");
            handles.push(h);
        }
        for (const h of handles) {
            times.push(await h.getResult());
        }

        // Verify all queue entries eventually get cleaned up.
        expect(await queueEntriesAreCleanedUp(testRuntime as TestingRuntimeImpl)).toBe(true);

        // Verify all workflows get the SUCCESS status eventually
        const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
        await dbosExec.flushWorkflowBuffers();

        // Verify that each "wave" of tasks started at the ~same time.
        for (let wave = 0; wave < numWaves; ++wave) {
            for (let i = wave * qlimit; i < (wave + 1) * qlimit -1; ++i) {
                expect(times[i + 1] - times[i]).toBeLessThan(100);
            }
        }

        // Verify that the gap between "waves" is ~equal to the period
        for (let wave = 1; wave < numWaves; ++wave) {
            expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeGreaterThan(qperiod*1000 - 200);
            expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeLessThan(qperiod*1000 + 200);
        }

        for (const h of handles) {
            expect((await h.getStatus())!.status).toBe(StatusString.SUCCESS);
        }
    }, 10000);

    test("test_multiple_queues", async() => {
        let wfRes: () => void = () => { };
        TestWFs2.wfPromise = new Promise<void>((resolve, _rj) => { wfRes = resolve; });
        const mainPromise = new Promise<void>((resolve, _rj) => { TestWFs2.mainResolve = resolve; });

        const wfh1 = await testRuntime.startWorkflow(TestWFs2, undefined, undefined, serialqueue).workflowOne();
        expect((await wfh1.getStatus())?.queueName).toBe(serialqueue.name);
        const wfh2 = await testRuntime.startWorkflow(TestWFs2, undefined, undefined, serialqueue).workflowTwo();
        expect((await wfh2.getStatus())?.queueName).toBe(serialqueue.name);
        // At this point Wf2 is stuck.

        const handles: WorkflowHandle<number>[] = [];
        const times: number[] = [];

        // Launch a number of tasks equal to three times the limit.
        // This should lead to three "waves" of the limit tasks being
        //   executed simultaneously, followed by a wait of the period,
        //   followed by the next wave.
        const numWaves = 3;

        for (let i = 0; i< qlimit * numWaves; ++i) {
            const h = await testRuntime.startWorkflow(TestWFs, undefined, undefined, rlqueue).testWorkflowTime("abc", "123");
            handles.push(h);
        }
        for (const h of handles) {
            times.push(await h.getResult());
        }

        // Verify all workflows get the SUCCESS status eventually
        const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
        await dbosExec.flushWorkflowBuffers();

        // Verify that each "wave" of tasks started at the ~same time.
        for (let wave = 0; wave < numWaves; ++wave) {
            for (let i = wave * qlimit; i < (wave + 1) * qlimit -1; ++i) {
                expect(times[i + 1] - times[i]).toBeLessThan(100);
            }
        }

        // Verify that the gap between "waves" is ~equal to the period
        for (let wave = 1; wave < numWaves; ++wave) {
            expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeGreaterThan(qperiod*1000 - 200);
            expect(times[qlimit * wave] - times[qlimit * wave - 1]).toBeLessThan(qperiod*1000 + 200);
        }

        for (const h of handles) {
            expect((await h.getStatus())!.status).toBe(StatusString.SUCCESS);
        }

        // Verify that during all this time, the second task
        //   was not launched on the concurrency-limited queue.
        // Then, finish the first task and verify the second
        //   task runs on schedule.
        await mainPromise;
        await sleepms(2000);
        expect(TestWFs2.flag).toBeFalsy();
        wfRes?.();
        await wfh1.getResult();
        await wfh2.getResult();
        expect(TestWFs2.flag).toBeTruthy();
        expect(TestWFs2.wfCounter).toBe(1);

        // Verify all queue entries eventually get cleaned up.
        expect(await queueEntriesAreCleanedUp(testRuntime as TestingRuntimeImpl)).toBe(true);
    }, 10000);

    test("test_one_at_a_time_with_crash", async() => {
        let wfqRes: () => void = () => { };
        const wfqPromise = new Promise<void>((resolve, _rj) => { wfqRes = resolve; });

        setDebugTrigger(DEBUG_TRIGGER_WORKFLOW_QUEUE_START, {
            callback: () => {
                wfqRes();
                throw new Error("Interrupt scheduler here");
            }
        });

        console.log("Starting WF inv 1");
        const wfh1 = await testRuntime.startWorkflow(TestWFs, undefined, undefined, serialqueue).testWorkflowSimple('a','b');
        console.log("Waiting for WF1 to get executed");
        await wfqPromise;
        console.log("Waiting for Runtime to get blasted away");
        await testRuntime.destroy();
        console.log("Waiting for New runtime creation");
        testRuntime = await createInternalTestRuntime(undefined, config);
        console.log("Starting WF2");
        const wfh2 = await testRuntime.startWorkflow(TestWFs, undefined, undefined, serialqueue).testWorkflowSimple('c','d');

        console.log("Waiting for WF Results");
        const wfh1b = testRuntime.retrieveWorkflow(wfh1.getWorkflowUUID());
        const wfh2b = testRuntime.retrieveWorkflow(wfh2.getWorkflowUUID());
        await wfh1b.getResult();
        await wfh2b.getResult();
        console.log("Yay, it worked");
    }, 10000);
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

    @Workflow()
    static async testWorkflowSimple(ctx: WorkflowContext, var1: string, var2: string) {
        ++TestWFs.wfCounter;
        return Promise.resolve(var1 + var2);
    }

    @Communicator()
    static async testStep(ctx: CommunicatorContext, str: string) {
        ++TestWFs.stepCounter;
        return Promise.resolve(str + 'd');
    }

    @Workflow()
    static async testWorkflowTime(_ctx: WorkflowContext, var1: string, var2: string): Promise<number> {
        expect (var1).toBe("abc");
        expect (var2).toBe("123");
        return Promise.resolve(new Date().getTime());
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