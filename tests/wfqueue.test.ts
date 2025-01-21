import { StatusString, WorkflowHandle, DBOS, ConfiguredInstance } from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { WorkflowQueue } from "../src";
import { v4 as uuidv4 } from "uuid";
import { sleepms } from "../src/utils";

import { WF } from './wfqtestprocess';

import { execFile } from 'child_process';
import { promisify } from 'util';

const execFileAsync = promisify(execFile);

import {
    clearDebugTriggers,
    DEBUG_TRIGGER_WORKFLOW_QUEUE_START,
    // DEBUG_TRIGGER_WORKFLOW_ENQUEUE,
    setDebugTrigger
} from "../src/debugpoint";
import { DBOSConflictingWorkflowError } from "../src/error";


const queue = new WorkflowQueue("testQ");
const serialqueue = new WorkflowQueue("serialQ", 1);
const serialqueueLimited = new WorkflowQueue("serialQL", 1, {limitPerPeriod: 10, periodSec: 1});
const childqueue = new WorkflowQueue("childQ", 3);

const qlimit = 5;
const qperiod = 2
const rlqueue = new WorkflowQueue("limited_queue", undefined, {limitPerPeriod: qlimit, periodSec: qperiod});

async function queueEntriesAreCleanedUp() {
    let maxTries = 10;
    let success = false;
    while (maxTries > 0) {
        const r = await DBOS.getWorkflowQueue({});
        if (r.workflows.length === 0) {
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
  
    beforeAll(async () => {
        config = generateDBOSTestConfig();
        await setUpDBOSTestDb(config);
        DBOS.setConfig(config);
    });

    beforeEach(async () => {
        TestWFs.reset();
        TestWFs2.reset();
        await DBOS.launch();
    });

    afterEach(async () => {
        await DBOS.shutdown();
    }, 10000);
  
    test("simple-queue", async () => {
        const wfid = uuidv4();
        TestWFs.wfid = wfid;

        const wfh = await DBOS.startWorkflow(TestWFs, {workflowID: wfid, queueName: queue.name}).testWorkflow('abc', '123');
        expect(await wfh.getResult()).toBe('abcd123');
        expect((await wfh.getStatus())?.queueName).toBe('testQ');

        await DBOS.withNextWorkflowID(wfid, async () => {
            expect(await TestWFs.testWorkflow('abc', '123')).toBe('abcd123');
        });
        expect(TestWFs.wfCounter).toBe(1);
        expect(TestWFs.stepCounter).toBe(1);

        expect((await wfh.getStatus())?.queueName).toBe('testQ');
    });

    test("one-at-a-time", async() => {
        await runOneAtATime(serialqueue);
    }, 10000);

    test("child-wfs-queue", async() => {
        expect (await TestChildWFs.testWorkflow('a','b')).toBe('adbdadbd');
    }, 10000);

    test("test_one_at_a_time_with_limiter", async() => {
        await runOneAtATime(serialqueueLimited);
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
            const h = await DBOS.startWorkflow(TestWFs, {queueName: rlqueue.name}).testWorkflowTime("abc", "123");
            handles.push(h);
        }
        for (const h of handles) {
            times.push(await h.getResult());
        }

        // Verify all queue entries eventually get cleaned up.
        expect(await queueEntriesAreCleanedUp()).toBe(true);

        // Verify all workflows get the SUCCESS status eventually
        await DBOS.executor.flushWorkflowBuffers();

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

        const wfh1 = await DBOS.startWorkflow(TestWFs2, {queueName: serialqueue.name}).workflowOne();
        expect((await wfh1.getStatus())?.queueName).toBe(serialqueue.name);
        const wfh2 = await DBOS.startWorkflow(TestWFs2, {queueName: serialqueue.name}).workflowTwo();
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
            const h = await DBOS.startWorkflow(TestWFs, {queueName: rlqueue.name}).testWorkflowTime("abc", "123");
            handles.push(h);
        }
        for (const h of handles) {
            times.push(await h.getResult());
        }

        // Verify all workflows get the SUCCESS status eventually
        await DBOS.executor.flushWorkflowBuffers();

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
        expect(await queueEntriesAreCleanedUp()).toBe(true);
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

        const wfh1 = await DBOS.startWorkflow(TestWFs, {queueName: serialqueue.name}).testWorkflowSimple('a','b');
        await wfqPromise;

        await DBOS.shutdown();
        clearDebugTriggers();
        await DBOS.launch();

        const wfh2 = await DBOS.startWorkflow(TestWFs, {queueName: serialqueue.name}).testWorkflowSimple('c','d');

        const wfh1b = DBOS.retrieveWorkflow(wfh1.workflowID);
        const wfh2b = DBOS.retrieveWorkflow(wfh2.workflowID);
        expect (await wfh1b.getResult()).toBe('ab');
        expect (await wfh2b.getResult()).toBe('cd');
    }, 10000);

    /*
    // Current result: WF1 does get created in system DB, but never starts running.
    //  WF2 does run.
    test("test_one_at_a_time_with_crash2", async() => {
        let wfqRes: () => void = () => { };
        const _wfqPromise = new Promise<void>((resolve, _rj) => { wfqRes = resolve; });

        setDebugTrigger(DEBUG_TRIGGER_WORKFLOW_ENQUEUE, {
            callback: () => {
                wfqRes();
                throw new Error("Interrupt start workflow here");
            }
        });

        const wfid1 = 'thisworkflowgetshit';
        console.log("Start WF1");
        try {
            const _wfh1 = await testRuntime.startWorkflow(TestWFs, {workflowID: wfid1, queueName: serialqueue.name}).testWorkflowSimple('a','b');
        }
        catch(e) {
            // Expected
            const err = e as Error;
            expect(err.message.includes('Interrupt')).toBeTruthy();
            console.log("Expected error caught");
        }
        console.log("Destroy runtime");
        await testRuntime.destroy();
        clearDebugTriggers();
        console.log("New runtime");
        testRuntime = await createInternalTestRuntime(undefined, config);
        console.log("Start WF2");
        const wfh2 = await DBOS.startWorkflow(TestWFs, {queueName: serialqueue.name}).testWorkflowSimple('c','d');

        const wfh1b = testRuntime.retrieveWorkflow(wfid1);
        const wfh2b = testRuntime.retrieveWorkflow(wfh2.workflowID);
        console.log("Wait");
        expect (await wfh2b.getResult()).toBe('cd');
        // Current behavior (undesired) WF1 got created but will stay ENQUEUED and not get run.
        expect((await wfh1b.getStatus())?.status).toBe('SUCCESS');
        expect (await wfh1b.getResult()).toBe('ab'); 
    }, 10000);
    */

    test("queue workflow in recovered workflow", async() => {
        expect(WF.x).toBe(5);
        console.log('shutdown');
        await DBOS.shutdown(); // DO not want to take queued jobs from here

        console.log('run side process');
        // We crash a workflow on purpose; this has queued some things up and awaited them...
        const { stdout, stderr } = await execFileAsync('npx', ['ts-node', './tests/wfqtestprocess.ts'], {
            cwd: process.cwd(),
            env: {
                ...process.env,
                'DIE_ON_PURPOSE': 'true',
            }
        });
    
        expect(stderr).toBeDefined();
        expect(stdout).toBeDefined();

        console.log('start again');
        await DBOS.launch();
        const wfh = DBOS.retrieveWorkflow('testqueuedwfcrash');
        expect((await wfh.getStatus())?.status).toBe('PENDING');

        // It should proceed.  And should not take too long, either...
        //  We could also recover the workflow
        console.log("Waiting for recovered WF to complete...");
        expect(await wfh.getResult()).toBe(5);

        expect((await wfh.getStatus())?.status).toBe('SUCCESS');
        expect(await queueEntriesAreCleanedUp()).toBe(true);
    }, 60000);

    class TestDuplicateID {
        @DBOS.workflow()
        static async testWorkflow(var1: string) {
            await DBOS.sleepms(10);
            return var1;
        }

        @DBOS.workflow()
        static async testDupWorkflow() {
            await DBOS.sleepms(10);
            return;
        }
    }

    class TestDuplicateIDdup {
        @DBOS.workflow()
        static async testWorkflow(var1: string) {
            await DBOS.sleepms(10);
            return var1;
        }
    }

    class TestDuplicateIDins extends ConfiguredInstance {
        constructor(name: string) {
            super(name);
        }

        async initialize() {
            return Promise.resolve();
        }

        @DBOS.workflow()
        async testWorkflow(var1: string) {
            await DBOS.sleepms(10);
            return var1;
        }
    }

    test("duplicate-workflow-id", async() => {
        const wfid = uuidv4();
        const handle1 = await DBOS.startWorkflow(TestDuplicateID, {workflowID: wfid}).testWorkflow('abc');
        // Call with a different function name within the same class is not allowed.
        await expect(DBOS.startWorkflow(TestDuplicateID, {workflowID: wfid}).testDupWorkflow()).rejects.toThrow(DBOSConflictingWorkflowError);
        // Call the same function name in a different class is not allowed.
        await expect(DBOS.startWorkflow(TestDuplicateIDdup, {workflowID: wfid}).testWorkflow('abc')).rejects.toThrow(DBOSConflictingWorkflowError);
        await expect(handle1.getResult()).resolves.toBe('abc');

        // Calling itself again should be fine
        const handle2 = await DBOS.startWorkflow(TestDuplicateID, {workflowID: wfid}).testWorkflow('abc');
        await expect(handle2.getResult()).resolves.toBe('abc');

        // Call the same function in a different configured class is not allowed.
        const myObj = DBOS.configureInstance(TestDuplicateIDins, 'myname');
        await expect(DBOS.startWorkflow(myObj, {workflowID: wfid}).testWorkflow('abc')).rejects.toThrow(DBOSConflictingWorkflowError);

        // Call the same function in a different queue would generate a warning, but is allowed.
        const handleQ = await DBOS.startWorkflow(TestDuplicateID, {workflowID: wfid, queueName: queue.name}).testWorkflow('abc');
        await expect(handleQ.getResult()).resolves.toBe('abc');

        // Call with a different input would generate a warning, but still use the recorded input.
        const handle3 = await DBOS.startWorkflow(TestDuplicateID, {workflowID: wfid}).testWorkflow('def');
        await expect(handle3.getResult()).resolves.toBe('abc');
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

    @DBOS.workflow()
    static async testWorkflow(var1: string, var2: string) {
        expect(DBOS.workflowID).toBe(TestWFs.wfid);
        ++TestWFs.wfCounter;
        var1 = await TestWFs.testStep(var1);
        return Promise.resolve(var1 + var2);
    }

    @DBOS.workflow()
    static async testWorkflowSimple(var1: string, var2: string) {
        ++TestWFs.wfCounter;
        return Promise.resolve(var1 + var2);
    }

    @DBOS.step()
    static async testStep(str: string) {
        ++TestWFs.stepCounter;
        return Promise.resolve(str + 'd');
    }

    @DBOS.workflow()
    static async testWorkflowTime(var1: string, var2: string): Promise<number> {
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

    @DBOS.workflow()
    static async workflowOne() {
        ++TestWFs2.wfCounter;
        TestWFs2.mainResolve?.();
        await TestWFs2.wfPromise;
        return Promise.resolve();
    }

    @DBOS.workflow()
    static async workflowTwo() {
        TestWFs2.flag = true; // Tell if this ran yet
        return Promise.resolve();
    }
}

class TestChildWFs
{
    @DBOS.workflow()
    static async testWorkflow(var1: string, var2: string) {
        const wfh1 = await DBOS.startWorkflow(TestChildWFs, {queueName: childqueue.name}).testChildWF(var1);
        const wfh2 = await DBOS.startWorkflow(TestChildWFs, {queueName: childqueue.name}).testChildWF(var2);
        const wfh3 = await DBOS.startWorkflow(TestChildWFs, {queueName: childqueue.name}).testChildWF(var1);
        const wfh4 = await DBOS.startWorkflow(TestChildWFs, {queueName: childqueue.name}).testChildWF(var2);

        await DBOS.sleepms(1000);
        expect((await wfh4.getStatus())?.status).toBe(StatusString.ENQUEUED);

        await DBOS.send(wfh1.workflowID, 'go', 'release');
        await DBOS.send(wfh2.workflowID, 'go', 'release');
        await DBOS.send(wfh3.workflowID, 'go', 'release');
        await DBOS.send(wfh4.workflowID, 'go', 'release');

        return (await wfh1.getResult() + await wfh2.getResult() +
           await wfh3.getResult() + await wfh4.getResult())
    }

    @DBOS.workflow()
    static async testChildWF(str: string) {
        await DBOS.recv('release', 30);
        return Promise.resolve(str + 'd');
    }
}

async function runOneAtATime(queue: WorkflowQueue) {
    let wfRes: () => void = () => { };
    TestWFs2.wfPromise = new Promise<void>((resolve, _rj) => { wfRes = resolve; });
    const mainPromise = new Promise<void>((resolve, _rj) => { TestWFs2.mainResolve = resolve; });
    const wfh1 = await DBOS.startWorkflow(TestWFs2, {queueName: queue.name}).workflowOne();
    expect((await wfh1.getStatus())?.queueName).toBe(queue.name);
    const wfh2 = await DBOS.startWorkflow(TestWFs2, {queueName: queue.name}).workflowTwo();
    expect((await wfh2.getStatus())?.queueName).toBe(queue.name);
    await mainPromise;
    await sleepms(2000);
    expect(TestWFs2.flag).toBeFalsy();
    wfRes?.();
    await wfh1.getResult();
    await wfh2.getResult();
    expect(TestWFs2.flag).toBeTruthy();
    expect(TestWFs2.wfCounter).toBe(1);
    expect(await queueEntriesAreCleanedUp()).toBe(true);
}
