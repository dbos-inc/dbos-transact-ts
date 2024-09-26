import { Communicator, CommunicatorContext, StatusString, TestingRuntime, Workflow, WorkflowContext } from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { WorkflowQueue } from "../src";
import { v4 as uuidv4 } from "uuid";
import { sleepms } from "../src/utils";


const queue = new WorkflowQueue("testQ");
const serialqueue = new WorkflowQueue("serialQ", 1);
const childqueue = new WorkflowQueue("childQ", 3);

describe("scheduled-wf-tests-simple", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {
        TestWFs.reset();
        TestWFs2.reset();
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
        expect((await wfh.getStatus())?.queueName).toBe('testQ');

        expect(await testRuntime.invokeWorkflow(TestWFs, wfid).testWorkflow('abc', '123')).toBe('abcd123');
        expect(TestWFs.wfCounter).toBe(2);
        expect(TestWFs.stepCounter).toBe(1);

        expect((await wfh.getStatus())?.queueName).toBe('testQ');
    });

    test("one-at-a-time", async() => {
        let wfRes: ()=>void = ()=>{};
        TestWFs2.wfPromise = new Promise<void>((resolve, _rj) => {wfRes = resolve});
        const mainPromise  = new Promise<void>((resolve, _rj) => {TestWFs2.mainResolve = resolve;});
        const wfh1 = await testRuntime.startWorkflow(TestWFs2, undefined, undefined, serialqueue).workflowOne();
        expect((await wfh1.getStatus())?.queueName).toBe('serialQ');
        const wfh2 = await testRuntime.startWorkflow(TestWFs2, undefined, undefined, serialqueue).workflowTwo();
        expect((await wfh2.getStatus())?.queueName).toBe('serialQ');
        await mainPromise;
        await sleepms(2000);
        expect(TestWFs2.flag).toBeFalsy();
        wfRes?.();
        await wfh1.getResult();
        await wfh2.getResult();
        expect(TestWFs2.flag).toBeTruthy();
        expect(TestWFs2.wfCounter).toBe(1);
    }, 10000);

    test("child-wfs-queue", async() => {
        expect (await testRuntime.invokeWorkflow(TestChildWFs).testWorkflow('a','b')).toBe('adbdadbd');
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
