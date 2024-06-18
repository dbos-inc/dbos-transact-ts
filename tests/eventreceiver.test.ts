import {
    DBOSContext,
    DBOSEventReceiver,
    DBOSExecutorEventReceiverInterface,
    TestingRuntime,
    Workflow,
    WorkflowContext,
    WorkflowFunction,
    associateClassWithEventReceiver,
    associateMethodWithEventReceiver,
} from "../src"
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig } from "./helpers";

export interface ERDefaults {
    classval?: string;
}

export interface ERSpecifics {
    methodval?: string;
}

// Listener class
const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

class ERD implements DBOSEventReceiver
{
    executor?: DBOSExecutorEventReceiverInterface;

    async deliver3Events() {
        for (let i=1; i<=3; ++i) {
            await sleepms(100);
            const mtds = this.executor!.getRegistrationsFor(this);
            for (const mtd of mtds) {
                const cs = (mtd.cinfo as ERDefaults).classval ?? "";
                const ms = (mtd.minfo as ERSpecifics).methodval ?? "";
                await this.executor!.workflow(mtd.method.registeredFunction as unknown as WorkflowFunction<[string, string, number], unknown>, {}, cs, ms, i);
            }
        }
    }

    async destroy() {}
    async initialize(executor: DBOSExecutorEventReceiverInterface) {
        this.executor = executor;
        const _dropPromise = this.deliver3Events();
        return Promise.resolve();
    }
    logRegisteredEndpoints() {}
}

const erd = new ERD();

// Decorators - class
export function EventReceiverConfigure(cfg: string) {
    function clsdec<T extends { new(...args: unknown[]): object }>(ctor: T) {
        const erInfo = associateClassWithEventReceiver(erd, ctor) as ERDefaults;
        erInfo.classval = cfg;
    }
    return clsdec;
}

// Decorators - method  
export function EventConsumer(config?: string) {
    function mtddec<This, Ctx extends DBOSContext, Return>(
        target: object,
        propertyKey: string,
        inDescriptor: TypedPropertyDescriptor<(this: This, ctx: Ctx, ...args: [string, string, number]) => Promise<Return>>
    ) {
        const {descriptor, receiverInfo} = associateMethodWithEventReceiver(erd, target, propertyKey, inDescriptor);
  
        const mRegistration = receiverInfo as ERSpecifics;
        mRegistration.methodval = config;
  
        return descriptor;
    }
    return mtddec;
}

@EventReceiverConfigure("myclass")
class MyEventReceiver {
    static callNumSum = 0;

    @EventConsumer("method1")
    @Workflow()
    static async method1(_ctx: WorkflowContext, cv: string, mv: string, en: number) {
        if (cv !== 'myclass' || mv !== 'method1') throw new Error("Info missing!");
        MyEventReceiver.callNumSum += en;
        return Promise.resolve();
    }

    @EventConsumer("method2")
    @Workflow()
    static async method2(_ctx: WorkflowContext, cv: string, mv: string, en: number) {
        if (cv !== 'myclass' || mv !== 'method2') throw new Error("Info missing!");
        MyEventReceiver.callNumSum += 10*en;
        return Promise.resolve();
    }
}

describe("event-receiver-tests", () => {
    let testRuntime: TestingRuntime;
  
    beforeAll(async () => {});
  
    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime(undefined, generateDBOSTestConfig());
    }, 30000);
  
    afterEach(async () => {
        await testRuntime.destroy();
    }, 30000);
  
    test("wf-event", async () => {
        // Things will naturally start happening
        //   We simply wait until we get all the calls, 10 seconds tops
        for (let i=0; i<100; ++i) {
            if (MyEventReceiver.callNumSum === 66) break;
            await sleepms(100);
        }
        expect(MyEventReceiver.callNumSum).toBe(66);
    });
});
  
