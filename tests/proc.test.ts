import { StoredProcedure, StoredProcedureContext, TestingRuntime, Workflow, WorkflowContext, WorkflowHandle } from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";

class ProcTest {
    @StoredProcedure()
    static async testProc(ctx: StoredProcedureContext, name: string): Promise<string> {
        await Promise.resolve();
        return 'Hello, ' + name;
    }

    @Workflow()
    static async testWorkflow(ctx: WorkflowContext, name: string): Promise<string> {
        return ctx.invoke(ProcTest).testProc(name);
    }
}

describe("dbos-tests", () => {
    let username: string;
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;

    beforeAll(async () => {
        config = generateDBOSTestConfig();
        username = config.poolConfig.user || "postgres";
        await setUpDBOSTestDb(config);
    });

    beforeEach(async () => {
        testRuntime = await createInternalTestRuntime([ProcTest], config);
    });

    afterEach(async () => {
        await testRuntime.destroy();
    });

    test("simple-function", async () => {
        const workflowHandle: WorkflowHandle<string> = await testRuntime.startWorkflow(ProcTest).testWorkflow(username);
        const workflowResult: string = await workflowHandle.getResult();
        expect(JSON.parse(workflowResult)).toEqual({ current_user: username });
      });
    
})