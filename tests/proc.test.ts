import { StoredProcedure, StoredProcedureContext, TestingRuntime, Workflow, WorkflowContext, WorkflowHandle } from "../src";
import { DBOSConfig } from "../src/dbos-executor";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";

class ProcTest {
    @StoredProcedure()
    static async testProc(ctx: StoredProcedureContext, name: string): Promise<string> {
        const { rows } = await ctx.query<TestKvTable>(`INSERT INTO dbos_test_kv(id, value) VALUES (1, $1) RETURNING id`, [name]);
        return `hello ${rows[0].id}`;
    }

    @Workflow()
    static async testWorkflow(ctx: WorkflowContext, name: string): Promise<string> {
        return ctx.invoke(ProcTest).testProc(name);
    }
}

describe("stored-proc-tests", () => {
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
        await testRuntime.queryUserDB(create);
    });

    afterEach(async () => {
        await testRuntime.queryUserDB(drop);
        await testRuntime.destroy();
    });

    test("simple-function", async () => {
        const workflowHandle: WorkflowHandle<string> = await testRuntime.startWorkflow(ProcTest).testWorkflow(username);
        const workflowResult: string = await workflowHandle.getResult();
        expect(workflowResult).toEqual("hello 1");
      });
})

const create = `
CREATE OR REPLACE PROCEDURE "ProcTest_testProc_p"(
    buffered_results JSONB,
    _workflow_uuid TEXT, 
    _function_id INT, 
    preset BOOLEAN, 
    _context JSONB,
    OUT return_value JSONB,
    "name" TEXT
)
LANGUAGE plpgsql
as $$
DECLARE
    _output JSONB;
    _error JSONB;
    _snapshot TEXT;
    _txn_id TEXT;
BEGIN

    -- fake output for the test
    SELECT jsonb_build_object('output', 'hello 1', 'txn_snapshot', '1234', 'txn_id', '1234:1234:') INTO return_value;

END; $$;
`

const drop = `DROP ROUTINE IF EXISTS "ProcTest_testProc_p";`