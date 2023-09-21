import { CommunicatorContext, Operon, OperonCommunicator, OperonWorkflow, WorkflowContext } from "src";
import { CONSOLE_EXPORTER } from "src/telemetry";
import { sleep } from "src/utils";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { v1 as uuidv1 } from "uuid";

describe("decorator-tests", () => {

    test("simple-communicator-decorator", async () => {
        let counter = 0;
        class TestClass {
            @OperonCommunicator()
            static async testCommunicator(commCtxt: CommunicatorContext) {
                void commCtxt;
                await sleep(1);
                return counter++;
            }

            @OperonWorkflow()
            static async testWorkflow(workflowCtxt: WorkflowContext) {
                const funcResult = await workflowCtxt.external(TestClass.testCommunicator);
                return funcResult ?? -1;
            }
        }

        const config = generateOperonTestConfig([CONSOLE_EXPORTER]);
        await setupOperonTestDb(config);
    
        const operon = new Operon(config);
        operon.useNodePostgres();
        await operon.init(TestClass);

        const workflowUUID: string = uuidv1();

        let result: number = await operon
            .workflow(TestClass.testWorkflow, { workflowUUID: workflowUUID })
            .getResult();
        expect(result).toBe(0);

        // Test OAOO. Should return the original result.
        result = await operon
            .workflow(TestClass.testWorkflow, { workflowUUID: workflowUUID })
            .getResult();
        expect(result).toBe(0);

        await operon.destroy();
    })

});
