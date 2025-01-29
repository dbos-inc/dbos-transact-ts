import { DBOS } from '../../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from '../helpers';
import { expect, jest, test } from '@jest/globals';

export class TestApp {

    static bgTaskValue: number = 0;

    @DBOS.workflow()
    static async backgroundTask(n: number): Promise<void> {
        TestApp.bgTaskValue = n;
        for (let i = 1; i <= n; i++) {
            await TestApp.backgroundTaskStep(i);
        }
    }

    static stepCount: number = 0;
    @DBOS.step()
    static async backgroundTaskStep(step: number): Promise<void> {
        await new Promise((resolve) => setTimeout(resolve, 100));
        TestApp.stepCount += step;
    }
}

describe("dbos-debug-v2-library", () => {

    it("should run a background task", async () => {
        const wfUUID = `wf-${Date.now()}`;

        const config = generateDBOSTestConfig(); // Optional.  If you don't, it'll open the YAML file...
        await setUpDBOSTestDb(config);
        DBOS.setConfig(config);

        await DBOS.launch();
        await DBOS.withNextWorkflowID(wfUUID, async () => {
            TestApp.bgTaskValue = 0;
            TestApp.stepCount = 0;
            await TestApp.backgroundTask(10);
            expect(TestApp.bgTaskValue).toBe(10);
            expect(TestApp.stepCount).toBe(55);
        });
        await DBOS.shutdown();

        process.env.DBOS_DEBUG_WORKFLOW_ID = wfUUID;
        const mockExit = jest.spyOn(process, 'exit')
            .mockImplementation((() => {}) as any);
        try {
            TestApp.bgTaskValue = 0;
            TestApp.stepCount = 0;
            await DBOS.launch();
            expect(TestApp.bgTaskValue).toBe(10);
            expect(TestApp.stepCount).toBe(0);
            expect(mockExit).toHaveBeenCalledWith(0);
        } finally {
            mockExit.mockRestore();
            delete process.env.DBOS_DEBUG_WORKFLOW_ID;
        }
    }, 30000);
});