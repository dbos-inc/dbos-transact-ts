import {
  Operon,
  OperonConfig,
  OperonWorkflowPermissionDeniedError,
  WorkflowConfig,
  WorkflowParams,
} from "src/";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";

describe("authorization", () => {
  let operon: Operon;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    operon.useNodePostgres();
    await operon.init();
  });

  afterEach(async () => {
    await operon.destroy();
  });

  describe("workflow authorization", () => {
    const testWorkflow = async () => {
      return await new Promise<void>((resolve) => resolve());
    };

    test("permission granted", async () => {
      const testWorkflowConfig: WorkflowConfig = {
        rolesThatCanRun: ["operonAppUser"],
      };
      operon.registerWorkflow(testWorkflow, testWorkflowConfig);
      const params: WorkflowParams = {
        runAs: "operonAppUser",
      };
      await expect(
        operon.workflow(testWorkflow, params).getResult()
      ).resolves.not.toThrow();
    });

    test("permission denied", async () => {
      const testWorkflowConfig: WorkflowConfig = {
        rolesThatCanRun: ["operonAppAdmin"],
      };
      operon.registerWorkflow(testWorkflow, testWorkflowConfig);

      const hasPermissionSpy = jest.spyOn(operon, "hasPermission");
      const params: WorkflowParams = {
        runAs: "operonAppUser",
      };
      await expect(
        operon.workflow(testWorkflow, params).getResult()
      ).rejects.toThrow(OperonWorkflowPermissionDeniedError);
      expect(hasPermissionSpy).toHaveBeenCalledWith(
        "operonAppUser",
        testWorkflowConfig
      );
    });

    test("default role: permission granted", async () => {
      operon.registerWorkflow(testWorkflow);
      await expect(
        operon.workflow(testWorkflow, {}).getResult()
      ).resolves.not.toThrow();
    });

    test("default role: permission denied", async () => {
      const testWorkflowConfig: WorkflowConfig = {
        rolesThatCanRun: ["operonAppUser"],
      };
      operon.registerWorkflow(testWorkflow, testWorkflowConfig);

      const hasPermissionSpy = jest.spyOn(operon, "hasPermission");
      await expect(
        operon.workflow(testWorkflow, {}).getResult()
      ).rejects.toThrow(OperonWorkflowPermissionDeniedError);
      expect(hasPermissionSpy).toHaveBeenCalledWith(
        "defaultRole",
        testWorkflowConfig
      );
    });
  });
});
