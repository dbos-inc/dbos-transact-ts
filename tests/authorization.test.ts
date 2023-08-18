import {
  Operon,
  OperonConfig,
  OperonTopicPermissionDeniedError,
  OperonWorkflowPermissionDeniedError,
  WorkflowContext,
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

  describe("topic authorization", () => {
    const recvWorkflow = async (ctxt: WorkflowContext) => {
      return ctxt.recv("testTopic", "key");
    };
    const sendWorkflow = async (ctxt: WorkflowContext) => {
      return ctxt.send("testTopic", "key", "value");
    };

    test("unregistered topic: fails sending", async () => {
      operon.registerWorkflow(sendWorkflow);
      await expect(
        operon.workflow(sendWorkflow, {}).getResult()
      ).rejects.toThrow("unregistered topic: testTopic");
    });

    test("unregistered topic: fails receiving", async () => {
      operon.registerWorkflow(recvWorkflow);
      await expect(
        operon.workflow(recvWorkflow, {}).getResult()
      ).rejects.toThrow("unregistered topic: testTopic");
    });

    test("permission-less topic: succeeds sending and receiving", async () => {
      operon.registerTopic("testTopic");
      operon.registerWorkflow(sendWorkflow);
      operon.registerWorkflow(recvWorkflow);
      const recv = operon.workflow(recvWorkflow, {}).getResult();
      const send = operon.workflow(sendWorkflow, {}).getResult();
      await expect(send).resolves.not.toThrow();
      await expect(recv).resolves.toBe("value");
    });

    test("permission-ed topic: succeeds sending and receiving", async () => {
      operon.registerTopic("testTopic", ["operonAppUser"]);
      operon.registerWorkflow(sendWorkflow, {
        rolesThatCanRun: ["operonAppUser"],
      });
      operon.registerWorkflow(recvWorkflow, {
        rolesThatCanRun: ["operonAppUser"],
      });
      const recv = operon
        .workflow(recvWorkflow, { runAs: "operonAppUser" })
        .getResult();
      const send = operon
        .workflow(sendWorkflow, { runAs: "operonAppUser" })
        .getResult();
      await expect(send).resolves.not.toThrow();
      await expect(recv).resolves.toBe("value");
    });

    test("unauthorized receiver: fails receiving", async () => {
      operon.registerTopic("testTopic", ["operonAppUser"]);
      operon.registerWorkflow(recvWorkflow);
      await expect(
        operon.workflow(recvWorkflow, {}).getResult()
      ).rejects.toThrow(OperonTopicPermissionDeniedError);
    });

    test("unauthorized sender: fails sending", async () => {
      operon.registerTopic("testTopic", ["operonAppUser"]);
      operon.registerWorkflow(sendWorkflow);
      await expect(
        operon.workflow(sendWorkflow, {}).getResult()
      ).rejects.toThrow(OperonTopicPermissionDeniedError);
    });
  });
});
