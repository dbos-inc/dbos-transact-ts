import {
  Operon,
  OperonConfig,
  OperonError,
  OperonTopicPermissionDeniedError,
  OperonWorkflowPermissionDeniedError,
  WorkflowContext,
  WorkflowConfig,
  WorkflowParams
} from "src/";
import { generateOperonTestConfig, teardownOperonTestDb } from "./helpers";

describe('authorization', () => {
  let operon: Operon;
  let config: OperonConfig;

  beforeAll(() => {
    config = generateOperonTestConfig();
  });

  beforeEach(async () => {
    operon = new Operon(config);
    await operon.init();
  });

  afterEach(async () => {
    await operon.destroy();
  });

  afterAll(async () => {
    await teardownOperonTestDb(config);
  });

  describe('workflow authorization', () => {
    const testWorkflow = async () => {
      return await new Promise<void>((resolve) => resolve());
    };

    test('permission granted', async() => {
      const testWorkflowConfig: WorkflowConfig = {
        rolesThatCanRun: ["operonAppUser"],
      }
      operon.registerWorkflow(testWorkflow, testWorkflowConfig);
      const params: WorkflowParams = {
        runAs: "operonAppUser",
      }
      await expect(operon.workflow(testWorkflow, params)).resolves.not.toThrow();
    });

    test('permission denied', async() => {
      const testWorkflowConfig: WorkflowConfig = {
        rolesThatCanRun: ["operonAppAdmin"],
      }
      operon.registerWorkflow(testWorkflow, testWorkflowConfig);

      const hasPermissionSpy = jest.spyOn(operon, 'hasPermission');
      const params: WorkflowParams = {
        runAs: "operonAppUser",
      }
      await expect(operon.workflow(testWorkflow, params)).rejects.toThrow(
        OperonWorkflowPermissionDeniedError
      );
      expect(hasPermissionSpy).toHaveBeenCalledWith(
        "operonAppUser",
        testWorkflowConfig
      );
    });

    test('default role: permission granted', async() => {
      operon.registerWorkflow(testWorkflow);
      await expect(operon.workflow(testWorkflow, {})).resolves.not.toThrow();
    });

    test('default role: permission denied', async() => {
      const testWorkflowConfig: WorkflowConfig = {
        rolesThatCanRun: ["operonAppUser"],
      }
      operon.registerWorkflow(testWorkflow, testWorkflowConfig);

      const hasPermissionSpy = jest.spyOn(operon, 'hasPermission');
      await expect(operon.workflow(testWorkflow, {})).rejects.toThrow(
        OperonWorkflowPermissionDeniedError
      );
      expect(hasPermissionSpy).toHaveBeenCalledWith(
        "defaultRole",
        testWorkflowConfig
      );
    });
  });

  describe('topic authorization', () => {
    const recvWorkflow = async (ctxt: WorkflowContext) => {
      return ctxt.recv("testTopic", "key");
    };
    const sendWorkflow = async (ctxt: WorkflowContext) => {
      return ctxt.send("testTopic", "key", "value");
    };

    test('unregistered topic: fails sending', async() => {
      operon.registerWorkflow(sendWorkflow);
      await expect(operon.workflow(sendWorkflow, {})).rejects.toThrow('unregistered topic: testTopic');
    });

    test('unregistered topic: fails receiving', async() => {
      operon.registerWorkflow(recvWorkflow);
      await expect(operon.workflow(recvWorkflow, {})).rejects.toThrow('unregistered topic: testTopic');
    });

    test('permission-less topic: succeeds sending and receiving', async() => {
      operon.registerTopic("testTopic");
      operon.registerWorkflow(sendWorkflow);
      operon.registerWorkflow(recvWorkflow);
      const recv = operon.workflow(recvWorkflow, {});
      const send = operon.workflow(sendWorkflow, {});
      await expect(send).resolves.not.toThrow();
      await expect(recv).resolves.toBe('value');
    });

    test('permission-ed topic: succeeds sending and receiving', async() => {
      operon.registerTopic("testTopic", ["operonAppUser"]);
      operon.registerWorkflow(sendWorkflow, { rolesThatCanRun: ["operonAppUser"] });
      operon.registerWorkflow(recvWorkflow, { rolesThatCanRun: ["operonAppUser"] });
      const recv = operon.workflow(recvWorkflow, { runAs: "operonAppUser" });
      const send = operon.workflow(sendWorkflow, { runAs: "operonAppUser" });
      await expect(send).resolves.not.toThrow();
      await expect(recv).resolves.toBe('value');
    });

    test('unauthorized receiver: fails receiving', async() => {
      operon.registerTopic("testTopic", ["operonAppUser"]);
      operon.registerWorkflow(recvWorkflow);
      await expect(operon.workflow(recvWorkflow, {})).rejects.toThrow(OperonTopicPermissionDeniedError);
    });

    test('unauthorized sender: fails sending', async() => {
      operon.registerTopic("testTopic", ["operonAppUser"]);
      operon.registerWorkflow(sendWorkflow);
      await expect(operon.workflow(sendWorkflow, {})).rejects.toThrow(OperonTopicPermissionDeniedError);
    });
  });
});
