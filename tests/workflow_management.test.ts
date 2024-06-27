import {
  Workflow,
  HandlerContext,
  PostApi,
  WorkflowContext,
  GetWorkflowsOutput,
  GetWorkflowsInput,
  StatusString,
  Authentication,
  MiddlewareContext,
} from "../src";
import request from "supertest";
import { DBOSConfig } from "../src/dbos-executor";
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { WorkflowInformation, listWorkflows } from "../src/dbos-runtime/workflow_management";

describe("workflow-management-tests", () => {
  const testTableName = "dbos_test_kv";

  let testRuntime: TestingRuntime;
  let config: DBOSConfig;

  beforeAll(() => {
    config = generateDBOSTestConfig();
  });

  beforeEach(async () => {
    process.env.DBOS__APPVERSION = "v0";
    await setUpDBOSTestDb(config);
    testRuntime = await createInternalTestRuntime([TestEndpoints], config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await testRuntime.destroy();
    process.env.DBOS__APPVERSION = undefined;
  });

  test("simple-getworkflows", async () => {
    let response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input: {}});
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
  });

  test("getworkflows-with-dates", async () => {
    let response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    const input: GetWorkflowsInput = {
      startTime: new Date(Date.now() - 10000).toISOString(),
      endTime: new Date(Date.now()).toISOString(),
    }
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.endTime = new Date(Date.now() - 10000).toISOString();
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
  });

  test("getworkflows-with-status", async () => {
    let response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    await dbosExec.flushWorkflowBuffers();

    const input: GetWorkflowsInput = {
      status: StatusString.SUCCESS,
    }
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.status = StatusString.PENDING;
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
  });

  test("getworkflows-with-wfname", async () => {
    let response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    const input: GetWorkflowsInput = {
      workflowName: "testWorkflow"
    }
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
  });

  test("getworkflows-with-authentication", async () => {
    let response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    const input: GetWorkflowsInput = {
      authenticatedUser: "alice"
    }
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
  });

  test("getworkflows-with-authentication", async () => {
    let response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    const input: GetWorkflowsInput = {
      applicationVersion: "v0"
    }
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);

    input.applicationVersion = "v1"
    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(0);
  });

  test("getworkflows-with-limit", async () => {

    let response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    const input: GetWorkflowsInput = {
      limit: 10
    }

    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    let workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
    const firstUUID = workflowUUIDs.workflowUUIDs[0];

    for (let i = 0 ; i < 10; i++) {
      response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
      expect(response.statusCode).toBe(200);
      expect(response.text).toBe("alice");
    }

    response = await request(testRuntime.getHandlersCallback()).post("/getWorkflows").send({input});
    expect(response.statusCode).toBe(200);
    workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(10);
    expect(workflowUUIDs.workflowUUIDs).not.toContain(firstUUID);
  });

  test("getworkflows-cli", async () => {
    const response = await request(testRuntime.getHandlersCallback()).post("/workflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");

    const input: GetWorkflowsInput = {
      workflowName: "testWorkflow"
    }
    const infos = await listWorkflows(config, input, false);
    expect(infos.length).toBe(1);
    const info = infos[0] as WorkflowInformation;
    expect(info.authenticatedUser).toBe("alice");
    expect(info.workflowName).toBe("testWorkflow");
  });

  async function testAuthMiddleware(_ctx: MiddlewareContext) {
    return Promise.resolve({
      authenticatedUser: "alice",
      authenticatedRoles: ["aliceRole"],
    })
  }

  @Authentication(testAuthMiddleware)
  class TestEndpoints {
    @PostApi("/workflow/:name")
    @Workflow()
    static async testWorkflow(_ctxt: WorkflowContext, name: string) {
      return Promise.resolve(name);
    }

    @PostApi("/getWorkflows")
    static async getWorkflows(ctxt: HandlerContext, input: GetWorkflowsInput) {
      return await ctxt.getWorkflows(input);
    }
  }
});
