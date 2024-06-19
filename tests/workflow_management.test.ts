/* eslint-disable @typescript-eslint/no-unsafe-member-access */
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

describe("workflow-management-tests", () => {
  const testTableName = "dbos_test_kv";

  let testRuntime: TestingRuntime;
  let config: DBOSConfig;

  beforeAll(() => {
    config = generateDBOSTestConfig();
  });

  beforeEach(async () => {
    await setUpDBOSTestDb(config);
    testRuntime = await createInternalTestRuntime([TestEndpoints], config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`);
  });

  afterEach(async () => {
    await testRuntime.destroy();
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
