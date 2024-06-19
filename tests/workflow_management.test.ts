/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import {
  Workflow,
  GetApi,
  HandlerContext,
  PostApi,
  WorkflowContext,
} from "../src";
import request from "supertest";
import { DBOSConfig } from "../src/dbos-executor";
import { TestingRuntime, createInternalTestRuntime } from "../src/testing/testing_runtime";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { GetWorkflowsOutput } from "../src/workflow";

describe("workflow-management-tests", () => {
  const testTableName = "dbos_test_kv";

  let testRuntime: TestingRuntime;
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
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

    response = await request(testRuntime.getHandlersCallback()).get("/getWorkflows");
    expect(response.statusCode).toBe(200);
    const workflowUUIDs = JSON.parse(response.text) as GetWorkflowsOutput;
    expect(workflowUUIDs.workflowUUIDs.length).toBe(1);
  });


  class TestEndpoints {
    @PostApi("/workflow/:name")
    @Workflow()
    static async testWorkflow(_ctxt: WorkflowContext, name: string) {
      return Promise.resolve(name);
    }

    @GetApi("/getWorkflows")
    static async getWorkflows(ctxt: HandlerContext) {
      return await ctxt.getWorkflows({});
    }
  }
});
