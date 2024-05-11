import { TRACE_PARENT_HEADER, TRACE_STATE_HEADER } from "@opentelemetry/core";
import { DBOSExecutor, DBOSConfig } from "../src/dbos-executor";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";
import { Transaction, Workflow, RequiredRole } from "../src/decorators";
import request from "supertest";
import { GetApi, HandlerContext, TestingRuntime, TransactionContext, WorkflowContext } from "../src";
import { PoolClient } from "pg";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";

type TestTransactionContext = TransactionContext<PoolClient>;

class TestClass {
  @Transaction({ readOnly: false })
  static async test_function(txnCtxt: TestTransactionContext, name: string): Promise<string> {
    const { rows } = await txnCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
    const result = JSON.stringify(rows[0]);
    txnCtxt.logger.info(`transaction result: ${result}`);
    return result;
  }

  @Workflow()
  @RequiredRole(["dbosAppAdmin", "dbosAppUser"])
  static async test_workflow(workflowCtxt: WorkflowContext, name: string): Promise<string> {
    const funcResult = await workflowCtxt.invoke(TestClass).test_function(name);
    return funcResult;
  }

  @GetApi("/hello")
  static async hello(_ctx: HandlerContext) {
    return Promise.resolve({ message: "hello!" });
  }
}

describe("dbos-telemetry", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test("DBOS init works with exporters", async () => {
    const dbosConfig = generateDBOSTestConfig();
    expect(dbosConfig.telemetry).not.toBeUndefined();
    if (dbosConfig.telemetry) {
      dbosConfig.telemetry.OTLPExporter = {
        tracesEndpoint: "http://localhost:4317/v1/traces",
        logsEndpoint: "http://localhost:4317/v1/logs",
      };
    }
    await setUpDBOSTestDb(dbosConfig);
    const dbosExec = new DBOSExecutor(dbosConfig);
    expect(dbosExec.telemetryCollector).not.toBeUndefined();
    expect(dbosExec.telemetryCollector.exporter).not.toBeUndefined();
    await dbosExec.init();
    await dbosExec.destroy();
  });

  // TODO write a test intercepting OTLP over HTTP requests and test span/logs payloads

  describe("http Tracer", () => {
    let config: DBOSConfig;
    let testRuntime: TestingRuntime;

    beforeAll(async () => {
      config = generateDBOSTestConfig();
      await setUpDBOSTestDb(config);
    });

    beforeEach(async () => {
      testRuntime = await createInternalTestRuntime([TestClass], config);
    });

    afterEach(async () => {
      await testRuntime.destroy();
    });

    test("Trace context is propagated in and out of workflow execution", async () => {
      const headers = {
        [TRACE_PARENT_HEADER]: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        [TRACE_STATE_HEADER]: "some_state=some_value",
      };

      const response = await request(testRuntime.getHandlersCallback()).get("/hello").set(headers);
      expect(response.statusCode).toBe(200);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.body.message).toBe("hello!");
      // traceId should be the same, spanId should be different (ID of the last operation's span)
      expect(response.headers.traceparent).toContain("00-4bf92f3577b34da6a3ce929d0e0e4736");
      expect(response.headers.tracestate).toBe(headers[TRACE_STATE_HEADER]);
    });

    test("New trace context is propagated out of workflow", async () => {
      const response = await request(testRuntime.getHandlersCallback()).get("/hello");
      expect(response.statusCode).toBe(200);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.body.message).toBe("hello!");
      // traceId should be the same, spanId should be different (ID of the last operation's span)
      expect(response.headers.traceparent).not.toBe(null);
    });
  });
});
