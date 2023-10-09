import { PostgresExporter, POSTGRES_EXPORTER } from "../src/telemetry/exporters";
import { TRACE_PARENT_HEADER, TRACE_STATE_HEADER } from "@opentelemetry/core";
import { Operon, OperonConfig } from "../src/operon";
import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import { OperonTransaction, OperonWorkflow, RequiredRole } from "../src/decorators";
import request from "supertest";
import { GetApi, HandlerContext, OperonHttpServer, TransactionContext, WorkflowContext } from "../src";
import { WorkflowHandle } from "../src/workflow";
import { OperonContextImpl } from "../src/context";
import { PoolClient } from "pg";

type TelemetrySignalDbFields = {
  workflow_uuid: string;
  function_name: string;
  run_as: string;
  timestamp: bigint;
  transaction_id: string;
  severity: string;
  log_message: string;
  trace_id: string;
  trace_span: JSON;
};

type TestTransactionContext = TransactionContext<PoolClient>;

class TestClass {
  @OperonTransaction({ readOnly: false })
  static async test_function(txnCtxt: TestTransactionContext, name: string): Promise<string> {
    const { rows } = await txnCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
    const result = JSON.stringify(rows[0]);
    txnCtxt.logger.info(`transaction result: ${result}`);
    return result;
  }

  @OperonWorkflow()
  @RequiredRole(["operonAppAdmin", "operonAppUser"])
  static async test_workflow(workflowCtxt: WorkflowContext, name: string): Promise<string> {
    const funcResult = await workflowCtxt.invoke(TestClass).test_function(name);
    return funcResult;
  }

  @GetApi("/hello")
  static async hello(_ctx: HandlerContext) {
    return Promise.resolve({ message: "hello!" });
  }
}

describe("operon-telemetry", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test("Operon init works with all exporters", async () => {
    const operonConfig = generateOperonTestConfig([POSTGRES_EXPORTER]);
    const operon = new Operon(operonConfig);
    await operon.init();
    await operon.destroy();
  });

  test("collector handles errors gracefully", async () => {
    const operonConfig = generateOperonTestConfig([POSTGRES_EXPORTER]);
    const operon = new Operon(operonConfig);
    await operon.init(TestClass);

    const collector = operon.telemetryCollector.exporters[0] as PostgresExporter;
    jest.spyOn(collector, "process").mockImplementation(() => {
      throw new Error("exporter crashed");
    });
    // "mute" console.error
    jest.spyOn(console, "error").mockImplementation(() => {});

    await expect(operon.telemetryCollector.processAndExportSignals()).resolves.not.toThrow();

    await operon.destroy();
  });

  describe("Postgres exporter", () => {
    let operon: Operon;
    let operonConfig: OperonConfig;
    beforeAll(async () => {
      operonConfig = generateOperonTestConfig([POSTGRES_EXPORTER]);
      operon = new Operon(operonConfig);
      await operon.init(TestClass);
      expect(operon.telemetryCollector.exporters.length).toBe(1);
      expect(operon.telemetryCollector.exporters[0]).toBeInstanceOf(PostgresExporter);
    });

    afterAll(async () => {
      await operon.destroy();
      // This attempts to clear all our DBs, including the observability one
      await setupOperonTestDb(operonConfig);
    });

    test("signal tables are correctly created", async () => {
      const pgExporter = operon.telemetryCollector.exporters[0] as PostgresExporter;
      const pgExporterPgClient = pgExporter.pgClient;
      const stfQueryResult = await pgExporterPgClient.query(`SELECT column_name, data_type FROM information_schema.columns where table_name='signal_test_function';`);
      const expectedStfColumns = [
        {
          column_name: "timestamp",
          data_type: "bigint",
        },
        {
          column_name: "trace_span",
          data_type: "jsonb",
        },
        {
          column_name: "transaction_id",
          data_type: "text",
        },
        {
          column_name: "trace_id",
          data_type: "text",
        },
        {
          column_name: "workflow_uuid",
          data_type: "text",
        },
        {
          column_name: "name",
          data_type: "text",
        },
        {
          column_name: "function_name",
          data_type: "text",
        },
        {
          column_name: "run_as",
          data_type: "text",
        },
      ];
      expect(stfQueryResult.rows).toEqual(expect.arrayContaining(expectedStfColumns));

      const stwQueryResult = await pgExporterPgClient.query(`SELECT column_name, data_type FROM information_schema.columns where table_name='signal_test_workflow';`);
      const expectedStwColumns = [
        {
          column_name: "trace_span",
          data_type: "jsonb",
        },
        {
          column_name: "transaction_id",
          data_type: "text",
        },
        {
          column_name: "timestamp",
          data_type: "bigint",
        },
        {
          column_name: "trace_id",
          data_type: "text",
        },
        {
          column_name: "workflow_uuid",
          data_type: "text",
        },
        {
          column_name: "name",
          data_type: "text",
        },
        {
          column_name: "function_name",
          data_type: "text",
        },
        {
          column_name: "run_as",
          data_type: "text",
        },
      ];
      expect(stwQueryResult.rows).toEqual(expect.arrayContaining(expectedStwColumns));
    });

    test("correctly exports log entries with single workflow single operation", async () => {
      jest.spyOn(console, "log").mockImplementation(); // "mute" console.log
      const span = operon.tracer.startSpan("test");
      const oc = new OperonContextImpl("testName", span, operon.config.logger);
      oc.authenticatedRoles = ["operonAppAdmin"];
      oc.authenticatedUser = "operonAppAdmin";

      const params = { parentCtx: oc };
      const username = operonConfig.poolConfig.user as string;
      const workflowHandle: WorkflowHandle<string> = await operon.workflow(TestClass.test_workflow, params, username);
      const result: string = await workflowHandle.getResult();

      // Workflow should have executed correctly
      expect(JSON.parse(result)).toEqual({ current_user: username });

      // Exporter should export the log entries
      await operon.telemetryCollector.processAndExportSignals();

      const pgExporter = operon.telemetryCollector.exporters[0] as PostgresExporter;
      const pgExporterPgClient = pgExporter.pgClient;

      // Exporter should export traces
      const txnTraceQueryResult = await pgExporterPgClient.query<TelemetrySignalDbFields>(`SELECT * FROM signal_test_function WHERE trace_id IS NOT NULL`);
      expect(txnTraceQueryResult.rows).toHaveLength(1);
      const txnTraceEntry = txnTraceQueryResult.rows[0];
      expect(txnTraceEntry.trace_id.length).toBe(32);
      expect(txnTraceEntry.trace_span).not.toBe(null);

      const wfTraceQueryResult = await pgExporterPgClient.query<TelemetrySignalDbFields>(`SELECT * FROM signal_test_workflow WHERE trace_id IS NOT NULL`);
      expect(wfTraceQueryResult.rows).toHaveLength(1);
      const wfTraceEntry = wfTraceQueryResult.rows[0];
      expect(wfTraceEntry.trace_id.length).toBe(32);
      expect(wfTraceEntry.trace_span).not.toBe(null);
    });
  });

  describe("http Tracer", () => {
    let operon: Operon;
    let httpServer: OperonHttpServer;
    let config: OperonConfig;

    beforeAll(async () => {
      config = generateOperonTestConfig();
      await setupOperonTestDb(config);
    });

    beforeEach(async () => {
      operon = new Operon(config);
      await operon.init(TestClass);
      httpServer = new OperonHttpServer(operon);
    });

    afterEach(async () => {
      await operon.destroy();
    });

    test("Trace context is propagated in and out Operon", async () => {
      const headers = {
        [TRACE_PARENT_HEADER]: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        [TRACE_STATE_HEADER]: "some_state=some_value",
      };

      const response = await request(httpServer.app.callback()).get("/hello").set(headers);
      expect(response.statusCode).toBe(200);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.body.message).toBe("hello!");
      // traceId should be the same, spanId should be different (ID of the last operation's span)
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.headers.traceparent).toContain("00-4bf92f3577b34da6a3ce929d0e0e4736");
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.headers.tracestate).toBe(headers[TRACE_STATE_HEADER]);
    });

    test("New trace context is propagated out of Operon", async () => {
      const response = await request(httpServer.app.callback()).get("/hello");
      expect(response.statusCode).toBe(200);
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.body.message).toBe("hello!");
      // traceId should be the same, spanId should be different (ID of the last operation's span)
      // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
      expect(response.headers.traceparent).not.toBe(null);
    });
  });
});
