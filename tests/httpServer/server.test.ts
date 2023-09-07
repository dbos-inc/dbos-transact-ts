/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
  OperonConfig,
  OperonContext,
  OperonTransaction,
  OperonWorkflow,
  PostApi,
  TransactionContext,
  WorkflowContext,
} from "src";
import { OperonHttpServer } from "src/httpServer/server";
import {
  TestKvTable,
  generateOperonTestConfig,
  setupOperonTestDb,
} from "tests/helpers";
import request from "supertest";
import { Response } from "express";

describe("httpserver-tests", () => {
  const testTableName = "operon_test_kv";

  let operon: Operon;
  let httpServer: OperonHttpServer;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    operon.useNodePostgres();
    operon.registerDecoratedWT();
    await operon.init();
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`
    );
    httpServer = new OperonHttpServer(operon);
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("get-hello", async () => {
    const response = await request(httpServer.app).get("/hello");
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe("hello!");
  });

  test("get-url", async () => {
    const response = await request(httpServer.app).get("/hello/qian");
    expect(response.statusCode).toBe(301);
    expect(response.text).toBe("wow qian");
  });

  test("get-query", async () => {
    const response = await request(httpServer.app).get("/query?name=qian");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello qian");
  });

  test("post-test", async () => {
    const response = await request(httpServer.app)
      .post("/testpost")
      .send({ name: "qian" });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello qian");
  });

  test("endpoint-transaction", async () => {
    const response = await request(httpServer.app).post("/transaction/qian");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-workflow", async () => {
    const response = await request(httpServer.app)
      .post("/workflow")
      .send({ name: "qian" });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-error", async () => {
    const response = await request(httpServer.app)
      .post("/error")
      .send({ name: "qian" });
    expect(response.statusCode).toBe(500);
  });

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  class TestEndpoints {
    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello")
    static async hello(_ctx: OperonContext) {
      void _ctx;
      return { message: "hello!" };
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello/:id")
    static async helloUrl(_ctx: OperonContext, id: string) {
      void _ctx;
      const res = _ctx.response as Response;
      // Customize status code and response.
      res.status(301).send(`wow ${id}`);
      return `hello ${id}`;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/query")
    static async helloQuery(_ctx: OperonContext, name: string) {
      void _ctx;
      return `hello ${name}`;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @PostApi("/testpost")
    static async testpost(_ctx: OperonContext, name: string) {
      void _ctx;
      return `hello ${name}`;
    }

    @PostApi("/transaction/:name")
    @OperonTransaction()
    static async testTranscation(txnCtxt: TransactionContext, name: string) {
      const { rows } = await txnCtxt.pgClient.query<TestKvTable>(
        `INSERT INTO ${testTableName}(id, value) VALUES (1, $1) RETURNING id`,
        [name]
      );
      return `hello ${rows[0].id}`;
    }

    @PostApi("/workflow")
    @OperonWorkflow()
    static async testWorkflow(wfCtxt: WorkflowContext, name: string) {
      const res = await wfCtxt.transaction(TestEndpoints.testTranscation, name);
      return res;
    }

    @PostApi("/error")
    @OperonWorkflow()
    static async testWorkflowError(wfCtxt: WorkflowContext, name: string) {
      // This workflow should encounter duplicate primary key error.
      let res = await wfCtxt.transaction(TestEndpoints.testTranscation, name);
      res = await wfCtxt.transaction(TestEndpoints.testTranscation, name);
      return res;
    }
  }
});
