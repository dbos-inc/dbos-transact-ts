/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
  OperonConfig,
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
import { ArgSource, ArgSources, HandlerContext } from "src/httpServer/handler";

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
    // TODO: Need to find a way to customize the list of middlewares. It's tricky because the order we use those middlewares matters.
    // For example, if we use logger() after we register routes, the logger cannot correctly log the request before the function executes.
    // httpServer.app.use(logger());
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("get-hello", async () => {
    const response = await request(httpServer.app.callback()).get("/hello");
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe("hello!");
  });

  test("get-url", async () => {
    const response = await request(httpServer.app.callback()).get("/hello/alice");
    expect(response.statusCode).toBe(301);
    expect(response.text).toBe("wow alice");
  });

  test("get-query", async () => {
    const response = await request(httpServer.app.callback()).get("/query?name=alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello alice");
  });

  test("post-test", async () => {
    const response = await request(httpServer.app.callback())
      .post("/testpost")
      .send({ name: "alice" });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello alice");
  });

  test("endpoint-transaction", async () => {
    const response = await request(httpServer.app.callback()).post("/transaction/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-workflow", async () => {
    const response = await request(httpServer.app.callback())
      .post("/workflow?name=alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-error", async () => {
    const response = await request(httpServer.app.callback())
      .post("/error")
      .send({ name: "alice" });
    expect(response.statusCode).toBe(400);
    expect(response.body.code).toBe('23505');  // Should be the expected error.
  });

  test("endpoint-handler", async () => {
    const response = await request(httpServer.app.callback()).get("/handler/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  class TestEndpoints {
    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello")
    static async hello(_ctx: HandlerContext) {
      void _ctx;
      return { message: "hello!" };
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello/:id")
    static async helloUrl(_ctx: HandlerContext, id: string) {
      // Customize status code and response.
      _ctx.koaContext.body = `wow ${id}`;
      _ctx.koaContext.status = 301;
      return `hello ${id}`;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/query")
    static async helloQuery(_ctx: HandlerContext, name: string) {
      void _ctx;
      return `hello ${name}`;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @PostApi("/testpost")
    static async testpost(_ctx: HandlerContext, name: string) {
      void _ctx;
      return `hello ${name}`;
    }

    @GetApi("/handler/:name")
    static async testHandler(ctxt: HandlerContext, name: string) {
      // Invoke a workflow using the provided Operon instance.
      return ctxt.operon.workflow(TestEndpoints.testWorkflow, {}, name).getResult();
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
    static async testWorkflow(wfCtxt: WorkflowContext, @ArgSource(ArgSources.QUERY) name: string) {
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
