/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
  OperonConfig,
  OperonNotAuthorizedError,
  OperonResponseError,
  OperonTransaction,
  OperonWorkflow,
  OperonHandlerRegistrationBase,
  PostApi,
  RequiredRole,
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
import { CONSOLE_EXPORTER } from "src/telemetry";

describe("httpserver-tests", () => {
  const testTableName = "operon_test_kv";

  let operon: Operon;
  let httpServer: OperonHttpServer;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig([CONSOLE_EXPORTER]);
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
    httpServer = new OperonHttpServer(operon,
      {
        authMiddleware: {
          authenticate(handler: OperonHandlerRegistrationBase, ctx: HandlerContext) : Promise<boolean> {
            if (handler.requiredRole.length > 0) {
              if (!ctx.request) {
                throw new Error("No request");
              }

              const { userid } = ctx.koaContext.request.query
              const uid = userid?.toString();

              if (!uid || uid.length === 0) {
                const err = new OperonNotAuthorizedError("Not logged in.", 401);
                throw err;
              }
              else {
                if (uid === 'go_away') {
                  return Promise.resolve(false);
                }
                ctx.authUser = uid;
                ctx.authRoles = (uid === 'a_real_user' ? ['user'] : ['other']);
              }
            }

            return Promise.resolve(true);
          }
        }
      }
    );
    // TODO: Need to find a way to customize the list of middlewares. It's tricky because the order we use those middlewares matters.
    // For example, if we use logger() after we register routes, the logger cannot correctly log the request before the function executes.
    // httpServer.app.use(logger());
    // In some cases, the aspects often covered by middleware can be made more explicit and more dev-friendly at the same time.
    //  We would run those at the appropriate time.
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
    expect(response.statusCode).toBe(500);
    expect(response.body.details.code).toBe('23505');  // Should be the expected error.
  });

  test("endpoint-handler", async () => {
    const response = await request(httpServer.app.callback()).get("/handler/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("response-error", async () => {
    const response = await request(httpServer.app.callback()).get("/operon-error");
    expect(response.statusCode).toBe(503);
    expect(response.body.message).toBe("customize error");
  });

  test("datavalidation-error", async () => {
    const response = await request(httpServer.app.callback()).get("/query");
    expect(response.statusCode).toBe(400);
    expect(response.body.details.operonErrorCode).toBe(9);
  });

  test("operon-redirect", async () => {
    const response = await request(httpServer.app.callback()).get("/redirect");
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe('/redirect-operon');
  });

  test("not-authenticated", async () => {
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice");
    expect(response.statusCode).toBe(401);
  });

  test("not-you", async () => {
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice&userid=go_away");
    expect(response.statusCode).toBe(401);
  });

  test("not-authorized", async () => {
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice&userid=bob");
    expect(response.statusCode).toBe(403);
  });

  test("authorized", async () => {
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice&userid=a_real_user");
    expect(response.statusCode).toBe(200);
  });

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  class TestEndpoints {
    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello")
    static async hello(_ctx: HandlerContext) {
      return { message: "hello!" };
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello/:id")
    static async helloUrl(ctx: HandlerContext, id: string) {
      // Customize status code and response.
      ctx.koaContext.body = `wow ${id}`;
      ctx.koaContext.status = 301;
      return `hello ${id}`;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/redirect")
    static async redirectUrl(ctx: HandlerContext) {
      const url = ctx.request?.url || "bad url"; // Get the raw url from request.
      ctx.koaContext.redirect(url + '-operon');
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/query")
    static async helloQuery(ctx: HandlerContext, name: string) {
      ctx.log("INFO", `query with name ${name}`);  // Test logging.
      return `hello ${name}`;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @PostApi("/testpost")
    static async testpost(_ctx: HandlerContext, name: string) {
      return `hello ${name}`;
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/operon-error")
    @OperonTransaction()
    static async operonErr(_ctx: TransactionContext) {
      throw new OperonResponseError("customize error", 503);
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

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/requireduser")
    @RequiredRole(['user'])
    static async testAuth(_ctxt: HandlerContext, name: string) {
      return `Please say hello to ${name}`;
    }
  }
});
