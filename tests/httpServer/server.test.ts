/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
  OperonTransaction,
  OperonWorkflow,
  MiddlewareContext,
  PostApi,
  RequiredRole,
  TransactionContext,
  WorkflowContext,
  StatusString,
} from "../../src";
import { OperonHttpServer, OperonWorkflowUUIDHeader } from "../../src/httpServer/server";
import {
  TestKvTable,
  generateOperonTestConfig,
  setupOperonTestDb,
} from "../helpers";
import request from "supertest";
import { ArgSource, ArgSources, HandlerContext } from "../../src/httpServer/handler";
import { Authentication } from "../../src/httpServer/middleware";
import { v1 as uuidv1 } from "uuid";
import { OperonConfig } from "../../src/operon";
import { OperonNotAuthorizedError, OperonResponseError } from "../../src/error";

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
    await operon.init(TestEndpoints);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id INT PRIMARY KEY, value TEXT);`
    );
    httpServer = new OperonHttpServer(operon);
  });

  afterEach(async () => {
    await operon.destroy();
    jest.restoreAllMocks();
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
    // "mute" console.info
    jest.spyOn(console, "info").mockImplementation(() => {});
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
    // "mute" console.error
    jest.spyOn(console, "error").mockImplementation(() => {});
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
    // "mute" console.error
    jest.spyOn(console, "error").mockImplementation(() => {});
    const response = await request(httpServer.app.callback()).get("/operon-error");
    expect(response.statusCode).toBe(503);
    expect(response.body.message).toBe("customize error");
  });

  test("datavalidation-error", async () => {
    // "mute" console.error
    jest.spyOn(console, "error").mockImplementation(() => {});
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
    // "mute" console.error
    jest.spyOn(console, "error").mockImplementation(() => {});
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice");
    expect(response.statusCode).toBe(401);
  });

  test("not-you", async () => {
    // "mute" console.error
    jest.spyOn(console, "error").mockImplementation(() => {});
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice&userid=go_away");
    expect(response.statusCode).toBe(401);
  });

  test("not-authorized", async () => {
    // "mute" console.error
    jest.spyOn(console, "error").mockImplementation(() => {});
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice&userid=bob");
    expect(response.statusCode).toBe(403);
  });

  test("authorized", async () => {
    const response = await request(httpServer.app.callback()).get("/requireduser?name=alice&userid=a_real_user");
    expect(response.statusCode).toBe(200);
  });

  test("test-workflowUUID-header", async () => {
    const workflowUUID = uuidv1();
    const response = await request(httpServer.app.callback())
      .post("/workflow?name=bob")
      .set({"operon-workflowuuid": workflowUUID});
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");

    await operon.flushWorkflowStatusBuffer();

    // Retrieve the workflow with UUID.
    const retrievedHandle = operon.retrieveWorkflow<string>(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe("hello 1");
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  test("endpoint-handler-UUID", async () => {
    const workflowUUID = uuidv1();
    const response = await request(httpServer.app.callback()).get("/handler/bob")
      .set({"operon-workflowuuid": workflowUUID});
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");

    // Retrieve the workflow with UUID.
    const retrievedHandle = operon.retrieveWorkflow<string>(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe("hello 1");
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  // eslint-disable-next-line @typescript-eslint/require-await
  async function testAuthMiddlware (ctx: MiddlewareContext) {
    if (ctx.requiredRole.length > 0) {
      const { userid } = ctx.koaContext.request.query
      const uid = userid?.toString();

      if (!uid || uid.length === 0) {
        const err = new OperonNotAuthorizedError("Not logged in.", 401);
        throw err;
      }
      else {
        if (uid === 'go_away') {
          throw new OperonNotAuthorizedError("Go away.", 401);
        }
        return {
          authenticatedUser: uid,
          authenticatedRoles: (uid === 'a_real_user' ? ['user'] : ['other'])
        };
      }
    }
    return;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  @Authentication(testAuthMiddlware)
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
      ctx.info(`query with name ${name}`);  // Test logging.
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
      const workflowUUID = ctxt.koaContext.get(OperonWorkflowUUIDHeader);
      // Invoke a workflow using the given UUID.
      return ctxt.invoke(TestEndpoints, workflowUUID).testWorkflow(name).then(x => x.getResult());
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
      const res: string = await wfCtxt.invoke(TestEndpoints).testTranscation(name);
      return res;
    }

    @PostApi("/error")
    @OperonWorkflow()
    static async testWorkflowError(wfCtxt: WorkflowContext, name: string) {
      // This workflow should encounter duplicate primary key error.
      let res: string = await wfCtxt.invoke(TestEndpoints).testTranscation(name);
      res = await wfCtxt.invoke(TestEndpoints).testTranscation(name);
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
