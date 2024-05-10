/* eslint-disable @typescript-eslint/no-unsafe-member-access */
import {
  GetApi,
  Transaction,
  Workflow,
  MiddlewareContext,
  PostApi,
  RequiredRole,
  TransactionContext,
  WorkflowContext,
  StatusString,
  Communicator,
  CommunicatorContext,
} from "../../src";
import { RequestIDHeader } from "../../src/httpServer/handler";
import { WorkflowUUIDHeader } from "../../src/httpServer/server";
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import request from "supertest";
import { ArgSource, HandlerContext } from "../../src/httpServer/handler";
import { ArgSources } from "../../src/httpServer/handlerTypes";
import { Authentication, KoaBodyParser } from "../../src/httpServer/middleware";
import { v1 as uuidv1, validate as uuidValidate } from "uuid";
import { DBOSConfig } from "../../src/dbos-executor";
import { DBOSNotAuthorizedError, DBOSResponseError } from "../../src/error";
import { PoolClient } from "pg";
import { TestingRuntime, TestingRuntimeImpl, createInternalTestRuntime } from "../../src/testing/testing_runtime";
import { IncomingMessage } from "http";
import { bodyParser } from "@koa/bodyparser";

describe("httpserver-tests", () => {
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

  test("get-hello", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/hello");
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe("hello!");
    // Expect uuidValidate to be true
    const requestID: string = response.headers[RequestIDHeader];
    expect(uuidValidate(requestID)).toBe(true);
  });

  test("get-url", async () => {
    const requestID = "my-request-id";
    const response = await request(testRuntime.getHandlersCallback()).get("/hello/alice").set(RequestIDHeader, requestID);
    expect(response.statusCode).toBe(301);
    expect(response.text).toBe("wow alice");
    expect(response.headers[RequestIDHeader]).toBe(requestID);
  });

  test("get-query", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/query?name=alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello alice");
  });

  test("post-test", async () => {
    const response = await request(testRuntime.getHandlersCallback()).post("/testpost").send({ name: "alice" });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello alice");
  });

  test("post-test-custom-body", async () => {
    let response = await request(testRuntime.getHandlersCallback()).post("/testpost").set('Content-Type', 'application/custom-content-type').send(JSON.stringify({ name: "alice" }));
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello alice");
    response = await request(testRuntime.getHandlersCallback()).post("/testpost").set('Content-Type', 'application/rejected-custom-content-type').send(JSON.stringify({ name: "alice" }));
    expect(response.statusCode).toBe(400);
  });

  test("endpoint-transaction", async () => {
    const response = await request(testRuntime.getHandlersCallback()).post("/transaction/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-communicator", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/communicator/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("alice");
  });

  test("endpoint-workflow", async () => {
    const response = await request(testRuntime.getHandlersCallback()).post("/workflow?name=alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-error", async () => {
    const response = await request(testRuntime.getHandlersCallback()).post("/error").send({ name: "alice" });
    expect(response.statusCode).toBe(500);
    expect(response.body.details.code).toBe("23505"); // Should be the expected error.
  });

  test("endpoint-handler", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/handler/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-testStartWorkflow", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/testStartWorkflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  test("endpoint-testInvokeWorkflow", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/testInvokeWorkflow/alice");
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");
  });

  // This feels unclean, but supertest doesn't expose the error message the people we want. See:
  //   https://github.com/ladjs/supertest/issues/95
  interface Res {
    res: IncomingMessage;
  }

  test("response-error", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/dbos-error");
    expect(response.statusCode).toBe(503);
    expect((response as unknown as Res).res.statusMessage).toBe("customize error");
    expect(response.body.message).toBe("customize error");
  });

  test("datavalidation-error", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/query");
    expect(response.statusCode).toBe(400);
    expect(response.body.details.dbosErrorCode).toBe(9);
  });

  test("dbos-redirect", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/redirect");
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/redirect-dbos");
  });

  test("not-authenticated", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/requireduser?name=alice");
    expect(response.statusCode).toBe(401);
  });

  test("not-you", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/requireduser?name=alice&userid=go_away");
    expect(response.statusCode).toBe(401);
  });

  test("not-authorized", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/requireduser?name=alice&userid=bob");
    expect(response.statusCode).toBe(403);
  });

  test("authorized", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/requireduser?name=alice&userid=a_real_user");
    expect(response.statusCode).toBe(200);
  });

  test("test-workflowUUID-header", async () => {
    const workflowUUID = uuidv1();
    const response = await request(testRuntime.getHandlersCallback()).post("/workflow?name=bob").set({ "dbos-idempotency-key": workflowUUID });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");

    const dbosExec = (testRuntime as TestingRuntimeImpl).getDBOSExec();
    await dbosExec.flushWorkflowBuffers();

    // Retrieve the workflow with UUID.
    const retrievedHandle = testRuntime.retrieveWorkflow<string>(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe("hello 1");
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
      request: { url: "/workflow?name=bob" },
    });
  });

  test("endpoint-handler-UUID", async () => {
    const workflowUUID = uuidv1();
    const response = await request(testRuntime.getHandlersCallback()).get("/handler/bob").set({ "dbos-idempotency-key": workflowUUID });
    expect(response.statusCode).toBe(200);
    expect(response.text).toBe("hello 1");

    // Retrieve the workflow with UUID.
    const retrievedHandle = testRuntime.retrieveWorkflow<string>(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getResult()).resolves.toBe("hello 1");
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
      request: { url: "/handler/bob" },
    });
  });

  async function testAuthMiddlware(ctx: MiddlewareContext) {
    if (ctx.requiredRole.length > 0) {
      const { userid } = ctx.koaContext.request.query;
      const uid = userid?.toString();

      if (!uid || uid.length === 0) {
        const err = new DBOSNotAuthorizedError("Not logged in.", 401);
        throw err;
      } else {
        if (uid === "go_away") {
          throw new DBOSNotAuthorizedError("Go away.", 401);
        }
        return Promise.resolve({
          authenticatedUser: uid,
          authenticatedRoles: uid === "a_real_user" ? ["user"] : ["other"],
        });
      }
    }
    return;
  }

  type TestTransactionContext = TransactionContext<PoolClient>;

  @Authentication(testAuthMiddlware)
  @KoaBodyParser(bodyParser({
    extendTypes: {
      json: ["application/json", "application/custom-content-type"],
    },
    encoding: "utf-8"
  }))
  class TestEndpoints {
    @GetApi("/hello")
    static async hello(_ctx: HandlerContext) {
      return Promise.resolve({ message: "hello!" });
    }

    @GetApi("/hello/:id")
    static async helloUrl(ctx: HandlerContext, id: string) {
      // Customize status code and response.
      ctx.koaContext.body = `wow ${id}`;
      ctx.koaContext.status = 301;
      return Promise.resolve(`hello ${id}`);
    }

    @GetApi("/redirect")
    static async redirectUrl(ctx: HandlerContext) {
      const url = ctx.request.url || "bad url"; // Get the raw url from request.
      ctx.koaContext.redirect(url + "-dbos");
      return Promise.resolve();
    }

    @GetApi("/query")
    static async helloQuery(ctx: HandlerContext, name: string) {
      ctx.logger.info(`query with name ${name}`); // Test logging.
      return Promise.resolve(`hello ${name}`);
    }

    @PostApi("/testpost")
    static async testpost(_ctx: HandlerContext, name: string) {
      return Promise.resolve(`hello ${name}`);
    }

    @GetApi("/dbos-error")
    @Transaction()
    static async dbosErr(_ctx: TestTransactionContext) {
      return Promise.reject(new DBOSResponseError("customize error", 503));
    }

    @GetApi("/handler/:name")
    static async testHandler(ctxt: HandlerContext, name: string) {
      const workflowUUID = ctxt.koaContext.get(WorkflowUUIDHeader);
      // Invoke a workflow using the given UUID.
      return ctxt
        .invoke(TestEndpoints, workflowUUID)
        .testWorkflow(name)
        .then((x) => x.getResult());
    }

    @GetApi("/testStartWorkflow/:name")
    static async testStartWorkflow(ctxt: HandlerContext, name: string): Promise<string> {
      return ctxt.startWorkflow(TestEndpoints).testWorkflow(name).then((x) => x.getResult());
    }

    @GetApi("/testInvokeWorkflow/:name")
    static async testInvokeWorkflow(ctxt: HandlerContext, name: string): Promise<string> {
      return ctxt.invokeWorkflow(TestEndpoints).testWorkflow(name);
    }

    @PostApi("/transaction/:name")
    @Transaction()
    static async testTranscation(txnCtxt: TestTransactionContext, name: string) {
      const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName}(id, value) VALUES (1, $1) RETURNING id`, [name]);
      return `hello ${rows[0].id}`;
    }

    @GetApi("/communicator/:input")
    @Communicator()
    static async testCommunicator(_ctxt: CommunicatorContext, input: string) {
      return Promise.resolve(input);
    }

    @PostApi("/workflow")
    @Workflow()
    static async testWorkflow(wfCtxt: WorkflowContext, @ArgSource(ArgSources.QUERY) name: string) {
      const res = await wfCtxt.invoke(TestEndpoints).testTranscation(name);
      return wfCtxt.invoke(TestEndpoints).testCommunicator(res);
    }

    @PostApi("/error")
    @Workflow()
    static async testWorkflowError(wfCtxt: WorkflowContext, name: string) {
      // This workflow should encounter duplicate primary key error.
      let res = await wfCtxt.invoke(TestEndpoints).testTranscation(name);
      res = await wfCtxt.invoke(TestEndpoints).testTranscation(name);
      return res;
    }

    @GetApi("/requireduser")
    @RequiredRole(["user"])
    static async testAuth(ctxt: HandlerContext, name: string) {
      if (ctxt.authenticatedUser !== "a_real_user") {
        throw new DBOSResponseError("uid not a real user!", 400);
      }
      if (!ctxt.authenticatedRoles.includes("user")) {
        throw new DBOSResponseError("roles don't include user!", 400);
      }
      if (ctxt.assumedRole !== "user") {
        throw new DBOSResponseError("Should never happen! Not assumed to be user", 400);
      }
      return Promise.resolve(`Please say hello to ${name}`);
    }
  }
});
