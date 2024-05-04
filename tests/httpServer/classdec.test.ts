import {
  GetApi,
  RequiredRole,
  DefaultRequiredRole,
  MiddlewareContext,
  Transaction,
  Workflow,
  TransactionContext,
  WorkflowContext,
  TestingRuntime,
} from "../../src";
import { TestKvTable, generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import request from "supertest";
import { HandlerContext } from "../../src/httpServer/handler";
import { Authentication, KoaMiddleware } from "../../src/httpServer/middleware";
import { Middleware } from "koa";
import { DBOSNotAuthorizedError } from "../../src/error";
import { DBOSConfig } from "../../src/dbos-executor";
import { PoolClient } from "pg";
import { createInternalTestRuntime } from "../../src/testing/testing_runtime";

describe("httpserver-defsec-tests", () => {
  const testTableName = "dbos_test_kv";

  let testRuntime: TestingRuntime;
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    testRuntime = await createInternalTestRuntime([TestEndpointDefSec], config);
    await testRuntime.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await testRuntime.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    middlewareCounter = 0;
    middlewareCounter2 = 0;
  });

  afterEach(async () => {
    await testRuntime.destroy();
    jest.restoreAllMocks();
  });

  test("get-hello", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/hello");
    expect(response.statusCode).toBe(200);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(response.body.message).toBe("hello!");
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounter2).toBe(2); // Middleware runs from left to right.
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

  // The handler is authorized, then its child workflow and transaction should also be authroized.
  test("cascade-authorized", async () => {
    const response = await request(testRuntime.getHandlersCallback()).get("/workflow?name=alice&userid=a_real_user");
    expect(response.statusCode).toBe(200);

    const txnResponse = await request(testRuntime.getHandlersCallback()).get("/transaction?name=alice&userid=a_real_user");
    expect(txnResponse.statusCode).toBe(200);
  });

  // We can directly test a transaction with passed in authorizedRoles.
  test("direct-transaction-test", async () => {
    const res = await testRuntime.invoke(TestEndpointDefSec, undefined, { authenticatedRoles: ["user"] }).testTranscation("alice");
    expect(res).toBe("hello 1");

    // Unauthorized.
    await expect(testRuntime.invoke(TestEndpointDefSec).testTranscation("alice")).rejects.toThrow(
      new DBOSNotAuthorizedError("User does not have a role with permission to call testTranscation", 403)
    );
  });

  async function authTestMiddleware(ctx: MiddlewareContext) {
    if (ctx.requiredRole.length > 0) {
      const { userid } = ctx.koaContext.request.query;
      const uid = userid?.toString();

      if (!uid || uid.length === 0) {
        return Promise.reject(new DBOSNotAuthorizedError("Not logged in.", 401));
      } else {
        if (uid === "go_away") {
          return Promise.reject(new DBOSNotAuthorizedError("Go away.", 401));
        }
        return Promise.resolve({
          authenticatedUser: uid,
          authenticatedRoles: uid === "a_real_user" ? ["user"] : ["other"],
        });
      }
    }
    return;
  }

  let middlewareCounter = 0;
  const testMiddleware: Middleware = async (ctx, next) => {
    middlewareCounter++;
    await next();
  };

  let middlewareCounter2 = 0;
  const testMiddleware2: Middleware = async (ctx, next) => {
    middlewareCounter2 = middlewareCounter + 1;
    await next();
  };

  @DefaultRequiredRole(["user"])
  @Authentication(authTestMiddleware)
  @KoaMiddleware(testMiddleware, testMiddleware2)
  class TestEndpointDefSec {
    @RequiredRole([])
    @GetApi("/hello")
    static async hello(_ctx: HandlerContext) {
      return Promise.resolve({ message: "hello!" });
    }

    @GetApi("/requireduser")
    static async testAuth(_ctxt: HandlerContext, name: string) {
      return Promise.resolve(`Please say hello to ${name}`);
    }

    @Transaction()
    static async testTranscation(txnCtxt: TransactionContext<PoolClient>, name: string) {
      const { rows } = await txnCtxt.client.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
      return `hello ${rows[0].id}`;
    }

    @Workflow()
    static async testWorkflow(wfCtxt: WorkflowContext, name: string) {
      const res = await wfCtxt.invoke(TestEndpointDefSec).testTranscation(name);
      return res;
    }

    @GetApi("/workflow")
    static async testWfEndpoint(ctxt: HandlerContext, name: string) {
      return ctxt
        .invoke(TestEndpointDefSec)
        .testWorkflow(name)
        .then((x) => x.getResult());
    }

    @GetApi("/transaction")
    static async testTxnEndpoint(ctxt: HandlerContext, name: string) {
      return ctxt.invoke(TestEndpointDefSec).testTranscation(name);
    }
  }
});
