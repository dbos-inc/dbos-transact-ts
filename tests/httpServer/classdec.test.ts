/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
  RequiredRole,
  DefaultRequiredRole,
  MiddlewareContext,
  OperonTransaction,
  OperonWorkflow,
  TransactionContext,
  WorkflowContext,
} from "../../src";
import { OperonHttpServer } from "../../src/httpServer/server";
import {
  TestKvTable,
  generateOperonTestConfig,
  setupOperonTestDb,
} from "../helpers";
import request from "supertest";
import { HandlerContext } from "../../src/httpServer/handler";
import { Authentication, KoaMiddleware } from "../../src/httpServer/middleware";
import { Middleware } from "koa";
import { OperonNotAuthorizedError } from "../../src/error";
import { OperonConfig } from "../../src/operon";

describe("httpserver-defsec-tests", () => {
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
    await operon.init(TestEndpointDefSec);
    await operon.userDatabase.query(`DROP TABLE IF EXISTS ${testTableName};`);
    await operon.userDatabase.query(
      `CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`
    );
    httpServer = new OperonHttpServer(operon);
    middlewareCounter = 0;
    middlewareCounter2 = 0;
  });

  afterEach(async () => {
    await operon.destroy();
    jest.restoreAllMocks();
  });

  test("get-hello", async () => {
    const response = await request(httpServer.app.callback()).get("/hello");
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe("hello!");
    expect(middlewareCounter).toBe(1);
    expect(middlewareCounter2).toBe(2);  // Middleware runs from left to right.
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

  // The handler is authorized, then its child workflow and transaction should also be authroized.
  test("cascade-authorized",async () => {
    const response = await request(httpServer.app.callback()).get("/workflow?name=alice&userid=a_real_user");
    expect(response.statusCode).toBe(200);

    const txnResponse = await request(httpServer.app.callback()).get("/transaction?name=alice&userid=a_real_user");
    expect(txnResponse.statusCode).toBe(200);
  });

  // eslint-disable-next-line @typescript-eslint/require-await
  async function authTestMiddleware (ctx: MiddlewareContext) {
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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  @DefaultRequiredRole(['user'])
  @Authentication(authTestMiddleware)
  @KoaMiddleware(testMiddleware, testMiddleware2)
  class TestEndpointDefSec {
    // eslint-disable-next-line @typescript-eslint/require-await
    @RequiredRole([])
    @GetApi("/hello")
    static async hello(_ctx: HandlerContext) {
      return { message: "hello!" };
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/requireduser")
    static async testAuth(_ctxt: HandlerContext, name: string) {
      return `Please say hello to ${name}`;
    }

    @OperonTransaction()
    static async testTranscation(txnCtxt: TransactionContext, name: string) {
      const { rows } = await txnCtxt.pgClient.query<TestKvTable>(
        `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
        [name]
      );
      return `hello ${rows[0].id}`;
    }

    @OperonWorkflow()
    static async testWorkflow(wfCtxt: WorkflowContext, name: string) {
      const res = await wfCtxt.invoke(TestEndpointDefSec).testTranscation(name);
      return res;
    }

    @GetApi("/workflow")
    static async testWfEndpoint(ctxt: HandlerContext, name: string) {
      const res = await ctxt
        .invoke(TestEndpointDefSec)
        .testWorkflow(name)
        .then((x) => x.getResult());
      return res;
    }

    @GetApi("/transaction")
    static async testTxnEndpoint(ctxt: HandlerContext, name: string) {
      const res = await ctxt.invoke(TestEndpointDefSec).testTranscation(name);
      return res;
    }
  }
});
