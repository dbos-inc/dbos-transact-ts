/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
  OperonConfig,
  RequiredRole,
  DefaultRequiredRole,
  MiddlewareContext,
  OperonNotAuthorizedError,
} from "../../src";
import { OperonHttpServer } from "../../src/httpServer/server";
import {
  generateOperonTestConfig,
  setupOperonTestDb,
} from "../helpers";
import request from "supertest";
import { HandlerContext } from "../../src/httpServer/handler";
import { Authentication, KoaMiddleware } from "../../src/httpServer/middleware";
import { Middleware } from "koa";

describe("httpserver-defsec-tests", () => {
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
    await operon.init(TestEndpointDefSec);
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
  }
});
