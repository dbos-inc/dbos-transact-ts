/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
  OperonConfig,
  OperonNotAuthorizedError,
  MiddlewareContext,
  RequiredRole,
  DefaultRequiredRole,
} from "src";
import { OperonHttpServer } from "src/httpServer/server";
import {
  generateOperonTestConfig,
  setupOperonTestDb,
} from "tests/helpers";
import request from "supertest";
import { HandlerContext } from "src/httpServer/handler";
import { CONSOLE_EXPORTER } from "src/telemetry";

describe("httpserver-defsec-tests", () => {
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
    await operon.init(TestEndpointDefSec);
    httpServer = new OperonHttpServer(operon,
      {
        // eslint-disable-next-line @typescript-eslint/require-await
        authMiddleware: async (ctx: MiddlewareContext) => {
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
      }
    );
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("get-hello", async () => {
    const response = await request(httpServer.app.callback()).get("/hello");
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe("hello!");
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
  @DefaultRequiredRole(['user'])
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
