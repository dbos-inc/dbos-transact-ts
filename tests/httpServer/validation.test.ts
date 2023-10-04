/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  Operon,
} from "../../src";
import { OperonHttpServer } from "../../src/httpServer/server";
import {
  generateOperonTestConfig,
  setupOperonTestDb,
} from "../helpers";
import request from "supertest";
import { HandlerContext } from "../../src/httpServer/handler";
import { OperonConfig } from "../../src/operon";

describe("httpserver-datavalidation-tests", () => {
  let operon: Operon;
  let httpServer: OperonHttpServer;
  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    operon = new Operon(config);
    await operon.init(TestEndpointDataVal);
    httpServer = new OperonHttpServer(operon);
  });

  afterEach(async () => {
    await operon.destroy();
  });

  test("get-hello", async () => {
    const response = await request(httpServer.app.callback()).get("/hello");
    expect(response.statusCode).toBe(200);
    expect(response.body.message).toBe("hello!");
  });

  test("not-there", async () => {
    // "mute" console.error
    const response = await request(httpServer.app.callback()).get("/nourl");
    expect(response.statusCode).toBe(404);
  });

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  class TestEndpointDataVal {
    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello")
    static async hello(_ctx: HandlerContext) {
      return { message: "hello!" };
    }
  }
});
