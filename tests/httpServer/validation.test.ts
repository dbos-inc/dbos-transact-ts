/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import {
  GetApi,
  PostApi,
  Operon,
  
  ArgVarchar,
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
    const response = await request(httpServer.app.callback()).get("/nourl");
    expect(response.statusCode).toBe(404);
  });

  // Plain string
  test("no string (get)", async () => {
    const response = await request(httpServer.app.callback()).get("/string");
    expect(response.statusCode).toBe(400);
  });
  test("no string (post)", async () => {
    const response = await request(httpServer.app.callback()).post("/string");
    expect(response.statusCode).toBe(400);
  });
  test("no string (post) 2", async () => {
    const response = await request(httpServer.app.callback()).post("/string")
    .send({});
    expect(response.statusCode).toBe(400);
  });
  test("no string (post) - something else", async () => {
    const response = await request(httpServer.app.callback()).post("/string")
    .send({foo:"bar"});
    expect(response.statusCode).toBe(400);
  });
  test("string get", async () => {
    const response = await request(httpServer.app.callback()).get("/string")
    .query({v:"AAA"});
    expect(response.statusCode).toBe(200);
  });
  test("string post", async () => {
    const response = await request(httpServer.app.callback()).post("/string")
    .send({v:"AAA"});
    expect(response.statusCode).toBe(200);
  });
  test("string post not a number", async () => {
    const response = await request(httpServer.app.callback()).post("/string")
    .send({v:1234});
    expect(response.statusCode).toBe(400);
  });

  // Varchar(10)
  test("no string (get)", async () => {
    const response = await request(httpServer.app.callback()).get("/varchar");
    expect(response.statusCode).toBe(400);
  });
  test("no string (post)", async () => {
    const response = await request(httpServer.app.callback()).post("/varchar");
    expect(response.statusCode).toBe(400);
  });
  test("no string (post) 2", async () => {
    const response = await request(httpServer.app.callback()).post("/varchar")
    .send({});
    expect(response.statusCode).toBe(400);
  });
  test("string get", async () => {
    const response = await request(httpServer.app.callback()).get("/varchar")
    .query({v:"AAA"});
    expect(response.statusCode).toBe(200);
  });
  test("string get - too long", async () => {
    const response = await request(httpServer.app.callback()).get("/varchar")
    .query({v:"AAAaaaAAAaaa"});
    expect(response.statusCode).toBe(400);
  });
  test("string post", async () => {
    const response = await request(httpServer.app.callback()).post("/varchar")
    .send({v:"AAA"});
    expect(response.statusCode).toBe(200);
  });
  test("string post - too long", async () => {
    const response = await request(httpServer.app.callback()).post("/varchar")
    .send({v:"AAAaaaAAAaaa"});
    expect(response.statusCode).toBe(400);
  });
  test("string post not a number", async () => {
    const response = await request(httpServer.app.callback()).post("/varchar")
    .send({v:1234});
    expect(response.statusCode).toBe(400);
  });
  test("varchar post boolean", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({v:false});
    expect(response.statusCode).toBe(400);
  });

  // Number (float)
  test("no number (get)", async () => {
    const response = await request(httpServer.app.callback()).get("/number");
    expect(response.statusCode).toBe(400);
  });
  test("no number (post)", async () => {
    const response = await request(httpServer.app.callback()).post("/number");
    expect(response.statusCode).toBe(400);
  });
  test("no number (post) 2", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({});
    expect(response.statusCode).toBe(400);
  });
  test("number get", async () => {
    const response = await request(httpServer.app.callback()).get("/number")
    .query({v:"10.1"});
    expect(response.statusCode).toBe(200);
  });
  test("number get", async () => {
    const response = await request(httpServer.app.callback()).get("/number")
    .query({v:10.5});
    expect(response.statusCode).toBe(200);
  });
  test("number get - bogus value", async () => {
    const response = await request(httpServer.app.callback()).get("/number")
    .query({v:"abc"});
    expect(response.statusCode).toBe(400);
  });
  test("number get - bigint", async () => {
    const response = await request(httpServer.app.callback()).get("/number")
    .query({v:12345678901234567890n});
    expect(response.statusCode).toBe(200);
  });
  test("number post", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({v:"20"});
    expect(response.statusCode).toBe(200);
  });
  test("number post", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({v:20.20});
    expect(response.statusCode).toBe(200);
  });
  test("number post - bogus value", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({v:"AAAaaaAAAaaa"});
    expect(response.statusCode).toBe(400);
  });
  test("number post not a number", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({v:false});
    expect(response.statusCode).toBe(400);
  });
  /* You can't do this - no bigint serialize to json
  test("number post bigint", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({v:234567890123456789n});
    expect(response.statusCode).toBe(200);
  });
  */
  test("number post bigint", async () => {
    const response = await request(httpServer.app.callback()).post("/number")
    .send({v:"12345678901234567890"});
    expect(response.statusCode).toBe(200);
  });

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  class TestEndpointDataVal {
    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/hello")
    static async hello(_ctx: HandlerContext) {
      return { message: "hello!" };
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/string")
    static async checkStringG(_ctx: HandlerContext, v: string) {
      if (typeof v !== 'string') {
        throw new Error("THIS SHOULD NEVER HAPPEN");
      }
      return { message: `This is a really nice string: ${v}` };
    }
    // eslint-disable-next-line @typescript-eslint/require-await
    @PostApi("/string")
    static async checkStringP(_ctx: HandlerContext, v: string) {
      if (typeof v !== 'string') {
        throw new Error("THIS SHOULD NEVER HAPPEN");
      }
      return { message: `This is a really nice string: ${v}` };
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/varchar")
    static async checkVarcharG(_ctx: HandlerContext, @ArgVarchar(10) v: string) {
      if (typeof v !== 'string') {
        throw new Error("THIS SHOULD NEVER HAPPEN");
      }
      return { message: `This is a really nice string (limited length): ${v}` };
    }
    // eslint-disable-next-line @typescript-eslint/require-await
    @PostApi("/varchar")
    static async checkVarcharP(_ctx: HandlerContext, @ArgVarchar(10) v: string) {
      if (typeof v !== 'string') {
        throw new Error("THIS SHOULD NEVER HAPPEN");
      }
      return { message: `This is a really nice string (limited length): ${v}` };
    }

    // eslint-disable-next-line @typescript-eslint/require-await
    @GetApi("/number")
    static async checkNumberG(_ctx: HandlerContext, v: number) {
      if (typeof v !== 'number') {
        throw new Error("THIS SHOULD NEVER HAPPEN");
      }
      return { message: `This is a really nice number: ${v}` };
    }
    // eslint-disable-next-line @typescript-eslint/require-await
    @PostApi("/number")
    static async checkNumberP(_ctx: HandlerContext, v: number) {
      return { message: `This is a really nice number: ${v}` };
    }
  }
});
