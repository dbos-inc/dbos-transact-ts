// import { PrismaClient, testkv } from "@prisma/client";

import request from "supertest";

import { Entity, Column, PrimaryColumn, PrimaryGeneratedColumn } from "typeorm";
import { EntityManager, Unique } from "typeorm";

import { generateOperonTestConfig, setupOperonTestDb } from "./helpers";
import {
   OperonTestingRuntime,
   OperonTransaction,
   OrmEntities,
   TransactionContext,
   Authentication,
   MiddlewareContext,
   GetApi,
   HandlerContext,
   RequiredRole,
   PostApi,
} from "../src";
import { OperonConfig } from "../src/operon";
import { v1 as uuidv1 } from "uuid";
import { UserDatabaseName } from "../src/user_database";
import { createInternalTestRuntime } from "../src/testing/testing_runtime";
import { OperonNotAuthorizedError } from "../src/error";

/**
 * Funtions used in tests.
 */
@Entity()
export class KV {
  @PrimaryColumn()
  id: string = "t";

  @Column()
  value: string = "v";
}

let globalCnt = 0;

type TestTransactionContext = TransactionContext<EntityManager>;

@OrmEntities([KV])
class KVController {
  @OperonTransaction()
  static async testTxn(txnCtxt: TestTransactionContext, id: string, value: string) {
    const kv: KV = new KV();
    kv.id = id;
    kv.value = value;
    const res = await txnCtxt.client.save(kv);
    globalCnt += 1;
    return res.id;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @OperonTransaction({ readOnly: true })
  static async readTxn(txnCtxt: TestTransactionContext, id: string) {
    globalCnt += 1;
    const kvp = await txnCtxt.client.findOneBy(KV, {id: id});
    return kvp?.value || "<Not Found>";
  }
}

describe("typeorm-tests", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig(UserDatabaseName.TYPEORM);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    testRuntime = await createInternalTestRuntime([KVController], config);
    await testRuntime.dropUserSchema();
    await testRuntime.createUserSchema();
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-typeorm", async () => {
    const workUUID = uuidv1();
    await expect(testRuntime.invoke(KVController, workUUID).testTxn("test", "value")).resolves.toBe("test");
    await expect(testRuntime.invoke(KVController, workUUID).testTxn("test", "value")).resolves.toBe("test");
  });

  test("typeorm-duplicate-transaction", async () => {
    // Run two transactions concurrently with the same UUID.
    // Both should return the correct result but only one should execute.
    const workUUID = uuidv1();
    let results = await Promise.allSettled([
      testRuntime.invoke(KVController, workUUID).testTxn("oaootest",
      "oaoovalue"),
      testRuntime.invoke(KVController, workUUID).testTxn("oaootest", "oaoovalue"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("oaootest");
    expect(globalCnt).toBe(1);

    // Read-only transactions would execute twice.
    globalCnt = 0;
    const readUUID = uuidv1();
    results = await Promise.allSettled([
      testRuntime.invoke(KVController, readUUID).readTxn("oaootest"),
      testRuntime.invoke(KVController, readUUID).readTxn("oaootest"),
    ]);
    expect((results[0] as PromiseFulfilledResult<string>).value).toBe("oaoovalue");
    expect((results[1] as PromiseFulfilledResult<string>).value).toBe("oaoovalue");
    expect(globalCnt).toBeGreaterThanOrEqual(1);
  });
});

@Entity()
@Unique("onlyone", ["username"])
export class User {
  @PrimaryGeneratedColumn('uuid')
  id: string | undefined = undefined;

  @Column()
  username: string = "user";
}

@OrmEntities([User])
@Authentication(UserManager.authMiddlware)
class UserManager {
  @OperonTransaction()
  @PostApi('/register')
  static async createUser(txnCtxt: TestTransactionContext, uname: string) {
    const u: User = new User();
    u.username = uname;
    const res = await txnCtxt.client.save(u);
    return res;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  @GetApi('/hello')
  @RequiredRole(['user'])
  static async hello(hCtxt: HandlerContext) {
    return {messge: "hello "+hCtxt.authenticatedUser};
  }

  static async authMiddlware(ctx: MiddlewareContext) {
    const cfg = ctx.getConfig<string>("shouldExist", "does not exist");
    if (cfg !== "exists") {
      throw Error("Auth is misconfigured.");
    }
    if (!ctx.requiredRole || !ctx.requiredRole.length) {
      return;
    }
    const {user} = ctx.koaContext.query;
    if (!user) {
      throw new OperonNotAuthorizedError("User not provided", 401);
    }
    const u = await ctx.query(
      (dbClient: EntityManager, uname: string) => {
        return dbClient.findOneBy(User, {username: uname});
      }, user as string
      );

    if (!u) {
      throw new OperonNotAuthorizedError("User does not exist", 403);
    }
    return {
      authenticatedUser: u.username,
      authenticatedRoles: ["user"],
    };
  }
}

describe("typeorm-auth-tests", () => {
  let config: OperonConfig;
  let testRuntime: OperonTestingRuntime;

  beforeAll(async () => {
    config = generateOperonTestConfig(UserDatabaseName.TYPEORM);
    await setupOperonTestDb(config);
  });

  beforeEach(async () => {
    globalCnt = 0;
    testRuntime = await createInternalTestRuntime([UserManager], config);
    await testRuntime.dropUserSchema();
    await testRuntime.createUserSchema();
  });

  afterEach(async () => {
    await testRuntime.destroy();
  });

  test("simple-typeorm", async () => {
    // No user name
    const response1 = await request(testRuntime.getHandlersCallback()).get("/hello");
    expect(response1.statusCode).toBe(401);

    // User name doesn't exist
    const response2 = await request(testRuntime.getHandlersCallback()).get("/hello?user=paul");
    expect(response2.statusCode).toBe(403);

    const response3 = await request(testRuntime.getHandlersCallback()).post("/register").send({uname: "paul"});
    expect(response3.statusCode).toBe(200);

    const response4 = await request(testRuntime.getHandlersCallback()).get("/hello?user=paul");
    expect(response4.statusCode).toBe(200);
  });
});
