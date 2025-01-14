import axios, { AxiosError } from "axios";
import { spawn, execSync, ChildProcess } from "child_process";
import { Writable } from "stream";
import { Client } from "pg";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import { HealthUrl } from "../../src/httpServer/server";

async function waitForMessageTest(command: ChildProcess, port: string, adminPort?: string) {
  const stdout = command.stdout as unknown as Writable;
  const stdin = command.stdin as unknown as Writable;
  const stderr = command.stderr as unknown as Writable;

  if (!adminPort) {
    adminPort = (Number(port) + 1).toString();
  }

  const waitForMessage = new Promise<void>((resolve, reject) => {
    const onData = (data: Buffer) => {
      const message = data.toString();
      process.stdout.write(message);
      if (message.includes("DBOS Admin Server is running at")) {
        stdout.off("data", onData); // remove listener
        resolve();
      }
    };

    stdout.on("data", onData);
    stderr.on("data", onData);

    command.on("error", (error) => {
      reject(error); // Reject promise on command error
    });
  });
  try {
    await waitForMessage;
    // Axios will throw an exception if the return status is 500
    // Trying and catching is the only way to debug issues in this test
    try {
      const response = await axios.get(`http://127.0.0.1:${port}/greeting/dbos`);
      expect(response.status).toBe(200);
      const healthRes = await axios.get(`http://127.0.0.1:${adminPort}${HealthUrl}`);
      expect(healthRes.status).toBe(200);
    } catch (error) {
      const errMsg = `Error sending test request: status: ${(error as AxiosError).response?.status}, statusText: ${(error as AxiosError).response?.statusText}`;
      console.error(errMsg);
      throw error;
    }
  } finally {
    stdin.end();
    stdout.destroy();
    stderr.destroy();
    command.kill();
  }
}

async function dropHelloSystemDB() {
  const config = generateDBOSTestConfig();
  config.poolConfig.database = "hello";
  await setUpDBOSTestDb(config);
  const pgSystemClient = new Client({
    user: config.poolConfig.user,
    port: config.poolConfig.port,
    host: config.poolConfig.host,
    password: config.poolConfig.password,
    database: "hello",
  });
  await pgSystemClient.connect();
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_typeorm_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_prisma_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_drizzle_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_knex_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_typeorm;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_prisma;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_drizzle;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_knex;`);
  await pgSystemClient.end();
}

function configureHelloExample() {
  execSync("npm i");
  execSync("npm run build");
  if (process.env.PGPASSWORD === undefined) {
    process.env.PGPASSWORD = "dbos";
  }
  execSync("npx dbos migrate", { env: process.env });
}

describe("runtime-tests-knex", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();
    process.chdir("packages/create/templates/hello-knex");
    configureHelloExample();
  });

  afterAll(() => {
    process.chdir("../../../..");
  });

  test("test hello-knex tests", () => {
    execSync("npm run lint", { env: process.env }); // Pass linter rules.
  });

  test("test hello-knex runtime", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });
});

describe("runtime-tests-typeorm", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();
    process.chdir("packages/create/templates/hello-typeorm");
    configureHelloExample();
  });

  afterAll(() => {
    process.chdir("../../../..");
  });

  test("test hello-typeorm tests", () => {
    execSync("npm run test", { env: process.env }); // Make sure hello-typeorm passes its own tests.
    execSync("npm run lint", { env: process.env }); // Pass linter rules.
  });

  test("test hello-typeorm runtime", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });
});

describe("runtime-tests-prisma", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();
    process.chdir("packages/create/templates/hello-prisma");
    configureHelloExample();
  });

  afterAll(() => {
    process.chdir("../../../..");
  });

  test("test hello-prisma tests", () => {
    execSync("npm run test", { env: process.env }); // Make sure hello-prisma passes its own tests.
    execSync("npm run lint", { env: process.env }); // Pass linter rules.
  });

  test("test hello-prisma runtime", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });
});

describe("runtime-tests-drizzle", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();
    process.chdir("packages/create/templates/hello-drizzle");
    configureHelloExample();
  });

  afterAll(() => {
    process.chdir("../../../..");
  });

  test("test hello-drizzle tests", () => {
    execSync("npm run test", { env: process.env }); // Make sure hello-drizzle passes its own tests.
    execSync("npm run lint", { env: process.env, stdio: 'inherit' }); // Pass linter rules.
  });

  test("test hello-drizzle runtime", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });
});
