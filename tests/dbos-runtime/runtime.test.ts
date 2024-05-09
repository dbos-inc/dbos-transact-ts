/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import axios, { AxiosError } from "axios";
import { spawn, execSync, ChildProcess } from "child_process";
import { Writable } from "stream";
import { Client } from "pg";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import fs from "fs";

async function waitForMessageTest(command: ChildProcess, port: string) {
  const stdout = command.stdout as unknown as Writable;
  const stdin = command.stdin as unknown as Writable;
  const stderr = command.stderr as unknown as Writable;

  const waitForMessage = new Promise<void>((resolve, reject) => {
    const onData = (data: Buffer) => {
      const message = data.toString();
      process.stdout.write(message);
      if (message.includes("Server is running at")) {
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
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_typeorm_dbos_sys;`);
  await pgSystemClient.end();
}

function configureHelloExample() {
  execSync("npm ci");
  execSync("npm run build");
  if (process.env.PGPASSWORD === undefined) {
    process.env.PGPASSWORD = "dbos";
  }
  execSync("npx dbos migrate", { env: process.env });
}

describe("runtime-entrypoint-tests", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();

    process.chdir("packages/create/templates/hello");
    execSync("mv src/operations.ts src/entrypoint.ts");
    configureHelloExample();
  });

  afterAll(() => {
    execSync("mv src/entrypoint.ts src/operations.ts");
    process.chdir("../../../..");
  });

  test("runtime-hello using entrypoint runtimeConfig", async () => {
    const mockDBOSConfigYamlString = `
database:
  hostname: 'localhost'
  port: 5432
  username: 'postgres'
  password: \${PGPASSWORD}
  app_db_name: 'hello'
  connectionTimeoutMillis: 3000
  app_db_client: 'knex'
runtimeConfig:
  entrypoints:
    - dist/entrypoint.js
`;
    const filePath = "dbos-config.yaml";
    fs.copyFileSync(filePath, `${filePath}.bak`);
    fs.writeFileSync(filePath, mockDBOSConfigYamlString, "utf-8");

    try {
      const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start", "--port", "1234"], {
        env: process.env,
      });
      await waitForMessageTest(command, "1234");
    } finally {
      fs.copyFileSync(`${filePath}.bak`, filePath);
      fs.unlinkSync(`${filePath}.bak`);
    }
  });
});

describe("runtime-tests", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();

    process.chdir("packages/create/templates/hello");
    configureHelloExample();
  });

  afterAll(() => {
    process.chdir("../../../..");
  });

  test("runtime-hello-jest", () => {
    execSync("npm run test", { env: process.env }); // Make sure the hello example passes its own tests.
  });

  // Attention! this test relies on example/hello/dbos-config.yaml not declaring a port!
  test("runtime-hello using default runtime configuration", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });

  test("runtime hello with port provided as CLI parameter", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start", "--port", "1234"], {
      env: process.env,
    });
    await waitForMessageTest(command, "1234");
  });

  test("runtime hello with appDir provided as CLI parameter", async () => {
    process.chdir("../../../..");
    try {
      const command = spawn("dist/src/dbos-runtime/cli.js", ["start", "--appDir", "packages/create/templates/hello"], {
        env: process.env,
      });
      await waitForMessageTest(command, "3000");
    } finally {
      process.chdir("packages/create/templates/hello");
    }
  });

  test("runtime hello with port provided in configuration file", async () => {
    const mockDBOSConfigYamlString = `
database:
  hostname: 'localhost'
  port: 5432
  username: 'postgres'
  password: \${PGPASSWORD}
  app_db_name: 'hello'
  connectionTimeoutMillis: 3000
  app_db_client: 'knex'
runtimeConfig:
  port: 6666
`;
    const filePath = "dbos-config.yaml";
    fs.copyFileSync(filePath, `${filePath}.bak`);
    fs.writeFileSync(filePath, mockDBOSConfigYamlString, "utf-8");

    try {
      const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
        env: process.env,
      });
      await waitForMessageTest(command, "6666");
    } finally {
      fs.copyFileSync(`${filePath}.bak`, filePath);
      fs.unlinkSync(`${filePath}.bak`);
    }
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
  });

  // Attention! this test relies on example/hello/dbos-config.yaml not declaring a port!
  test("test hello-typeorm runtime", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });
});
