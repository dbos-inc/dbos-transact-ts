import axios, { AxiosError } from "axios";
import { spawn, execSync, ChildProcess } from "child_process";
import { Writable } from "stream";
import { Client } from "pg";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "../helpers";
import fs from "fs";
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
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_typeorm_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_prisma_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_drizzle_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_express_dbos_sys;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_typeorm;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_prisma;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_drizzle;`);
  await pgSystemClient.query(`DROP DATABASE IF EXISTS hello_express;`);
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

describe("runtime-entrypoint-tests", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();

    process.chdir("packages/create/templates/hello-contexts");
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

    process.chdir("packages/create/templates/hello-contexts");
    configureHelloExample();
  });

  afterAll(() => {
    process.chdir("../../../..");
  });

  test("runtime-hello-jest", () => {
    execSync("npm run test", { env: process.env }); // Make sure the hello example passes its own tests.
    execSync("npm run lint", { env: process.env }); // Pass linter rules.
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
      const command = spawn("dist/src/dbos-runtime/cli.js", ["start", "--appDir", "packages/create/templates/hello-contexts"], {
        env: process.env,
      });
      await waitForMessageTest(command, "3000");
    } finally {
      process.chdir("packages/create/templates/hello-contexts");
    }
  });

  test("runtime hello with ports provided in configuration file", async () => {
    const mockDBOSConfigYamlString = `
database:
  hostname: 'localhost'
  port: 5432
  username: 'postgres'
  password: \${PGPASSWORD}
  connectionTimeoutMillis: 3000
  app_db_client: 'knex'
runtimeConfig:
  port: 6666
  admin_port: 6789
`;
    const filePath = "dbos-config.yaml";
    fs.copyFileSync(filePath, `${filePath}.bak`);
    fs.writeFileSync(filePath, mockDBOSConfigYamlString, "utf-8");

    try {
      const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
        env: process.env,
      });
      await waitForMessageTest(command, "6666", "6789");
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
    execSync("npm run lint", { env: process.env }); // Pass linter rules.
  });

  // Attention! this test relies on example/hello/dbos-config.yaml not declaring a port!
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

  // Attention! this test relies on example/hello/dbos-config.yaml not declaring a port!
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
    execSync("npm run test", { env: process.env }); // Make sure hello-typeorm passes its own tests.
    console.log("linting hello-drizzle");
    execSync("npm run lint", { env: process.env, stdio: 'inherit'}); // Pass linter rules.
  });

  // Attention! this test relies on example/hello/dbos-config.yaml not declaring a port!
  test("test hello-drizzle runtime", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });
});

describe("runtime-tests-express", () => {
  beforeAll(async () => {
    await dropHelloSystemDB();
    process.chdir("packages/create/templates/hello-express");
    configureHelloExample();
  });

  afterAll(() => {
    process.chdir("../../../..");
  });

  test("test hello-express tests", () => {
    execSync("npm run test", { env: process.env }); // Make sure hello-express passes its own tests.
  });

  test("test hello-express runtime", async () => {
    const command = spawn("node_modules/@dbos-inc/dbos-sdk/dist/src/dbos-runtime/cli.js", ["start"], {
      env: process.env,
    });
    await waitForMessageTest(command, "3000");
  });
});
