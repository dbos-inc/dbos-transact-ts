/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import axios from "axios";
import { spawn, execSync, ChildProcess } from "child_process";
import { Writable } from "stream";
import { Client } from "pg";
import * as utils from "src/utils";
import { generateOperonTestConfig, setupOperonTestDb } from "tests/helpers";

async function waitForMessageTest(command: ChildProcess, port: string) {
    const stdout = command.stdout as unknown as Writable;
    const stdin = command.stdin as unknown as Writable;
    const stderr = command.stderr as unknown as Writable;

    const waitForMessage = new Promise<void>((resolve, reject) => {
      const onData = (data: Buffer) => {
        const message = data.toString();
        process.stdout.write(message);
        if (message.includes('Server is running at')) {
          stdout.off('data', onData);  // remove listener
          resolve();
        }
      };

      stdout.on('data', onData);

      command.on('error', (error) => {
        reject(error);  // Reject promise on command error
      });
    });
    try {
      await waitForMessage;
      const response = await axios.get(`http://127.0.0.1:${port}/greeting/operon`);
      expect(response.status).toBe(200);
    } finally {
      stdin.end();
      stdout.destroy();
      stderr.destroy();
      command.kill();
    }
}

describe("runtime-tests", () => {
  beforeAll(async () => {
    const config = generateOperonTestConfig();
    config.poolConfig.database = "hello";
    await setupOperonTestDb(config);
    const pgSystemClient = new Client({
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: "hello",
    });
    await pgSystemClient.connect();
    await pgSystemClient.query(`CREATE TABLE IF NOT EXISTS OperonHello (greeting_id SERIAL PRIMARY KEY, greeting TEXT);`);
    await pgSystemClient.end();

    process.chdir('examples/hello');
    execSync('npm i');
    execSync('npm run build');
  });

  afterAll(() => {
    process.chdir('../..');
  });

  test("runtime-hello using default runtime configuration", async () => {
    const command = spawn('../../dist/src/operon-runtime/cli.js', ['start'], {
      env: process.env
    });
    await waitForMessageTest(command, '3000');
  });

  test("runtime hello with port provided as CLI parameter", async () => {
    const command = spawn('../../dist/src/operon-runtime/cli.js', ['start', '--port', '1234'], {
      env: process.env
    });
    await waitForMessageTest(command, '1234');
  });

  test("runtime hello with port provided in configuration file", async () => {
     const mockOperonConfigYamlString = `
      database:
        hostname: 'localhost'
        port: 5432
        username: 'postgres'
        connectionTimeoutMillis: 3000
      localRuntimeConfig:
          port: 1234
    `;
    jest
      .spyOn(utils, "readFileSync")
      .mockReturnValueOnce(mockOperonConfigYamlString);

    const command = spawn('../../dist/src/operon-runtime/cli.js', ['start'], {
      env: process.env
    });
    await waitForMessageTest(command, '1234');
  });
});
