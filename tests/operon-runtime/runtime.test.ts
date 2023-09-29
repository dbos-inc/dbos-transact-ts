/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import axios from "axios";
import { spawn, execSync, ChildProcess } from "child_process";
import { Writable } from "stream";
import { Client } from "pg";
import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import fs from "fs";

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
      // Axios will throw an exception if the return status is 500
      // Trying and catching is the only way to debug issues in this test
      try {
        const response = await axios.get(`http://127.0.0.1:${port}/greeting/operon`);
        expect(response.status).toBe(200);
      } catch (error) {
        console.error(error);
        throw error;
      }
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

  // Attention! this test relies on example/hello/operon-config.yaml not declaring a port!
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
  user_database: 'hello'
localRuntimeConfig:
  port: 6666
`;
    const filePath = 'operon-config.yaml';
    fs.copyFileSync(filePath, `${filePath}.bak`);
    fs.writeFileSync(filePath, mockOperonConfigYamlString, 'utf-8');

    const command = spawn('../../dist/src/operon-runtime/cli.js', ['start'], {
      env: process.env
    });

    try {
        await waitForMessageTest(command, '6666');
    } catch (error) {
        fs.copyFileSync(`${filePath}.bak`, filePath);
        fs.unlinkSync(`${filePath}.bak`);
        throw error;
    }
  });
});
