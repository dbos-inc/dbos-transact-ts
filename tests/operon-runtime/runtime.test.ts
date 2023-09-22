/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import axios from "axios";
import { spawn, execSync } from "child_process";
import { Client } from "pg";
import { OperonRuntime } from "src/operon-runtime/runtime";
import { sleep } from "src/utils";
import { generateOperonTestConfig, setupOperonTestDb } from "tests/helpers";

describe("runtime-tests", () => {

  let runtime: OperonRuntime;

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

  test("runtime-hello", async () => {
    const command = spawn('../../dist/src/operon-runtime/cli.js', ['start']);

    const waitForMessage = new Promise<void>((resolve, reject) => {
      const onData = (data: Buffer) => {
        const message = data.toString();
        process.stdout.write(message);
        if (message.includes('Starting server on port: 3000')) {
          command.stdout.off('data', onData);  // remove listener
          resolve();
        }
      };

      command.stdout.on('data', onData);

      command.on('error', (error) => {
        reject(error);  // Reject promise on command error
      });

      command.stderr.on('data', (data) => {
        process.stderr.write(data.toString());
      });
    });
    try {
      await waitForMessage;
      await sleep(100);
      const response = await axios.get('http://127.0.0.1:3000/greeting/operon');
      expect(response.status).toBe(200);
    } finally {
      command.stdin.end();
      command.stdout.destroy();
      command.stderr.destroy();
      command.kill();
    }
  });
});
