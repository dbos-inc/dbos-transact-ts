/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
<<<<<<< HEAD
import axios from "axios";
import { spawn, execSync } from "child_process";
import { Client } from "pg";
import { generateOperonTestConfig, setupOperonTestDb } from "tests/helpers";

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
=======
import { execSync } from "child_process";
import { OperonRuntime } from "src/operon-runtime/runtime";
import axios from "axios";

describe("runtime-tests", () => {

  let runtime: OperonRuntime;

  beforeAll(() => {
    process.chdir('examples/hello');
    execSync('npm run build').toString();
>>>>>>> 5649fb6 (broken tests)
  });

  afterAll(() => {
    process.chdir('../..');
  });

<<<<<<< HEAD
  test("runtime-hello", async () => {
    const command = spawn('../../dist/src/operon-runtime/cli.js', ['start']);

    const waitForMessage = new Promise<void>((resolve, reject) => {
      const onData = (data: Buffer) => {
        const message = data.toString();
        process.stdout.write(message);
        if (message.includes('Server is running at')) {
          command.stdout.off('data', onData);  // remove listener
          resolve();
        }
      };

      command.stdout.on('data', onData);

      command.on('error', (error) => {
        reject(error);  // Reject promise on command error
      });
    });
    try {
      await waitForMessage;
      const response = await axios.get('http://127.0.0.1:3000/greeting/operon');
      expect(response.status).toBe(200);
    } finally {
      command.stdin.end();
      command.stdout.destroy();
      command.stderr.destroy();
      command.kill();
    }
=======
  beforeEach(async () => {
    runtime = new OperonRuntime();
    await runtime.startServer(3000);
  });

  afterEach(async () => {
    await runtime.destroy();
  });


  test("runtime-hello", async () => {
    const bob = await axios.get('http://localhost:3000/greeting/operon');
>>>>>>> 5649fb6 (broken tests)
  });
});
