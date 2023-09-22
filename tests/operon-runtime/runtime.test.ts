/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import axios from "axios";
import { spawn, execSync } from "child_process";
import { OperonRuntime } from "src/operon-runtime/runtime";
import { sleep } from "src/utils";

describe("runtime-tests", () => {

  let runtime: OperonRuntime;

  beforeAll(() => {
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
      const response = await axios.get('http://localhost:3000/greeting/operon');
      expect(response.status).toBe(200);
    } finally {
      command.stdin.end();
      command.stdout.destroy();
      command.stderr.destroy();
      command.kill();
    }
  });
});
