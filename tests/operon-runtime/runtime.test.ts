/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { execSync } from "child_process";
import { OperonRuntime } from "src/operon-runtime/runtime";
import axios from "axios";

describe("runtime-tests", () => {

  let runtime: OperonRuntime;

  beforeAll(() => {
    process.chdir('examples/hello');
    execSync('npm run build').toString();
  });

  afterAll(() => {
    process.chdir('../..');
  });

  beforeEach(async () => {
    runtime = new OperonRuntime();
    await runtime.startServer(3000);
  });

  afterEach(async () => {
    await runtime.destroy();
  });


  test("runtime-hello", async () => {
    const bob = await axios.get('http://localhost:3000/greeting/operon');
  });
});
