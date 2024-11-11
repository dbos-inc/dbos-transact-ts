import { DBOS } from '../src';
import { generateDBOSTestConfig } from './helpers';

class TestFunctions
{
  @DBOS.transaction()
  static async doTransaction() {
    return Promise.resolve();
  }

  @DBOS.workflow()
  static async doWorkflow() {
    return TestFunctions.doTransaction();
  }
}

async function main() {
  // First hurdle - configuration.
  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);

  await DBOS.launch();

  await TestFunctions.doWorkflow();

  await DBOS.shutdown();
}

describe("dbos-v2api-tests-main", () => {
  test("simple-functions", async () => {
    await main();
  })
});
