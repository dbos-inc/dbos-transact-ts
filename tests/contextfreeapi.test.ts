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
  const config = generateDBOSTestConfig(); // Optional.  If you don't, it'll open the YAML file...
  DBOS.setConfig(config);

  await DBOS.launch();

  await TestFunctions.doWorkflow();

  // Check for this to have run
  const wfs = await DBOS.getWorkflows({workflowName: 'doWorkflow'});
  expect(wfs.workflowUUIDs.length).toBe(1);

  await DBOS.shutdown();
}

describe("dbos-v2api-tests-main", () => {
  test("simple-functions", async () => {
    await main();
  })
});
