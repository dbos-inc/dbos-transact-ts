import { DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

class TestFunctions
{
  @DBOS.transaction()
  static async doTransaction() {
    await DBOS.pgClient.query("SELECT 1");
    return Promise.resolve();
  }

  @DBOS.workflow()
  static async doWorkflow() {
    await TestFunctions.doTransaction();
    return 'done';
  }
}

async function main() {
  // First hurdle - configuration.
  const config = generateDBOSTestConfig(); // Optional.  If you don't, it'll open the YAML file...
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);

  await DBOS.launch();

  const res = await TestFunctions.doWorkflow();
  expect (res).toBe('done');

  // Check for this to have run
  const wfs = await DBOS.getWorkflows({workflowName: 'doWorkflow'});
  expect(wfs.workflowUUIDs.length).toBeGreaterThanOrEqual(1);
  expect(wfs.workflowUUIDs.length).toBe(1);
  await DBOS.executor.flushWorkflowBuffers();
  const wfh = DBOS.retrieveWorkflow(wfs.workflowUUIDs[0]);
  expect((await wfh.getStatus())?.status).toBe('SUCCESS');
  const wfstat = await DBOS.getWorkflowStatus(wfs.workflowUUIDs[0]);
  expect(wfstat?.status).toBe('SUCCESS');

  await DBOS.shutdown();
}

// TODO:
//  Start workflow
//  Workflow UUID
//  Workflow Q
//  Send/Recv; SetEvent/ GetEvent
//  Bare Tx
//  Bare Communicator
//  Roles / Auth
//  Recovery

describe("dbos-v2api-tests-main", () => {
  test("simple-functions", async () => {
    await main();
  })
});
