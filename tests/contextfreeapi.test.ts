import { DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

class TestFunctions
{
  @DBOS.transaction()
  static async doTransaction(arg: string) {
    await DBOS.pgClient.query("SELECT 1");
    return Promise.resolve(`selected ${arg}`);
  }

  @DBOS.step()
  static async doStep(name: string) {
    return Promise.resolve(`step ${name} done`);
  }

  @DBOS.workflow()
  static async doWorkflow() {
    await TestFunctions.doTransaction("");
    return 'done';
  }

  @DBOS.workflow()
  static async doWorkflowAAAAA() {
    expect(DBOS.workflowID).toBe('aaaaa');
    await TestFunctions.doTransaction("");
    return 'done';
  }

  @DBOS.workflow()
  static async doWorkflowArg(arg: string) {
    await TestFunctions.doTransaction("");
    return `done ${arg}`;
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

  // Try a second run
  await DBOS.launch();
  const res2 = await TestFunctions.doWorkflow();
  expect (res2).toBe('done');
  await DBOS.shutdown();  
}

async function main2() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);

  await DBOS.launch();
  const res = await DBOS.withNextWorkflowID('aaaaa', async ()=>{
    return await TestFunctions.doWorkflowAAAAA();
  });
  expect (res).toBe('done');

  // Validate that it had the ID given...
  const wfh = DBOS.retrieveWorkflow('aaaaa');
  expect (await wfh.getResult()).toBe('done');

  await DBOS.shutdown();
}

async function main3() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const handle = await DBOS.startWorkflow(TestFunctions.doWorkflowArg, 'a');
  expect (await handle.getResult()).toBe('done a');

  await DBOS.shutdown();
}

async function main4() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const tres = await TestFunctions.doTransaction('a');
  expect(tres).toBe("selected a");

  const sres = await TestFunctions.doStep('a');
  expect(sres).toBe("step a done");

  await DBOS.shutdown();
}

// TODO:
//  Workflow Q
//  Child workflows
//  Send/Recv; SetEvent/ GetEvent
//  Roles / Auth
//  Recovery
//  Configured instances
//  Cleanup

describe("dbos-v2api-tests-main", () => {
  test("simple-functions", async () => {
    await main();
  }, 15000);

  test("assign_workflow_id", async() => {
    await main2();
  }, 15000);

  test("start_workflow", async() => {
    await main3();
  }, 15000);

  test("temp_step_transaction", async() => {
    await main4();
  }, 15000);
});
