import { DBOS, WorkflowQueue } from '../src';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from './helpers';

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

  static nSchedCalls = 0;
  @DBOS.scheduled({crontab: '* * * * * *'})
  @DBOS.workflow()
  static async doCron(_sdate: Date, _cdate: Date) {
    ++TestFunctions.nSchedCalls;
    return Promise.resolve();
  }

  @DBOS.workflow()
  static async receiveWorkflow(v: string) {
    const message1 = await DBOS.recv<string>();
    const message2 = await DBOS.recv<string>();
    return v + ':' + message1 + '|' + message2;
  }

  @DBOS.workflow()
  static async sendWorkflow(destinationID: string) {
    await DBOS.send(destinationID, "message1");
    await DBOS.send(destinationID, "message2");
  }

  @DBOS.workflow()
  static async setEventWorkflow(v1: string, v2: string) {
    await DBOS.setEvent("key1", v1);
    await DBOS.setEvent("key2", v2);
    return Promise.resolve(0);
  }

  @DBOS.workflow()
  static async getEventWorkflow(wfid: string) {
    const kv1 = await DBOS.getEvent<string>(wfid, "key1");
    const kv2 = await DBOS.getEvent<string>(wfid, "key2");
    return kv1+','+kv2;
  }
}

const testTableName = "dbos_test_kv";

@DBOS.defaultRequiredRole(["user"])
class TestSec {
  @DBOS.requiredRole([])
  @DBOS.workflow()
  static async testAuth(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.transaction()
  static async testTranscation(name: string) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
    return `hello ${rows[0].id}`;
  }

  @DBOS.workflow()
  static async testWorkflow(name: string) {
    const res = await TestSec.testTranscation(name);
    return res;
  }
}

class TestSec2 {
  @DBOS.requiredRole(["user"])
  @DBOS.workflow()
  static async bye() {
    return Promise.resolve({ message: `bye ${DBOS.assumedRole} ${DBOS.authenticatedUser}!` });
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

  const handle = await DBOS.startWorkflow(TestFunctions).doWorkflowArg('a');
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

async function main5() {
  const wfq = new WorkflowQueue('wfq');
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const res = await DBOS.withWorkflowQueue(wfq.name, async ()=>{
    return await TestFunctions.doWorkflow();
  });
  expect(res).toBe('done');

  // Validate that it had the queue
  /*
  // To do when workflow can be suspended...
  const wfqcontent = await DBOS.getWorkflowQueue({queueName: wfq.name});
  expect (wfqcontent.workflows.length).toBe(1);
  */
  const wfs = await DBOS.getWorkflows({workflowName: 'doWorkflow'});
  expect(wfs.workflowUUIDs.length).toBeGreaterThanOrEqual(1);
  expect(wfs.workflowUUIDs.length).toBe(1);
  const wfstat = await DBOS.getWorkflowStatus(wfs.workflowUUIDs[0]);
  expect(wfstat?.queueName).toBe('wfq');

  await sleepms(2000);
  expect (TestFunctions.nSchedCalls).toBeGreaterThanOrEqual(2);

  await DBOS.shutdown();
}

async function main6() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  // This or: DBOS.startWorkflow(TestFunctions).getEventWorkflow('wfidset'); ?
  const wfhandle = await DBOS.startWorkflow(TestFunctions).getEventWorkflow('wfidset');
  await DBOS.withNextWorkflowID('wfidset', async() => {
    await TestFunctions.setEventWorkflow('a', 'b');
  });
  const res = await wfhandle.getResult();

  expect(res).toBe("a,b");
  expect(await DBOS.getEvent('wfidset', 'key1')).toBe('a');
  expect(await DBOS.getEvent('wfidset', 'key2')).toBe('b');

  const wfhandler = await DBOS.withNextWorkflowID('wfidrecv', async() => {
    return await DBOS.startWorkflow(TestFunctions).receiveWorkflow('r');
  });
  await TestFunctions.sendWorkflow('wfidrecv');
  const rres = await wfhandler.getResult();
  expect(rres).toBe('r:message1|message2');

  const wfhandler2 = await DBOS.withNextWorkflowID('wfidrecv2', async() => {
    return await DBOS.startWorkflow(TestFunctions).receiveWorkflow('r2');
  });
  await DBOS.send('wfidrecv2', 'm1');
  await DBOS.send('wfidrecv2', 'm2');
  const rres2 = await wfhandler2.getResult();
  expect(rres2).toBe('r2:m1|m2');

  await DBOS.shutdown();
}

async function main7() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  await expect(async()=>{
    await TestSec.testWorkflow('unauthorized');
  }).rejects.toThrow('User does not have a role with permission to call testWorkflow');
  
  const res = await TestSec.testAuth('and welcome');
  expect(res).toBe('hello and welcome');

  await expect(async()=>{
    await TestSec2.bye();
  }).rejects.toThrow('User does not have a role with permission to call bye');

  await DBOS.shutdown();
}

// TODO:
//  Child workflows
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

  test("assign_workflow_queue", async() => {
    await main5();
  }, 15000);

  test("send_recv_get_set", async() => {
    await main6();
  }, 15000);

  test("roles", async() => {
    await main7();
  }, 15000);
});
