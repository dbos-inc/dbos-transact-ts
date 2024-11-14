import { ConfiguredInstance, configureInstance, DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from './helpers';

class TestFunctions extends ConfiguredInstance
{
  constructor(name: string) {
    super(name);
  }

  async initialize() {
    return Promise.resolve();
  }

  @DBOS.transaction()
  async doTransaction(arg: string) {
    await DBOS.pgClient.query("SELECT 1");
    return Promise.resolve(`selected ${arg} from ${this.name}`);
  }

  @DBOS.step()
  async doStep(name: string) {
    return Promise.resolve(`step ${name} done from ${this.name}`);
  }

  @DBOS.workflow()
  async doWorkflow() {
    await this.doTransaction("");
    return `done ${this.name}`;
  }

  @DBOS.workflow()
  async doWorkflowAAAAA() {
    expect(DBOS.workflowID).toBe('aaaaa');
    await this.doTransaction("");
    return `done ${this.name}`;
  }

  @DBOS.workflow()
  async doWorkflowArg(arg: string) {
    await this.doTransaction("");
    return `done ${arg} from ${this.name}`;
  }

  @DBOS.workflow()
  async receiveWorkflow(v: string) {
    const message1 = await DBOS.recv<string>();
    const message2 = await DBOS.recv<string>();
    return this.name + '/' + v + ':' + message1 + '|' + message2;
  }

  @DBOS.workflow()
  async sendWorkflow(destinationID: string) {
    await DBOS.send(destinationID, "message1");
    await DBOS.send(destinationID, "message2");
  }

  @DBOS.workflow()
  async setEventWorkflow(v1: string, v2: string) {
    await DBOS.setEvent("key1", v1);
    await DBOS.setEvent("key2", v2);
    return Promise.resolve(0);
  }

  @DBOS.workflow()
  async getEventWorkflow(wfid: string) {
    const kv1 = await DBOS.getEvent<string>(wfid, "key1");
    const kv2 = await DBOS.getEvent<string>(wfid, "key2");
    return kv1+','+kv2;
  }
}

const testTableName = "dbos_test_kv";

@DBOS.defaultRequiredRole(["user"])
class _TestSec extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  async initialize() {
    return Promise.resolve();
  }

  @DBOS.requiredRole([])
  @DBOS.workflow()
  async testAuth(name: string) {
    return Promise.resolve(`hello ${name} from ${this.name}`);
  }

  @DBOS.transaction()
  async testTranscation(name: string) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(`INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`, [name]);
    return `hello ${rows[0].id} from ${this.name}`;
  }

  @DBOS.workflow()
  async testWorkflow(name: string) {
    const res = await this.testTranscation(name);
    return res;
  }
}

class _TestSec2 extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  async initialize() {
    return Promise.resolve();
  }

  @DBOS.requiredRole(["user"])
  @DBOS.workflow()
  static async bye() {
    return Promise.resolve({ message: `bye ${DBOS.assumedRole} ${DBOS.authenticatedUser}!` });
  }
}

const instA = configureInstance(TestFunctions, 'A');
const instB = configureInstance(TestFunctions, 'B');

async function main() {
  // First hurdle - configuration.
  const config = generateDBOSTestConfig(); // Optional.  If you don't, it'll open the YAML file...
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const resA = await instA.doWorkflow();
  expect (resA).toBe('done A');
  const resB = await instB.doWorkflow();
  expect (resB).toBe('done B');

  // Check for this to have run
  const wfs = await DBOS.getWorkflows({workflowName: 'doWorkflow'});
  expect(wfs.workflowUUIDs.length).toBeGreaterThanOrEqual(2);
  expect(wfs.workflowUUIDs.length).toBe(2);
  await DBOS.executor.flushWorkflowBuffers();
  const wfh1 = DBOS.retrieveWorkflow(wfs.workflowUUIDs[0]);
  const wfh2 = DBOS.retrieveWorkflow(wfs.workflowUUIDs[1]);
  const stat1 = await wfh1.getStatus();
  const stat2 = await wfh2.getStatus();
  expect(stat1?.status).toBe('SUCCESS');
  expect(stat2?.status).toBe('SUCCESS');
  const arr = [stat1?.workflowConfigName, stat2?.workflowConfigName];
  expect(arr.includes('A')).toBeTruthy();
  expect(arr.includes('B')).toBeTruthy();
  await DBOS.shutdown();
}

async function main2() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const res = await DBOS.withNextWorkflowID('aaaaa', async ()=>{
    return await instA.doWorkflowAAAAA();
  });
  expect (res).toBe('done A');

  // Validate that it had the ID given...
  const wfh = DBOS.retrieveWorkflow('aaaaa');
  expect (await wfh.getResult()).toBe('done A');

  await DBOS.shutdown();
}

/*
async function main3() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const handle = await DBOS.startWorkflow(TestFunctions.doWorkflowArg, 'a');
  expect (await handle.getResult()).toBe('done a');

  await DBOS.shutdown();
}
*/

async function main4() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const tres1 = await instA.doTransaction('a');
  expect(tres1).toBe("selected a from A");
  const tres2 = await instB.doTransaction('b');
  expect(tres2).toBe("selected b from B");

  const sres1 = await instA.doStep('a');
  expect(sres1).toBe("step a done from A");
  const sres2 = await instB.doStep('b');
  expect(sres2).toBe("step b done from B");

  await DBOS.shutdown();
}

/*
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
  // To do when workflow can be suspended...
  //const wfqcontent = await DBOS.getWorkflowQueue({queueName: wfq.name});
  //expect (wfqcontent.workflows.length).toBe(1);

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
  const wfhandle = await DBOS.startWorkflow(TestFunctions.getEventWorkflow, 'wfidset');
  await DBOS.withNextWorkflowID('wfidset', async() => {
    await TestFunctions.setEventWorkflow('a', 'b');
  });
  const res = await wfhandle.getResult();

  expect(res).toBe("a,b");
  expect(await DBOS.getEvent('wfidset', 'key1')).toBe('a');
  expect(await DBOS.getEvent('wfidset', 'key2')).toBe('b');

  const wfhandler = await DBOS.withNextWorkflowID('wfidrecv', async() => {
    return await DBOS.startWorkflow(TestFunctions.receiveWorkflow, 'r');
  });
  await TestFunctions.sendWorkflow('wfidrecv');
  const rres = await wfhandler.getResult();
  expect(rres).toBe('r:message1|message2');

  const wfhandler2 = await DBOS.withNextWorkflowID('wfidrecv2', async() => {
    return await DBOS.startWorkflow(TestFunctions.receiveWorkflow, 'r2');
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
*/

// TODO:
//  Child workflows
//  Roles / Auth
//  Recovery
//  Cleanup

describe("dbos-v2api-tests-main", () => {
  test("simple-functions", async () => {
    await main();
  }, 15000);

  test("assign_workflow_id", async() => {
    await main2();
  }, 15000);

  /*
  test("start_workflow", async() => {
    await main3();
  }, 15000);
  */

  test("temp_step_transaction", async() => {
    await main4();
  }, 15000);

  /*
  test("assign_workflow_queue", async() => {
    await main5();
  }, 15000);

  test("send_recv_get_set", async() => {
    await main6();
  }, 15000);

  test("roles", async() => {
    await main7();
  }, 15000);
  */
});
