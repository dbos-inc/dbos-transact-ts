import { ConfiguredInstance, DBOS, DBOSConfig, WorkflowQueue } from '../src';
import { generateDBOSTestConfig, recoverPendingWorkflows, setUpDBOSTestDb } from './helpers';

class TestFunctions extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  @DBOS.step()
  async doStep(name: string) {
    return Promise.resolve(`step ${name} done from ${this.name}`);
  }

  @DBOS.workflow()
  async doWorkflow() {
    await this.doStep('');
    return `done ${this.name}`;
  }

  @DBOS.workflow()
  async doWorkflowAAAAA() {
    expect(DBOS.workflowID).toBe('aaaaa');
    await this.doStep('');
    return `done ${this.name}`;
  }

  @DBOS.workflow()
  async doWorkflowArg(arg: string) {
    await this.doStep('');
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
    await DBOS.send(destinationID, 'message1');
    await DBOS.send(destinationID, 'message2');
  }

  @DBOS.workflow()
  async setEventWorkflow(v1: string, v2: string) {
    await DBOS.setEvent('key1', v1);
    await DBOS.setEvent('key2', v2);
    return Promise.resolve(0);
  }

  @DBOS.workflow()
  async getEventWorkflow(wfid: string) {
    const kv1 = await DBOS.getEvent<string>(wfid, 'key1');
    const kv2 = await DBOS.getEvent<string>(wfid, 'key2');
    return kv1 + ',' + kv2;
  }

  awaitThis: Promise<void> | undefined = undefined;
  @DBOS.workflow()
  async awaitAPromise() {
    await this.awaitThis;
  }
}

@DBOS.defaultRequiredRole(['user'])
class TestSec extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  @DBOS.requiredRole([])
  @DBOS.workflow()
  async testAuth(name: string) {
    return Promise.resolve(`hello ${name} from ${this.name}`);
  }

  @DBOS.workflow()
  async testWorkflow(name: string) {
    return Promise.resolve(name);
  }
}

class TestSec2 extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  async initialize() {
    return Promise.resolve();
  }

  @DBOS.requiredRole(['user'])
  @DBOS.workflow()
  async bye() {
    return Promise.resolve(`bye ${DBOS.assumedRole} ${DBOS.authenticatedUser} from ${this.name}!`);
  }
}

const instA = new TestFunctions('A');
const instB = new TestFunctions('B');

async function main() {
  // First hurdle - configuration.
  const config = generateDBOSTestConfig(); // Optional.  If you don't, it'll open the YAML file...
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const resA = await instA.doWorkflow();
  expect(resA).toBe('done A');
  const resB = await instB.doWorkflow();
  expect(resB).toBe('done B');

  // Check for this to have run
  const wfs = await DBOS.listWorkflows({ workflowName: 'doWorkflow' });
  expect(wfs.length).toBeGreaterThanOrEqual(2);
  expect(wfs.length).toBe(2);
  const wfh1 = DBOS.retrieveWorkflow(wfs[0].workflowID);
  const wfh2 = DBOS.retrieveWorkflow(wfs[1].workflowID);
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

  const res = await DBOS.withNextWorkflowID('aaaaa', async () => {
    return await instA.doWorkflowAAAAA();
  });
  expect(res).toBe('done A');

  // Validate that it had the ID given...
  const wfh = DBOS.retrieveWorkflow('aaaaa');
  expect(await wfh.getResult()).toBe('done A');

  await DBOS.shutdown();
}

async function main3() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const handle = await DBOS.startWorkflow(instA).doWorkflowArg('a');
  expect(await handle.getResult()).toBe('done a from A');

  await DBOS.shutdown();
}

async function main4() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const sres1 = await instA.doStep('a');
  expect(sres1).toBe('step a done from A');
  const sres2 = await instB.doStep('b');
  expect(sres2).toBe('step b done from B');

  await DBOS.shutdown();
}

async function main5() {
  const wfq = new WorkflowQueue('wfq');
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const res = await DBOS.withWorkflowQueue(wfq.name, async () => {
    return await instA.doWorkflow();
  });
  expect(res).toBe('done A');

  const wfs = await DBOS.listWorkflows({ workflowName: 'doWorkflow' });
  expect(wfs.length).toBeGreaterThanOrEqual(1);
  expect(wfs.length).toBe(1);
  const wfstat = await DBOS.getWorkflowStatus(wfs[0].workflowID);
  expect(wfstat?.queueName).toBe('wfq');
  expect(wfstat?.workflowConfigName).toBe('A');

  // Check queues in startWorkflow
  let resolve: () => void = () => {};
  instA.awaitThis = new Promise<void>((r) => {
    resolve = r;
  });

  const wfhq = await DBOS.startWorkflow(instA, { workflowID: 'waitPromiseWFI', queueName: wfq.name }).awaitAPromise();
  const wfstatsw = await DBOS.getWorkflowStatus('waitPromiseWFI');
  expect(wfstatsw?.queueName).toBe('wfq');

  // Validate that it had the queue
  const wfqcontent = await DBOS.listQueuedWorkflows({ queueName: wfq.name });
  expect(wfqcontent.length).toBe(1);
  expect(wfqcontent[0].workflowID).toBe('waitPromiseWFI');

  resolve(); // Let WF finish
  await wfhq.getResult();

  await DBOS.shutdown();
}

async function main6() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  const wfhandle = await DBOS.startWorkflow(instA).getEventWorkflow('wfidset');
  await DBOS.withNextWorkflowID('wfidset', async () => {
    await instA.setEventWorkflow('a', 'b');
  });
  const res = await wfhandle.getResult();

  expect(res).toBe('a,b');
  expect(await DBOS.getEvent('wfidset', 'key1')).toBe('a');
  expect(await DBOS.getEvent('wfidset', 'key2')).toBe('b');

  const wfhandler = await DBOS.withNextWorkflowID('wfidrecv', async () => {
    return await DBOS.startWorkflow(instA).receiveWorkflow('r');
  });
  await instA.sendWorkflow('wfidrecv');
  const rres = await wfhandler.getResult();
  expect(rres).toBe('A/r:message1|message2');

  const wfhandler2 = await DBOS.withNextWorkflowID('wfidrecv2', async () => {
    return await DBOS.startWorkflow(instA).receiveWorkflow('r2');
  });
  await DBOS.send('wfidrecv2', 'm1');
  await DBOS.send('wfidrecv2', 'm2');
  const rres2 = await wfhandler2.getResult();
  expect(rres2).toBe('A/r2:m1|m2');

  await DBOS.shutdown();
}

async function main7() {
  const testSecInst = new TestSec('Sec1');
  const testSec2Inst = new TestSec2('Sec2');

  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  await expect(async () => {
    await testSecInst.testWorkflow('unauthorized');
  }).rejects.toThrow('User does not have a role with permission to call testWorkflow');

  const res = await testSecInst.testAuth('and welcome');
  expect(res).toBe('hello and welcome from Sec1');

  await expect(async () => {
    await testSec2Inst.bye();
  }).rejects.toThrow('User does not have a role with permission to call bye');

  const hijoe = await DBOS.withAuthedContext('joe', ['user'], async () => {
    return await testSecInst.testWorkflow('joe');
  });
  expect(hijoe).toBe('joe');

  const byejoe = await DBOS.withAuthedContext('joe', ['user'], async () => {
    return await testSec2Inst.bye();
  });
  expect(byejoe).toBe('bye user joe from Sec2!');

  await DBOS.shutdown();
}

// TODO:
//  Child workflows

describe('dbos-v2api-tests-main', () => {
  test('simple-functions', async () => {
    await main();
  }, 15000);

  test('assign_workflow_id', async () => {
    await main2();
  }, 15000);

  test('start_workflow', async () => {
    await main3();
  }, 15000);

  test('temp_step_transaction', async () => {
    await main4();
  }, 15000);

  test('assign_workflow_queue', async () => {
    await main5();
  }, 15000);

  test('send_recv_get_set', async () => {
    await main6();
  }, 15000);

  test('roles', async () => {
    await main7();
  }, 15000);
});

type RF = () => void;
class CCRConfig {
  resolve1: RF | undefined = undefined;
  promise1: Promise<void>;
  resolve2: RF | undefined = undefined;
  promise2: Promise<void>;

  constructor() {
    this.promise1 = new Promise<void>((resolve) => {
      this.resolve1 = resolve;
    });
    this.promise2 = new Promise<void>((resolve) => {
      this.resolve2 = resolve;
    });
  }

  count: number = 0;
}

/**
 * Test for the default local workflow recovery for configured classes.
 */
class CCRecovery extends ConfiguredInstance {
  constructor(
    name: string,
    readonly config: CCRConfig,
  ) {
    super(name);
  }

  initialized: boolean = false;

  initialize(): Promise<void> {
    this.initialized = true;
    return Promise.resolve();
  }

  @DBOS.workflow()
  async testRecoveryWorkflow(input: number) {
    expect(this.initialized).toBeTruthy();
    this.config.count += input;

    // Signal the workflow has been executed more than once.
    if (this.config.count > input) {
      this.config.resolve2!();
    }

    await this.config.promise1;
    return this.name;
  }
}

const configA = new CCRecovery('configA', new CCRConfig());
const configB = new CCRecovery('configB', new CCRConfig());

describe('recovery-cc-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    process.env.DBOS__VMID = '';
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('local-recovery', async () => {
    const handleA = await DBOS.startWorkflow(configA).testRecoveryWorkflow(5);
    const handleB = await DBOS.startWorkflow(configB).testRecoveryWorkflow(5);

    const recoverHandles = await recoverPendingWorkflows();
    await configA.config.promise2; // Wait for the recovery to be done.
    await configB.config.promise2; // Wait for the recovery to be done.
    configA.config.resolve1!(); // Both A can finish now.
    configB.config.resolve1!(); // Both B can finish now.

    expect(recoverHandles.length).toBe(2);
    await expect(recoverHandles[0].getResult()).resolves.toBeTruthy();
    await expect(recoverHandles[1].getResult()).resolves.toBeTruthy();
    await expect(handleA.getResult()).resolves.toBe('configA');
    await expect(handleB.getResult()).resolves.toBe('configB');
    expect(configA.config.count).toBe(10); // Should run twice.
    expect(configB.config.count).toBe(10); // Should run twice.
  });
});
