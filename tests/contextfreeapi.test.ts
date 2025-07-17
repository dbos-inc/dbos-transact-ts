import {
  ArgOptional,
  ArgRequired,
  Authentication,
  DBOS,
  DBOSResponseError,
  DefaultArgValidate,
  KoaMiddleware,
  MiddlewareContext,
  WorkflowQueue,
} from '../src';
import { UserDatabaseName } from '../src/user_database';
import { generateDBOSTestConfig, generatePublicDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from './helpers';
import jwt from 'koa-jwt';

DBOS.logger.info('This should not cause a kaboom.');

class TestFunctions {
  @DBOS.transaction()
  static async doTransaction(arg: string) {
    await DBOS.pgClient.query('SELECT 1');
    return Promise.resolve(`selected ${arg}`);
  }

  @DBOS.step()
  static async doStep(name: string) {
    return Promise.resolve(`step ${name} done`);
  }

  @DBOS.workflow()
  static async doWorkflow() {
    await TestFunctions.doTransaction('');
    return 'done';
  }

  @DBOS.workflow()
  static async doWorkflowAAAAA() {
    expect(DBOS.workflowID).toBe('aaaaa');
    await TestFunctions.doTransaction('');
    return 'done';
  }

  @DBOS.workflow()
  static async doWorkflowArg(arg: string) {
    await TestFunctions.doTransaction('');
    return `done ${arg}`;
  }

  static nSchedCalls = 0;
  @DBOS.scheduled({ crontab: '* * * * * *' })
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
    await DBOS.send(destinationID, 'message1');
    await DBOS.send(destinationID, 'message2');
  }

  @DBOS.workflow()
  static async setEventWorkflow(v1: string, v2: string) {
    await DBOS.setEvent('key1', v1);
    await DBOS.setEvent('key2', v2);
    return Promise.resolve(0);
  }

  @DBOS.workflow()
  static async getEventWorkflow(wfid: string) {
    const kv1 = await DBOS.getEvent<string>(wfid, 'key1');
    const kv2 = await DBOS.getEvent<string>(wfid, 'key2');
    return kv1 + ',' + kv2;
  }

  static awaitThis: Promise<void> | undefined = undefined;
  @DBOS.workflow()
  static async awaitAPromise() {
    await TestFunctions.awaitThis;
  }

  @DBOS.workflow()
  static async argOptionalWorkflow(arg?: string) {
    return Promise.resolve(arg);
  }

  @DBOS.workflow()
  static async argRequiredWorkflow(@ArgRequired arg: string) {
    return Promise.resolve(arg);
  }
}

@DefaultArgValidate
class OptionalArgs {
  @DBOS.workflow()
  static async argOptionalWorkflow(@ArgOptional arg?: string) {
    return Promise.resolve(arg);
  }

  @DBOS.workflow()
  static async argRequiredWorkflow(arg: string) {
    return Promise.resolve(arg);
  }

  @DBOS.workflow()
  static async argOptionalOops(@ArgOptional arg?: string) {
    return OptionalArgs.argRequiredWorkflow(arg!);
  }
}

const testTableName = 'dbos_test_kv';

@DBOS.defaultRequiredRole(['user'])
class TestSec {
  @DBOS.requiredRole([])
  @DBOS.workflow()
  static async testAuth(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.transaction()
  static async testTranscation(name: string) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(
      `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
      [name],
    );
    return `hello ${name} ${rows[0].id}`;
  }

  @DBOS.workflow()
  static async testWorkflow(name: string) {
    const res = await TestSec.testTranscation(name);
    return res;
  }
}

class TestSec2 {
  @DBOS.requiredRole(['user'])
  @DBOS.workflow()
  static async bye() {
    return Promise.resolve(`bye ${DBOS.assumedRole} ${DBOS.authenticatedUser}!`);
  }
}

class ChildWorkflows {
  @DBOS.transaction()
  static async childTx() {
    await DBOS.pgClient.query('SELECT 1');
    return Promise.resolve(`selected ${DBOS.workflowID}`);
  }

  @DBOS.workflow()
  static async childWF() {
    const tres = await ChildWorkflows.childTx();
    return `ChildID:${DBOS.workflowID}|${tres}`;
  }

  @DBOS.workflow()
  static async callSubWF() {
    const cres = await ChildWorkflows.childWF();
    return `ParentID:${DBOS.workflowID}|${cres}`;
  }

  @DBOS.workflow()
  static async startSubWF() {
    const cwfh = await DBOS.startWorkflow(ChildWorkflows).childWF();
    const cres = await cwfh.getResult();
    return `ParentID:${DBOS.workflowID}|${cres}`;
  }
}

const testJwt = jwt({
  secret: 'your-secret-goes-here',
});

export async function testAuthMiddleware(ctx: MiddlewareContext) {
  // Only extract user and roles if the operation specifies required roles.
  if (ctx.requiredRole.length > 0) {
    //console.log("required role: ", ctx.requiredRole);
    if (!ctx.koaContext.state.user) {
      throw new DBOSResponseError('No authenticated user!', 401);
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
    const authenticatedUser: string = ctx.koaContext.state.user['preferred_username'] ?? '';
    //console.log("current user: ", authenticatedUser);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access
    const authenticatedRoles: string[] = ctx.koaContext.state.user['realm_access']['roles'] ?? [];
    //console.log("JWT claimed roles: ", authenticatedRoles);
    if (authenticatedRoles.includes('appAdmin')) {
      // appAdmin role has more priviledges than appUser.
      authenticatedRoles.push('appUser');
    }
    //console.log("authenticated roles: ", authenticatedRoles);
    return Promise.resolve({ authenticatedUser: authenticatedUser, authenticatedRoles: authenticatedRoles });
  }
}

@DBOS.defaultRequiredRole(['appUser'])
@Authentication(testAuthMiddleware)
@KoaMiddleware(testJwt)
export class AuthTestOps {
  @DBOS.transaction()
  @DBOS.getApi('/api/list_all')
  static async listAccountsFunc() {
    return Promise.resolve('ok');
  }

  @DBOS.transaction()
  @DBOS.postApi('/api/create_account')
  @DBOS.requiredRole(['appAdmin']) // Only an admin can create a new account.
  static async createAccountFunc() {
    return Promise.resolve('ok');
  }
}

export class TransitionTests {
  @DBOS.workflow()
  static async calledWorkflow() {
    return Promise.resolve('ohno');
  }

  @DBOS.transaction()
  static async leafTransaction() {
    return Promise.resolve('ok');
  }

  @DBOS.transaction()
  static async oopsCallTransaction() {
    return await TransitionTests.leafTransaction();
  }

  @DBOS.transaction()
  static async oopsCallSleep() {
    await DBOS.sleepms(100);
  }

  @DBOS.step({ retriesAllowed: false })
  static async sleepStep() {
    await DBOS.sleepms(100);
  }

  @DBOS.transaction()
  static async oopsCallStep() {
    await TransitionTests.sleepStep();
  }

  @DBOS.step({ retriesAllowed: false })
  static async oopsCallTransactionFromStep() {
    return TransitionTests.leafTransaction();
  }

  @DBOS.step({ retriesAllowed: false })
  static async callStepFromStep() {
    return TransitionTests.sleepStep();
  }

  @DBOS.transaction()
  static async oopsCallSendFromTx() {
    await DBOS.send('aaa', 'a', 'aa');
  }

  @DBOS.step({ retriesAllowed: false })
  static async oopsCallSendFromStep() {
    await DBOS.send('aaa', 'a', 'aa');
  }

  @DBOS.transaction()
  static async oopsCallGetFromTx() {
    await DBOS.getEvent('aaa', 'a');
  }

  @DBOS.step({ retriesAllowed: false })
  static async oopsCallGetFromStep() {
    await DBOS.getEvent('aaa', 'a');
  }

  @DBOS.transaction()
  static async oopsCallWFFromTransaction() {
    return await TransitionTests.calledWorkflow();
  }

  @DBOS.step()
  static async oopsCallWFFromStep() {
    return await TransitionTests.calledWorkflow();
  }

  @DBOS.transaction()
  static async oopsCallStartWFFromTransaction() {
    return await DBOS.startWorkflow(TransitionTests).calledWorkflow();
  }

  @DBOS.step()
  static async oopsCallStartWFFromStep() {
    return await DBOS.startWorkflow(TransitionTests).calledWorkflow();
  }
}

async function main() {
  // First hurdle - configuration.
  const config = generatePublicDBOSTestConfig({
    userDbClient: UserDatabaseName.PGNODE,
  });
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();
  try {
    const res = await TestFunctions.doWorkflow();
    expect(res).toBe('done');

    // Check for this to have run
    const wfs = await DBOS.listWorkflows({ workflowName: 'doWorkflow' });
    expect(wfs.length).toBeGreaterThanOrEqual(1);
    expect(wfs.length).toBe(1);
    const wfh = DBOS.retrieveWorkflow(wfs[0].workflowID);
    expect((await wfh.getStatus())?.status).toBe('SUCCESS');
    const wfstat = await DBOS.getWorkflowStatus(wfs[0].workflowID);
    expect(wfstat?.status).toBe('SUCCESS');
  } finally {
    await DBOS.shutdown();
  }
  // Try a second run
  await DBOS.launch();
  try {
    const res2 = await TestFunctions.doWorkflow();
    expect(res2).toBe('done');
  } finally {
    await DBOS.shutdown();
  }
}

async function main2() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  try {
    const res = await DBOS.withNextWorkflowID('aaaaa', async () => {
      return await TestFunctions.doWorkflowAAAAA();
    });
    expect(res).toBe('done');

    // Validate that it had the ID given...
    const wfh = DBOS.retrieveWorkflow('aaaaa');
    expect(await wfh.getResult()).toBe('done');
  } finally {
    await DBOS.shutdown();
  }
}

async function main3() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();
  try {
    const handle = await DBOS.startWorkflow(TestFunctions).doWorkflowArg('a');
    expect(await handle.getResult()).toBe('done a');
  } finally {
    await DBOS.shutdown();
  }
}

async function main4() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  try {
    const tres = await TestFunctions.doTransaction('a');
    expect(tres).toBe('selected a');

    const sres = await TestFunctions.doStep('a');
    expect(sres).toBe('step a done');
  } finally {
    await DBOS.shutdown();
  }
}

async function main5() {
  const wfq = new WorkflowQueue('wfq');
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  try {
    const res = await DBOS.withWorkflowQueue(wfq.name, async () => {
      return await TestFunctions.doWorkflow();
    });
    expect(res).toBe('done');

    const wfs = await DBOS.listWorkflows({ workflowName: 'doWorkflow' });
    expect(wfs.length).toBe(1);
    const wfstat = await DBOS.getWorkflowStatus(wfs[0].workflowID);
    expect(wfstat?.queueName).toBe('wfq');

    // Check queues in startWorkflow
    let resolve: () => void = () => {};
    TestFunctions.awaitThis = new Promise<void>((r) => {
      resolve = r;
    });

    const wfhq = await DBOS.startWorkflow(TestFunctions, {
      workflowID: 'waitPromiseWF',
      queueName: wfq.name,
    }).awaitAPromise();
    const wfstatsw = await DBOS.getWorkflowStatus('waitPromiseWF');
    expect(wfstatsw?.queueName).toBe('wfq');

    // Validate that it had the queue
    const wfqcontent = await DBOS.listQueuedWorkflows({ queueName: wfq.name });
    expect(wfqcontent.length).toBe(1);
    expect(wfqcontent[0].workflowID).toBe('waitPromiseWF');

    resolve(); // Let WF finish
    await wfhq.getResult();

    // Quick check on scheduled WFs
    await DBOS.sleepSeconds(2);
    expect(TestFunctions.nSchedCalls).toBeGreaterThanOrEqual(2);
  } finally {
    await DBOS.shutdown();
  }
}

async function main6() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  try {
    const wfhandle = await DBOS.startWorkflow(TestFunctions).getEventWorkflow('wfidset');
    await DBOS.withNextWorkflowID('wfidset', async () => {
      await TestFunctions.setEventWorkflow('a', 'b');
    });
    const res = await wfhandle.getResult();

    expect(res).toBe('a,b');
    expect(await DBOS.getEvent('wfidset', 'key1')).toBe('a');
    expect(await DBOS.getEvent('wfidset', 'key2')).toBe('b');

    const wfhandler = await DBOS.withNextWorkflowID('wfidrecv', async () => {
      return await DBOS.startWorkflow(TestFunctions).receiveWorkflow('r');
    });
    await TestFunctions.sendWorkflow('wfidrecv');
    const rres = await wfhandler.getResult();
    expect(rres).toBe('r:message1|message2');

    const wfhandler2 = await DBOS.withNextWorkflowID('wfidrecv2', async () => {
      return await DBOS.startWorkflow(TestFunctions).receiveWorkflow('r2');
    });
    await DBOS.send('wfidrecv2', 'm1');
    await DBOS.send('wfidrecv2', 'm2');
    const rres2 = await wfhandler2.getResult();
    expect(rres2).toBe('r2:m1|m2');
  } finally {
    await DBOS.shutdown();
  }
}

async function main7() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();
  try {
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);

    await expect(async () => {
      await TestSec.testWorkflow('unauthorized');
    }).rejects.toThrow('User does not have a role with permission to call testWorkflow');

    const res = await TestSec.testAuth('and welcome');
    expect(res).toBe('hello and welcome');

    await expect(async () => {
      await TestSec2.bye();
    }).rejects.toThrow('User does not have a role with permission to call bye');

    const hijoe = await DBOS.withAuthedContext('joe', ['user'], async () => {
      return await TestSec.testWorkflow('joe');
    });
    expect(hijoe).toBe('hello joe 1');

    const byejoe = await DBOS.withAuthedContext('joe', ['user'], async () => {
      return await TestSec2.bye();
    });
    expect(byejoe).toBe('bye user joe!');

    await DBOS.withAuthedContext('admin', ['appAdmin'], async () => {
      expect(await AuthTestOps.createAccountFunc()).toBe('ok');
    });
  } finally {
    await DBOS.shutdown();
  }
}

async function main8() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();

  try {
    const res = await DBOS.withNextWorkflowID('child-direct', async () => {
      return await ChildWorkflows.callSubWF();
    });
    expect(res).toBe('ParentID:child-direct|ChildID:child-direct-0|selected child-direct-0');

    const cres = await DBOS.withNextWorkflowID('child-start', async () => {
      return await ChildWorkflows.startSubWF();
    });
    expect(cres).toBe('ParentID:child-start|ChildID:child-start-0|selected child-start-0');
  } finally {
    await DBOS.shutdown();
  }
}

async function main9() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();
  try {
    await TransitionTests.leafTransaction();
    await expect(() => TransitionTests.oopsCallTransaction()).rejects.toThrow(
      'Invalid call to a `transaction` function from within a `transaction`',
    );
    await expect(() => TransitionTests.oopsCallSleep()).rejects.toThrow(
      'Invalid call to `DBOS.sleep` inside a `transaction`',
    );
    await TransitionTests.sleepStep();
    await expect(() => TransitionTests.oopsCallStep()).rejects.toThrow(
      'Invalid call to a `step` function from within a `transaction`',
    );
    await TransitionTests.callStepFromStep();
    await expect(() => TransitionTests.oopsCallTransactionFromStep()).rejects.toThrow(
      'Invalid call to a `transaction` function from within a `step`',
    );

    await expect(() => TransitionTests.oopsCallSendFromTx()).rejects.toThrow(
      'Invalid call to `DBOS.send` inside a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallSendFromStep()).rejects.toThrow(
      'Invalid call to `DBOS.send` inside a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallGetFromTx()).rejects.toThrow(
      'Invalid call to `DBOS.getEvent` inside a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallGetFromStep()).rejects.toThrow(
      'Invalid call to `DBOS.getEvent` inside a `step` or `transaction`',
    );

    await expect(() => TransitionTests.oopsCallWFFromTransaction()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallWFFromStep()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallStartWFFromTransaction()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallStartWFFromStep()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
  } finally {
    await DBOS.shutdown();
  }
}

async function main10() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();
  try {
    // Shouldn't throw a validation error
    await TestFunctions.argOptionalWorkflow('a');
    await TestFunctions.argOptionalWorkflow();
    await expect(async () => {
      await TestFunctions.argRequiredWorkflow((undefined as string | undefined)!);
    }).rejects.toThrow();

    await OptionalArgs.argOptionalWorkflow('a');
    await OptionalArgs.argOptionalWorkflow();

    // await OptionalArgs.argRequiredWorkflow(); // Using the compiler for what it is good at
    await OptionalArgs.argRequiredWorkflow('a');

    await OptionalArgs.argOptionalOops('a');
    await expect(async () => {
      await OptionalArgs.argOptionalOops();
    }).rejects.toThrow();
  } finally {
    await DBOS.shutdown();
  }
}

async function main11() {
  expect(() => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    require('./baddecorator'); // Load it dynamically inside expect
  }).toThrow('Operation (Name: BadDecoratorClass.cantBeBoth) is already registered with a conflicting function type');
  return Promise.resolve();
}

describe('dbos-v2api-tests-main', () => {
  test.skip('simple-functions', async () => {
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

  test('child-wf', async () => {
    await main8();
  }, 15000);

  test('transitions', async () => {
    await main9();
  }, 15000);

  test('argvalidate', async () => {
    await main10();
  }, 15000);

  test('double decorator error', async () => {
    await main11();
  }, 15000);
});
