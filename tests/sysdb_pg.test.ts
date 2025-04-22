// This test covers various system DB settings for the Postgres version
//  Basically, will it function without notifications, by falling back to polling?
//  Does the notification work if polling is so slow as to be discounted?

import { DBOS, DBOSConfig } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { DBOSWorkflowCancelledError } from '../src/error';
import { PostgresSystemDatabase } from '../src/system_database';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

import { v1 as uuidv1 } from 'uuid';

// We cover:  Things that wait within the DB: getResult, Send/Recv, Get/set event
//
// These are tried inside/outside a workflow (when that matters)
//  and with an immediate result, a delayed result, and lapsed timeout (if applicable).

class PGSDBTests {
  @DBOS.workflow()
  static async doRecv(topic: string, timeout: number) {
    return await DBOS.recv(topic, timeout);
  }

  @DBOS.workflow()
  static async doSend(wfid: string, topic: string, msg: string, delay?: number) {
    if (delay) await sleepms(delay);
    return await DBOS.send(wfid, msg, topic);
  }

  @DBOS.workflow()
  static async doGetEvent(wfid: string, topic: string, timeout: number) {
    return await DBOS.getEvent(wfid, topic, timeout);
  }

  @DBOS.workflow()
  static async doSetEvent(topic: string, val: string, delay?: number) {
    if (delay) await sleepms(delay);
    return await DBOS.setEvent(topic, val);
  }

  @DBOS.workflow()
  static async returnAResult(res: string, delay?: number) {
    if (delay) await sleepms(delay);
    return res;
  }

  @DBOS.workflow()
  static async doGetResult(wfid: string, timeout: number) {
    return await DBOS.getResult(wfid, timeout);
  }

  @DBOS.workflow()
  static async doRecvFromChild() {
    await PGSDBTests.doSend(DBOS.workflowID!, 'topic', 'msg');
    const ct = Date.now();
    await expect(DBOS.recv('topic', 10000)).resolves.toBe('msg');
    expect(Date.now() - ct).toBeLessThan(2000);
  }
}

const sysDB = () => DBOSExecutor.globalInstance!.systemDatabase as PostgresSystemDatabase;

async function doTheNonWFTimeoutTest() {
  let ct = Date.now();

  ct = Date.now();
  await expect(DBOS.getEvent('nosuchwfid', 'key', 0.1)).resolves.toBeNull();
  expect(Date.now() - ct).toBeLessThan(2000);

  // Recv cannot be tested, as it must be inside a workflow

  ct = Date.now();
  await expect(DBOS.getResult('nosuchwfid', 0.1)).resolves.toBeNull();
  expect(Date.now() - ct).toBeLessThan(2000);
}

async function doTheNonWFInstantTest() {
  let ct = Date.now();

  ct = Date.now();
  const wfid1 = uuidv1();
  await DBOS.withNextWorkflowID(wfid1, async () => {
    await PGSDBTests.doSetEvent('key', 'val');
  });
  await expect(DBOS.getEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  expect(Date.now() - ct).toBeLessThan(2000);

  // Recv cannot be tested, as it must be inside a workflow

  const wfid2 = uuidv1();
  await DBOS.withNextWorkflowID(wfid2, async () => {
    await PGSDBTests.returnAResult('result');
  });
  ct = Date.now();
  await expect(DBOS.getResult(wfid2, 10000)).resolves.toBe('result');
  expect(Date.now() - ct).toBeLessThan(2000);
}

async function doTheNonWFDelayedTest() {
  let ct = Date.now();

  ct = Date.now();
  const wfid1 = uuidv1();
  const wfh1 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid1 }).doSetEvent('key', 'val', 100);
  await expect(DBOS.getEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  await wfh1.getResult();
  expect(Date.now() - ct).toBeLessThan(2000);

  // Recv cannot be tested, as it must be inside a workflow

  const wfid3 = uuidv1();
  const wfh3 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid3 }).returnAResult('result', 100);
  ct = Date.now();
  await expect(DBOS.getResult(wfid3, 10000)).resolves.toBe('result');
  await wfh3.getResult();
  expect(Date.now() - ct).toBeLessThan(2000);
}

async function doTheWFTimeoutTest() {
  let ct = Date.now();

  ct = Date.now();
  await expect(PGSDBTests.doGetEvent('nosuchwfid', 'key', 0.1)).resolves.toBeNull();
  expect(Date.now() - ct).toBeLessThan(2000);

  ct = Date.now();
  await expect(PGSDBTests.doRecv('topic', 0.1)).resolves.toBeNull();
  expect(Date.now() - ct).toBeLessThan(2000);

  ct = Date.now();
  await expect(PGSDBTests.doGetResult('nosuchwfid', 0.1)).resolves.toBeNull();
  expect(Date.now() - ct).toBeLessThan(2000);
}

async function doTheWFInstantTest() {
  let ct = Date.now();

  ct = Date.now();
  const wfid1 = uuidv1();
  await DBOS.withNextWorkflowID(wfid1, async () => {
    await PGSDBTests.doSetEvent('key', 'val');
  });
  await expect(PGSDBTests.doGetEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  expect(Date.now() - ct).toBeLessThan(2000);

  await PGSDBTests.doRecvFromChild();

  ct = Date.now();
  await expect(PGSDBTests.doRecv('topic', 0.1)).resolves.toBeNull();
  expect(Date.now() - ct).toBeLessThan(2000);

  const wfid3 = uuidv1();
  await DBOS.withNextWorkflowID(wfid3, async () => {
    await PGSDBTests.returnAResult('result');
  });
  ct = Date.now();
  await expect(PGSDBTests.doGetResult(wfid3, 10000)).resolves.toBe('result');
  expect(Date.now() - ct).toBeLessThan(2000);
}

async function doTheWFDelayedTest() {
  let ct = Date.now();

  ct = Date.now();
  const wfid1 = uuidv1();
  const wfh1 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid1 }).doSetEvent('key', 'val', 100);
  await expect(PGSDBTests.doGetEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  await wfh1.getResult();
  expect(Date.now() - ct).toBeLessThan(2000);

  const wfid2 = uuidv1();
  const wfh2 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid2 }).doRecv('topic', 100000);
  await PGSDBTests.doSend(wfid2, 'topic', 'msg', 100);
  ct = Date.now();
  await expect(wfh2.getResult()).resolves.toBe('msg');
  expect(Date.now() - ct).toBeLessThan(2000);

  const wfid3 = uuidv1();
  const wfh3 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid3 }).returnAResult('result', 100);
  ct = Date.now();
  await expect(PGSDBTests.doGetResult(wfid3, 10000)).resolves.toBe('result');
  await wfh3.getResult();
  expect(Date.now() - ct).toBeLessThan(2000);
}

async function doTheWFCancelTest() {
  let ct = Date.now();

  ct = Date.now();
  const wfid1 = uuidv1();
  const wfh1 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid1 }).doRecv('key', 100000000);
  await sleepms(100);
  await DBOS.cancelWorkflow(wfid1);
  await expect(wfh1.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  expect(Date.now() - ct).toBeLessThan(2000);
}

describe('queued-wf-tests-simple', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  // Test functions - notification variant
  test('run-sysdb-nopoller', async () => {
    const prev = sysDB().dbPollingIntervalResultMs;
    //sysDB().dbPollingIntervalMs = 1000000;
    //The intent was that this would check the notifier.
    //   But notifier does not exist on WF status table for performance reasons.
    sysDB().dbPollingIntervalResultMs = 1000;
    sysDB().dbPollingIntervalEventMs = 1000;
    sysDB().shouldUseDBNotifications = true;
    await doTheNonWFTimeoutTest();
    await doTheNonWFInstantTest();
    await doTheNonWFDelayedTest();

    await doTheWFTimeoutTest();
    await doTheWFInstantTest();
    await doTheWFDelayedTest();

    await doTheWFCancelTest();
    sysDB().dbPollingIntervalResultMs = prev;
  }, 10000);

  // Test functions - poller variant
  test('run-sysdb-onlypoller', async () => {
    const prev = sysDB().dbPollingIntervalResultMs;
    sysDB().dbPollingIntervalResultMs = 100;
    sysDB().dbPollingIntervalEventMs = 100;
    sysDB().shouldUseDBNotifications = false;
    await doTheNonWFTimeoutTest();
    await doTheNonWFInstantTest();
    await doTheNonWFDelayedTest();

    await doTheWFTimeoutTest();
    await doTheWFInstantTest();
    await doTheWFDelayedTest();

    await doTheWFCancelTest();
    sysDB().dbPollingIntervalResultMs = prev;
  }, 10000);
});
