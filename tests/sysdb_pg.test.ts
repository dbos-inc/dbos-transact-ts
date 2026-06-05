// This test covers various system DB settings for the Postgres version
//  Basically, will it function without notifications, by falling back to polling?
//  Does the notification work if polling is so slow as to be discounted?

import { DBOS, DBOSClient, DBOSConfig } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { DBOSWorkflowCancelledError } from '../src/error';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

import { randomUUID } from 'node:crypto';

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
  static async doRecvWithOptions(topic: string, timeout: number, pollingIntervalMs: number) {
    return await DBOS.recv(topic, { timeoutSeconds: timeout, pollingIntervalMs });
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
  static async doGetEventWithOptions(wfid: string, topic: string, timeout: number, pollingIntervalMs: number) {
    return await DBOS.getEvent(wfid, topic, { timeoutSeconds: timeout, pollingIntervalMs });
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
  static async doGetResultWithOptions(wfid: string, timeout: number, pollingIntervalMs: number) {
    return await DBOS.getResult(wfid, { timeoutSeconds: timeout, pollingIntervalMs });
  }

  @DBOS.workflow()
  static async doRecvFromChild() {
    await PGSDBTests.doSend(DBOS.workflowID!, 'topic', 'msg');
    const ct = Date.now();
    await expect(DBOS.recv('topic', 10000)).resolves.toBe('msg');
    expect(Date.now() - ct).toBeLessThan(2000);
  }
}

const sysDB = () => DBOSExecutor.globalInstance!.systemDatabase;

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
  const wfid1 = randomUUID();
  await DBOS.withNextWorkflowID(wfid1, async () => {
    await PGSDBTests.doSetEvent('key', 'val');
  });
  await expect(DBOS.getEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  expect(Date.now() - ct).toBeLessThan(2000);

  // Recv cannot be tested, as it must be inside a workflow

  const wfid2 = randomUUID();
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
  const wfid1 = randomUUID();
  const wfh1 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid1 }).doSetEvent('key', 'val', 100);
  await expect(DBOS.getEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  await wfh1.getResult();
  expect(Date.now() - ct).toBeLessThan(2000);

  // Recv cannot be tested, as it must be inside a workflow

  const wfid3 = randomUUID();
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
  const wfid1 = randomUUID();
  await DBOS.withNextWorkflowID(wfid1, async () => {
    await PGSDBTests.doSetEvent('key', 'val');
  });
  await expect(PGSDBTests.doGetEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  expect(Date.now() - ct).toBeLessThan(2000);

  await PGSDBTests.doRecvFromChild();

  ct = Date.now();
  await expect(PGSDBTests.doRecv('topic', 0.1)).resolves.toBeNull();
  expect(Date.now() - ct).toBeLessThan(2000);

  const wfid3 = randomUUID();
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
  const wfid1 = randomUUID();
  const wfh1 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid1 }).doSetEvent('key', 'val', 100);
  await expect(PGSDBTests.doGetEvent(wfid1, 'key', 10000)).resolves.toBe('val');
  await wfh1.getResult();
  expect(Date.now() - ct).toBeLessThan(2000);

  const wfid2 = randomUUID();
  const wfh2 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid2 }).doRecv('topic', 100000);
  await PGSDBTests.doSend(wfid2, 'topic', 'msg', 100);
  ct = Date.now();
  await expect(wfh2.getResult()).resolves.toBe('msg');
  expect(Date.now() - ct).toBeLessThan(2000);

  const wfid3 = randomUUID();
  const wfh3 = await DBOS.startWorkflow(PGSDBTests, { workflowID: wfid3 }).returnAResult('result', 100);
  ct = Date.now();
  await expect(PGSDBTests.doGetResult(wfid3, 10000)).resolves.toBe('result');
  await wfh3.getResult();
  expect(Date.now() - ct).toBeLessThan(2000);
}

async function doTheWFCancelTest() {
  let ct = Date.now();

  ct = Date.now();
  const wfid1 = randomUUID();
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
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

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
  });

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
  });
});

describe('sysdb-no-listen-notify', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = { ...generateDBOSTestConfig(), useListenNotify: false };
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('polling-without-listen-notify', async () => {
    expect(sysDB().shouldUseDBNotifications).toBe(false);
    sysDB().dbPollingIntervalResultMs = 100;
    sysDB().dbPollingIntervalEventMs = 100;

    await doTheNonWFTimeoutTest();
    await doTheNonWFInstantTest();
    await doTheNonWFDelayedTest();

    await doTheWFTimeoutTest();
    await doTheWFInstantTest();
    await doTheWFDelayedTest();

    await doTheWFCancelTest();
  });

  test('per-call-polling-interval-options', async () => {
    expect(sysDB().shouldUseDBNotifications).toBe(false);
    const previousResultInterval = sysDB().dbPollingIntervalResultMs;
    const previousEventInterval = sysDB().dbPollingIntervalEventMs;
    sysDB().dbPollingIntervalResultMs = 5000;
    sysDB().dbPollingIntervalEventMs = 5000;

    const assertQuick = (start: number) => {
      expect(Date.now() - start).toBeLessThan(1000);
    };

    try {
      let ct = Date.now();
      const resultWorkflowID = randomUUID();
      const resultHandle = await DBOS.startWorkflow(PGSDBTests, { workflowID: resultWorkflowID }).returnAResult(
        'result',
        100,
      );
      await expect(DBOS.getResult(resultWorkflowID, { pollingIntervalMs: 50 })).resolves.toBe('result');
      assertQuick(ct);
      await resultHandle.getResult({ pollingIntervalMs: 50 });

      ct = Date.now();
      const retrievedWorkflowID = randomUUID();
      const retrievedHandle = await DBOS.startWorkflow(PGSDBTests, { workflowID: retrievedWorkflowID }).returnAResult(
        'retrieved',
        100,
      );
      await expect(
        DBOS.retrieveWorkflow<string>(retrievedWorkflowID).getResult({ pollingIntervalMs: 50 }),
      ).resolves.toBe('retrieved');
      assertQuick(ct);
      await retrievedHandle.getResult({ pollingIntervalMs: 50 });

      ct = Date.now();
      const waitedWorkflowID = randomUUID();
      const waitedHandle = await DBOS.startWorkflow(PGSDBTests, { workflowID: waitedWorkflowID }).returnAResult(
        'waited',
        100,
      );
      const completed = await DBOS.waitFirst([DBOS.retrieveWorkflow(waitedWorkflowID)], { pollingIntervalMs: 50 });
      expect(completed.workflowID).toBe(waitedWorkflowID);
      assertQuick(ct);
      await waitedHandle.getResult({ pollingIntervalMs: 50 });

      ct = Date.now();
      const eventWorkflowID = randomUUID();
      const eventHandle = await DBOS.startWorkflow(PGSDBTests, { workflowID: eventWorkflowID }).doSetEvent(
        'key',
        'value',
        100,
      );
      await expect(DBOS.getEvent(eventWorkflowID, 'key', { timeoutSeconds: 5, pollingIntervalMs: 50 })).resolves.toBe(
        'value',
      );
      assertQuick(ct);
      await eventHandle.getResult({ pollingIntervalMs: 50 });

      ct = Date.now();
      const recvWorkflowID = randomUUID();
      const recvHandle = await DBOS.startWorkflow(PGSDBTests, { workflowID: recvWorkflowID }).doRecvWithOptions(
        'topic',
        5,
        50,
      );
      await sleepms(100);
      await DBOS.send(recvWorkflowID, 'message', 'topic');
      await expect(recvHandle.getResult({ pollingIntervalMs: 50 })).resolves.toBe('message');
      assertQuick(ct);

      const workflowGetResultTargetID = randomUUID();
      const workflowGetResultTarget = await DBOS.startWorkflow(PGSDBTests, {
        workflowID: workflowGetResultTargetID,
      }).returnAResult('workflow-get-result', 100);
      ct = Date.now();
      const workflowGetResultHandle = await DBOS.startWorkflow(PGSDBTests).doGetResultWithOptions(
        workflowGetResultTargetID,
        5,
        50,
      );
      await expect(workflowGetResultHandle.getResult({ pollingIntervalMs: 50 })).resolves.toBe('workflow-get-result');
      assertQuick(ct);
      await workflowGetResultTarget.getResult({ pollingIntervalMs: 50 });

      const workflowGetEventTargetID = randomUUID();
      const workflowGetEventSetter = await DBOS.startWorkflow(PGSDBTests, {
        workflowID: workflowGetEventTargetID,
      }).doSetEvent('workflow-key', 'workflow-event', 100);
      ct = Date.now();
      const workflowGetEventHandle = await DBOS.startWorkflow(PGSDBTests).doGetEventWithOptions(
        workflowGetEventTargetID,
        'workflow-key',
        5,
        50,
      );
      await expect(workflowGetEventHandle.getResult({ pollingIntervalMs: 50 })).resolves.toBe('workflow-event');
      assertQuick(ct);
      await workflowGetEventSetter.getResult({ pollingIntervalMs: 50 });

      const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
      try {
        ct = Date.now();
        const clientResultWorkflowID = randomUUID();
        const clientResultHandle = await DBOS.startWorkflow(PGSDBTests, {
          workflowID: clientResultWorkflowID,
        }).returnAResult('client-result', 100);
        await expect(
          client.retrieveWorkflow<string>(clientResultWorkflowID).getResult({ pollingIntervalMs: 50 }),
        ).resolves.toBe('client-result');
        assertQuick(ct);
        await clientResultHandle.getResult({ pollingIntervalMs: 50 });

        ct = Date.now();
        const clientWaitWorkflowID = randomUUID();
        const clientWaitHandle = await DBOS.startWorkflow(PGSDBTests, {
          workflowID: clientWaitWorkflowID,
        }).returnAResult('client-wait', 100);
        const clientCompleted = await client.waitFirst([client.retrieveWorkflow(clientWaitWorkflowID)], {
          pollingIntervalMs: 50,
        });
        expect(clientCompleted.workflowID).toBe(clientWaitWorkflowID);
        assertQuick(ct);
        await clientWaitHandle.getResult({ pollingIntervalMs: 50 });

        ct = Date.now();
        const clientEventWorkflowID = randomUUID();
        const clientEventHandle = await DBOS.startWorkflow(PGSDBTests, {
          workflowID: clientEventWorkflowID,
        }).doSetEvent('client-key', 'client-event', 100);
        await expect(
          client.getEvent(clientEventWorkflowID, 'client-key', { timeoutSeconds: 5, pollingIntervalMs: 50 }),
        ).resolves.toBe('client-event');
        assertQuick(ct);
        await clientEventHandle.getResult({ pollingIntervalMs: 50 });
      } finally {
        await client.destroy();
      }
    } finally {
      sysDB().dbPollingIntervalResultMs = previousResultInterval;
      sysDB().dbPollingIntervalEventMs = previousEventInterval;
    }
  });

  test('invalid-polling-interval-options', async () => {
    const invalidIntervals = [0, -1, Number.NaN, Number.POSITIVE_INFINITY];
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      for (const pollingIntervalMs of invalidIntervals) {
        await expect(DBOS.getResult(randomUUID(), { timeoutSeconds: 0, pollingIntervalMs })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
        await expect(DBOS.getEvent(randomUUID(), 'key', { timeoutSeconds: 0, pollingIntervalMs })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
        await expect(DBOS.waitFirst([DBOS.retrieveWorkflow(randomUUID())], { pollingIntervalMs })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
        await expect(DBOS.retrieveWorkflow(randomUUID()).getResult({ pollingIntervalMs })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
        const invalidRecvHandle = await DBOS.startWorkflow(PGSDBTests).doRecvWithOptions(
          'invalid',
          0,
          pollingIntervalMs,
        );
        await expect(invalidRecvHandle.getResult({ pollingIntervalMs: 50 })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
        await expect(client.retrieveWorkflow(randomUUID()).getResult({ pollingIntervalMs })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
        await expect(client.getEvent(randomUUID(), 'key', { timeoutSeconds: 0, pollingIntervalMs })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
        await expect(client.waitFirst([client.retrieveWorkflow(randomUUID())], { pollingIntervalMs })).rejects.toThrow(
          'pollingIntervalMs must be a positive finite number',
        );
      }
    } finally {
      await client.destroy();
    }
  });
});
