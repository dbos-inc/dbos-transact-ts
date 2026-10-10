import { GetWorkflowsInput, StatusString, DBOS, DBOSClient } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import {
  generateDBOSTestConfig,
  setUpDBOSTestSysDb,
  Event,
  recoverPendingWorkflows,
  reexecuteWorkflowById,
} from './helpers';
import { Client } from 'pg';
import { WorkflowHandle, WorkflowStatus } from '../src/workflow';
import { randomUUID } from 'node:crypto';
import { setTimeout as abortableSleep } from 'node:timers/promises';
import { dbRetryConfig, globalParams, sleepms } from '../src/utils';
import { SystemDatabase } from '../src/system_database';
import { GlobalLogger } from '../src/telemetry/logs';
import { getWorkflow, listQueuedWorkflows, listWorkflows } from '../src/workflow_management';
import {
  DBOSAwaitedWorkflowCancelledError,
  DBOSNonExistentWorkflowError,
  DBOSStepTimeoutError,
  DBOSWorkflowCancelledError,
} from '../src/error';
import assert from 'node:assert';
import { DBOSJSON } from '../src/serialization';

describe('workflow-management-tests', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    process.env.DBOS__APPVERSION = 'v0';
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();

    systemDBClient = new Client({
      connectionString: config.systemDatabaseUrl,
    });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
    process.env.DBOS__APPVERSION = undefined;
  });

  test('simple-getworkflows', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const workflows = await DBOS.listWorkflows({});
    expect(workflows.length).toBe(1);
  });

  test('getworkflows-with-dates', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      startTime: new Date(Date.now() - 10000).toISOString(),
      endTime: new Date(Date.now()).toISOString(),
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.endTime = new Date(Date.now() - 10000).toISOString();
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-status', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      status: StatusString.SUCCESS,
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.status = StatusString.PENDING;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-wfname', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      workflowName: 'testWorkflow',
    };
    const workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);
  });

  test('getworkflows-with-applicationVersion', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      applicationVersion: DBOS.applicationVersion,
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.applicationVersion = 'v1';
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-executorID', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    const input: GetWorkflowsInput = {
      executorId: DBOS.executorID,
    };
    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);

    input.executorId = 'fake-id';
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-list-filters', async () => {
    // Run two workflows with different names and statuses
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');
    await expect(TestEndpoints.failWorkflow('bob')).rejects.toThrow();

    // Test status as a list: [SUCCESS, ERROR] should return both
    let workflows = await DBOS.listWorkflows({ status: [StatusString.SUCCESS, StatusString.ERROR] });
    expect(workflows.length).toBe(2);

    // Test status list with only one matching value
    workflows = await DBOS.listWorkflows({ status: [StatusString.SUCCESS] });
    expect(workflows.length).toBe(1);
    expect(workflows[0].status).toBe(StatusString.SUCCESS);

    // Test status list with no matching values
    workflows = await DBOS.listWorkflows({ status: [StatusString.PENDING, StatusString.CANCELLED] });
    expect(workflows.length).toBe(0);

    // Test workflowName as a list
    workflows = await DBOS.listWorkflows({ workflowName: ['testWorkflow', 'failWorkflow'] });
    expect(workflows.length).toBe(2);

    workflows = await DBOS.listWorkflows({ workflowName: ['testWorkflow', 'nonExistent'] });
    expect(workflows.length).toBe(1);

    workflows = await DBOS.listWorkflows({ workflowName: ['nonExistent'] });
    expect(workflows.length).toBe(0);

    // Test applicationVersion as a list
    workflows = await DBOS.listWorkflows({ applicationVersion: [DBOS.applicationVersion, 'v999'] });
    expect(workflows.length).toBe(2);

    workflows = await DBOS.listWorkflows({ applicationVersion: ['v999'] });
    expect(workflows.length).toBe(0);

    // Test executorId as a list
    workflows = await DBOS.listWorkflows({ executorId: [DBOS.executorID, 'fake-id'] });
    expect(workflows.length).toBe(2);

    workflows = await DBOS.listWorkflows({ executorId: ['fake-id'] });
    expect(workflows.length).toBe(0);

    // Test workflow_id_prefix as a list
    const allWorkflows = await DBOS.listWorkflows({});
    expect(allWorkflows.length).toBe(2);
    const prefix0 = allWorkflows[0].workflowID.substring(0, 8);
    const prefix1 = allWorkflows[1].workflowID.substring(0, 8);

    workflows = await DBOS.listWorkflows({ workflow_id_prefix: [prefix0, prefix1] });
    expect(workflows.length).toBe(2);

    workflows = await DBOS.listWorkflows({ workflow_id_prefix: [prefix0] });
    expect(workflows.length).toBe(1);

    workflows = await DBOS.listWorkflows({ workflow_id_prefix: ['nonexistent-prefix'] });
    expect(workflows.length).toBe(0);
  });

  test('getworkflows-with-limit', async () => {
    const workflowIDs: string[] = [];
    let wfid = await TestEndpoints.testWorkflowGetID();
    assert.ok(wfid);
    expect(wfid).toBeTruthy();
    expect(wfid.length).toBeGreaterThan(0);
    workflowIDs.push(wfid);

    const input: GetWorkflowsInput = {
      limit: 10,
    };

    let workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);
    expect(workflows[0].workflowID).toBe(workflowIDs[0]);

    for (let i = 0; i < 10; i++) {
      wfid = await TestEndpoints.testWorkflowGetID();
      assert.ok(wfid);
      expect(wfid.length).toBeGreaterThan(0);
      workflowIDs.push(wfid);
    }

    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      // The order should be ascending by default
      expect(workflows[i].workflowID).toBe(workflowIDs[i]);
    }

    // Test sort_desc inverts the order
    input.sortDesc = true;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(10);
    for (let i = 0; i < 10; i++) {
      expect(workflows[i].workflowID).toBe(workflowIDs[10 - i]);
    }

    // Test LIMIT 2 OFFSET 2 returns the third and fourth workflows
    input.limit = 2;
    input.offset = 2;
    input.sortDesc = false;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(2);
    for (let i = 0; i < workflows.length; i++) {
      expect(workflows[i].workflowID).toBe(workflowIDs[i + 2]);
    }

    // Test OFFSET 10 returns the last workflow
    input.offset = 10;
    workflows = await DBOS.listWorkflows(input);
    expect(workflows.length).toBe(1);
    for (let i = 0; i < workflows.length; i++) {
      expect(workflows[i].workflowID).toBe(workflowIDs[i + 10]);
    }

    // Test search by workflow ID.
    const wfidInput: GetWorkflowsInput = {
      workflowIDs: [workflowIDs[5], workflowIDs[7]],
    };
    workflows = await DBOS.listWorkflows(wfidInput);
    expect(workflows.length).toBe(2);
    expect(workflows[0].workflowID).toBe(workflowIDs[5]);
    expect(workflows[1].workflowID).toBe(workflowIDs[7]);
  });

  test('getworkflows-cli', async () => {
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    await expect(TestEndpoints.failWorkflow('alice')).rejects.toThrow();

    const logger = new GlobalLogger();
    expect(config.systemDatabaseUrl).toBeDefined();
    const sysdb = new SystemDatabase(config.systemDatabaseUrl!, logger, DBOSJSON);
    try {
      const input: GetWorkflowsInput = {};
      const infos = await listWorkflows(sysdb, input);
      expect(infos.length).toBe(2);
      let info = infos[0];
      expect(info.workflowName).toBe('testWorkflow');
      expect(info.status).toBe(StatusString.SUCCESS);
      expect(info.workflowClassName).toBe('TestEndpoints');
      expect(info.assumedRole).toBe('');
      expect(info.workflowConfigName).toBe('');
      expect(info.error).toBeUndefined();
      expect(info.output).toBe('alice');
      expect(info.input).toEqual(['alice']);
      expect(info.applicationVersion).toBe(globalParams.appVersion);
      expect(info.createdAt).toBeGreaterThan(0);
      expect(info.updatedAt).toBeGreaterThan(0);
      expect(info.executorId).toBe(globalParams.executorID);
      expect(info.deduplicationID).toBeUndefined();
      expect(info.priority).toBe(0);
      expect(info.queuePartitionKey).toBeUndefined();
      expect(info.forkedFrom).toBeUndefined();

      info = infos[1];
      expect(info.workflowName).toBe('failWorkflow');
      expect(info.status).toBe(StatusString.ERROR);
      expect(info.workflowClassName).toBe('TestEndpoints');
      expect(info.assumedRole).toBe('');
      expect(info.workflowConfigName).toBe('');
      const error = info.error as Error;
      expect(error.message).toBe('alice');
      expect(info.output).toBeUndefined();
      expect(info.input).toEqual(['alice']);
      expect(info.applicationVersion).toBe(globalParams.appVersion);
      expect(info.createdAt).toBeGreaterThan(0);
      expect(info.updatedAt).toBeGreaterThan(0);
      expect(info.executorId).toBe(globalParams.executorID);

      const getInfo = await getWorkflow(sysdb, info.workflowID);
      expect(info).toEqual(getInfo);

      // Test ignoring input and output
      input.loadInput = false;
      input.loadOutput = false;
      const noIOInfos = await listWorkflows(sysdb, input);
      expect(noIOInfos.length).toBe(2);
      expect(noIOInfos[0].input).toBeUndefined();
      expect(noIOInfos[0].output).toBeUndefined();
      expect(noIOInfos[0].error).toBeUndefined();
      expect(noIOInfos[1].input).toBeUndefined();
      expect(noIOInfos[1].output).toBeUndefined();
      expect(noIOInfos[1].error).toBeUndefined();
    } finally {
      await sysdb.destroy();
    }
  });

  test('test-cancel-after-completion', async () => {
    TestEndpoints.tries = 0;

    const workflowID = `test-cancel-after-completion-${Date.now()}`;
    const handle = await DBOS.startWorkflow(TestEndpoints, { workflowID }).waitingWorkflow(42);
    await DBOS.send(workflowID, 'message');
    await expect(handle.getResult()).resolves.toEqual(`42-message`);

    let result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    let rows = result.rows;
    expect(rows[0].attempts).toBe(String(1));
    expect(rows[0].status).toBe(StatusString.SUCCESS);
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });

    await DBOS.cancelWorkflow(workflowID);

    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    rows = result.rows;
    expect(rows[0].attempts).toBe(String(1));
    expect(rows[0].status).toBe(StatusString.SUCCESS);
  });

  test('test-cancel-retry-restart', async () => {
    TestEndpoints.tries = 0;

    // A blocked workflow observes cancellation on its next poll, not instantly. Shorten the
    // poll interval so the cancelled recv below stops promptly (the launch in beforeEach builds
    // a fresh system database, so this does not leak to other tests).
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    sysdb.dbPollingIntervalEventMs = 100;

    const workflowID = `test-cancel-resume-fork-${Date.now()}`;
    const handle = await DBOS.startWorkflow(TestEndpoints, { workflowID }).waitingWorkflow(42);
    expect(TestEndpoints.tries).toBe(1);
    expect(handle.workflowID).toBe(workflowID);

    // waitingWorkflow is blocked waiting for a message to be sent, but we're going to cancel instead
    await DBOS.cancelWorkflow(workflowID);

    let result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.CANCELLED);

    // Wait for the cancelled execution to fully stop before resuming. Otherwise the stale
    // coroutine (still blocked in recv) could consume the message and complete the workflow
    // itself, instead of the fresh execution that resume is meant to start.
    while (sysdb.checkForRunningWorkflow(workflowID)) {
      await sleepms(50);
    }

    await recoverPendingWorkflows(); // Does nothing as the workflow is CANCELLED
    expect(TestEndpoints.tries).toBe(1);

    // Retry the workflow, resetting the attempts counter
    const handle2 = await DBOS.resumeWorkflow<number>(workflowID);
    await DBOS.send(workflowID, 'message');
    await expect(handle2.getResult()).resolves.toEqual(`42-message`);

    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [workflowID],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(TestEndpoints.tries).toBe(2);
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);

    await expect(DBOS.resumeWorkflow('fake-workflow')).rejects.toThrow(DBOSNonExistentWorkflowError);

    // fork the workflow
    const wfh = await DBOS.forkWorkflow(workflowID, 0);
    await DBOS.send(wfh.workflowID, 'fork-message');
    await expect(wfh.getResult()).resolves.toEqual(`42-fork-message`);
    expect(TestEndpoints.tries).toBe(3);

    // Validate a new workflow is started and successful
    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid!=$1`,
      [wfh.workflowID],
    );
    expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);

    // Validate the original workflow status hasn't changed
    result = await systemDBClient.query<{ status: string; attempts: number }>(
      `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
      [handle.workflowID],
    );
    // expect(result.rows[0].attempts).toBe(String(1));
    expect(result.rows[0].status).toBe(StatusString.SUCCESS);
  });

  test('test-resume-nonexistent-workflow', async () => {
    const missingID = randomUUID();

    // Resuming a missing ID must fail, not return a handle whose getResult() polls forever
    await expect(DBOS.resumeWorkflow(missingID)).rejects.toThrow(DBOSNonExistentWorkflowError);
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await expect(client.resumeWorkflow(missingID)).rejects.toThrow(DBOSNonExistentWorkflowError);

      // Resuming a workflow that exists but already completed is still a legal no-op
      const wfid = randomUUID();
      await DBOS.withNextWorkflowID(wfid, () => simpleResumeWorkflow(5));
      await expect((await DBOS.resumeWorkflow<number>(wfid)).getResult()).resolves.toBe(5);
      await expect((await client.resumeWorkflow<number>(wfid)).getResult()).resolves.toBe(5);

      // A bulk resume containing a missing ID is all-or-nothing: nothing is re-enqueued
      await expect(DBOS.resumeWorkflows([wfid, missingID])).rejects.toThrow(DBOSNonExistentWorkflowError);
      await expect(client.resumeWorkflows([wfid, missingID])).rejects.toThrow(DBOSNonExistentWorkflowError);
      expect((await DBOS.getWorkflowStatus(wfid))?.status).toBe(StatusString.SUCCESS);
    } finally {
      await client.destroy();
    }
  });

  test('test-cancel-after-final-step', async () => {
    // A workflow cancelled after its final step completes (but before it
    // finishes) must not be able to complete successfully. CANCELLED is terminal.
    TestEndpoints.stepsCompleted = 0;
    const input = 5;
    const wfid = randomUUID();

    const cancelledHandle = await DBOS.startWorkflow(TestEndpoints, { workflowID: wfid }).cancelAfterFinalStepWorkflow(
      input,
    );
    await TestEndpoints.mainThreadEvent.wait();
    await DBOS.cancelWorkflow(wfid);
    TestEndpoints.workflowEvent.set();

    // The workflow must not complete successfully.
    await expect(cancelledHandle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    expect(TestEndpoints.stepsCompleted).toBe(1);
    await expect(DBOS.getWorkflowStatus(wfid)).resolves.toMatchObject({ status: StatusString.CANCELLED });

    // Resuming it should let it complete successfully.
    const handle = await DBOS.resumeWorkflow<number>(wfid);
    await expect(handle.getResult()).resolves.toBe(input);
    await expect(DBOS.getWorkflowStatus(wfid)).resolves.toMatchObject({ status: StatusString.SUCCESS });
    expect(TestEndpoints.stepsCompleted).toBe(1); // cancelStep was already recorded, not re-run
  });

  test('test-resumed-dispatch-runs-alongside-a-stale-run', async () => {
    // A resume that lands while run 1's stale outcome write is still in flight:
    // this same executor dequeues the resumed workflow and must run it to
    // completion alongside the stale run, which parks once its write is refused.
    TestEndpoints.staleWriteRuns = 0;
    TestEndpoints.staleWriteEntered.clear();
    TestEndpoints.staleWriteReleaseRun1.clear();
    TestEndpoints.staleWriteSecondRunDone.clear();

    const wfid = randomUUID();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;

    const parked = new Event();
    const releaseStaleWrite = new Event();
    let parkedOnce = false;

    const originalRecordOutput = sysdb.recordWorkflowOutput.bind(sysdb);
    sysdb.recordWorkflowOutput = async (...fnargs: Parameters<SystemDatabase['recordWorkflowOutput']>) => {
      if (fnargs[0] === wfid && !parkedOnce) {
        parkedOnce = true;
        parked.set();
        await releaseStaleWrite.wait();
      }
      return originalRecordOutput(...fnargs);
    };

    try {
      await DBOS.startWorkflow(TestEndpoints, { workflowID: wfid }).staleWriteBlockingWorkflow();
      await TestEndpoints.staleWriteEntered.wait();

      await DBOS.cancelWorkflow(wfid);

      // Run 1 returns; its stale outcome write is held in flight here.
      TestEndpoints.staleWriteReleaseRun1.set();
      await parked.wait();
      await expect(DBOS.getWorkflowStatus(wfid)).resolves.toMatchObject({ status: StatusString.CANCELLED });

      const resumedHandle = await DBOS.resumeWorkflow<string>(wfid);

      // While the stale write is still parked, the resumed workflow must be
      // dequeued and executed by this same executor.
      let timer: NodeJS.Timeout | undefined;
      const blocked = await Promise.race([
        TestEndpoints.staleWriteSecondRunDone.wait().then(() => false),
        new Promise<boolean>((resolve) => {
          timer = setTimeout(() => resolve(true), 15000);
        }),
      ]);
      clearTimeout(timer);
      expect(blocked).toBe(false); // the stale run did not block the resumed dispatch

      await expect(resumedHandle.getResult()).resolves.toBe('completed');
      expect(TestEndpoints.staleWriteRuns).toBe(2);
    } finally {
      releaseStaleWrite.set();
      sysdb.recordWorkflowOutput = originalRecordOutput;
    }
  });

  test('getworkflows-with-completed-at', async () => {
    // Successful workflow gets completedAt set.
    const beforeSuccess = new Date().toISOString();
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');
    // Tight window: stop here so subsequent workflows complete outside it.
    await sleepms(50);
    const afterSuccess = new Date().toISOString();
    await sleepms(50);

    const successList = await DBOS.listWorkflows({ workflowName: 'testWorkflow' });
    expect(successList.length).toBe(1);
    const successStatus = successList[0];
    expect(successStatus.status).toBe(StatusString.SUCCESS);
    expect(successStatus.completedAt).toBeDefined();
    expect(successStatus.completedAt!).toBeGreaterThanOrEqual(successStatus.createdAt);

    // Errored workflow gets completedAt set.
    await expect(TestEndpoints.failWorkflow('bob')).rejects.toThrow();
    const errorList = await DBOS.listWorkflows({ workflowName: 'failWorkflow' });
    expect(errorList.length).toBe(1);
    const errorStatus = errorList[0];
    expect(errorStatus.status).toBe(StatusString.ERROR);
    expect(errorStatus.completedAt).toBeDefined();

    // Cancelled workflow gets completedAt set; resumed workflow clears it.
    const cancelID = randomUUID();
    const cancelledHandle = await DBOS.startWorkflow(TestEndpoints, { workflowID: cancelID }).waitingWorkflow(42);
    await DBOS.cancelWorkflow(cancelID);
    const cancelled = await DBOS.getWorkflowStatus(cancelID);
    expect(cancelled).toBeDefined();
    expect(cancelled!.status).toBe(StatusString.CANCELLED);
    expect(cancelled!.completedAt).toBeDefined();

    const resumedHandle = await DBOS.resumeWorkflow<string>(cancelID);
    const resumed = await DBOS.getWorkflowStatus(cancelID);
    expect(resumed).toBeDefined();
    expect(resumed!.completedAt).toBeUndefined();

    // completedAfter / completedBefore only match terminal workflows in range.
    const inRange = await DBOS.listWorkflows({
      completedAfter: beforeSuccess,
      completedBefore: afterSuccess,
    });
    const idsInRange = new Set(inRange.map((w) => w.workflowID));
    expect(idsInRange.has(successStatus.workflowID)).toBe(true);
    // The error and resumed-pending workflows complete outside this window.
    expect(idsInRange.has(errorStatus.workflowID)).toBe(false);
    expect(idsInRange.has(cancelID)).toBe(false);

    // completedAfter alone excludes never-completed workflows.
    const onlyCompleted = await DBOS.listWorkflows({ completedAfter: beforeSuccess });
    const completedIds = new Set(onlyCompleted.map((w) => w.workflowID));
    expect(completedIds.has(successStatus.workflowID)).toBe(true);
    expect(completedIds.has(errorStatus.workflowID)).toBe(true);
    expect(completedIds.has(cancelID)).toBe(false);

    // A window before any work happened matches nothing.
    const farPast = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    const noneYet = await DBOS.listWorkflows({ completedBefore: farPast });
    expect(noneYet.length).toBe(0);

    // Release the resumed workflow and wait for it to finish.
    await DBOS.send(cancelID, 'message');
    await expect(resumedHandle.getResult()).resolves.toEqual(`42-message`);
    // Suppress unused-variable lint for cancelledHandle.
    expect(cancelledHandle.workflowID).toBe(cancelID);
  });

  test('systemdb-migration-backward-compatible', async () => {
    // Make sure the system DB migration failure is handled correctly.
    // If there is a migration failure, the system DB should still be able to start.
    // This happens when the old code is running with a new system DB schema.
    await DBOS.shutdown();
    await systemDBClient.query(`UPDATE "dbos"."dbos_migrations" SET "version" = 10000;`);
    await DBOS.launch();
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');

    // Test schema install idempotence
    await DBOS.shutdown();
    await systemDBClient.query(`UPDATE "dbos"."dbos_migrations" SET "version" = 0;`);
    await DBOS.launch();
    await expect(TestEndpoints.testWorkflow('alice')).resolves.toBe('alice');
  });

  const simpleResumeWorkflow = DBOS.registerWorkflow(async (x: number) => Promise.resolve(x), {
    name: 'simpleResumeWorkflow',
  });

  const forkTargetWorkflow = DBOS.registerWorkflow(async (x: number) => Promise.resolve(x), {
    name: 'forkTargetWorkflow',
  });
  const forkMaybeMissingWorkflow = DBOS.registerWorkflow(
    async (targetID: string) => {
      try {
        await DBOS.forkWorkflow(targetID, 1);
      } catch {
        return 'missing';
      }
      return 'forked';
    },
    { name: 'forkMaybeMissingWorkflow' },
  );

  test('fork-nonexistent-workflow-replays-recorded-error', async () => {
    const missingID = randomUUID();
    const handle = await DBOS.startWorkflow(forkMaybeMissingWorkflow)(missingID);
    await expect(handle.getResult()).resolves.toBe('missing');
    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    expect(steps?.map((s) => s.name)).toEqual(['DBOS.forkWorkflow']);
    expect(steps![0].error?.message).toContain(missingID);

    // Once the target exists, a replay still takes the recorded branch and forks nothing.
    await DBOS.withNextWorkflowID(missingID, () => forkTargetWorkflow(1));
    const replayed = await reexecuteWorkflowById(handle.workflowID);
    await expect(replayed.getResult()).resolves.toBe('missing');
    expect(await DBOS.listWorkflows({ forkedFrom: missingID })).toEqual([]);
  });

  const failingChildWorkflow = DBOS.registerWorkflow(
    async () => {
      await Promise.resolve();
      throw new Error('child failed');
    },
    { name: 'failingChildWorkflow' },
  );
  const awaitFailingChildWorkflow = DBOS.registerWorkflow(
    async (childID: string) => {
      try {
        await DBOS.getResult(childID);
      } catch (e) {
        return (e as Error).message;
      }
      return 'succeeded';
    },
    { name: 'awaitFailingChildWorkflow' },
  );

  test('get-result-replays-recorded-error', async () => {
    const childID = randomUUID();
    const childHandle = await DBOS.startWorkflow(failingChildWorkflow, { workflowID: childID })();
    await expect(childHandle.getResult()).rejects.toThrow('child failed');

    const handle = await DBOS.startWorkflow(awaitFailingChildWorkflow)(childID);
    await expect(handle.getResult()).resolves.toBe('child failed');
    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    expect(steps![0].error?.message).toBe('child failed');

    // The replay re-throws the recorded error rather than awaiting the now-deleted child.
    await DBOS.deleteWorkflow(childID);
    const replayed = await reexecuteWorkflowById(handle.workflowID);
    await expect(replayed.getResult()).resolves.toBe('child failed');
  });

  const cancelWithChildrenWorkflow = DBOS.registerWorkflow(
    async (targetID: string) => {
      await DBOS.cancelWorkflow(targetID, { cancelChildren: true });
    },
    { name: 'cancelWithChildrenWorkflow' },
  );

  test('workflow-command-transient-error-is-retried-not-recorded', async () => {
    const savedBackoff = { ...dbRetryConfig };
    dbRetryConfig.initialBackoffSec = 0.05;
    const targetID = randomUUID();
    const pool = DBOSExecutor.globalInstance!.systemDatabase.pool;
    const originalQuery = pool.query.bind(pool) as (text: unknown, params?: unknown) => Promise<unknown>;
    let calls = 0;
    // Fail the first child lookup for the target with a connection error.
    const spy = jest.spyOn(pool, 'query').mockImplementation(((text: unknown, params?: unknown) => {
      const ids = Array.isArray(params) ? (params[0] as unknown) : undefined;
      if (
        typeof text === 'string' &&
        text.includes('parent_workflow_id = ANY') &&
        Array.isArray(ids) &&
        ids.includes(targetID)
      ) {
        calls += 1;
        if (calls === 1) {
          return Promise.reject(Object.assign(new Error('connection lost'), { code: 'ECONNRESET' }));
        }
      }
      return originalQuery(text, params);
    }) as never);
    try {
      const handle = await DBOS.startWorkflow(cancelWithChildrenWorkflow)(targetID);
      await handle.getResult();
      expect(calls).toBe(2);
      const steps = await DBOS.listWorkflowSteps(handle.workflowID);
      expect(steps?.map((s) => s.name)).toEqual(['DBOS.cancelWorkflow']);
      expect(steps![0].error).toBeNull();
    } finally {
      spy.mockRestore();
      Object.assign(dbRetryConfig, savedBackoff);
    }
  });

  class TestEndpoints {
    @DBOS.workflow()
    static async testWorkflow(name: string) {
      return Promise.resolve(name);
    }

    @DBOS.workflow()
    static async testWorkflowGetID() {
      return Promise.resolve(DBOS.workflowID);
    }

    @DBOS.workflow()
    static async failWorkflow(name: string) {
      await Promise.resolve(name);
      throw new Error(name);
    }

    static tries = 0;
    static testResolve: () => void;
    static testPromise = new Promise<void>((resolve) => {
      TestEndpoints.testResolve = resolve;
    });

    @DBOS.workflow()
    static async waitingWorkflow(value: number) {
      TestEndpoints.tries += 1;
      const msg = await DBOS.recv<string>();
      await TestEndpoints.stepOne();
      return `${value}-${msg}`;
    }

    @DBOS.step()
    static async stepOne() {
      return Promise.resolve();
    }

    static stepsCompleted = 0;
    static workflowEvent = new Event();
    static mainThreadEvent = new Event();

    @DBOS.step()
    static async cancelStep() {
      TestEndpoints.stepsCompleted += 1;
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async cancelAfterFinalStepWorkflow(x: number) {
      // The only step runs and records its output...
      await TestEndpoints.cancelStep();
      // ...then the workflow is cancelled before it returns.
      TestEndpoints.mainThreadEvent.set();
      await TestEndpoints.workflowEvent.wait();
      return x;
    }

    static staleWriteRuns = 0;
    static staleWriteEntered = new Event();
    static staleWriteReleaseRun1 = new Event();
    static staleWriteSecondRunDone = new Event();

    @DBOS.workflow()
    static async staleWriteBlockingWorkflow() {
      TestEndpoints.staleWriteRuns += 1;
      if (TestEndpoints.staleWriteRuns === 1) {
        TestEndpoints.staleWriteEntered.set();
        await TestEndpoints.staleWriteReleaseRun1.wait();
        return '';
      }
      TestEndpoints.staleWriteSecondRunDone.set();
      return 'completed';
    }
  }
});

describe('test-list-queues', () => {
  let config: DBOSConfig;
  // The retention tests below read the payload tables directly.
  let systemDBClient: Client;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.registerQueue(TestListQueues.queue.name, { onConflict: 'always_update' });
    await DBOS.registerQueue(TestGarbageCollection.queue.name, { onConflict: 'always_update' });
    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
  });

  class TestListQueues {
    static queuedSteps = 5;
    static event = new Event();
    static taskEvents = Array.from({ length: TestListQueues.queuedSteps }, () => new Event());
    static queue = { name: 'testQueueRecovery' };

    @DBOS.workflow()
    static async testWorkflow() {
      const handles: WorkflowHandle<unknown>[] = [];
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        const h = await DBOS.startWorkflow(TestListQueues, { queueName: TestListQueues.queue.name }).blockingTask(i);
        handles.push(h);
      }
      return await Promise.all(handles.map((h) => h.getResult()));
    }

    @DBOS.workflow()
    static async blockingTask(i: number) {
      TestListQueues.taskEvents[i].set();
      await TestListQueues.event.wait();
      return i;
    }
  }

  test('test-list-queues', async () => {
    const wfid = randomUUID();

    // Start the workflow. Wait for all five tasks to start. Verify that they started.
    const originalHandle = await DBOS.startWorkflow(TestListQueues, { workflowID: wfid }).testWorkflow();
    for (const e of TestListQueues.taskEvents) {
      await e.wait();
    }

    const logger = new GlobalLogger();
    expect(config.systemDatabaseUrl).toBeDefined();
    const sysdb = new SystemDatabase(config.systemDatabaseUrl!, logger, DBOSJSON);
    try {
      let input: GetWorkflowsInput = {};
      let output: WorkflowStatus[] = [];
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);

      // Test workflowName
      input = {
        workflowName: 'blockingTask',
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        expect(output[i].input).toEqual([i]);
      }

      // Test ignoring input
      input.loadInput = false;
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        expect(output[i].input).toBeUndefined();
      }

      input = {
        workflowName: 'no',
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test sortDesc reverts the order
      input = {
        sortDesc: true,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      for (let i = 0; i < TestListQueues.queuedSteps; i++) {
        expect(output[i].input).toEqual([TestListQueues.queuedSteps - i - 1]);
      }

      // Test startTime and endTime
      input = {
        startTime: new Date(Date.now() - 10000).toISOString(),
        endTime: new Date(Date.now()).toISOString(),
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      input = {
        startTime: new Date(Date.now() + 10000).toISOString(),
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test status
      input = {
        status: 'PENDING',
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);
      input = {
        status: 'SUCCESS',
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test queue name
      input = {
        queueName: TestListQueues.queue.name,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);

      input = {
        queueName: 'no',
      };

      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test queue name as a list
      input = {
        queueName: [TestListQueues.queue.name, 'otherQueue'],
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);

      input = {
        queueName: ['no', 'alsoNo'],
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test status as a list
      input = {
        status: ['PENDING', 'ENQUEUED'],
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);

      input = {
        status: ['SUCCESS', 'ERROR'],
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test workflowName as a list
      input = {
        workflowName: ['blockingTask', 'otherWorkflow'],
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(TestListQueues.queuedSteps);

      input = {
        workflowName: ['no', 'alsoNo'],
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(0);

      // Test limit
      input = {
        limit: 2,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(input.limit);
      for (let i = 0; i < input.limit!; i++) {
        expect(output[i].input).toEqual([i]);
      }

      // Test offset
      input = {
        limit: 2,
        offset: 2,
      };
      output = await listQueuedWorkflows(sysdb, input);
      expect(output.length).toBe(input.limit);
      for (let i = 0; i < input.limit!; i++) {
        expect(output[i].input).toEqual([i + 2]);
      }

      // Confirm the workflow finishes and nothing is in the queue afterwards
      TestListQueues.event.set();
      await expect(originalHandle.getResult()).resolves.toEqual([0, 1, 2, 3, 4]);

      input = {};
      await expect(listQueuedWorkflows(sysdb, input)).resolves.toEqual([]);
    } finally {
      await sysdb.destroy();
    }
  });

  class TestGarbageCollection {
    static event = new Event();
    static readonly queue = { name: 'gc-test-queue' };

    @DBOS.step()
    static async testStep(x: number) {
      return Promise.resolve(x);
    }

    @DBOS.workflow()
    static async testWorkflow(x: number) {
      await TestGarbageCollection.testStep(x);
      return x;
    }

    @DBOS.workflow()
    static async blockedWorkflow() {
      // A recorded step, so what the sweep spares covers the checkpoints a replay would
      // read and not only the inputs.
      await TestGarbageCollection.testStep(0);
      await TestGarbageCollection.event.wait();
      return DBOS.workflowID;
    }

    @DBOS.workflow()
    static async gcQueuedWorkflow() {
      await Promise.resolve();
    }
  }
});

describe('legacy-payload-rows', () => {
  let config: DBOSConfig;
  let systemDBClient: Client;

  class LegacyPayload {
    @DBOS.workflow()
    static async workflow(x: number) {
      return Promise.resolve(x);
    }
  }

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    systemDBClient = new Client({ connectionString: config.systemDatabaseUrl });
    await systemDBClient.connect();
  });

  afterEach(async () => {
    await systemDBClient.end();
    await DBOS.shutdown();
  });

  test('test-legacy-payload-rows-still-read', async () => {
    await expect(LegacyPayload.workflow(11)).resolves.toBe(11);
    const workflowID = (await DBOS.listWorkflows({}))[0].workflowID;

    const legacy = await systemDBClient.query<{ inputs: string | null; output: string | null }>(
      `SELECT inputs, output FROM dbos.workflow_status WHERE workflow_uuid = $1`,
      [workflowID],
    );
    const split = await systemDBClient.query<{ inputs: string }>(
      `SELECT inputs FROM dbos.workflow_input WHERE workflow_uuid = $1`,
      [workflowID],
    );
    const out = await systemDBClient.query<{ output: string }>(
      `SELECT output FROM dbos.workflow_output WHERE workflow_uuid = $1`,
      [workflowID],
    );

    // Payloads land only in their own tables, so the legacy columns stay empty.
    expect(legacy.rows[0].inputs).toBeNull();
    expect(legacy.rows[0].output).toBeNull();
    expect(split.rows[0].inputs).not.toBeNull();
    expect(out.rows[0].output).not.toBeNull();

    // Move them onto the legacy columns: a row written before the split looks exactly like
    // this, and every read path must still resolve it from workflow_status.
    await systemDBClient.query(`UPDATE dbos.workflow_status SET inputs = $2, output = $3 WHERE workflow_uuid = $1`, [
      workflowID,
      split.rows[0].inputs,
      out.rows[0].output,
    ]);
    await systemDBClient.query(`DELETE FROM dbos.workflow_input WHERE workflow_uuid = $1`, [workflowID]);
    await systemDBClient.query(`DELETE FROM dbos.workflow_output WHERE workflow_uuid = $1`, [workflowID]);

    const listed = (await DBOS.listWorkflows({ workflowIDs: [workflowID] }))[0];
    expect(listed.input).toEqual([11]);
    expect(listed.output).toBe(11);
    await expect(DBOS.retrieveWorkflow(workflowID).getResult()).resolves.toBe(11);
    const forked = await DBOS.forkWorkflow(workflowID, 0);
    await expect(forked.getResult()).resolves.toBe(11);
  });
});

describe('test-list-steps', () => {
  let config: DBOSConfig;
  const queue = { name: 'child_queue' };
  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });
  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
    await DBOS.registerQueue(queue.name, { onConflict: 'always_update' });
  });
  afterEach(async () => {
    await DBOS.shutdown();
  });

  class TestListSteps {
    @DBOS.workflow()
    static async testWorkflow() {
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      await DBOS.sleep(10);
      return DBOS.workflowID;
    }

    @DBOS.step()
    static async stepOne() {
      return Promise.resolve(DBOS.workflowID);
    }
    @DBOS.step()
    static async stepTwo() {
      return Promise.resolve(DBOS.workflowID);
    }

    @DBOS.workflow()
    static async sendWorkflow(target: string) {
      await DBOS.send(target, 'message1');
    }

    @DBOS.workflow()
    static async recvWorkflow(target: string) {
      const msg = await DBOS.recv(target, 1);
      console.log('received message:', msg);
    }

    @DBOS.workflow()
    static async setEventWorkflow() {
      await DBOS.setEvent('key', 'value');
      await DBOS.getEvent('fakewid', 'key', 1);
    }

    @DBOS.workflow()
    static async callChildWorkflowfirst() {
      const handle = await DBOS.startWorkflow(TestListSteps).testWorkflow();
      const childID = await handle.getResult();
      await handle.getStatus();
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      return childID;
    }
    @DBOS.workflow()
    static async callChildWorkflowMiddle() {
      await TestListSteps.stepOne();
      const handle = await DBOS.startWorkflow(TestListSteps).testWorkflow();
      await handle.getStatus();
      const childID = await handle.getResult();
      await TestListSteps.stepTwo();
      return childID;
    }
    @DBOS.workflow()
    static async callChildWorkflowLast() {
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      const handle = await DBOS.startWorkflow(TestListSteps).testWorkflow();
      await handle.getStatus();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async enqueueChildWorkflowFirst() {
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).testWorkflow();
      const childID = await handle.getResult();
      await handle.getStatus();
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      return childID;
    }

    @DBOS.workflow()
    static async enqueueChildWorkflowMiddle() {
      await TestListSteps.stepOne();
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).testWorkflow();
      await handle.getStatus();
      const childID = await handle.getResult();
      await TestListSteps.stepTwo();
      return childID;
    }

    @DBOS.workflow()
    static async enqueueChildWorkflowLast() {
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).testWorkflow();
      await handle.getStatus();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async directCallWorkflow() {
      const childID = await TestListSteps.testWorkflow();
      await TestListSteps.stepOne();
      await TestListSteps.stepTwo();
      return childID;
    }

    @DBOS.workflow()
    // eslint-disable-next-line  @typescript-eslint/require-await
    static async childWorkflowWithCounter(id: string) {
      return id;
    }

    @DBOS.step()
    static async failingStep() {
      await Promise.resolve();
      throw Error('fail');
    }

    @DBOS.workflow()
    static async callFailingStep() {
      await TestListSteps.failingStep();
    }

    @DBOS.workflow()
    static async startFailingChild() {
      const handle = await DBOS.startWorkflow(TestListSteps).callFailingStep();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async enqueueFailingChild() {
      const handle = await DBOS.startWorkflow(TestListSteps, { queueName: queue.name }).callFailingStep();
      return await handle.getResult();
    }

    @DBOS.workflow()
    static async CounterParent() {
      const childwfid = randomUUID();
      const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: childwfid }).childWorkflowWithCounter(
        childwfid,
      );
      return await handle.getResult();
    }
  }

  class ListWorkflows {
    @DBOS.workflow()
    static async listingWorkflow() {
      return (await DBOS.listWorkflows({})).length;
    }

    @DBOS.workflow()
    static async simpleWorkflow() {
      return Promise.resolve();
    }
  }

  const numStepTimingSteps = 5;
  async function stepTimingStep() {
    await sleepms(100);
  }

  const stepTimingWorkflow = DBOS.registerWorkflow(async () => {
    for (let i = 0; i < numStepTimingSteps; i++) {
      await DBOS.runStep(() => stepTimingStep());
    }
    await DBOS.setEvent('key', 'value');
    await DBOS.listWorkflows({});
    await DBOS.recv(undefined, 0);
  });

  test('test-step-timing', async () => {
    const startTime = Date.now();
    const handle = await DBOS.startWorkflow(stepTimingWorkflow)();
    await handle.getResult();

    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    assert(steps);
    assert(steps.length > 0);
    for (const s of steps) {
      assert(s.startedAtEpochMs);
      assert(s.completedAtEpochMs);
      assert.strictEqual(typeof s.startedAtEpochMs, 'number');
      assert.strictEqual(typeof s.completedAtEpochMs, 'number');
      assert(s.startedAtEpochMs >= startTime);
      assert(s.completedAtEpochMs >= s.startedAtEpochMs);
      if (s.functionID < numStepTimingSteps) {
        assert(s.completedAtEpochMs - s.startedAtEpochMs >= 100);
      }
    }
  });

  test('test-list-steps', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].name).toBe('stepTwo');
    expect(wfsteps[2].functionID).toBe(2);
    expect(wfsteps[2].name).toBe('DBOS.sleep');
  });

  test('test-list-steps-invalid-wfid', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(randomUUID());
    expect(wfsteps).toBeUndefined();
  });

  test('test-list-steps-pagination', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();

    // All steps returned without pagination
    const allSteps = await DBOS.listWorkflowSteps(wfid);
    expect(allSteps!.length).toBe(3);

    // Limit 2 returns the first two steps
    const limited = await DBOS.listWorkflowSteps(wfid, { limit: 2 });
    expect(limited!.length).toBe(2);
    expect(limited![0].name).toBe('stepOne');
    expect(limited![1].name).toBe('stepTwo');

    // Limit 2 offset 1 returns the second and third steps
    const paginated = await DBOS.listWorkflowSteps(wfid, { limit: 2, offset: 1 });
    expect(paginated!.length).toBe(2);
    expect(paginated![0].name).toBe('stepTwo');
    expect(paginated![1].name).toBe('DBOS.sleep');

    // Offset 2 returns only the last step
    const offsetOnly = await DBOS.listWorkflowSteps(wfid, { offset: 2 });
    expect(offsetOnly!.length).toBe(1);
    expect(offsetOnly![0].name).toBe('DBOS.sleep');
  });

  test('test-list-steps-load-output-false-skips-payloads', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).testWorkflow();
    await handle.getResult();

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const withOutput = await sysdb.getAllOperationResults(wfid);
    expect(withOutput.length).toBe(3);
    expect(withOutput[0].output).not.toBeUndefined();

    const withoutOutput = await sysdb.getAllOperationResults(wfid, undefined, undefined, false);
    expect(withoutOutput.length).toBe(3);
    for (const row of withoutOutput) {
      expect(row.output).toBeUndefined();
      expect(row.error).toBeUndefined();
    }

    const steps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid, false);
    expect(steps!.length).toBe(3);
    expect(steps![0].name).toBe('stepOne');
    expect(steps![0].output).toBeNull();
    expect(steps![0].error).toBeNull();
  });

  test('test-list-workflows-has-parent', async () => {
    // Run a parent workflow that starts a child
    const parentId = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: parentId }).callChildWorkflowfirst();
    const childId = await handle.getResult();

    // Also run a standalone workflow (no parent)
    const standaloneId = randomUUID();
    await DBOS.startWorkflow(TestListSteps, { workflowID: standaloneId }).testWorkflow();

    // hasParent=true returns only the child workflow
    const withParent = await DBOS.listWorkflows({ hasParent: true });
    expect(withParent.length).toBe(1);
    expect(withParent[0].workflowID).toBe(childId);

    // hasParent=false returns workflows without a parent
    const withoutParent = await DBOS.listWorkflows({ hasParent: false });
    const ids = new Set(withoutParent.map((w) => w.workflowID));
    expect(ids).toContain(parentId);
    expect(ids).toContain(standaloneId);
    expect(ids).not.toContain(childId);
  });

  test('test-send-recv', async () => {
    const wfid1 = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid1 }).recvWorkflow('message1');

    const wfid2 = randomUUID();
    await DBOS.startWorkflow(TestListSteps, { workflowID: wfid2 }).sendWorkflow(wfid1);

    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid1);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(2);
    expect(wfsteps[1].name).toBe('DBOS.sleep');
    expect(wfsteps[0].name).toBe('DBOS.recv');

    const wfsteps2 = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid2);
    if (!wfsteps2) {
      throw new Error('wfsteps2 is undefined');
    }
    expect(wfsteps2[0].functionID).toBe(0);
    expect(wfsteps2[0].name).toBe('DBOS.send');
  });

  test('test-set-getEvent', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).setEventWorkflow();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(3);
    expect(wfsteps[0].name).toBe('DBOS.setEvent');
    expect(wfsteps[1].name).toBe('DBOS.getEvent');
  });

  test('test-call-child-workflow-first', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowfirst();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('testWorkflow');
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(childID);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].childWorkflowID).toBe(childID);
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[2].functionID).toBe(2);
    expect(wfsteps[2].output).toBeTruthy();
    expect(wfsteps[2].error).toBe(null);
    expect(wfsteps[2].childWorkflowID).toBe(null);
    expect(wfsteps[3].name).toBe('stepOne');
    expect(wfsteps[3].functionID).toBe(3);
    expect(wfsteps[3].output).toBe(wfid);
    expect(wfsteps[3].error).toBe(null);
    expect(wfsteps[3].childWorkflowID).toBe(null);
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-call-child-workflow-middle', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowMiddle();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('testWorkflow');
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[3].name).toBe('DBOS.getResult');
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-call-child-workflow-last', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callChildWorkflowLast();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('stepTwo');
    expect(wfsteps[2].name).toBe('testWorkflow');
    expect(wfsteps[3].name).toBe('getStatus');
    expect(wfsteps[4].name).toBe('DBOS.getResult');
  });

  test('test-queue-child-workflow-first', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowFirst();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('testWorkflow');
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(childID);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].childWorkflowID).toBe(childID);
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[3].name).toBe('stepOne');
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-queue-child-workflow-middle', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowMiddle();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('testWorkflow');
    expect(wfsteps[2].name).toBe('getStatus');
    expect(wfsteps[3].name).toBe('DBOS.getResult');
    expect(wfsteps[4].name).toBe('stepTwo');
  });

  test('test-queue-child-workflow-last', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).enqueueChildWorkflowLast();
    await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(5);
    expect(wfsteps[0].name).toBe('stepOne');
    expect(wfsteps[1].name).toBe('stepTwo');
    expect(wfsteps[2].name).toBe('testWorkflow');
    expect(wfsteps[3].name).toBe('getStatus');
    expect(wfsteps[4].name).toBe('DBOS.getResult');
  });

  test('test-direct-call-workflow', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).directCallWorkflow();
    const childID = await handle.getResult();
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(4);
    expect(wfsteps[0].name).toBe('testWorkflow');
    expect(wfsteps[0].functionID).toBe(0);
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBe(null);
    expect(wfsteps[0].childWorkflowID).toBe(childID);
    expect(wfsteps[1].name).toBe('DBOS.getResult');
    expect(wfsteps[1].functionID).toBe(1);
    expect(wfsteps[1].output).toBe(childID);
    expect(wfsteps[1].error).toBe(null);
    expect(wfsteps[1].childWorkflowID).toBe(childID);
    expect(wfsteps[2].name).toBe('stepOne');
    expect(wfsteps[3].name).toBe('stepTwo');
  });

  test('test-list-failing-step', async () => {
    // Test calling a failing step directly
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).callFailingStep();
    await expect(handle.getResult()).rejects.toThrow(new Error('fail'));
    const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
    if (!wfsteps) {
      throw new Error('wfsteps is undefined');
    }
    expect(wfsteps.length).toBe(1);
    expect(wfsteps[0].name).toBe('failingStep');
    expect(wfsteps[0].output).toBe(null);
    expect(wfsteps[0].error).toBeInstanceOf(Error);
    expect(wfsteps[0].childWorkflowID).toBe(null);
  });

  test('test-list-failing-child-workflow', async () => {
    // The child's failure is recorded on the parent's getResult checkpoint, not on the start checkpoint.
    for (const start of [
      (id: string) => DBOS.startWorkflow(TestListSteps, { workflowID: id }).startFailingChild(),
      (id: string) => DBOS.startWorkflow(TestListSteps, { workflowID: id }).enqueueFailingChild(),
    ]) {
      const wfid = randomUUID();
      const handle = await start(wfid);
      await expect(handle.getResult()).rejects.toThrow(new Error('fail'));
      const wfsteps = await DBOSExecutor.globalInstance!.listWorkflowSteps(wfid);
      if (!wfsteps) {
        throw new Error('wfsteps is undefined');
      }
      expect(wfsteps.length).toBe(2);
      expect(wfsteps[0].name).toBe('callFailingStep');
      expect(wfsteps[0].output).toBe(null);
      expect(wfsteps[0].error).toBe(null);
      expect(wfsteps[0].childWorkflowID).toBe(`${wfid}-0`);
      expect(wfsteps[1].name).toBe('DBOS.getResult');
      expect(wfsteps[1].output).toBe(null);
      expect(wfsteps[1].error).toBeInstanceOf(Error);
      expect(wfsteps[1].childWorkflowID).toBe(`${wfid}-0`);
    }
  });

  test('test-child-rerun', async () => {
    const wfid = randomUUID();
    let handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).CounterParent();
    const result1 = await handle.getResult();
    // call again with same wfid
    handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid }).CounterParent();
    const result2 = await handle.getResult();
    expect(result1).toEqual(result2);

    expect(config.systemDatabaseUrl).toBeDefined();
    const sysdb = new SystemDatabase(config.systemDatabaseUrl!, new GlobalLogger(), DBOSJSON);
    try {
      const wfs = await listWorkflows(sysdb, {});
      expect(wfs.length).toBe(2);

      const wfid1 = randomUUID();
      // call with different wfid we should get different result
      handle = await DBOS.startWorkflow(TestListSteps, { workflowID: wfid1 }).CounterParent();
      const result3 = await handle.getResult();

      expect(result3).not.toEqual(result1);
    } finally {
      await sysdb.destroy();
    }
  });

  test('test-list-workflows-as-step', async () => {
    const wfid = randomUUID();
    const c1 = await DBOS.withNextWorkflowID(wfid, async () => {
      return await ListWorkflows.listingWorkflow();
    });
    expect(c1).toBe(1);

    await ListWorkflows.simpleWorkflow();

    // Let this start over
    const c2 = await (await reexecuteWorkflowById(wfid))?.getResult();
    expect(c2).toBe(1);
  });

  test('test-parent-workflow-id', async () => {
    const parentWfid = randomUUID();
    const handle = await DBOS.startWorkflow(TestListSteps, { workflowID: parentWfid }).callChildWorkflowfirst();
    const childID = await handle.getResult();
    expect(childID).toBeDefined();

    // Verify the child workflow's status has parentWorkflowID set to the parent's ID
    const childStatus = await DBOS.getWorkflowStatus(childID!);
    expect(childStatus).not.toBeNull();
    expect(childStatus!.parentWorkflowID).toBe(parentWfid);

    // Verify the parent workflow does not have a parentWorkflowID
    const parentStatus = await DBOS.getWorkflowStatus(parentWfid);
    expect(parentStatus).not.toBeNull();
    expect(parentStatus!.parentWorkflowID).toBeUndefined();

    // Test filtering by parentWorkflowID
    const childWorkflows = await DBOS.listWorkflows({ parentWorkflowID: parentWfid });
    expect(childWorkflows.length).toBe(1);
    expect(childWorkflows[0].workflowID).toBe(childID);
    expect(childWorkflows[0].parentWorkflowID).toBe(parentWfid);

    // Verify filtering with a non-existent parentWorkflowID returns no results
    const noWorkflows = await DBOS.listWorkflows({ parentWorkflowID: 'non-existent-id' });
    expect(noWorkflows.length).toBe(0);

    // Test filtering by parentWorkflowID as a list
    const childWorkflows2 = await DBOS.listWorkflows({ parentWorkflowID: [parentWfid, 'non-existent-id'] });
    expect(childWorkflows2.length).toBe(1);
    expect(childWorkflows2[0].workflowID).toBe(childID);

    const childWorkflows3 = await DBOS.listWorkflows({ parentWorkflowID: ['non-existent-id', 'also-non-existent'] });
    expect(childWorkflows3.length).toBe(0);

    // Test dequeuedAt with a queued child workflow
    const queuedParentWfid = randomUUID();
    const queuedHandle = await DBOS.startWorkflow(TestListSteps, {
      workflowID: queuedParentWfid,
    }).enqueueChildWorkflowFirst();
    const queuedChildID = await queuedHandle.getResult();
    expect(queuedChildID).toBeDefined();

    // Verify the queued child workflow has dequeuedAt set and it's greater than createdAt
    const queuedChildStatus = await DBOS.getWorkflowStatus(queuedChildID!);
    expect(queuedChildStatus).not.toBeNull();
    expect(queuedChildStatus!.parentWorkflowID).toBe(queuedParentWfid);
    expect(queuedChildStatus!.dequeuedAt).toBeDefined();
    expect(queuedChildStatus!.dequeuedAt).toBeGreaterThanOrEqual(queuedChildStatus!.createdAt);
  });
});

describe('test-fork', () => {
  let config: DBOSConfig;
  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });
  beforeEach(async () => {
    ExampleWorkflow.stepOneCount = 0;
    ExampleWorkflow.stepTwoCount = 0;
    ExampleWorkflow.stepThreeCount = 0;
    ExampleWorkflow.stepFourCount = 0;
    ExampleWorkflow.stepFiveCount = 0;
    ExampleWorkflow.transactionOneCount = 0;
    ExampleWorkflow.transactionTwoCount = 0;
    ExampleWorkflow.transactionThreeCount = 0;
    ExampleWorkflow.childWorkflowCount = 0;
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
    await DBOS.registerQueue('test_resume_fork_queue', { onConflict: 'always_update' });
  });
  afterEach(async () => {
    await DBOS.shutdown();
  });

  class ExampleWorkflow {
    static stepOneCount = 0;
    static stepTwoCount = 0;
    static stepThreeCount = 0;
    static stepFourCount = 0;
    static stepFiveCount = 0;
    static transactionOneCount = 0;
    static transactionTwoCount = 0;
    static transactionThreeCount = 0;
    static childWorkflowCount = 0;
    static steplessCount = 0;

    @DBOS.workflow()
    static async steplessWorkflow(): Promise<number> {
      ExampleWorkflow.steplessCount += 1;
      return Promise.resolve(42);
    }

    @DBOS.workflow()
    static async stepsWorkflow(input: number): Promise<number> {
      let result = await ExampleWorkflow.stepOne(1);
      result += await ExampleWorkflow.stepTwo(2);
      result += await ExampleWorkflow.stepThree(3);
      result += await ExampleWorkflow.stepFour(4);
      result += await ExampleWorkflow.stepFive(5);
      return result * input;
    }

    @DBOS.step()
    static async stepOne(input: number): Promise<number> {
      ExampleWorkflow.stepOneCount += 1;
      return Promise.resolve(1 * input);
    }

    @DBOS.step()
    static async stepTwo(input: number): Promise<number> {
      ExampleWorkflow.stepTwoCount += 1;
      return Promise.resolve(2 * input);
    }

    @DBOS.step()
    static async stepThree(input: number): Promise<number> {
      ExampleWorkflow.stepThreeCount += 1;
      return Promise.resolve(3 * input);
    }

    @DBOS.step()
    static async stepFour(input: number): Promise<number> {
      ExampleWorkflow.stepFourCount += 1;
      return Promise.resolve(4 * input);
    }

    @DBOS.step()
    static async stepFive(input: number): Promise<number> {
      ExampleWorkflow.stepFiveCount += 1;
      return Promise.resolve(5 * input);
    }

    @DBOS.workflow()
    static async childWorkflow() {
      ExampleWorkflow.childWorkflowCount += 1;
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async forkWorkflow(id: string, stepID: number): Promise<string> {
      const handle = await DBOS.forkWorkflow(id, stepID);
      await handle.getResult();
      return handle.workflowID;
    }

    @DBOS.workflow()
    static async parentWorkflow() {
      await ExampleWorkflow.stepOne(1);
      const handle = await DBOS.startWorkflow(ExampleWorkflow).childWorkflow();
      await handle.getResult();
      await ExampleWorkflow.stepTwo(1);
    }

    @DBOS.step()
    static async failableStepOne(): Promise<number> {
      ExampleWorkflow.stepOneCount++;
      return Promise.resolve(1);
    }

    @DBOS.step()
    static async failableStepTwo(): Promise<number> {
      ExampleWorkflow.stepTwoCount++;
      if (ExampleWorkflow.stepTwoCount === 1) {
        throw new Error('step two failed');
      }
      return Promise.resolve(2);
    }

    @DBOS.step()
    static async failableStepThree(): Promise<number> {
      ExampleWorkflow.stepThreeCount++;
      if (ExampleWorkflow.stepThreeCount === 1) {
        throw new Error('step three failed');
      }
      return Promise.resolve(3);
    }

    @DBOS.workflow()
    static async failableThreeStepWorkflow(): Promise<number> {
      const a = await ExampleWorkflow.failableStepOne();
      const b = await ExampleWorkflow.failableStepTwo();
      const c = await ExampleWorkflow.failableStepThree();
      return a + b + c;
    }
  }

  test('test-fork-steps', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).stepsWorkflow(10);
    const result: number = await handle.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);
    expect(ExampleWorkflow.stepThreeCount).toBe(1);
    expect(ExampleWorkflow.stepFourCount).toBe(1);
    expect(ExampleWorkflow.stepFiveCount).toBe(1);

    const forkedHandle = await DBOS.forkWorkflow(wfid, 0);
    const forkedStatus = await forkedHandle.getStatus();
    expect(forkedStatus?.forkedFrom).toBe(wfid);
    expect(forkedStatus?.timeoutMS).toBeUndefined(); // No timeout when not specified
    let forkresult = await forkedHandle.getResult();
    expect(forkresult).toBe(550);

    // Fork with an explicit timeout and verify it's set
    const forkedWithTimeout = await DBOS.forkWorkflow(wfid, 0, { timeoutMS: 60000 });
    const timeoutStatus = await forkedWithTimeout.getStatus();
    expect(timeoutStatus?.timeoutMS).toBe(60000);
    await forkedWithTimeout.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(3);
    expect(ExampleWorkflow.stepTwoCount).toBe(3);
    expect(ExampleWorkflow.stepThreeCount).toBe(3);
    expect(ExampleWorkflow.stepFourCount).toBe(3);
    expect(ExampleWorkflow.stepFiveCount).toBe(3);

    const forkedHandle2 = await DBOS.forkWorkflow(wfid, 2);
    expect((await forkedHandle2.getStatus())?.forkedFrom).toBe(wfid);
    forkresult = await forkedHandle2.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(3);
    expect(ExampleWorkflow.stepTwoCount).toBe(3);
    expect(ExampleWorkflow.stepThreeCount).toBe(4);
    expect(ExampleWorkflow.stepFourCount).toBe(4);
    expect(ExampleWorkflow.stepFiveCount).toBe(4);

    const forkedHandle3 = await DBOS.forkWorkflow(wfid, 4);
    expect((await forkedHandle3.getStatus())?.forkedFrom).toBe(wfid);
    forkresult = await forkedHandle3.getResult();
    expect(forkresult).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(3);
    expect(ExampleWorkflow.stepTwoCount).toBe(3);
    expect(ExampleWorkflow.stepThreeCount).toBe(4);
    expect(ExampleWorkflow.stepFourCount).toBe(4);
    expect(ExampleWorkflow.stepFiveCount).toBe(5);

    const forkedWorkflows = await DBOS.listWorkflows({ forkedFrom: handle.workflowID });
    expect(forkedWorkflows.length).toBe(4);
    expect(forkedWorkflows[0].workflowID).toBe(forkedHandle.workflowID);
    expect(forkedWorkflows[1].workflowID).toBe(forkedWithTimeout.workflowID);
    expect(forkedWorkflows[2].workflowID).toBe(forkedHandle2.workflowID);
    expect(forkedWorkflows[3].workflowID).toBe(forkedHandle3.workflowID);

    // Test forkedFrom as a list
    const forkedWorkflows2 = await DBOS.listWorkflows({ forkedFrom: [handle.workflowID, 'nonexistent-id'] });
    expect(forkedWorkflows2.length).toBe(4);

    const forkedWorkflows3 = await DBOS.listWorkflows({ forkedFrom: ['nonexistent-id'] });
    expect(forkedWorkflows3.length).toBe(0);

    // The original workflow should be marked as having been forked from.
    const originalStatus = await handle.getStatus();
    expect(originalStatus?.wasForkedFrom).toBe(true);
    // Forked workflows are not themselves forked from.
    for (const fh of [forkedHandle, forkedWithTimeout, forkedHandle2, forkedHandle3]) {
      const forkStatus = await fh.getStatus();
      expect(forkStatus?.wasForkedFrom).toBe(false);
    }

    // Filter by wasForkedFrom=true returns only the original; false returns only the forks.
    const forkedFromWorkflows = await DBOS.listWorkflows({ wasForkedFrom: true });
    expect(forkedFromWorkflows.length).toBe(1);
    expect(forkedFromWorkflows[0].workflowID).toBe(wfid);
    const notForkedFromWorkflows = await DBOS.listWorkflows({ wasForkedFrom: false });
    const notForkedFromIDs = new Set(notForkedFromWorkflows.map((w) => w.workflowID));
    expect(notForkedFromIDs).toContain(forkedHandle.workflowID);
    expect(notForkedFromIDs).toContain(forkedWithTimeout.workflowID);
    expect(notForkedFromIDs).toContain(forkedHandle2.workflowID);
    expect(notForkedFromIDs).toContain(forkedHandle3.workflowID);

    // isFork filters the other end of the relationship: the forks themselves.
    const forkIDs = [
      forkedHandle.workflowID,
      forkedWithTimeout.workflowID,
      forkedHandle2.workflowID,
      forkedHandle3.workflowID,
    ];
    const isForkWorkflows = await DBOS.listWorkflows({ isFork: true });
    const isForkIDs = new Set(isForkWorkflows.map((w) => w.workflowID));
    for (const w of isForkWorkflows) {
      expect(w.forkedFrom).toBeDefined();
    }
    for (const id of forkIDs) {
      expect(isForkIDs).toContain(id);
    }
    expect(isForkIDs).not.toContain(wfid);

    const notForkWorkflows = await DBOS.listWorkflows({ isFork: false });
    const notForkIDs = new Set(notForkWorkflows.map((w) => w.workflowID));
    for (const w of notForkWorkflows) {
      expect(w.forkedFrom).toBeUndefined();
    }
    expect(notForkIDs).toContain(wfid);
    for (const id of forkIDs) {
      expect(notForkIDs).not.toContain(id);
    }
  });

  test('test-fork-childwf', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).parentWorkflow();
    await handle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.childWorkflowCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);

    const forkedHandle = await DBOS.forkWorkflow(wfid, 2);
    expect((await forkedHandle.getStatus())?.forkedFrom).toBe(wfid);
    await forkedHandle.getResult();
    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.childWorkflowCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
  });

  test('test-fork-fromaworklow', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).parentWorkflow();
    await handle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.childWorkflowCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);

    const forkwfid = randomUUID();
    const forkHandle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: forkwfid }).forkWorkflow(wfid, 0);
    const firstforkedid = await forkHandle.getResult();

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.childWorkflowCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);

    // Fork the workflow again
    const forkHandle2 = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: forkwfid }).forkWorkflow(wfid, 0);
    const secondforkedid = await forkHandle2.getResult();

    expect(firstforkedid).toEqual(secondforkedid);
    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.childWorkflowCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
  });

  test('test-fork-WithNextWorkflowId', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).stepsWorkflow(10);
    const result: number = await handle.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);
    expect(ExampleWorkflow.stepThreeCount).toBe(1);
    expect(ExampleWorkflow.stepFourCount).toBe(1);
    expect(ExampleWorkflow.stepFiveCount).toBe(1);

    const forkedWfid = randomUUID();

    await DBOS.withNextWorkflowID(forkedWfid, async () => {
      const forkedHandle = await DBOS.forkWorkflow(wfid, 0);
      expect((await forkedHandle.getStatus())?.forkedFrom).toBe(wfid);
      const forkresult = await forkedHandle.getResult();
      expect(forkresult).toBe(550);
      expect(forkedHandle.workflowID).toBe(forkedWfid);
    });

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(2);
    expect(ExampleWorkflow.stepFourCount).toBe(2);
    expect(ExampleWorkflow.stepFiveCount).toBe(2);
  });

  test('test-fork-version', async () => {
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ExampleWorkflow, { workflowID: wfid }).stepsWorkflow(10);
    const result: number = await handle.getResult();
    expect(result).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(1);
    expect(ExampleWorkflow.stepTwoCount).toBe(1);
    expect(ExampleWorkflow.stepThreeCount).toBe(1);
    expect(ExampleWorkflow.stepFourCount).toBe(1);
    expect(ExampleWorkflow.stepFiveCount).toBe(1);

    const applicationVersion = 'newVersion';

    globalParams.appVersion = applicationVersion;
    const forkedHandle = await DBOS.forkWorkflow(wfid, 0, { applicationVersion });

    const status = await forkedHandle.getStatus();
    const returnedVersion = status?.applicationVersion;
    expect(returnedVersion).toBe(applicationVersion);

    const forkresult = await forkedHandle.getResult();
    expect(forkresult).toBe(550);

    expect(ExampleWorkflow.stepOneCount).toBe(2);
    expect(ExampleWorkflow.stepTwoCount).toBe(2);
    expect(ExampleWorkflow.stepThreeCount).toBe(2);
    expect(ExampleWorkflow.stepFourCount).toBe(2);
    expect(ExampleWorkflow.stepFiveCount).toBe(2);
  });

  const testForkStreamsKey = 'key';
  const testForkStreamsEvent = new Event();
  // Step that writes to stream
  const streamStep = DBOS.registerStep(
    async (val: number) => {
      await DBOS.writeStream(testForkStreamsKey, val);
    },
    { name: 'stream-step' },
  );

  // Workflow: waits on event, writes 0, writes 1, calls step(2), closes stream
  const streamWorkflow = DBOS.registerWorkflow(
    async () => {
      await testForkStreamsEvent.wait();
      await DBOS.writeStream(testForkStreamsKey, 0); // function_id = 0
      await DBOS.writeStream(testForkStreamsKey, 1); // function_id = 1
      await streamStep(2); // function_id = 2
      await DBOS.closeStream(testForkStreamsKey); // function_id = 3
      return DBOS.workflowID!;
    },
    { name: 'stream-fork-workflow' },
  );

  test('test-fork-streams', async () => {
    // Helper to read N values from a stream without blocking forever
    async function readStreamN(workflowID: string, n: number): Promise<number[]> {
      if (n === 0) return [];
      const values: number[] = [];
      for await (const value of DBOS.readStream(workflowID, testForkStreamsKey)) {
        values.push(value as number);
        if (values.length >= n) break;
      }
      return values;
    }

    // Run workflow to completion first
    testForkStreamsEvent.set();
    const handle = await DBOS.startWorkflow(streamWorkflow, {})();
    expect(await handle.getResult()).toBe(handle.workflowID);

    // Verify original stream has [0, 1, 2]
    const allValues: number[] = [];
    for await (const v of DBOS.readStream(handle.workflowID, testForkStreamsKey)) {
      allValues.push(v as number);
    }
    expect(allValues).toEqual([0, 1, 2]);

    // Block workflow so forks can't advance
    testForkStreamsEvent.clear();

    // Fork from different points, verify streams have appropriate values
    const forkOne = await DBOS.forkWorkflow(handle.workflowID, 0);
    expect(await readStreamN(forkOne.workflowID, 0)).toEqual([]);

    const forkTwo = await DBOS.forkWorkflow(handle.workflowID, 1);
    expect(await readStreamN(forkTwo.workflowID, 1)).toEqual([0]);

    const forkThree = await DBOS.forkWorkflow(handle.workflowID, 2);
    expect(await readStreamN(forkThree.workflowID, 2)).toEqual([0, 1]);

    const forkFour = await DBOS.forkWorkflow(handle.workflowID, 3);
    expect(await readStreamN(forkFour.workflowID, 3)).toEqual([0, 1, 2]);

    const forkFive = await DBOS.forkWorkflow(handle.workflowID, 4);
    const forkFiveValues: number[] = [];
    for await (const value of DBOS.readStream(forkFive.workflowID, testForkStreamsKey)) {
      forkFiveValues.push(value as number);
    }
    expect(forkFiveValues).toEqual([0, 1, 2]);

    // Unblock the forked workflows, verify they successfully complete
    testForkStreamsEvent.set();
    for (const forkHandle of [forkOne, forkTwo, forkThree, forkFour, forkFive]) {
      expect(await forkHandle.getResult()).toBeTruthy();
      const finalValues: number[] = [];
      for await (const value of DBOS.readStream(forkHandle.workflowID, testForkStreamsKey)) {
        finalValues.push(value as number);
      }
      expect(finalValues).toEqual([0, 1, 2]);
    }
  });

  const testForkEventsKey = 'event_key';
  const testForkEventsEvent = new Event();

  // Workflow: waits on event, sets event to 0, 1, 2
  const eventWorkflow = DBOS.registerWorkflow(
    async () => {
      await testForkEventsEvent.wait();
      await DBOS.setEvent(testForkEventsKey, 0); // function_id = 0
      await DBOS.setEvent(testForkEventsKey, 1); // function_id = 1
      await DBOS.setEvent(testForkEventsKey, 2); // function_id = 2
      return DBOS.workflowID!;
    },
    { name: 'event-fork-workflow' },
  );

  test('test-fork-events', async () => {
    // Run workflow to completion first
    testForkEventsEvent.set();
    const handle = await DBOS.startWorkflow(eventWorkflow, {})();
    expect(await handle.getResult()).toBe(handle.workflowID);

    // Verify the event's final value is 2
    expect(await DBOS.getEvent(handle.workflowID, testForkEventsKey)).toBe(2);

    // Block workflow so forks can't advance
    testForkEventsEvent.clear();

    // Fork from different points, verify events have appropriate values
    const forkOne = await DBOS.forkWorkflow(handle.workflowID, 0);
    expect(await DBOS.getEvent(forkOne.workflowID, testForkEventsKey, 0)).toBeNull();

    const forkTwo = await DBOS.forkWorkflow(handle.workflowID, 1);
    expect(await DBOS.getEvent(forkTwo.workflowID, testForkEventsKey)).toBe(0);

    const forkThree = await DBOS.forkWorkflow(handle.workflowID, 2);
    expect(await DBOS.getEvent(forkThree.workflowID, testForkEventsKey)).toBe(1);

    const forkFour = await DBOS.forkWorkflow(handle.workflowID, 3);
    expect(await DBOS.getEvent(forkFour.workflowID, testForkEventsKey)).toBe(2);

    // Fork from a fork
    const forkFive = await DBOS.forkWorkflow(forkFour.workflowID, 3);
    expect(await DBOS.getEvent(forkFive.workflowID, testForkEventsKey)).toBe(2);

    // Unblock the forked workflows, verify they successfully complete
    testForkEventsEvent.set();
    for (const forkHandle of [forkOne, forkTwo, forkThree, forkFour, forkFive]) {
      expect(await forkHandle.getResult()).toBeTruthy();
      expect(await DBOS.getEvent(forkHandle.workflowID, testForkEventsKey)).toBe(2);
    }
  });

  class ResumeForkQueueWorkflow {
    static stepOneCount = 0;
    static stepTwoCount = 0;
    static step1Gate = new Event();
    static step1Started = new Event();

    @DBOS.step()
    static async stepOne(x: number): Promise<number> {
      await Promise.resolve();
      ResumeForkQueueWorkflow.stepOneCount++;
      return x + 1;
    }

    @DBOS.step()
    static async stepTwo(x: number): Promise<number> {
      await Promise.resolve();
      ResumeForkQueueWorkflow.stepTwoCount++;
      return x + 2;
    }

    @DBOS.workflow()
    static async simpleWorkflow(x: number): Promise<number> {
      const a = await ResumeForkQueueWorkflow.stepOne(x);
      ResumeForkQueueWorkflow.step1Started.set();
      await ResumeForkQueueWorkflow.step1Gate.wait();
      const b = await ResumeForkQueueWorkflow.stepTwo(x);
      return a + b;
    }
  }

  test('test-resume-and-fork-to-queue', async () => {
    ResumeForkQueueWorkflow.stepOneCount = 0;
    ResumeForkQueueWorkflow.stepTwoCount = 0;
    ResumeForkQueueWorkflow.step1Gate = new Event();
    ResumeForkQueueWorkflow.step1Started = new Event();

    const input = 5;
    const expectedOutput = input + 1 + (input + 2);

    // Enqueue workflow, let stepOne run, then cancel before stepTwo
    const wfid = randomUUID();
    const handle = await DBOS.startWorkflow(ResumeForkQueueWorkflow, {
      workflowID: wfid,
      queueName: 'test_resume_fork_queue',
    }).simpleWorkflow(input);
    await ResumeForkQueueWorkflow.step1Started.wait();
    await DBOS.cancelWorkflow(wfid);
    ResumeForkQueueWorkflow.step1Gate.set();
    await expect(handle.getResult()).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(DBOS.getWorkflowStatus(wfid)).resolves.toMatchObject({ status: StatusString.CANCELLED });
    expect(ResumeForkQueueWorkflow.stepOneCount).toBe(1);
    expect(ResumeForkQueueWorkflow.stepTwoCount).toBe(0);

    // Resume the workflow onto the queue and verify queue_name
    ResumeForkQueueWorkflow.step1Gate = new Event();
    ResumeForkQueueWorkflow.step1Gate.set(); // Don't block on replay
    ResumeForkQueueWorkflow.step1Started = new Event();
    const resumedHandle = await DBOS.resumeWorkflow(wfid, { queueName: 'test_resume_fork_queue' });
    const resumedStatus = await resumedHandle.getStatus();
    expect(resumedStatus?.queueName).toBe('test_resume_fork_queue');
    await expect(resumedHandle.getResult()).resolves.toBe(expectedOutput);
    expect(ResumeForkQueueWorkflow.stepOneCount).toBe(1); // Step 1 replayed from checkpoint
    expect(ResumeForkQueueWorkflow.stepTwoCount).toBe(1);

    // Fork the workflow onto the queue from step 1 and verify queue_name
    ResumeForkQueueWorkflow.step1Gate = new Event();
    ResumeForkQueueWorkflow.step1Gate.set();
    ResumeForkQueueWorkflow.step1Started = new Event();
    const forkedHandle = await DBOS.forkWorkflow(wfid, 1, {
      queueName: 'test_resume_fork_queue',
      queuePartitionKey: 'my_partition',
    });
    const forkedStatus = await forkedHandle.getStatus();
    expect(forkedStatus?.queueName).toBe('test_resume_fork_queue');
    expect(forkedStatus?.forkedFrom).toBe(wfid);
    await expect(forkedHandle.getResult()).resolves.toBe(expectedOutput);
    expect(ResumeForkQueueWorkflow.stepOneCount).toBe(1); // Step 1 replayed from checkpoint
    expect(ResumeForkQueueWorkflow.stepTwoCount).toBe(2); // Step 2 was re-executed
  });

  let replacementChildMultiplier = 2;

  class ReplacementChildTest {
    static childIds: string[] = [];

    @DBOS.step()
    static async childStep(x: number): Promise<number> {
      return Promise.resolve(x * replacementChildMultiplier);
    }

    @DBOS.workflow()
    static async childWf(x: number): Promise<number> {
      return await ReplacementChildTest.childStep(x);
    }

    @DBOS.step()
    static async combine(results: number[]): Promise<number> {
      return Promise.resolve(results.reduce((a, b) => a + b, 0));
    }

    @DBOS.workflow()
    static async parentWf(): Promise<number> {
      const h1 = await DBOS.startWorkflow(ReplacementChildTest).childWf(10);
      const h2 = await DBOS.startWorkflow(ReplacementChildTest).childWf(20);
      const h3 = await DBOS.startWorkflow(ReplacementChildTest).childWf(30);
      const h4 = await DBOS.startWorkflow(ReplacementChildTest).childWf(40);
      const h5 = await DBOS.startWorkflow(ReplacementChildTest).childWf(50);
      ReplacementChildTest.childIds = [h1, h2, h3, h4, h5].map((h) => h.workflowID);
      const results = await Promise.all([h1, h2, h3, h4, h5].map((h) => h.getResult()));
      return await ReplacementChildTest.combine(results);
    }
  }

  test('test-fork-replacement-children', async () => {
    replacementChildMultiplier = 2;

    const parentHandle = await DBOS.startWorkflow(ReplacementChildTest).parentWf();
    const originalResult = await parentHandle.getResult();
    expect(originalResult).toBe(300); // sum of x*2 for x in [10,20,30,40,50]
    expect(ReplacementChildTest.childIds.length).toBe(5);
    const origIds = [...ReplacementChildTest.childIds];

    replacementChildMultiplier = 10;

    // Fork children 0, 2, and 4 from step 0 (re-run childStep with new multiplier)
    const forkedChild0 = await DBOS.forkWorkflow(origIds[0], 0);
    const forkedChild2 = await DBOS.forkWorkflow(origIds[2], 0);
    const forkedChild4 = await DBOS.forkWorkflow(origIds[4], 0);
    expect(await forkedChild0.getResult()).toBe(100);
    expect(await forkedChild2.getResult()).toBe(300);
    expect(await forkedChild4.getResult()).toBe(500);

    // Fork the parent from step 5 (combine): replays steps 0-4 with replaced child IDs, re-runs combine
    const forkedParent = await DBOS.forkWorkflow(parentHandle.workflowID, 5, {
      replacementChildren: {
        [origIds[0]]: forkedChild0.workflowID,
        [origIds[2]]: forkedChild2.workflowID,
        [origIds[4]]: forkedChild4.workflowID,
      },
    });
    const forkedResult = await forkedParent.getResult();
    expect(forkedResult).toBe(1020); // [100, 40, 300, 80, 500]
  });
});

describe('wf-cancel-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    WFwith2Steps.stepsExecuted = 0;
    WFwith2Steps.step1Started = new Event();
    WFwith2Steps.step1Gate = new Event();
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('test-two-steps-cancel-resume', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    // Wait for step1 to start, then cancel before it completes
    await WFwith2Steps.step1Started.wait();
    await DBOS.cancelWorkflow(wfid);
    WFwith2Steps.step1Gate.set();

    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    expect(WFwith2Steps.stepsExecuted).toBe(1);

    const wfstatus = await DBOS.getWorkflowStatus(wfid);
    expect(wfstatus?.status).toBe(StatusString.CANCELLED);

    // Resume and let it complete - reset the gate for the replayed step1
    WFwith2Steps.step1Gate = new Event();
    WFwith2Steps.step1Gate.set();
    const wfh2 = await DBOS.resumeWorkflow(wfid);
    await wfh2.getResult();
    const resstatus = await DBOS.getWorkflowStatus(wfid);
    expect(resstatus?.status).toBe(StatusString.SUCCESS);
  });

  test('test-resume-on-a-completed-ws', async () => {
    const wfid = randomUUID();
    WFwith2Steps.step1Gate.set();
    const wfh = await DBOS.startWorkflow(WFwith2Steps, { workflowID: wfid }).workflowWithSteps();

    await wfh.getResult();

    expect(WFwith2Steps.stepsExecuted).toBe(2);

    await DBOS.resumeWorkflow(wfid);
    await DBOS.getWorkflowStatus(wfid);

    expect(WFwith2Steps.stepsExecuted).toBe(2);
  });

  test('test-preempt-getresult', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).getResultTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-getevent', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).getEventTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  test('test-preempt-recv', async () => {
    const wfid = randomUUID();
    const wfh = await DBOS.startWorkflow(DeepSleep, { workflowID: wfid }).recvTooLong();

    await expect(DBOS.getResult(wfh.workflowID, 0.2)).resolves.toBeNull();
    await DBOS.cancelWorkflow(wfid);

    await expect(DBOS.getResult(wfh.workflowID)).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    await expect(wfh.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  // A workflow cancelled while blocked awaiting other workflows must not durably
  // record its own DBOSWorkflowCancelledError as the awaiting step's checkpoint:
  // CANCELLED is resumable, so on resume the parent must re-execute the await and
  // pick up the child's result instead of replaying the recorded cancellation.
  // Affected internal steps: DBOS.getResult, DBOS.waitFirst, DBOS.waitAll — the ones
  // built on runInternalStep whose callbacks poll the caller's own status.
  describe('cancel-during-await', () => {
    class CancelDuringAwait {
      static childEvent = new Event();
      static parentPolling = new Event();

      @DBOS.workflow()
      static async blockedChild(value: number) {
        await CancelDuringAwait.childEvent.wait();
        return value;
      }

      @DBOS.workflow()
      static async getResultParent(childID: string) {
        CancelDuringAwait.parentPolling.set();
        return await DBOS.getResult<number>(childID, { pollingIntervalMs: 50 });
      }

      @DBOS.workflow()
      static async waitFirstParent(childID: string) {
        const handle = DBOS.retrieveWorkflow<number>(childID);
        CancelDuringAwait.parentPolling.set();
        await DBOS.waitFirst([handle], { pollingIntervalMs: 50 });
        return await DBOS.getResult<number>(childID, { pollingIntervalMs: 50 });
      }

      @DBOS.workflow()
      static async waitAllParent(childID: string) {
        const handle = DBOS.retrieveWorkflow<number>(childID);
        CancelDuringAwait.parentPolling.set();
        await DBOS.waitAll([handle], { pollingIntervalMs: 50 });
        return await DBOS.getResult<number>(childID, { pollingIntervalMs: 50 });
      }
    }

    beforeEach(() => {
      CancelDuringAwait.childEvent = new Event();
      CancelDuringAwait.parentPolling = new Event();
    });

    afterEach(() => {
      CancelDuringAwait.childEvent.set(); // unblock any still-pending child
    });

    const variants: { step: string; start: (childID: string) => Promise<WorkflowHandle<number | null>> }[] = [
      { step: 'DBOS.getResult', start: (childID) => DBOS.startWorkflow(CancelDuringAwait).getResultParent(childID) },
      { step: 'DBOS.waitFirst', start: (childID) => DBOS.startWorkflow(CancelDuringAwait).waitFirstParent(childID) },
      { step: 'DBOS.waitAll', start: (childID) => DBOS.startWorkflow(CancelDuringAwait).waitAllParent(childID) },
    ];

    async function cancelParentDuringAwait(start: (childID: string) => Promise<WorkflowHandle<number | null>>) {
      const childID = randomUUID();

      const childHandle = await DBOS.startWorkflow(CancelDuringAwait, { workflowID: childID }).blockedChild(42);
      const parentHandle = await start(childID);
      const parentID = parentHandle.workflowID;

      // Let the parent get past runInternalStep's entry checks and into
      // the sysdb poll loop before cancelling it.
      await CancelDuringAwait.parentPolling.wait();
      await sleepms(300);
      await DBOS.cancelWorkflow(parentID);

      // The parent observes its own cancellation inside the poll loop.
      await expect(parentHandle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
      await expect(DBOS.getWorkflowStatus(parentID)).resolves.toMatchObject({ status: StatusString.CANCELLED });

      return { parentID, childHandle };
    }

    describe.each(variants)('cancelled during $step', ({ step, start }) => {
      test('parent cancellation is not checkpointed as the step outcome', async () => {
        const { parentID } = await cancelParentDuringAwait(start);

        // The interrupted step must not be checkpointed, so it re-executes on resume.
        const steps = await DBOS.listWorkflowSteps(parentID);
        const awaitStep = steps?.find((s) => s.name === step);
        expect(awaitStep?.error).toBeFalsy();
      });

      test('parent picks up child result after resume', async () => {
        const { parentID, childHandle } = await cancelParentDuringAwait(start);

        // The child completes successfully.
        CancelDuringAwait.childEvent.set();
        await expect(childHandle.getResult()).resolves.toBe(42);

        // Resuming the parent should re-execute the await and return the child's result.
        const resumed = await DBOS.resumeWorkflow<number>(parentID);
        await expect(resumed.getResult()).resolves.toBe(42);
        await expect(DBOS.getWorkflowStatus(parentID)).resolves.toMatchObject({ status: StatusString.SUCCESS });
      });
    });
  });

  class WFwith2Steps {
    static stepsExecuted = 0;
    static step1Started = new Event();
    static step1Gate = new Event();

    @DBOS.step()
    static async step1() {
      WFwith2Steps.stepsExecuted++;
      WFwith2Steps.step1Started.set();
      await WFwith2Steps.step1Gate.wait();
    }

    @DBOS.step()
    static async step2() {
      await Promise.resolve();
      WFwith2Steps.stepsExecuted++;
    }

    @DBOS.workflow()
    static async workflowWithSteps() {
      await WFwith2Steps.step1();
      await WFwith2Steps.step2();
    }
  }

  class DeepSleep {
    @DBOS.workflow()
    static async getResultTooLong() {
      await DBOS.getResult('bogusbogusbogus', 1000);
      return 'Done';
    }

    @DBOS.workflow()
    static async recvTooLong() {
      await DBOS.recv('bogusbogusbogus', 1000);
      return 'Done';
    }

    @DBOS.workflow()
    static async getEventTooLong() {
      await DBOS.getEvent('bogusbogusbogus', 'notopic', 1000);
      return 'Done';
    }
  }

  // Delete workflow test
  class DeleteWorkflowTest {
    @DBOS.workflow()
    static async childWorkflow(x: number): Promise<number> {
      return Promise.resolve(x * 2);
    }

    @DBOS.workflow()
    static async parentWorkflow(x: number): Promise<number> {
      const handle = await DBOS.startWorkflow(DeleteWorkflowTest).childWorkflow(x);
      return handle.getResult();
    }
  }

  test('test-delete-workflow', async () => {
    // Run the parent workflow which starts a child workflow
    const parentWfid = randomUUID();
    const handle = await DBOS.startWorkflow(DeleteWorkflowTest, { workflowID: parentWfid }).parentWorkflow(5);
    const result = await handle.getResult();
    expect(result).toBe(10);

    // Get the child workflow ID
    const steps = await DBOS.listWorkflowSteps(parentWfid);
    const childWfid = steps!.find((s) => s.childWorkflowID)?.childWorkflowID;
    expect(childWfid).toBeDefined();

    // Verify both workflows exist
    expect(await DBOS.getWorkflowStatus(parentWfid)).not.toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid!)).not.toBeNull();

    // Delete without deleteChildren - only parent should be deleted
    await DBOS.deleteWorkflow(parentWfid, false);
    expect(await DBOS.getWorkflowStatus(parentWfid)).toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid!)).not.toBeNull();

    // Run again to test deleteChildren=true
    const parentWfid2 = randomUUID();
    const handle2 = await DBOS.startWorkflow(DeleteWorkflowTest, { workflowID: parentWfid2 }).parentWorkflow(7);
    const result2 = await handle2.getResult();
    expect(result2).toBe(14);

    const steps2 = await DBOS.listWorkflowSteps(parentWfid2);
    const childWfid2 = steps2!.find((s) => s.childWorkflowID)?.childWorkflowID;
    expect(childWfid2).toBeDefined();

    // Verify both workflows exist
    expect(await DBOS.getWorkflowStatus(parentWfid2)).not.toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid2!)).not.toBeNull();

    // Delete with deleteChildren=true - both should be deleted
    await DBOS.deleteWorkflow(parentWfid2, true);
    expect(await DBOS.getWorkflowStatus(parentWfid2)).toBeNull();
    expect(await DBOS.getWorkflowStatus(childWfid2!)).toBeNull();

    // Verify deleting a non-existent workflow doesn't error
    await DBOS.deleteWorkflow(parentWfid2, false);
  });

  // ==================== Bulk Cancel/Resume/Delete Tests ====================

  class BulkCancelTest {
    static stepsCompleted = 0;
    static workflowEvents: Record<string, Event> = {};
    static mainEvents: Record<string, Event> = {};

    @DBOS.step()
    static async stepOne(): Promise<void> {
      await Promise.resolve();
      BulkCancelTest.stepsCompleted++;
    }

    @DBOS.step()
    static async stepTwo(): Promise<void> {
      await Promise.resolve();
      BulkCancelTest.stepsCompleted++;
    }

    @DBOS.workflow()
    static async blockingWorkflow(): Promise<string> {
      const wfid = DBOS.workflowID!;
      await BulkCancelTest.stepOne();
      BulkCancelTest.mainEvents[wfid].set();
      await BulkCancelTest.workflowEvents[wfid].wait();
      await BulkCancelTest.stepTwo();
      return wfid;
    }
  }

  test('test-bulk-cancel', async () => {
    BulkCancelTest.stepsCompleted = 0;
    BulkCancelTest.workflowEvents = {};
    BulkCancelTest.mainEvents = {};

    const wfids: string[] = [];
    const handles: WorkflowHandle<string>[] = [];
    for (let i = 0; i < 3; i++) {
      const wfid = randomUUID();
      wfids.push(wfid);
      BulkCancelTest.workflowEvents[wfid] = new Event();
      BulkCancelTest.mainEvents[wfid] = new Event();
      const h = await DBOS.startWorkflow(BulkCancelTest, { workflowID: wfid }).blockingWorkflow();
      handles.push(h);
      await BulkCancelTest.mainEvents[wfid].wait();
    }

    expect(BulkCancelTest.stepsCompleted).toBe(3);

    await DBOS.cancelWorkflows(wfids);

    for (const wfid of wfids) {
      BulkCancelTest.workflowEvents[wfid].set();
    }

    for (const handle of handles) {
      await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    }

    expect(BulkCancelTest.stepsCompleted).toBe(3);
  });

  class CancelChildrenTest {
    static childID: string;
    static grandchildID: string;
    static workflowEvents: Record<string, Event> = {};
    static mainEvents: Record<string, Event> = {};

    @DBOS.step()
    static async noop(): Promise<void> {
      await Promise.resolve();
    }

    @DBOS.workflow()
    static async grandchildWorkflow(): Promise<string> {
      const wfid = DBOS.workflowID!;
      CancelChildrenTest.mainEvents[wfid].set();
      await CancelChildrenTest.workflowEvents[wfid].wait();
      // A step after the wait so the workflow observes its cancellation.
      await CancelChildrenTest.noop();
      return wfid;
    }

    @DBOS.workflow()
    static async childWorkflow(): Promise<string> {
      const wfid = DBOS.workflowID!;
      await DBOS.startWorkflow(CancelChildrenTest, {
        workflowID: CancelChildrenTest.grandchildID,
      }).grandchildWorkflow();
      CancelChildrenTest.mainEvents[wfid].set();
      await CancelChildrenTest.workflowEvents[wfid].wait();
      await CancelChildrenTest.noop();
      return wfid;
    }

    @DBOS.workflow()
    static async parentWorkflow(): Promise<string> {
      const wfid = DBOS.workflowID!;
      await DBOS.startWorkflow(CancelChildrenTest, { workflowID: CancelChildrenTest.childID }).childWorkflow();
      CancelChildrenTest.mainEvents[wfid].set();
      await CancelChildrenTest.workflowEvents[wfid].wait();
      await CancelChildrenTest.noop();
      return wfid;
    }
  }

  test('test-cancel-workflow-children', async () => {
    // Build a three-level tree: parent -> child -> grandchild, each blocking.
    const parentID = randomUUID();
    const childID = randomUUID();
    const grandchildID = randomUUID();
    const ids = [parentID, childID, grandchildID];

    CancelChildrenTest.childID = childID;
    CancelChildrenTest.grandchildID = grandchildID;
    CancelChildrenTest.workflowEvents = {};
    CancelChildrenTest.mainEvents = {};
    for (const id of ids) {
      CancelChildrenTest.workflowEvents[id] = new Event();
      CancelChildrenTest.mainEvents[id] = new Event();
    }

    const parentHandle = await DBOS.startWorkflow(CancelChildrenTest, { workflowID: parentID }).parentWorkflow();

    // Wait until the whole tree is running and blocked
    for (const id of ids) {
      await CancelChildrenTest.mainEvents[id].wait();
    }

    // The cascade should discover the full descendant tree
    const children = await DBOSExecutor.globalInstance!.systemDatabase.getWorkflowChildren(parentID);
    expect(new Set(children)).toEqual(new Set([childID, grandchildID]));

    // Cancelling without cancelChildren only affects the parent
    await DBOS.cancelWorkflow(parentID);
    expect((await DBOS.getWorkflowStatus(parentID))!.status).toBe(StatusString.CANCELLED);
    expect((await DBOS.getWorkflowStatus(childID))!.status).not.toBe(StatusString.CANCELLED);
    expect((await DBOS.getWorkflowStatus(grandchildID))!.status).not.toBe(StatusString.CANCELLED);

    // Cancelling with cancelChildren cancels the entire subtree
    await DBOS.cancelWorkflow(parentID, { cancelChildren: true });
    for (const id of ids) {
      expect((await DBOS.getWorkflowStatus(id))!.status).toBe(StatusString.CANCELLED);
    }

    // Release the workflows so they observe the cancellation
    for (const id of ids) {
      CancelChildrenTest.workflowEvents[id].set();
    }

    await expect(parentHandle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
  });

  class StepCancelSignalTest {
    static plainRuns = 0;
    static attempts = 0;
    static signals: AbortSignal[] = [];
    static blocked = new Event();
    static stopped = new Event();
    static finish = false;

    @DBOS.step()
    static async plainStep(): Promise<string> {
      StepCancelSignalTest.plainRuns++;
      return Promise.resolve('plain');
    }

    // After attempt 2 the backoff would be 10s, so a prompt cancellation shows no further retry was scheduled
    @DBOS.step({ retriesAllowed: true, maxAttempts: 3, intervalSeconds: 0.1, backoffRate: 100 })
    static async cooperativeStep(): Promise<string> {
      const attempt = ++StepCancelSignalTest.attempts;
      const signal = DBOS.stepStatus!.cancelSignal;
      StepCancelSignalTest.signals.push(signal);
      if (attempt === 1) throw new Error('transient failure');
      if (StepCancelSignalTest.finish) return `done-${attempt}`;
      StepCancelSignalTest.blocked.set();
      try {
        await abortableSleep(10_000, undefined, { signal });
      } finally {
        StepCancelSignalTest.stopped.set();
      }
      return `slept-${attempt}`;
    }

    @DBOS.workflow()
    static async signalWorkflow(): Promise<string> {
      const plain = await StepCancelSignalTest.plainStep();
      const result = await StepCancelSignalTest.cooperativeStep();
      return `${plain}-${result}`;
    }
  }

  test('test-step-cancel-signal', async () => {
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    sysdb.dbPollingIntervalCancelMs = 100;
    StepCancelSignalTest.plainRuns = 0;
    StepCancelSignalTest.attempts = 0;
    StepCancelSignalTest.signals = [];
    StepCancelSignalTest.blocked = new Event();
    StepCancelSignalTest.stopped = new Event();
    StepCancelSignalTest.finish = false;
    const wfid = randomUUID();

    // Attempt 1 fails and is retried; attempt 2 blocks until the signal fires
    const handle = await DBOS.startWorkflow(StepCancelSignalTest, { workflowID: wfid }).signalWorkflow();
    await StepCancelSignalTest.blocked.wait();
    const [first, second] = StepCancelSignalTest.signals;
    expect(second).toBe(first);
    expect(first.aborted).toBe(false);
    expect((await DBOS.getWorkflowStatus(wfid))!.status).toBe(StatusString.PENDING);

    // An in-process cancel fires the signal; the aborted step is neither retried nor recorded
    let cancelledAt = Date.now();
    await DBOS.cancelWorkflow(wfid);
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    expect(Date.now() - cancelledAt).toBeLessThan(5000);
    expect(first.aborted).toBe(true);
    expect(first.reason).toBeInstanceOf(DBOSWorkflowCancelledError);
    expect((first.reason as DBOSWorkflowCancelledError).workflowID).toBe(wfid);
    expect(StepCancelSignalTest.attempts).toBe(2);
    expect((await DBOS.getWorkflowStatus(wfid))!.status).toBe(StatusString.CANCELLED);
    let steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps!.map((s) => s.name)).toEqual(['plainStep']);

    // On resume the step runs again with a fresh signal, which a cancel from a client also fires
    while (sysdb.checkForRunningWorkflow(wfid)) {
      await sleepms(20);
    }
    StepCancelSignalTest.blocked = new Event();
    StepCancelSignalTest.stopped = new Event();
    const resumed = await DBOS.resumeWorkflow<string>(wfid);
    await StepCancelSignalTest.blocked.wait();
    const third = StepCancelSignalTest.signals[2];
    expect(third).not.toBe(first);
    expect(third.aborted).toBe(false);
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      cancelledAt = Date.now();
      await client.cancelWorkflow(wfid);
      // The handle reports the cancelled status at once, so wait for the step itself to stop
      await StepCancelSignalTest.stopped.wait();
      expect(Date.now() - cancelledAt).toBeLessThan(5000);
      await expect(resumed.getResult()).rejects.toThrow(DBOSAwaitedWorkflowCancelledError);
    } finally {
      await client.destroy();
    }
    expect(third.reason).toBeInstanceOf(DBOSWorkflowCancelledError);
    expect(StepCancelSignalTest.attempts).toBe(3);
    expect(StepCancelSignalTest.plainRuns).toBe(1);
    steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps!.map((s) => s.name)).toEqual(['plainStep']);

    // A final resume lets the step complete: its result is recorded and its signal never fires
    while (sysdb.checkForRunningWorkflow(wfid)) {
      await sleepms(20);
    }
    StepCancelSignalTest.finish = true;
    const completed = await DBOS.resumeWorkflow<string>(wfid);
    await expect(completed.getResult()).resolves.toBe('plain-done-4');
    expect(StepCancelSignalTest.signals[3].aborted).toBe(false);
    expect(StepCancelSignalTest.plainRuns).toBe(1);
    expect((await DBOS.getWorkflowStatus(wfid))!.status).toBe(StatusString.SUCCESS);
    steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps!.map((s) => [s.name, s.output])).toEqual([
      ['plainStep', 'plain'],
      ['cooperativeStep', 'done-4'],
    ]);
  });

  // Leaves ample margin for cancel detection to beat the running attempt's timeout on a slow runner
  const cancelTestStepTimeoutMS = 2000;

  class StepCancelAndTimeoutTest {
    static attempts = 0;
    static timeoutSignals: AbortSignal[] = [];
    static cancelSignals: AbortSignal[] = [];
    static observed: unknown[] = []; // The abort reason each attempt stopped on
    static thirdAttemptStarted = new Event();
    static abandonedAttemptEnded = new Event();

    // Fires with the reason of whichever of `signals` fires first
    static anySignal(signals: AbortSignal[]): AbortSignal {
      const controller = new AbortController();
      for (const signal of signals) {
        if (signal.aborted) controller.abort(signal.reason);
        else signal.addEventListener('abort', () => controller.abort(signal.reason), { once: true });
      }
      return controller.signal;
    }

    @DBOS.step({ retriesAllowed: true, maxAttempts: 3, intervalSeconds: 0, timeoutMS: cancelTestStepTimeoutMS })
    static async timedStep(): Promise<string> {
      const attempt = ++StepCancelAndTimeoutTest.attempts;
      const { timeoutSignal, cancelSignal } = DBOS.stepStatus!;
      StepCancelAndTimeoutTest.timeoutSignals.push(timeoutSignal!);
      StepCancelAndTimeoutTest.cancelSignals.push(cancelSignal);
      if (attempt > 3) return `done-${attempt}`;
      // Attempt 2 heeds only cancellation, so it keeps running after its timeout abandons it
      const signal = attempt === 2 ? cancelSignal : StepCancelAndTimeoutTest.anySignal([timeoutSignal!, cancelSignal]);
      if (attempt === 3) StepCancelAndTimeoutTest.thirdAttemptStarted.set();
      try {
        await abortableSleep(10_000, undefined, { signal });
      } catch (e) {
        StepCancelAndTimeoutTest.observed[attempt - 1] = signal.reason;
        throw e;
      } finally {
        if (attempt === 2) StepCancelAndTimeoutTest.abandonedAttemptEnded.set();
      }
      return `slept-${attempt}`;
    }

    @DBOS.workflow()
    static async timedWorkflow(): Promise<string> {
      return await StepCancelAndTimeoutTest.timedStep();
    }
  }

  test('test-step-cancel-signal-with-timeout', async () => {
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    sysdb.dbPollingIntervalCancelMs = 100;
    StepCancelAndTimeoutTest.attempts = 0;
    StepCancelAndTimeoutTest.timeoutSignals = [];
    StepCancelAndTimeoutTest.cancelSignals = [];
    StepCancelAndTimeoutTest.observed = [];
    StepCancelAndTimeoutTest.thirdAttemptStarted = new Event();
    StepCancelAndTimeoutTest.abandonedAttemptEnded = new Event();
    const wfid = randomUUID();

    // Attempts 1 and 2 time out; attempt 3 blocks until its timeout or the workflow's cancellation
    const handle = await DBOS.startWorkflow(StepCancelAndTimeoutTest, { workflowID: wfid }).timedWorkflow();
    expect((await DBOS.getWorkflowStatus(wfid))!.status).toBe(StatusString.PENDING);
    await StepCancelAndTimeoutTest.thirdAttemptStarted.wait();
    const { timeoutSignals, cancelSignals, observed } = StepCancelAndTimeoutTest;
    const cancelSignal = cancelSignals[0];
    expect(observed[0]).toBeInstanceOf(DBOSStepTimeoutError);
    expect(timeoutSignals[0].reason).toBe(observed[0]);
    expect(timeoutSignals[1].reason).toBeInstanceOf(DBOSStepTimeoutError);
    expect(observed[1]).toBeUndefined();
    expect(timeoutSignals[2].aborted).toBe(false);

    // Each attempt has its own timeout signal but all share one cancel signal, which timeouts never fire
    expect(new Set(timeoutSignals).size).toBe(3);
    expect(cancelSignals.every((s) => s === cancelSignal)).toBe(true);
    expect(cancelSignal.aborted).toBe(false);

    // Cancelling stops the running attempt before its timeout, and the abandoned attempt too
    await DBOS.cancelWorkflow(wfid);
    await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    await StepCancelAndTimeoutTest.abandonedAttemptEnded.wait();
    expect(cancelSignal.reason).toBeInstanceOf(DBOSWorkflowCancelledError);
    expect(observed[1]).toBe(cancelSignal.reason);
    expect(observed[2]).toBe(cancelSignal.reason);
    expect(StepCancelAndTimeoutTest.attempts).toBe(3);

    // Neither the timeouts nor the cancelled attempt were recorded as the step's outcome
    expect((await DBOS.getWorkflowStatus(wfid))!.status).toBe(StatusString.CANCELLED);
    expect(await DBOS.listWorkflowSteps(wfid)).toEqual([]);

    // The cancelled attempt's timer was cleared, so its timeout signal never fires
    await sleepms(cancelTestStepTimeoutMS + 200);
    expect(timeoutSignals[2].aborted).toBe(false);

    // On resume the step gets fresh signals and completes
    while (sysdb.checkForRunningWorkflow(wfid)) {
      await sleepms(20);
    }
    const resumed = await DBOS.resumeWorkflow<string>(wfid);
    await expect(resumed.getResult()).resolves.toBe('done-4');
    expect(cancelSignals[3]).not.toBe(cancelSignal);
    expect(cancelSignals[3].aborted).toBe(false);
    expect(timeoutSignals[3].aborted).toBe(false);
    expect((await DBOS.getWorkflowStatus(wfid))!.status).toBe(StatusString.SUCCESS);
    const steps = await DBOS.listWorkflowSteps(wfid);
    expect(steps!.map((s) => [s.name, s.output])).toEqual([['timedStep', 'done-4']]);
  });

  class BulkResumeTest {
    static stepsCompleted = 0;
    static workflowEvents: Record<string, Event> = {};
    static mainEvents: Record<string, Event> = {};

    @DBOS.step()
    static async stepOne(): Promise<void> {
      await Promise.resolve();
      BulkResumeTest.stepsCompleted++;
    }

    @DBOS.step()
    static async stepTwo(): Promise<void> {
      await Promise.resolve();
      BulkResumeTest.stepsCompleted++;
    }

    @DBOS.workflow()
    static async blockingWorkflow(x: number): Promise<number> {
      const wfid = DBOS.workflowID!;
      await BulkResumeTest.stepOne();
      BulkResumeTest.mainEvents[wfid].set();
      await BulkResumeTest.workflowEvents[wfid].wait();
      await BulkResumeTest.stepTwo();
      return x;
    }
  }

  test('test-bulk-resume', async () => {
    BulkResumeTest.stepsCompleted = 0;
    BulkResumeTest.workflowEvents = {};
    BulkResumeTest.mainEvents = {};

    const wfids: string[] = [];
    const handles: WorkflowHandle<number>[] = [];
    for (let i = 0; i < 3; i++) {
      const wfid = randomUUID();
      wfids.push(wfid);
      BulkResumeTest.workflowEvents[wfid] = new Event();
      BulkResumeTest.mainEvents[wfid] = new Event();
      const h = await DBOS.startWorkflow(BulkResumeTest, { workflowID: wfid }).blockingWorkflow(i);
      handles.push(h);
      await BulkResumeTest.mainEvents[wfid].wait();
    }

    expect(BulkResumeTest.stepsCompleted).toBe(3);

    await DBOS.cancelWorkflows(wfids);
    for (const wfid of wfids) {
      BulkResumeTest.workflowEvents[wfid].set();
    }
    for (const handle of handles) {
      await expect(handle.getResult()).rejects.toThrow(DBOSWorkflowCancelledError);
    }
    expect(BulkResumeTest.stepsCompleted).toBe(3);

    const resumedHandles = await DBOS.resumeWorkflows(wfids);
    expect(resumedHandles.length).toBe(3);
    for (let i = 0; i < resumedHandles.length; i++) {
      expect(await resumedHandles[i].getResult()).toBe(i);
    }
    expect(BulkResumeTest.stepsCompleted).toBe(6);
  });

  class BulkDeleteTest {
    @DBOS.workflow()
    static async simpleWorkflow(x: number): Promise<number> {
      await Promise.resolve();
      return x;
    }
  }

  test('test-bulk-delete', async () => {
    const wfids: string[] = [];
    for (let i = 0; i < 3; i++) {
      const wfid = randomUUID();
      wfids.push(wfid);
      const h = await DBOS.startWorkflow(BulkDeleteTest, { workflowID: wfid }).simpleWorkflow(i);
      expect(await h.getResult()).toBe(i);
    }

    for (const wfid of wfids) {
      expect(await DBOS.getWorkflowStatus(wfid)).not.toBeNull();
    }

    await DBOS.deleteWorkflows(wfids);

    for (const wfid of wfids) {
      expect(await DBOS.getWorkflowStatus(wfid)).toBeNull();
    }
  });

  test('test-client-delete-workflows', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      // Single delete
      const wfid1 = randomUUID();
      const h1 = await DBOS.startWorkflow(BulkDeleteTest, { workflowID: wfid1 }).simpleWorkflow(1);
      expect(await h1.getResult()).toBe(1);
      expect(await DBOS.getWorkflowStatus(wfid1)).not.toBeNull();

      await client.deleteWorkflow(wfid1);
      expect(await DBOS.getWorkflowStatus(wfid1)).toBeNull();

      // Bulk delete
      const wfids: string[] = [];
      for (let i = 0; i < 3; i++) {
        const wfid = randomUUID();
        wfids.push(wfid);
        const h = await DBOS.startWorkflow(BulkDeleteTest, { workflowID: wfid }).simpleWorkflow(i);
        expect(await h.getResult()).toBe(i);
      }

      await client.deleteWorkflows(wfids);

      for (const wfid of wfids) {
        expect(await DBOS.getWorkflowStatus(wfid)).toBeNull();
      }
    } finally {
      await client.destroy();
    }
  });

  // ==================== Observability Tests ====================
});
