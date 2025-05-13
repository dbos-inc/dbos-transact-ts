import { WorkflowHandle, DBOSInitializer, InitContext, DBOS } from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from './helpers';
import { randomUUID } from 'node:crypto';
import { StatusString } from '../src/workflow';
import { DBOSConfigInternal } from '../src/dbos-executor';
import { Client } from 'pg';
import { transaction_outputs } from '../schemas/user_db_schema';
import { DBOSFailedSqlTransactionError, DBOSWorkflowCancelledError } from '../src/error';

const testTableName = 'dbos_test_kv';

describe('dbos-tests', () => {
  let username: string;
  let config: DBOSConfigInternal;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    username = config.poolConfig.user || 'postgres';
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    DBOSTestClass.cnt = 0;
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('simple-function', async () => {
    const workflowHandle: WorkflowHandle<string> = await DBOS.startWorkflow(DBOSTestClass).testWorkflow(username);
    const workflowResult: string = await workflowHandle.getResult();
    expect(JSON.parse(workflowResult)).toEqual({ current_user: username });
  });

  test('simple-workflow-attempts-counter', async () => {
    const systemDBClient = new Client({
      user: config.poolConfig.user,
      port: config.poolConfig.port,
      host: config.poolConfig.host,
      password: config.poolConfig.password,
      database: config.system_database,
    });
    try {
      await systemDBClient.connect();
      const handle = await DBOS.startWorkflow(DBOSTestClass).noopWorkflow();
      for (let i = 0; i < 10; i++) {
        await DBOS.startWorkflow(DBOSTestClass, { workflowID: handle.workflowID }).noopWorkflow();
        const result = await systemDBClient.query<{ status: string; attempts: number }>(
          `SELECT status, recovery_attempts as attempts FROM dbos.workflow_status WHERE workflow_uuid=$1`,
          [handle.workflowID],
        );
        expect(result.rows[0].attempts).toBe(String(i + 2));
      }
    } finally {
      await systemDBClient.end();
    }
  });

  test('return-void', async () => {
    const workflowUUID = randomUUID();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await DBOSTestClass.testVoidFunction();
    });
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(DBOSTestClass.testVoidFunction()).resolves.toBeFalsy();
    });
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(DBOSTestClass.testVoidFunction()).resolves.toBeFalsy();
    });
  });

  test('tight-loop', async () => {
    for (let i = 0; i < 100; i++) {
      await expect(DBOSTestClass.testNameWorkflow(username)).resolves.toBe(username);
    }
  });

  test('abort-function', async () => {
    for (let i = 0; i < 10; i++) {
      expect(await DBOSTestClass.testFailWorkflow(username)).toBe(i + 1);
    }

    // Should not appear in the database.
    await expect(DBOSTestClass.testFailWorkflow('fail')).rejects.toThrow('fail');
  });

  test('simple-step', async () => {
    const workflowUUID: string = randomUUID();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(DBOSTestClass.testStep()).resolves.toBe(0);
    });
    await expect(DBOSTestClass.testStep()).resolves.toBe(1);
  });

  test('simple-workflow-notifications', async () => {
    // Send to non-existent workflow should fail
    await expect(DBOSTestClass.sendWorkflow('1234567')).rejects.toThrow(
      'Sent to non-existent destination workflow UUID',
    );

    const workflowUUID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTestClass, { workflowID: workflowUUID }).receiveWorkflow();
    await expect(DBOSTestClass.sendWorkflow(handle.workflowID)).resolves.toBeFalsy(); // return void.
    expect(await handle.getResult()).toBe(true);
  });

  test('simple-workflow-events', async () => {
    const handle: WorkflowHandle<number> = await DBOS.startWorkflow(DBOSTestClass).setEventWorkflow();
    const workflowUUID = handle.workflowID;
    await handle.getResult();
    await expect(DBOS.getEvent(workflowUUID, 'key1')).resolves.toBe('value1');
    await expect(DBOS.getEvent(workflowUUID, 'key2')).resolves.toBe('value2');
    await expect(DBOS.getEvent(workflowUUID, 'fail', 0)).resolves.toBe(null);
  });

  test('simple-workflow-events-multiple', async () => {
    const handle: WorkflowHandle<number> = await DBOS.startWorkflow(DBOSTestClass).setEventMultipleWorkflow();
    const workflowUUID = handle.workflowID;
    await handle.getResult();
    await expect(DBOS.getEvent(workflowUUID, 'key1')).resolves.toBe('value1b');
    await expect(DBOS.getEvent(workflowUUID, 'key2')).resolves.toBe('value2');
    await expect(DBOS.getEvent(workflowUUID, 'fail', 0)).resolves.toBe(null);
  });

  class ReadRecording {
    static cnt: number = 0;
    static wfCnt: number = 0;

    @DBOS.transaction({ readOnly: true })
    static async testReadFunction(id: number) {
      const { rows } = await DBOS.pgClient.query<TestKvTable>(`SELECT value FROM ${testTableName} WHERE id=$1`, [id]);
      ReadRecording.cnt++;
      await DBOS.pgClient.query(`SELECT pg_current_xact_id()`); // Increase transaction ID, testing if we can capture xid and snapshot correctly.
      if (rows.length === 0) {
        return null;
      }
      return rows[0].value;
    }

    @DBOS.transaction()
    static async updateFunction(id: number, name: string) {
      const { rows } = await DBOS.pgClient.query<TestKvTable>(
        `INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value=EXCLUDED.value RETURNING value;`,
        [id, name],
      );
      return rows[0].value;
    }

    @DBOS.workflow()
    static async testRecordingWorkflow(id: number, name: string) {
      await ReadRecording.testReadFunction(id);
      ReadRecording.wfCnt++;
      await ReadRecording.updateFunction(id, name);
      ReadRecording.wfCnt++;
      // Make sure the workflow actually runs.
      throw Error('dumb test error');
    }
  }

  test('txn-snapshot-recording', async () => {
    // Test the recording of transaction snapshot information in our transaction_outputs table.
    const workflowUUID = randomUUID();
    // Invoke the workflow, should get the error.
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(ReadRecording.testRecordingWorkflow(123, 'test')).rejects.toThrow(new Error('dumb test error'));
    });

    // Check the transaction output table and make sure we record transaction information correctly.
    const readRec = await DBOS.queryUserDB<transaction_outputs>(
      'SELECT txn_id, txn_snapshot FROM dbos.transaction_outputs WHERE workflow_uuid = $1 AND function_id = $2',
      [workflowUUID, 0],
    );
    expect(readRec[0].txn_id).toBeTruthy();
    expect(readRec[0].txn_snapshot).toBeTruthy();

    const writeRec = await DBOS.queryUserDB<transaction_outputs>(
      'SELECT txn_id, txn_snapshot FROM dbos.transaction_outputs WHERE workflow_uuid = $1 AND function_id = $2',
      [workflowUUID, 1],
    );
    expect(writeRec[0].txn_id).toBeTruthy();
    expect(writeRec[0].txn_snapshot).toBeTruthy();

    // Two snapshots must be different because we bumped transaction ID.
    expect(readRec[0].txn_snapshot).not.toEqual(writeRec[0].txn_snapshot);
  });

  class RetrieveWorkflowStatus {
    // Test workflow status changes correctly.
    static resolve1: () => void;
    static promise1 = new Promise<void>((resolve) => {
      RetrieveWorkflowStatus.resolve1 = resolve;
    });

    static resolve2: () => void;
    static promise2 = new Promise<void>((resolve) => {
      RetrieveWorkflowStatus.resolve2 = resolve;
    });

    static resolve3: () => void;
    static promise3 = new Promise<void>((resolve) => {
      RetrieveWorkflowStatus.resolve3 = resolve;
    });

    @DBOS.transaction()
    static async testWriteFunction(id: number, name: string) {
      const { rows } = await DBOS.pgClient.query<TestKvTable>(
        `INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value=EXCLUDED.value RETURNING value;`,
        [id, name],
      );
      return rows[0].value;
    }

    @DBOS.workflow()
    static async testStatusWorkflow(id: number, name: string) {
      await RetrieveWorkflowStatus.promise1;
      const value = await RetrieveWorkflowStatus.testWriteFunction(id, name);
      RetrieveWorkflowStatus.resolve3(); // Signal the execution has done.
      await RetrieveWorkflowStatus.promise2;
      return value;
    }
  }

  test('retrieve-workflowstatus', async () => {
    const workflowUUID = randomUUID();

    const workflowHandle = await DBOS.startWorkflow(RetrieveWorkflowStatus, {
      workflowID: workflowUUID,
    }).testStatusWorkflow(123, 'hello');

    expect(workflowHandle.workflowID).toBe(workflowUUID);
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      workflowID: workflowUUID,
      status: StatusString.PENDING,
      workflowName: RetrieveWorkflowStatus.testStatusWorkflow.name,
    });
    await expect(workflowHandle.getWorkflowInputs()).resolves.toMatchObject([123, 'hello']);

    // getResult with a timeout ... it'll time out.
    await expect(DBOS.getResult<string>(workflowUUID, 0.1)).resolves.toBeNull();

    RetrieveWorkflowStatus.resolve1();
    await RetrieveWorkflowStatus.promise3;

    // Retrieve handle, should get the pending status.
    await expect(DBOS.retrieveWorkflow<string>(workflowUUID).getStatus()).resolves.toMatchObject({
      status: StatusString.PENDING,
      workflowName: RetrieveWorkflowStatus.testStatusWorkflow.name,
    });

    // Proceed to the end.
    RetrieveWorkflowStatus.resolve2();
    await expect(workflowHandle.getResult()).resolves.toBe('hello');

    // The status should transition to SUCCESS.
    const retrievedHandle = DBOS.retrieveWorkflow<string>(workflowUUID);
    expect(retrievedHandle).not.toBeNull();
    expect(retrievedHandle.workflowID).toBe(workflowUUID);
    await expect(retrievedHandle.getResult()).resolves.toBe('hello');
    await expect(workflowHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  test('aborted-transaction', async () => {
    const workflowUUID: string = randomUUID();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(DBOSTestClass.attemptToCatchAbortingStoredProc()).rejects.toThrow(
        new DBOSFailedSqlTransactionError(workflowUUID, 'attemptToCatchAbortingStoredProc'),
      );
    });
  });

  /**
   *  WORKFLOW TIMEOUTS
   */

  test('workflow-withWorkflowTimeout', async () => {
    const workflowID: string = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await DBOS.withWorkflowTimeout(100, async () => {
        await expect(DBOSTimeoutTestClass.blockedWorkflow()).rejects.toThrow(
          new DBOSWorkflowCancelledError(workflowID),
        );
      });
    });
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
  });

  test('workflow-timeout-startWorkflow-params', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 100 }).blockedWorkflow();
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
  });

  test('parent-workflow-withWorkflowTimeout', async () => {
    const workflowID: string = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await DBOS.withWorkflowTimeout(100, async () => {
        await expect(DBOSTimeoutTestClass.blockingParentStartWF()).rejects.toThrow(
          new DBOSWorkflowCancelledError(workflowID),
        );
      });
    });
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
  });

  test('parent-workflow-timeout-startWorkflow-params', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      timeoutMS: 100,
    }).blockingParentStartWF();
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
  });

  test('direct-parent-workflow-withWorkflowTimeout', async () => {
    const workflowID: string = randomUUID();
    await DBOS.withNextWorkflowID(workflowID, async () => {
      await DBOS.withWorkflowTimeout(100, async () => {
        await expect(DBOSTimeoutTestClass.blockingParentDirect()).rejects.toThrow(
          new DBOSWorkflowCancelledError(workflowID),
        );
      });
    });
    const statuses = await DBOS.listWorkflows({ workflow_id_prefix: workflowID });
    expect(statuses.length).toBe(2);
    statuses.forEach((status) => {
      expect(status.status).toBe('CANCELLED');
    });
    const deadline = statuses[0].deadlineEpochMS;
    statuses.slice(1).forEach((status) => {
      expect(status.deadlineEpochMS).toBe(deadline);
    });
  });

  test('direct-parent-workflow-timeout-startWorkflow-params', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      timeoutMS: 100,
    }).blockingParentDirect();
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    const statuses = await DBOS.listWorkflows({ workflow_id_prefix: workflowID });
    expect(statuses.length).toBe(2);
    statuses.forEach((status) => {
      expect(status.status).toBe('CANCELLED');
    });
    const deadline = statuses[0].deadlineEpochMS;
    statuses.slice(1).forEach((status) => {
      expect(status.deadlineEpochMS).toBe(deadline);
    });
  });

  test('child-wf-timeout-simple', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID }).timeoutParentStartWF(100);
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(`${workflowID}-0`));
    const statuses = await DBOS.listWorkflows({ workflow_id_prefix: workflowID });
    expect(statuses.length).toBe(2);
    statuses.forEach((status) => {
      expect(status.status).toBe('CANCELLED');
    });
  });

  test('child-wf-timeout-before-parent', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 1000 }).timeoutParentStartWF(
      100,
    );
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(`${workflowID}-0`));
    const statuses = await DBOS.listWorkflows({ workflow_id_prefix: workflowID });
    expect(statuses.length).toBe(2);
    statuses.forEach((status) => {
      expect(status.status).toBe('CANCELLED');
    });
  });

  test('child-wf-timeout-after-parent', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 100 }).timeoutParentStartWF(
      1000,
    );
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    const statuses = await DBOS.listWorkflows({ workflow_id_prefix: workflowID });
    expect(statuses.length).toBe(2);
    statuses.forEach((status) => {
      expect(status.status).toBe('CANCELLED');
    });
  });

  test('sleeping-workflow-timed-out', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 100 }).sleepingWorkflow(
      1000,
    );
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.CANCELLED);
  });

  test('sleeping-workflow-not-timed-out', async () => {
    const workflowID = randomUUID();
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, { workflowID, timeoutMS: 2000 }).sleepingWorkflow(
      1000,
    );
    await expect(handle.getResult()).resolves.toBe(42);
    const status = await DBOS.getWorkflowStatus(workflowID);
    expect(status?.status).toBe(StatusString.SUCCESS);
  });

  test('child-wf-detach-deadline', async () => {
    const workflowID = randomUUID();
    const childID = `${workflowID}-0`;
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      timeoutMS: 100,
    }).timeoutParentStartDetachedChild(100);
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getResult()).resolves.toBe(42);
    await expect(childHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });

  test('child-wf-detach-deadline-with-syntax', async () => {
    const workflowID = randomUUID();
    const childID = `${workflowID}-0`;
    const handle = await DBOS.startWorkflow(DBOSTimeoutTestClass, {
      workflowID,
      timeoutMS: 100,
    }).timeoutParentStartDetachedChildWithSyntax(100);
    await expect(handle.getResult()).rejects.toThrow(new DBOSWorkflowCancelledError(workflowID));
    await expect(handle.getStatus()).resolves.toMatchObject({
      status: StatusString.CANCELLED,
    });
    const childHandle = DBOS.retrieveWorkflow(childID);
    await expect(childHandle.getResult()).resolves.toBe(42);
    await expect(childHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.SUCCESS,
    });
  });
});

class DBOSTimeoutTestClass {
  @DBOS.workflow()
  static async sleepingWorkflow(duration: number) {
    await DBOS.sleep(duration);
    return 42;
  }

  @DBOS.workflow()
  static async blockedWorkflow() {
    while (true) {
      await DBOS.sleep(100);
    }
  }

  @DBOS.workflow()
  static async blockingParentStartWF() {
    await DBOS.startWorkflow(DBOSTimeoutTestClass)
      .blockedWorkflow()
      .then((h) => h.getResult());
  }

  @DBOS.workflow()
  static async blockingParentDirect() {
    await DBOSTimeoutTestClass.blockedWorkflow();
  }

  @DBOS.workflow()
  static async timeoutParentStartWF(timeout: number) {
    await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: timeout })
      .blockedWorkflow()
      .then((h) => h.getResult());
  }

  @DBOS.workflow()
  static async timeoutParentStartDetachedChild(duration: number) {
    await DBOS.startWorkflow(DBOSTimeoutTestClass, { timeoutMS: undefined })
      .sleepingWorkflow(duration * 2)
      .then((h) => h.getResult());
  }

  @DBOS.workflow()
  static async timeoutParentStartDetachedChildWithSyntax(duration: number) {
    await DBOS.withWorkflowTimeout(undefined, async () => {
      await DBOSTimeoutTestClass.sleepingWorkflow(duration * 2);
    });
  }
}

class DBOSTestClass {
  static initialized = false;
  static cnt: number = 0;

  @DBOSInitializer()
  static async init(ctx: InitContext) {
    DBOSTestClass.initialized = true;
    // ctx and DBOS should be interchangeable
    expect(ctx.getConfig('counter')).toBe(3);
    expect(DBOS.getConfig('counter')).toBe(3);
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await ctx.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);
    await ctx.queryUserDB(`CREATE OR REPLACE FUNCTION test_proc_raise() returns void as $$
    BEGIN
      raise 'something bad happened';
    END
    $$ language plpgsql;`);
  }

  @DBOS.transaction()
  static async testFunction(name: string) {
    expect(DBOS.getConfig<number>('counter')).toBe(3);
    const { rows } = await DBOS.pgClient.query(`select current_user from current_user where current_user=$1;`, [name]);
    return JSON.stringify(rows[0]);
  }

  @DBOS.workflow()
  static async testWorkflow(name: string) {
    expect(DBOSTestClass.initialized).toBe(true);
    expect(DBOS.getConfig<number>('counter')).toBe(3);
    const funcResult = await DBOSTestClass.testFunction(name);
    return funcResult;
  }

  @DBOS.transaction()
  static async testVoidFunction() {
    return Promise.resolve();
  }

  @DBOS.transaction()
  static async testNameFunction(name: string) {
    return Promise.resolve(name);
  }

  @DBOS.workflow()
  static async testNameWorkflow(name: string) {
    return DBOSTestClass.testNameFunction(name); // Missing await is intentional
  }

  @DBOS.transaction()
  static async testFailFunction(name: string) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(
      `INSERT INTO ${testTableName}(value) VALUES ($1) RETURNING id`,
      [name],
    );
    if (name === 'fail') {
      throw new Error('fail');
    }
    return Number(rows[0].id);
  }

  @DBOS.transaction({ readOnly: true })
  static async testKvFunctionRead(id: number) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(`SELECT id FROM ${testTableName} WHERE id=$1`, [id]);
    if (rows.length > 0) {
      return Number(rows[0].id);
    } else {
      // Cannot find, return a negative number.
      return -1;
    }
  }

  @DBOS.transaction()
  static async attemptToCatchAbortingStoredProc() {
    try {
      return await DBOS.pgClient.query('select xx()');
    } catch (e) {
      return 'all good';
    }
  }

  @DBOS.workflow()
  static async testFailWorkflow(name: string) {
    expect(DBOSTestClass.initialized).toBe(true);
    const funcResult: number = await DBOSTestClass.testFailFunction(name);
    const checkResult: number = await DBOSTestClass.testKvFunctionRead(funcResult);
    return checkResult;
  }

  @DBOS.step()
  static async testStep() {
    expect(DBOS.getConfig<number>('counter')).toBe(3);
    return Promise.resolve(DBOSTestClass.cnt++);
  }

  @DBOS.workflow()
  static async receiveWorkflow() {
    expect(DBOSTestClass.initialized).toBe(true);
    const message1 = await DBOS.recv<string>();
    const message2 = await DBOS.recv<string>();
    const fail = await DBOS.recv('fail', 0);
    return message1 === 'message1' && message2 === 'message2' && fail === null;
  }

  @DBOS.workflow()
  static async sendWorkflow(destinationUUID: string) {
    await DBOS.send(destinationUUID, 'message1');
    await DBOS.send(destinationUUID, 'message2');
  }

  @DBOS.workflow()
  static async setEventWorkflow() {
    await DBOS.setEvent('key1', 'value1');
    await DBOS.setEvent('key2', 'value2');
    return 0;
  }

  @DBOS.workflow()
  static async setEventMultipleWorkflow() {
    await DBOS.setEvent('key1', 'value1');
    await DBOS.setEvent('key2', 'value2');
    await DBOS.setEvent('key1', 'value1b');
    return 0;
  }

  @DBOS.workflow()
  static async noopWorkflow() {
    return Promise.resolve();
  }
}
