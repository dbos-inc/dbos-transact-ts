import {
  WorkflowContext,
  TransactionContext,
  StepContext,
  Step,
  Workflow,
  Transaction,
  ArgOptional,
  DBOS,
  ConfiguredInstance,
  InitContext,
} from '../src/';
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from './helpers';
import { DatabaseError, PoolClient } from 'pg';
import { v1 as uuidv1 } from 'uuid';
import { StatusString } from '../src/workflow';
import { DBOSError, DBOSMaxStepRetriesError, DBOSNotRegisteredError } from '../src/error';
import { DBOSConfig } from '../src/dbos-executor';

const testTableName = 'dbos_failure_test_kv';
type TestTransactionContext = TransactionContext<PoolClient>;

describe('failures-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
    await DBOS.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
    await DBOS.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id INTEGER PRIMARY KEY, value TEXT);`);
    FailureTestClass.cnt = 0;
    FailureTestClass.success = '';
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('dbos-error', async () => {
    const wfUUID1 = uuidv1();
    await DBOS.withNextWorkflowID(
      wfUUID1,
      async () =>
        await expect(FailureTestClass.testStep(11)).rejects.toThrow(new DBOSError('test dbos error with code.', 11)),
    );

    const retrievedHandle = DBOS.retrieveWorkflow<string>(wfUUID1);
    expect(retrievedHandle).not.toBeNull();
    await expect(retrievedHandle.getStatus()).resolves.toMatchObject({
      status: StatusString.ERROR,
    });
    await expect(retrievedHandle.getResult()).rejects.toThrow(new DBOSError('test dbos error with code.', 11));

    // Test without code.
    await expect(FailureTestClass.testStep()).rejects.toThrow(new DBOSError('test dbos error without code.'));
  });

  test('readonly-error', async () => {
    const testUUID = uuidv1();
    await DBOS.withNextWorkflowID(
      testUUID,
      async () => await expect(FailureTestClass.testReadonlyError()).rejects.toThrow(new Error('test error')),
    );
    expect(FailureTestClass.cnt).toBe(1);

    // The error should be recorded in the database, so the function shouldn't run again.
    await DBOS.withNextWorkflowID(
      testUUID,
      async () => await expect(FailureTestClass.testReadonlyError()).rejects.toThrow(new Error('test error')),
    );
    expect(FailureTestClass.cnt).toBe(1);

    // A run with a generated UUID should fail normally
    await expect(DBOS.invoke(FailureTestClass).testReadonlyError()).rejects.toThrow(new Error('test error'));
    expect(FailureTestClass.cnt).toBe(2);
  });

  test('simple-keyconflict', async () => {
    const workflowUUID1 = uuidv1();
    const workflowUUID2 = uuidv1();

    // Start two concurrent transactions.
    const results = await Promise.allSettled([
      (
        await DBOS.startWorkflow(FailureTestClass, { workflowID: workflowUUID1 }).testKeyConflict(10, workflowUUID1)
      ).getResult(),
      (
        await DBOS.startWorkflow(FailureTestClass, { workflowID: workflowUUID2 }).testKeyConflict(10, workflowUUID2)
      ).getResult(),
    ]);
    const errorResult = results.find((result) => result.status === 'rejected');
    const err: DatabaseError = (errorResult as PromiseRejectedResult).reason as DatabaseError;
    expect(err.code).toBe('23505');
    expect(err.table?.toLowerCase()).toBe(testTableName.toLowerCase());

    expect(FailureTestClass.cnt).toBe(1);

    // Retry with the same failed UUID, should throw the same error.
    const failUUID = FailureTestClass.success === workflowUUID1 ? workflowUUID2 : workflowUUID1;
    try {
      await DBOS.withNextWorkflowID(failUUID, async () => await FailureTestClass.testKeyConflict(10, failUUID));
    } catch (error) {
      const err: DatabaseError = error as DatabaseError;
      expect(err.code).toBe('23505');
      expect(err.table?.toLowerCase()).toBe(testTableName.toLowerCase());
    }
    // Retry with the succeed UUID, should return the expected result.
    await DBOS.withNextWorkflowID(
      FailureTestClass.success,
      async () =>
        await expect(FailureTestClass.testKeyConflict(10, FailureTestClass.success)).resolves.toStrictEqual({ id: 10 }),
    );
  });

  test('serialization-error', async () => {
    // Should succeed after retrying 10 times.
    await expect(DBOS.invoke(FailureTestClass).testSerialWorkflow(10)).resolves.toBe(10);
    expect(FailureTestClass.cnt).toBe(10);
  });

  test('failing-step', async () => {
    let startTime = Date.now();
    await expect(DBOS.invoke(FailureTestClass).testFailStep()).resolves.toBe(2);
    expect(Date.now() - startTime).toBeGreaterThanOrEqual(1000);

    startTime = Date.now();
    try {
      await DBOS.invoke(FailureTestClass).testFailStep();
      expect(true).toBe(false); // An exception should be thrown first
    } catch (error) {
      const e = error as DBOSMaxStepRetriesError;
      expect(e.message).toContain('Step testFailStep has exceeded its maximum of 2 retries.');
      expect(e.errors.length).toBe(2);
      expect(e.errors[0].message).toBe('bad number');
      expect(e.errors[1].message).toBe('bad number');
    }
    expect(Date.now() - startTime).toBeGreaterThanOrEqual(1000);
  });

  test('nonretry-step', async () => {
    const workflowUUID = uuidv1();

    // Should throw an error.
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(FailureTestClass.testNoRetry()).rejects.toThrow(new Error('failed no retry'));
    });
    expect(FailureTestClass.cnt).toBe(1);

    // If we retry again, we should get the same error, but numRun should still be 1 (OAOO).
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(FailureTestClass.testNoRetry()).rejects.toThrow(new Error('failed no retry'));
    });
    expect(FailureTestClass.cnt).toBe(1);
  });

  test('no-registration-invoke', async () => {
    // Note: since we use invoke() in testing runtime, it throws "TypeError: ...is not a function" instead of NotRegisteredError.

    // Invoke an unregistered workflow.
    expect(() => DBOS.invoke(FailureTestClass).noRegWorkflow(10)).toThrow(DBOSNotRegisteredError);

    // Invoke an unregistered transaction.
    expect(() => DBOS.invoke(FailureTestClass).noRegTransaction(10)).toThrow(DBOSNotRegisteredError);

    // Invoke an unregistered step in a workflow.
    await expect(DBOS.invoke(FailureTestClass).testCommWorkflow()).rejects.toThrow();
  });

  test('no-registration-startwf', async () => {
    // Note: since we use invoke() in testing runtime, it throws "TypeError: ...is not a function" instead of NotRegisteredError.

    // Invoke an unregistered workflow.
    expect(() => DBOS.startWorkflow(FailureTestClass).noRegWorkflow2(10)).toThrow(DBOSNotRegisteredError);

    // Invoke an unregistered transaction.
    expect(() => DBOS.startWorkflow(new FailureTestClass()).noRegFunction(10)).toThrow(DBOSNotRegisteredError);

    return Promise.resolve();
  });

  class TestStepStatus {
    static count = 0;
    static max_attempts = 5;

    @DBOS.step({ retriesAllowed: true, maxAttempts: TestStepStatus.max_attempts, intervalSeconds: 0 })
    static async stepOne() {
      TestStepStatus.count += 1;
      return Promise.resolve();
    }

    @DBOS.step()
    static async stepTwo() {
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async workflow() {
      await TestStepStatus.stepOne();
      await TestStepStatus.stepTwo();
      return DBOS.workflowID;
    }
  }

  test('test-step-status', async () => {
    await expect(TestStepStatus.workflow()).resolves.toBeTruthy();
  });
});

class FailureTestClass extends ConfiguredInstance {
  initialize(_ctx: InitContext): Promise<void> {
    return Promise.resolve();
  }

  constructor() {
    super('name');
  }

  static cnt = 0;
  static success: string = '';

  @DBOS.step({ retriesAllowed: false })
  static async testStep(@ArgOptional code?: number) {
    const err = code
      ? new DBOSError('test dbos error with code.', code)
      : new DBOSError('test dbos error without code.');
    return Promise.reject(err);
  }

  @DBOS.transaction({ readOnly: true })
  static async testReadonlyError() {
    FailureTestClass.cnt++;
    return Promise.reject(new Error('test error'));
  }

  @DBOS.transaction()
  static async testKeyConflict(id: number, name: string) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(
      `INSERT INTO ${testTableName} (id, value) VALUES ($1, $2) RETURNING id`,
      [id, name],
    );
    FailureTestClass.cnt += 1;
    FailureTestClass.success = name;
    return rows[0];
  }

  @Transaction()
  static async testSerialError(_ctxt: TestTransactionContext, maxRetry: number) {
    if (FailureTestClass.cnt !== maxRetry) {
      const err = new DatabaseError('serialization error', 10, 'error');
      err.code = '40001';
      FailureTestClass.cnt += 1;
      return Promise.reject(err);
    }
    return Promise.resolve(maxRetry);
  }

  @Workflow()
  static async testSerialWorkflow(ctxt: WorkflowContext, maxRetry: number) {
    return await ctxt.invoke(FailureTestClass).testSerialError(maxRetry);
  }

  @Step({ retriesAllowed: true, intervalSeconds: 1, maxAttempts: 2 })
  static async testFailStep(ctxt: StepContext) {
    FailureTestClass.cnt++;
    if (ctxt.retriesAllowed && FailureTestClass.cnt !== ctxt.maxAttempts) {
      throw new Error('bad number');
    }
    return Promise.resolve(FailureTestClass.cnt);
  }

  @DBOS.step({ retriesAllowed: false })
  static async testNoRetry() {
    FailureTestClass.cnt++;
    return Promise.reject(new Error('failed no retry'));
  }

  // Test decorator registration works.
  static async noRegComm(_ctxt: StepContext, code: number) {
    return Promise.resolve(code + 1);
  }

  static async noRegTransaction(_ctxt: TestTransactionContext, code: number) {
    return Promise.resolve(code + 1);
  }

  static async noRegWorkflow(_ctxt: WorkflowContext, code: number) {
    return Promise.resolve(code + 1);
  }

  static async noRegWorkflow2(code: number) {
    return Promise.resolve(code + 1);
  }

  async noRegFunction(code: number) {
    return Promise.resolve(code + 1);
  }

  @Workflow()
  static async testCommWorkflow(ctxt: WorkflowContext) {
    return await ctxt.invoke(FailureTestClass).noRegComm(1);
  }
}
