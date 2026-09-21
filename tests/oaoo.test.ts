import { Client } from 'pg';
import { DBOS, DBOSClient, Debouncer, StatusString } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { DBOSQueueDuplicatedError, DBOSWorkflowIDInUseError, isWorkflowIDInUseError } from '../src/error';
import { clearDebugTriggers, DEBUG_TRIGGER_INITWF_COMMIT, setDebugTrigger } from '../src/debugpoint';
import { dropDatabase, generateDBOSTestConfig, reexecuteWorkflowById, setUpDBOSTestSysDb } from './helpers';
import { randomUUID } from 'node:crypto';

describe('oaoo-tests', () => {
  let username: string;
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    expect(config.systemDatabaseUrl).toBeDefined();
    const url = new URL(config.systemDatabaseUrl!);
    username = url.username;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    expect(config.systemDatabaseUrl).toBeDefined();
    await dropDatabase(config.systemDatabaseUrl!);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  /**
   * Step OAOO tests.
   */
  class StepOAOO {
    static #counter = 0;
    static get counter() {
      return StepOAOO.#counter;
    }
    @DBOS.step()
    static async testStep() {
      return Promise.resolve(StepOAOO.#counter++);
    }

    @DBOS.workflow()
    static async testCommWorkflow() {
      const funcResult = StepOAOO.testStep();
      return funcResult ?? -1;
    }
  }

  test('step-oaoo', async () => {
    const workflowUUID: string = randomUUID();

    let result: number = -222;
    result = await DBOS.withNextWorkflowID(workflowUUID, async () => await StepOAOO.testCommWorkflow());
    expect(result).toBe(0);
    expect(StepOAOO.counter).toBe(1);

    // Test OAOO. Should return the original result.
    result = await DBOS.withNextWorkflowID(workflowUUID, async () => await StepOAOO.testCommWorkflow());
    expect(result).toBe(0);
    expect(StepOAOO.counter).toBe(1);

    // Should be a new run.
    expect(await StepOAOO.testCommWorkflow()).toBe(1);
    expect(StepOAOO.counter).toBe(2);
  });

  /**
   * Workflow OAOO tests.
   */
  class WorkflowOAOO {
    @DBOS.step()
    static async stepOne(name: string) {
      return Promise.resolve(name);
    }

    @DBOS.step()
    static async stepTwo(name: string) {
      return Promise.resolve(name);
    }

    @DBOS.workflow()
    static async testTxWorkflow(name: string) {
      await WorkflowOAOO.stepOne(name);
      await WorkflowOAOO.stepTwo(name);
      return name;
    }

    @DBOS.workflow()
    static async nestedWorkflow(name: string) {
      return await WorkflowOAOO.testTxWorkflow(name);
    }

    static numberOfChildInvocationsMax1 = 0;

    @DBOS.step()
    static async nestedWorkflowStepToRunOnce() {
      ++WorkflowOAOO.numberOfChildInvocationsMax1;
      return Promise.resolve();
    }

    @DBOS.workflow()
    static async nestedWorkflowChildToRunOnce() {
      return await WorkflowOAOO.nestedWorkflowStepToRunOnce();
    }

    @DBOS.workflow()
    static async nestedWorkflowRunChildOnce() {
      return DBOS.withNextWorkflowID(
        'constant-idempotency-run-once',
        async () => await WorkflowOAOO.nestedWorkflowChildToRunOnce(),
      );
    }

    @DBOS.workflow()
    static async sleepWorkflow(durationSec: number) {
      await DBOS.sleepSeconds(durationSec);
      return;
    }

    @DBOS.workflow()
    static async recvWorkflow(timeoutSeconds: number) {
      await DBOS.recv('a-topic', timeoutSeconds);
      return;
    }

    @DBOS.workflow()
    static async getEventWorkflow(timeoutSeconds: number) {
      await DBOS.getEvent(randomUUID(), 'a-key', timeoutSeconds);
      return;
    }
  }

  test('workflow-sleep-oaoo', async () => {
    const workflowUUID = randomUUID();
    const initTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.sleepWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - initTime).toBeGreaterThanOrEqual(1950);

    // The sleep step's recorded timing must reflect the full sleep length, not zero.
    const sleepSteps = (await DBOS.listWorkflowSteps(workflowUUID))!;
    expect(sleepSteps.length).toBe(1);
    expect(sleepSteps[0].name).toBe('DBOS.sleep');
    expect(sleepSteps[0].completedAtEpochMs! - sleepSteps[0].startedAtEpochMs!).toBeGreaterThanOrEqual(2000);

    // Rerunning should skip the sleep
    const startTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.sleepWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - startTime).toBeLessThanOrEqual(200);
  });

  // Regression: a fractional duration must not reach the BIGINT timing columns.
  test('workflow-sleep-fractional-duration', async () => {
    const workflowUUID = randomUUID();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.sleepWorkflow(0.0015)).resolves.toBeFalsy(); // 1.5ms
    });

    const steps = (await DBOS.listWorkflowSteps(workflowUUID))!;
    expect(steps.length).toBe(1);
    expect(steps[0].name).toBe('DBOS.sleep');
    expect(Number.isInteger(steps[0].startedAtEpochMs!)).toBe(true);
    expect(Number.isInteger(steps[0].completedAtEpochMs!)).toBe(true);
    expect(steps[0].completedAtEpochMs! - steps[0].startedAtEpochMs!).toBe(2); // Math.ceil(1.5)

    // Non-finite durations are rejected rather than reaching the database.
    await expect(DBOS.sleep(NaN)).rejects.toThrow('finite');
    await expect(DBOS.sleep(Infinity)).rejects.toThrow('finite');
  });

  test('workflow-recv-oaoo', async () => {
    const workflowUUID = randomUUID();
    const initTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.recvWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - initTime).toBeGreaterThanOrEqual(1950);

    // The recv step records the full wait; its durable timeout marker stays zero-duration.
    const recvSteps = (await DBOS.listWorkflowSteps(workflowUUID))!;
    const recvStep = recvSteps.find((s) => s.name === 'DBOS.recv')!;
    const recvTimeout = recvSteps.find((s) => s.name === 'DBOS.sleep')!;
    expect(recvStep.completedAtEpochMs! - recvStep.startedAtEpochMs!).toBeGreaterThanOrEqual(1950);
    expect(recvTimeout.completedAtEpochMs! - recvTimeout.startedAtEpochMs!).toBeLessThan(50);

    // Rerunning should skip the sleep
    const startTime = Date.now();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.recvWorkflow(2)).resolves.toBeFalsy();
    });
    expect(Date.now() - startTime).toBeLessThanOrEqual(200);
  });

  test('workflow-getEvent-oaoo', async () => {
    const workflowUUID = randomUUID();
    const initTime = Date.now();
    await DBOS.withNextWorkflowID(
      workflowUUID,
      async () => await expect(WorkflowOAOO.getEventWorkflow(2)).resolves.toBeFalsy(),
    );
    expect(Date.now() - initTime).toBeGreaterThanOrEqual(1950);

    // The getEvent step records the full wait; its durable timeout marker stays zero-duration.
    const geSteps = (await DBOS.listWorkflowSteps(workflowUUID))!;
    const geStep = geSteps.find((s) => s.name === 'DBOS.getEvent')!;
    const geTimeout = geSteps.find((s) => s.name === 'DBOS.sleep')!;
    expect(geStep.completedAtEpochMs! - geStep.startedAtEpochMs!).toBeGreaterThanOrEqual(1950);
    expect(geTimeout.completedAtEpochMs! - geTimeout.startedAtEpochMs!).toBeLessThan(50);

    // Rerunning should skip the sleep
    const startTime = Date.now();
    await DBOS.withNextWorkflowID(
      workflowUUID,
      async () => await expect(WorkflowOAOO.getEventWorkflow(2)).resolves.toBeFalsy(),
    );
    expect(Date.now() - startTime).toBeLessThanOrEqual(200);
  });

  test('workflow-oaoo', async () => {
    let workflowResult: string;
    const uuidArray: string[] = [];

    for (let i = 0; i < 10; i++) {
      const workflowHandle = await DBOS.startWorkflow(WorkflowOAOO).testTxWorkflow(username);
      const workflowUUID: string = workflowHandle.workflowID;
      uuidArray.push(workflowUUID);
      workflowResult = await workflowHandle.getResult();
      expect(workflowResult).toEqual(username);
    }

    // Rerunning with the same workflow UUID should return the same output.
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidArray[i];
      const workflowResult: string = await DBOS.withNextWorkflowID(
        workflowUUID,
        async () => await WorkflowOAOO.testTxWorkflow(username),
      );
      expect(workflowResult).toEqual(username);
    }
  });

  test('nested-workflow-oaoo', async () => {
    const workflowUUID = randomUUID();
    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.nestedWorkflow(username)).resolves.toBe(username);
    });

    await DBOS.withNextWorkflowID(workflowUUID, async () => {
      await expect(WorkflowOAOO.nestedWorkflow(username)).resolves.toBe(username);
    });

    // Retrieve output of the child workflow.
    const retrievedHandle = DBOS.retrieveWorkflow(workflowUUID + '-0');
    await expect(retrievedHandle.getResult()).resolves.toBe(username);

    // Nested with OAOO key calculated
    await WorkflowOAOO.nestedWorkflowRunChildOnce();
    await WorkflowOAOO.nestedWorkflowRunChildOnce();
    expect(WorkflowOAOO.numberOfChildInvocationsMax1).toBe(1);
  });

  /**
   * Workflow notification OAOO tests.
   */
  class NotificationOAOO {
    @DBOS.workflow()
    static async receiveOaooWorkflow(topic: string, timeout: number) {
      // This returns true if and only if exactly one message is sent to it.
      const succeeds = await DBOS.recv<number>(topic, timeout);
      const fails = await DBOS.recv<number>(topic, 0);
      return succeeds === 123 && fails === null;
    }
  }

  test('notification-oaoo', async () => {
    const recvWorkflowUUID = randomUUID();
    const idempotencyKey = 'test-suffix';

    // Receive twice with the same UUID.  Each should get the same result of true.
    const recvHandle1 = await DBOS.startWorkflow(NotificationOAOO, {
      workflowID: recvWorkflowUUID,
    }).receiveOaooWorkflow('testTopic', 1);
    const recvHandle2 = await DBOS.startWorkflow(NotificationOAOO, {
      workflowID: recvWorkflowUUID,
    }).receiveOaooWorkflow('testTopic', 1);

    // Send twice with the same idempotency key.  Only one message should be sent.
    await expect(DBOS.send(recvWorkflowUUID, 123, 'testTopic', idempotencyKey)).resolves.not.toThrow();
    await expect(DBOS.send(recvWorkflowUUID, 123, 'testTopic', idempotencyKey)).resolves.not.toThrow();

    await expect(recvHandle1.getResult()).resolves.toBe(true);
    await expect(recvHandle2.getResult()).resolves.toBe(true);

    // A receive with a different UUID should return false.
    await expect(NotificationOAOO.receiveOaooWorkflow('testTopic', 0)).resolves.toBe(false);
  });

  /**
   * GetEvent/Status OAOO tests.
   */
  class EventStatusOAOO {
    static wfCnt: number = 0;
    static resolve: () => void;
    static promise = new Promise<void>((r) => {
      EventStatusOAOO.resolve = r;
    });

    static resolve3: () => void;
    static promise3 = new Promise<void>((r) => {
      EventStatusOAOO.resolve3 = r;
    });

    @DBOS.workflow()
    static async setEventWorkflow() {
      await DBOS.setEvent('key1', 'value1');
      await DBOS.setEvent('key2', 'value2');
      await EventStatusOAOO.promise;
      throw Error('Failed workflow');
    }

    @DBOS.workflow()
    static async getEventRetrieveWorkflow(targetUUID: string): Promise<string> {
      let res = '';
      const getValue = await DBOS.getEvent<string>(targetUUID, 'key1', 0);
      EventStatusOAOO.wfCnt++;
      if (getValue === null) {
        res = 'valueNull';
      } else {
        res = getValue;
      }

      const handle = DBOS.retrieveWorkflow(targetUUID);
      const status = await handle.getStatus();
      EventStatusOAOO.wfCnt++;
      if (status === null) {
        res += '-statusNull';
      } else {
        res += '-' + status.status;
      }

      // Set the child workflow UUID to targetUUID.
      const invokedHandle = await DBOS.startWorkflow(EventStatusOAOO, { workflowID: targetUUID }).setEventWorkflow();
      const ires = await invokedHandle.getStatus();
      res += '-' + ires?.status;
      try {
        if (EventStatusOAOO.wfCnt > 2) {
          await invokedHandle.getResult();
        }
      } catch (e) {
        // Ignore error.
        DBOS.logger.error(e);
      }
      EventStatusOAOO.resolve3();
      return res;
    }
  }

  test('workflow-getevent-retrieve', async () => {
    // Execute a workflow (w/ getUUID) to get an event and retrieve a workflow that doesn't exist, then invoke the setEvent workflow as a child workflow.
    // If we execute the get workflow without UUID, both getEvent and retrieveWorkflow should return values.
    // But if we run the get workflow again with getUUID, getEvent/retrieveWorkflow should still return null.
    const getUUID = randomUUID();
    const setUUID = randomUUID();

    const handle1 = await DBOS.startWorkflow(EventStatusOAOO, { workflowID: getUUID }).getEventRetrieveWorkflow(
      setUUID,
    );

    await EventStatusOAOO.promise3;
    expect(EventStatusOAOO.wfCnt).toBe(2);
    await expect(DBOS.getEvent(setUUID, 'key1')).resolves.toBe('value1');

    EventStatusOAOO.resolve();

    // Wait for the child workflow to finish.
    const handle = DBOS.retrieveWorkflow(setUUID);
    await expect(handle.getResult()).rejects.toThrow('Failed workflow');

    // Wait for parent to finish
    await expect(handle1.getResult()).resolves.toBe('valueNull-statusNull-PENDING');

    // Test OAOO for getEvent and getWorkflowStatus by reexecuting.
    const handle2 = await reexecuteWorkflowById(getUUID);
    await expect(handle2.getResult()).resolves.toBe('valueNull-statusNull-PENDING');

    // Run without UUID, should get the new result.
    await expect(EventStatusOAOO.getEventRetrieveWorkflow(setUUID)).resolves.toBe('value1-ERROR-ERROR');

    expect(EventStatusOAOO.wfCnt).toBe(6); // Should re-execute the workflow because we forced it
  });

  describe('workflow-id-reuse-policy', () => {
    const QUEUE = 'reuse_policy_queue';

    class ReuseTest {
      static resolveGate: () => void = () => {};
      static gate: Promise<void> = Promise.resolve();
      static resetGate() {
        ReuseTest.gate = new Promise<void>((resolve) => {
          ReuseTest.resolveGate = resolve;
        });
      }

      @DBOS.workflow()
      static echo(input: string): Promise<string> {
        return input === 'fail' ? Promise.reject(new Error('echo failed')) : Promise.resolve(input);
      }

      @DBOS.workflow()
      static otherWorkflow(input: string): Promise<string> {
        return Promise.resolve(input);
      }

      @DBOS.workflow()
      static async gated(input: string): Promise<string> {
        await ReuseTest.gate;
        return input;
      }

      @DBOS.workflow()
      static async startChild(childID: string): Promise<string> {
        try {
          const handle = await DBOS.startWorkflow(ReuseTest, {
            workflowID: childID,
            workflowIDReusePolicy: 'reject',
          }).echo('from-parent');
          return await handle.getResult();
        } catch (e) {
          return `rejected:${(e as Error).name}`;
        }
      }

      @DBOS.workflow()
      static async startSameChildTwice(childID: string): Promise<string> {
        const start = () =>
          DBOS.startWorkflow(ReuseTest, { workflowID: childID, workflowIDReusePolicy: 'reject' }).echo('from-parent');
        await (await start()).getResult();
        try {
          await start();
          return 'second-attached';
        } catch (e) {
          return isWorkflowIDInUseError(e) ? 'second-rejected' : `unexpected:${(e as Error).name}`;
        }
      }
    }

    beforeEach(async () => {
      ReuseTest.resetGate();
      await DBOS.registerQueue(QUEUE, { onConflict: 'always_update' });
    });

    afterEach(() => {
      clearDebugTriggers();
      ReuseTest.resolveGate();
    });

    const setups: [string, (id: string) => Promise<void>][] = [
      [
        StatusString.SUCCESS,
        async (id) => {
          await (await DBOS.startWorkflow(ReuseTest, { workflowID: id }).echo('original')).getResult();
        },
      ],
      [
        StatusString.ERROR,
        async (id) => {
          const handle = await DBOS.startWorkflow(ReuseTest, { workflowID: id }).echo('fail');
          await expect(handle.getResult()).rejects.toThrow('echo failed');
        },
      ],
      [
        StatusString.PENDING,
        async (id) => {
          await DBOS.startWorkflow(ReuseTest, { workflowID: id }).gated('original');
        },
      ],
      [
        StatusString.CANCELLED,
        async (id) => {
          await DBOS.startWorkflow(ReuseTest, { workflowID: id }).gated('original');
          await DBOS.cancelWorkflow(id);
        },
      ],
      [
        StatusString.ENQUEUED,
        async (id) => {
          await DBOS.startWorkflow(ReuseTest, {
            workflowID: id,
            queueName: QUEUE,
            enqueueOptions: { applicationVersion: 'no-executor-runs-this' },
          }).echo('original');
        },
      ],
      [
        StatusString.DELAYED,
        async (id) => {
          await DBOS.startWorkflow(ReuseTest, {
            workflowID: id,
            queueName: QUEUE,
            enqueueOptions: { delaySeconds: 3600 },
          }).echo('original');
        },
      ],
    ];

    test.each(setups)('reject-existing-%s', async (status, setup) => {
      const id = `reuse-${status}-${randomUUID()}`;
      await setup(id);
      const before = await DBOS.getWorkflowStatus(id);
      expect(before?.status).toBe(status);

      const attempt = DBOS.startWorkflow(ReuseTest, { workflowID: id, workflowIDReusePolicy: 'reject' }).echo('new');
      await expect(attempt).rejects.toThrow(DBOSWorkflowIDInUseError);
      await expect(attempt).rejects.toMatchObject({ workflowID: id, status });

      const after = await DBOS.getWorkflowStatus(id);
      expect(after?.updatedAt).toBe(before?.updatedAt);
    });

    test('reject-fresh-id-runs-and-default-attaches', async () => {
      const id = `reuse-fresh-${randomUUID()}`;
      const handle = await DBOS.startWorkflow(ReuseTest, { workflowID: id, workflowIDReusePolicy: 'reject' }).echo(
        'first',
      );
      await expect(handle.getResult()).resolves.toBe('first');

      const again = await DBOS.startWorkflow(ReuseTest, { workflowID: id }).echo('second');
      await expect(again.getResult()).resolves.toBe('first');
    });

    test('reject-name-mismatch-throws-in-use', async () => {
      const id = `reuse-mismatch-${randomUUID()}`;
      await (await DBOS.startWorkflow(ReuseTest, { workflowID: id }).echo('original')).getResult();

      await expect(
        DBOS.startWorkflow(ReuseTest, { workflowID: id, workflowIDReusePolicy: 'reject' }).otherWorkflow('new'),
      ).rejects.toMatchObject({ name: 'DBOSWorkflowIDInUseError', workflowName: 'echo' });
    });

    test('reject-survives-retried-init-commit', async () => {
      // The first commit lands but reports a connection error, so the retry sees its own row.
      let failures = 1;
      setDebugTrigger(DEBUG_TRIGGER_INITWF_COMMIT, {
        asyncCallback: () => {
          if (failures-- > 0) throw new Error('ECONNRESET');
          return Promise.resolve();
        },
      });
      const id = `reuse-retry-${randomUUID()}`;
      const handle = await DBOS.startWorkflow(ReuseTest, { workflowID: id, workflowIDReusePolicy: 'reject' }).echo(
        'retried',
      );
      await expect(handle.getResult()).resolves.toBe('retried');
      expect(failures).toBeLessThan(0);
    });

    test('reject-dedup-interplay', async () => {
      const id = `reuse-dedup-${randomUUID()}`;
      const dedupID = `dedup-${randomUUID()}`;
      const enqueue = (workflowID: string) =>
        DBOS.startWorkflow(ReuseTest, {
          workflowID,
          queueName: QUEUE,
          enqueueOptions: { deduplicationID: dedupID, delaySeconds: 3600 },
          workflowIDReusePolicy: 'reject',
        }).echo('x');
      await enqueue(id);

      await expect(enqueue(id)).rejects.toThrow(DBOSWorkflowIDInUseError);
      await expect(enqueue(`reuse-dedup-other-${randomUUID()}`)).rejects.toThrow(DBOSQueueDuplicatedError);
    });

    test('reject-enqueue-with-options', async () => {
      const id = `reuse-ewo-${randomUUID()}`;
      await (await DBOS.startWorkflow(ReuseTest, { workflowID: id }).echo('original')).getResult();

      await expect(
        DBOS.enqueueWorkflowWithOptions(
          {
            queueName: QUEUE,
            workflowName: 'echo',
            workflowClassName: 'ReuseTest',
            workflowID: id,
            workflowIDReusePolicy: 'reject',
          },
          'new',
        ),
      ).rejects.toThrow(DBOSWorkflowIDInUseError);
    });

    test('reject-client-enqueue', async () => {
      const id = `reuse-client-${randomUUID()}`;
      await (await DBOS.startWorkflow(ReuseTest, { workflowID: id }).echo('original')).getResult();

      const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
      const txClient = new Client({ connectionString: config.systemDatabaseUrl! });
      try {
        const options = {
          queueName: QUEUE,
          workflowName: 'echo',
          workflowClassName: 'ReuseTest',
          workflowIDReusePolicy: 'reject' as const,
        };
        await expect(client.enqueue({ ...options, workflowID: id }, 'new')).rejects.toThrow(DBOSWorkflowIDInUseError);

        const fresh = await client.enqueue({ ...options, workflowID: `reuse-client-fresh-${randomUUID()}` }, 'fresh');
        await expect(fresh.getResult()).resolves.toBe('fresh');

        await txClient.connect();
        await txClient.query('BEGIN');
        await expect(client.enqueueInTransaction(txClient, { ...options, workflowID: id }, 'new')).rejects.toThrow(
          DBOSWorkflowIDInUseError,
        );
        await txClient.query('ROLLBACK');
      } finally {
        await txClient.end();
        await client.destroy();
      }
    });

    test('reject-child-is-recorded-for-replay', async () => {
      const childID = `reuse-child-${randomUUID()}`;
      await (await DBOS.startWorkflow(ReuseTest, { workflowID: childID }).echo('original')).getResult();

      const parentID = `reuse-parent-${randomUUID()}`;
      const parent = await DBOS.startWorkflow(ReuseTest, { workflowID: parentID }).startChild(childID);
      await expect(parent.getResult()).resolves.toBe('rejected:DBOSWorkflowIDInUseError');

      const steps = await DBOS.listWorkflowSteps(parentID);
      expect(steps?.[0].error?.name).toBe('DBOSWorkflowIDInUseError');

      // With the child gone, a fresh start would succeed, so the fork's rejection must come from the checkpoint.
      await DBOS.deleteWorkflow(childID);
      const forked = await DBOS.forkWorkflow<string>(parentID, 1);
      await expect(forked.getResult()).resolves.toBe('rejected:DBOSWorkflowIDInUseError');
    });

    test('reject-same-parent-reuse', async () => {
      const childID = `reuse-same-parent-${randomUUID()}`;
      const parentID = `reuse-same-parent-p-${randomUUID()}`;
      const parent = await DBOS.startWorkflow(ReuseTest, { workflowID: parentID }).startSameChildTwice(childID);
      await expect(parent.getResult()).resolves.toBe('second-rejected');

      // Replay serves the recorded rejection, which the helper still matches.
      const replayed = await reexecuteWorkflowById(parentID);
      await expect(replayed.getResult()).resolves.toBe('second-rejected');
    });

    test('debouncer-rejects-reject-policy', async () => {
      const debouncer = new Debouncer({
        workflow: ReuseTest.echo,
        startWorkflowParams: { workflowIDReusePolicy: 'reject' },
      });
      await expect(debouncer.debounce('key', 1000, 'x')).rejects.toThrow("workflowIDReusePolicy 'reject'");
    });
  });
});
