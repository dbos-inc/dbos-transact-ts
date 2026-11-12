/**
 * Public DBOS methods called from a workflow are either checkpointed as steps,
 * refused, or deterministic. These tests cover the methods that used to be none
 * of the three.
 */
import { randomUUID } from 'node:crypto';
import { DBOS } from '../src';
import { WorkflowQueue } from '../src/wfqueue';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, usingSharding } from './helpers';
import { dbRetryConfig, sleepms } from '../src/utils';

const OTHER_APP = 'durability-other-app';

type StepRow = [number, string];

async function stepsOf(workflowID: string): Promise<StepRow[]> {
  const steps = await DBOS.listWorkflowSteps(workflowID);
  return steps!.map((s) => [s.functionID, s.name]);
}

/** The step after the last recorded one, so a fork from it replays every step. */
function afterLast(steps: StepRow[]): number {
  return steps[steps.length - 1][0] + 1;
}

type SysDbMethods = Record<string, (...args: unknown[]) => Promise<unknown>>;
const restores: Array<() => void> = [];

function sysdb(): SysDbMethods {
  return DBOSExecutor.globalInstance!.systemDatabase as unknown as SysDbMethods;
}

/** Replace a system database method until the end of the test. */
function patchSysDb(
  name: string,
  impl: (real: (...args: unknown[]) => Promise<unknown>) => (...args: unknown[]) => Promise<unknown>,
) {
  const db = sysdb();
  const real = db[name].bind(db);
  db[name] = impl(real);
  // The methods live on the prototype, so deleting the own property restores them.
  restores.push(() => delete db[name]);
}

/**
 * Make each system database method drop its connection once when a workflow calls it,
 * leaving DBOS's own background callers alone. Returns the failures left.
 */
function failOnceInWorkflows(...names: string[]): Record<string, number> {
  const remaining: Record<string, number> = {};
  for (const name of names) {
    remaining[name] = 1;
    patchSysDb(name, (real) => async (...args) => {
      if (DBOS.isWithinWorkflow() && remaining[name] > 0) {
        remaining[name]--;
        throw new Error('read ECONNRESET');
      }
      return real(...args);
    });
  }
  return remaining;
}

function queueFields(queue: WorkflowQueue) {
  // The fields come from the checkpointed record; nothing here reads the live row.
  return {
    name: queue.name,
    applicationName: queue.applicationName,
    concurrency: queue.concurrency,
    workerConcurrency: queue.workerConcurrency,
    rateLimit: queue.rateLimit,
    partitionConcurrency: queue.partitionConcurrency,
    partitionWorkerConcurrency: queue.partitionWorkerConcurrency,
    partitionRateLimit: queue.partitionRateLimit,
    minPollingIntervalMs: queue.minPollingIntervalMs,
  };
}

const queueParams = {
  globalConcurrency: 7,
  workerConcurrency: 3,
  rateLimit: { limitPerPeriod: 5, periodSec: 1.5 },
  minPollingIntervalMs: 250,
};
// A partitioned queue, so the partition fields go through the checkpoint too.
const partitionedParams = {
  globalConcurrency: 3,
  partitionConcurrency: 2,
  partitionWorkerConcurrency: 1,
  partitionRateLimit: { limitPerPeriod: 4, periodSec: 2.5 },
};

const names = { queue: '', partitioned: '', other: '', schedule: '', version: '', migrateUrl: '' };

const child = DBOS.registerWorkflow(async (x: number) => Promise.resolve(x), { name: 'durabilityChild' });

const queueWorkflow = DBOS.registerWorkflow(
  async () => {
    // A validation error throws before the call takes a step ID.
    let invalid = false;
    try {
      await DBOS.registerQueue('_dbos_reserved');
    } catch {
      invalid = true;
    }
    // Another application's queue makes this fail, and the error is recorded.
    let collision = '';
    try {
      await DBOS.registerQueue(names.other);
    } catch (e) {
      collision = (e as Error).message;
    }
    const registered = await DBOS.registerQueue(names.queue, queueParams);
    const partitioned = await DBOS.registerQueue(names.partitioned, partitionedParams);
    // A plain read that takes no step.
    await DBOS.retrieveQueue(names.queue);
    const result = await (await DBOS.startWorkflow(child, { queueName: registered.name })(3)).getResult();
    const listed = (await DBOS.listQueues())
      .filter((q) => q.name === names.queue || q.name === names.partitioned)
      .map(queueFields)
      .sort((a, b) => (a.name === names.queue ? -1 : b.name === names.queue ? 1 : 0));
    await DBOS.deleteQueue(names.queue);
    await DBOS.deleteQueue(names.partitioned);
    return {
      invalid,
      collision,
      registered: queueFields(registered),
      partitioned: queueFields(partitioned),
      listed,
      result,
    };
  },
  { name: 'durabilityQueueWorkflow' },
);

const promote = DBOS.registerStep(async () => DBOS.setLatestApplicationVersion(names.version), {
  name: 'durabilityPromote',
});

const versionWorkflow = DBOS.registerWorkflow(
  async () => {
    // Promoting a version is refused in a workflow body, before it takes a step ID,
    // but allowed in a step.
    let refused = '';
    try {
      await DBOS.setLatestApplicationVersion(names.version);
    } catch (e) {
      refused = (e as Error).message;
    }
    await promote();
    const listedAt = (await DBOS.listApplicationVersions()).find(
      (v) => v.versionName === names.version,
    )!.versionTimestamp;
    const latest = await DBOS.getLatestApplicationVersion();
    return { refused, listedAt, latestName: latest.versionName, latestAt: latest.versionTimestamp };
  },
  { name: 'durabilityVersionWorkflow' },
);

const scheduledTarget = DBOS.registerWorkflow(async (_scheduledAt: Date, _ctx: unknown) => Promise.resolve(), {
  name: 'durabilityScheduledTarget',
});

const concurrentWorkflow = DBOS.registerWorkflow(
  async () => {
    // Validation errors and refusals throw before the calls take a step ID.
    const refusals = await Promise.allSettled([
      DBOS.registerQueue('_dbos_reserved'),
      DBOS.createSchedule({ scheduleName: 'bad', workflowFn: scheduledTarget, schedule: 'nope' }),
      DBOS.setLatestApplicationVersion(names.version),
    ]);
    // Every call claims its step ID before its first await, so the numbering follows call
    // order. Each group ends with an extra call, so even its last variant has a later
    // claim to land behind.
    await Promise.all([
      DBOS.createSchedule({
        scheduleName: names.schedule,
        workflowFn: scheduledTarget,
        schedule: '0 0 1 1 *',
        context: { n: 1 },
      }),
      DBOS.listSchedules(),
      DBOS.listQueues(),
    ]);
    await Promise.all([
      DBOS.updateSchedule(names.schedule, { context: { n: 2 } }),
      DBOS.getSchedule(names.schedule),
      DBOS.deleteSchedule(`no-such-schedule-${randomUUID()}`),
      DBOS.registerQueue(`durable-q-${randomUUID()}`),
      DBOS.listQueues(),
      DBOS.deleteQueue(`no-such-queue-${randomUUID()}`),
      DBOS.getLatestApplicationVersion(),
      DBOS.listApplicationVersions(),
      DBOS.listQueues(),
    ]);
    await DBOS.deleteSchedule(names.schedule);
    return refusals.map((r) => r.status);
  },
  { name: 'durabilityConcurrentWorkflow' },
);

const migrateStep = DBOS.registerStep(async () => DBOS.migrate(names.migrateUrl), { name: 'durabilityMigrate' });

const migrateWorkflow = DBOS.registerWorkflow(
  async (fromStep: boolean) => {
    if (fromStep) {
      await migrateStep();
      return 'migrated';
    }
    try {
      await DBOS.migrate(names.migrateUrl);
      return 'migrated';
    } catch (e) {
      return (e as Error).message;
    }
  },
  { name: 'durabilityMigrateWorkflow' },
);

describe('api-durability', () => {
  let config: DBOSConfig;
  const savedBackoff = { ...dbRetryConfig };

  beforeAll(() => {
    config = generateDBOSTestConfig();
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    // Retries of injected connection errors stay quick.
    dbRetryConfig.initialBackoffSec = 0.05;
    await setUpDBOSTestSysDb(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    restores.splice(0).forEach((restore) => restore());
    Object.assign(dbRetryConfig, savedBackoff);
    await DBOS.shutdown();
  });

  test('queue-api-is-checkpointed', async () => {
    names.queue = `durable-q-${randomUUID()}`;
    names.partitioned = `durable-pq-${randomUUID()}`;
    names.other = `durable-q-${randomUUID()}`;
    const executor = DBOSExecutor.globalInstance!;
    await executor.systemDatabase.upsertQueue(
      { ...WorkflowQueue.recordFromParams(names.other, {}), applicationName: OTHER_APP },
      true,
    );
    // Steps retry a dropped connection rather than record it as their result.
    const remaining = failOnceInWorkflows('upsertQueue', 'listQueues', 'deleteQueue');

    const appName = executor.systemDatabase.appName;
    const expected = {
      invalid: true,
      registered: {
        name: names.queue,
        applicationName: appName,
        concurrency: 7,
        workerConcurrency: 3,
        rateLimit: { limitPerPeriod: 5, periodSec: 1.5 },
        partitionConcurrency: undefined,
        partitionWorkerConcurrency: undefined,
        partitionRateLimit: undefined,
        minPollingIntervalMs: 250,
      },
      partitioned: {
        name: names.partitioned,
        applicationName: appName,
        concurrency: 3,
        workerConcurrency: undefined,
        rateLimit: undefined,
        partitionConcurrency: 2,
        partitionWorkerConcurrency: 1,
        partitionRateLimit: { limitPerPeriod: 4, periodSec: 2.5 },
        minPollingIntervalMs: 1000,
      },
      result: 3,
    };

    const handle = await DBOS.startWorkflow(queueWorkflow)();
    const result = await handle.getResult();
    expect(result).toMatchObject(expected);
    expect(result.listed).toEqual([expected.registered, expected.partitioned]);
    expect(result.collision).toContain('already registered by application');

    const steps = await stepsOf(handle.workflowID);
    expect(steps.map((s) => s[1])).toEqual([
      'DBOS.registerQueue',
      'DBOS.registerQueue',
      'DBOS.registerQueue',
      'durabilityChild',
      'DBOS.getResult',
      'DBOS.listQueues',
      'DBOS.deleteQueue',
      'DBOS.deleteQueue',
    ]);
    // Consecutive IDs: the invalid name and retrieveQueue took none.
    expect(steps.map((s) => s[0])).toEqual(steps.map((_, i) => steps[0][0] + i));
    const stepInfos = (await DBOS.listWorkflowSteps(handle.workflowID))!;
    expect(stepInfos[0].error).not.toBeNull();
    expect(stepInfos.slice(1).every((s) => s.error === null)).toBe(true);
    expect(remaining).toEqual({ upsertQueue: 0, listQueues: 0, deleteQueue: 0 });

    // Outside a workflow the same calls fail fast instead of retrying.
    patchSysDb('listQueues', () => () => Promise.reject(new Error('read ECONNRESET')));
    const timeout = sleepms(5000).then(() => 'still retrying');
    await expect(Promise.race([DBOS.listQueues(), timeout])).rejects.toThrow('ECONNRESET');
    restores.pop()!();

    // With all three queues gone, a replay returns the recorded results, including the
    // recorded error, instead of registering the other queue.
    await DBOS.deleteQueue(names.other);
    const firstDelete = steps[6][0];
    let forked = await DBOS.forkWorkflow(handle.workflowID, firstDelete);
    expect(await forked.getResult()).toEqual(result);
    expect(await stepsOf(forked.workflowID)).toEqual(steps);
    expect(await DBOS.retrieveQueue(names.other)).toBeNull();

    // Replaying registerQueue returns a queue that still enqueues.
    await DBOS.registerQueue(names.queue, queueParams);
    await DBOS.registerQueue(names.partitioned, partitionedParams);
    const enqueueStep = steps[3][0];
    forked = await DBOS.forkWorkflow(handle.workflowID, enqueueStep);
    expect(await forked.getResult()).toEqual(result);
    const childID = (await DBOS.listWorkflowSteps(forked.workflowID))![3].childWorkflowID!;
    expect((await DBOS.getWorkflowStatus(childID))?.queueName).toBe(names.queue);
  }, 60000);

  test('application-version-api-is-checkpointed', async () => {
    names.version = DBOS.applicationVersion;
    // The read step retries a dropped connection rather than record it.
    const remaining = failOnceInWorkflows('listApplicationVersions');

    const handle = await DBOS.startWorkflow(versionWorkflow)();
    const result = await handle.getResult();
    const promotedAt = result.latestAt;
    expect(result).toEqual({
      refused: 'DBOS.setLatestApplicationVersion cannot be called from a workflow; call it from a step',
      listedAt: promotedAt,
      latestName: names.version,
      latestAt: promotedAt,
    });
    const steps = await stepsOf(handle.workflowID);
    expect(steps.map((s) => s[1])).toEqual([
      'durabilityPromote',
      'DBOS.listApplicationVersions',
      'DBOS.getLatestApplicationVersion',
    ]);
    expect(remaining).toEqual({ listApplicationVersions: 0 });

    // Replaying every step does not promote the version again.
    await sleepms(10);
    let forked = await DBOS.forkWorkflow(handle.workflowID, afterLast(steps));
    expect(await forked.getResult()).toEqual(result);
    expect(await stepsOf(forked.workflowID)).toEqual(steps);
    expect((await DBOS.getLatestApplicationVersion()).versionTimestamp).toBe(promotedAt);

    // Once the live timestamp moves, a full replay can only return the old one from
    // the recorded reads.
    await DBOS.setLatestApplicationVersion(names.version);
    expect((await DBOS.getLatestApplicationVersion()).versionTimestamp).toBeGreaterThan(promotedAt);
    forked = await DBOS.forkWorkflow(handle.workflowID, afterLast(steps));
    expect(await forked.getResult()).toEqual(result);
  }, 60000);

  test('concurrent-calls-number-steps-in-call-order', async () => {
    names.schedule = `durable-sched-${randomUUID()}`;
    names.version = DBOS.applicationVersion;

    const handle = await DBOS.startWorkflow(concurrentWorkflow)();
    expect(await handle.getResult()).toEqual(['rejected', 'rejected', 'rejected']);
    const steps = await stepsOf(handle.workflowID);
    expect(steps.map((s) => s[1])).toEqual([
      'DBOS.createSchedule',
      'DBOS.listSchedules',
      'DBOS.listQueues',
      'DBOS.updateSchedule',
      'DBOS.getSchedule',
      'DBOS.deleteSchedule',
      'DBOS.registerQueue',
      'DBOS.listQueues',
      'DBOS.deleteQueue',
      'DBOS.getLatestApplicationVersion',
      'DBOS.listApplicationVersions',
      'DBOS.listQueues',
      'DBOS.deleteSchedule',
    ]);
    // Consecutive IDs: the refused and invalid calls took none.
    expect(steps.map((s) => s[0])).toEqual(steps.map((_, i) => steps[0][0] + i));
  }, 60000);

  test('migrate-refused-in-workflow-body', async () => {
    names.migrateUrl = config.systemDatabaseUrl!;
    await expect(DBOS.startWorkflow(migrateWorkflow)(false).then((h) => h.getResult())).resolves.toBe(
      'DBOS.migrate cannot be called from a workflow; call it from a step',
    );
    // A step is the right place for the side effect. Migrating is idempotent, so the
    // test's own system database serves; a sharded one goes through its router instead.
    if (!usingSharding()) {
      await expect(DBOS.startWorkflow(migrateWorkflow)(true).then((h) => h.getResult())).resolves.toBe('migrated');
    }
  }, 60000);
});
