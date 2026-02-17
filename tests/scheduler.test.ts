import { DBOS, ConfiguredInstance } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, dropDatabase } from './helpers';
import { sleepms } from '../src/utils';

describe('dynamic-scheduler-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    config.schedulerPollingIntervalMs = 1000;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    expect(config.systemDatabaseUrl).toBeDefined();
    await dropDatabase(config.systemDatabaseUrl!);
  });

  beforeEach(async () => {
    await DBOS.launch();
    // Clean up leftover schedules for test isolation
    const existing = await DBOS.listSchedules();
    for (const s of existing) {
      await DBOS.deleteSchedule(s.scheduleName);
    }
  });

  afterEach(async () => {
    await DBOS.shutdown();
  }, 10000);

  // ---------------------------------------------------------------------------
  // schedule-crud
  // ---------------------------------------------------------------------------

  async function myWorkflow(_scheduledDate: Date, _context: unknown) {}
  async function otherWorkflow(_scheduledDate: Date, _context: unknown) {}

  const regMyWf = DBOS.registerWorkflow(myWorkflow, { name: 'myWorkflow' });
  const regOtherWf = DBOS.registerWorkflow(otherWorkflow, { name: 'otherWorkflow' });

  // Matches Python test_schedule_crud
  test('schedule-crud', async () => {
    // Create a schedule with context
    await DBOS.createSchedule({
      scheduleName: 'test-schedule',
      workflowFn: regMyWf,
      schedule: '* * * * *',
      context: { env: 'test' },
    });

    // List schedules and verify
    const schedules = await DBOS.listSchedules();
    expect(schedules.length).toBe(1);
    expect(schedules[0].scheduleName).toBe('test-schedule');
    expect(schedules[0].workflowName).toBe('myWorkflow');
    expect(schedules[0].schedule).toBe('* * * * *');
    expect(schedules[0].context).toEqual({ env: 'test' });

    // Get schedule by name
    const sched = await DBOS.getSchedule('test-schedule');
    expect(sched).not.toBeNull();
    expect(sched!.scheduleName).toBe('test-schedule');
    expect(sched!.workflowName).toBe('myWorkflow');
    expect(sched!.schedule).toBe('* * * * *');
    expect(sched!.scheduleId).toBe(schedules[0].scheduleId);
    expect(sched!.context).toEqual({ env: 'test' });

    // Get nonexistent schedule
    expect(await DBOS.getSchedule('nonexistent')).toBeNull();

    // Reject invalid cron expression
    await expect(
      DBOS.createSchedule({
        scheduleName: 'bad-schedule',
        workflowFn: regMyWf,
        schedule: 'not a cron',
      }),
    ).rejects.toThrow();

    // Reject duplicate schedule name
    await expect(
      DBOS.createSchedule({
        scheduleName: 'test-schedule',
        workflowFn: regMyWf,
        schedule: '0 0 * * *',
      }),
    ).rejects.toThrow(/already exists/);

    // --- list_schedules filters ---
    await DBOS.createSchedule({
      scheduleName: 'other-schedule',
      workflowFn: regOtherWf,
      schedule: '0 0 * * *',
    });
    await DBOS.pauseSchedule('other-schedule');

    // Filter by status
    expect((await DBOS.listSchedules({ status: 'ACTIVE' })).length).toBe(1);
    expect((await DBOS.listSchedules({ status: 'ACTIVE' }))[0].scheduleName).toBe('test-schedule');
    expect((await DBOS.listSchedules({ status: 'PAUSED' })).length).toBe(1);
    expect((await DBOS.listSchedules({ status: 'NONEXISTENT' })).length).toBe(0);

    // Filter by workflow_name
    expect((await DBOS.listSchedules({ workflowName: 'myWorkflow' })).length).toBe(1);
    expect((await DBOS.listSchedules({ workflowName: 'otherWorkflow' })).length).toBe(1);

    // Filter by schedule_name_prefix
    expect((await DBOS.listSchedules({ scheduleNamePrefix: 'test-' })).length).toBe(1);
    expect((await DBOS.listSchedules({ scheduleNamePrefix: 'other-' })).length).toBe(1);
    expect((await DBOS.listSchedules({ scheduleNamePrefix: 'nonexistent-' })).length).toBe(0);

    // Combine filters
    expect((await DBOS.listSchedules({ status: 'ACTIVE', scheduleNamePrefix: 'test-' })).length).toBe(1);
    expect((await DBOS.listSchedules({ status: 'PAUSED', scheduleNamePrefix: 'test-' })).length).toBe(0);

    // Delete schedules
    await DBOS.deleteSchedule('other-schedule');
    await DBOS.deleteSchedule('test-schedule');
    expect(await DBOS.getSchedule('test-schedule')).toBeNull();
    expect((await DBOS.listSchedules()).length).toBe(0);
  });

  // ---------------------------------------------------------------------------
  // apply-schedules
  // ---------------------------------------------------------------------------

  async function targetWorkflow(_scheduledDate: Date, _context: unknown) {}
  const regTargetWf = DBOS.registerWorkflow(targetWorkflow, { name: 'targetWorkflow' });

  async function badApplyWorkflow() {
    await DBOS.applySchedules([
      {
        scheduleName: 'x',
        workflowFn: regTargetWf,
        schedule: '* * * * *',
      },
    ]);
  }
  const regBadApplyWf = DBOS.registerWorkflow(badApplyWorkflow, { name: 'badApplyWorkflow' });

  // Matches Python test_apply_schedules
  test('apply-schedules', async () => {
    // Apply two schedules at once
    await DBOS.applySchedules([
      {
        scheduleName: 'sched-a',
        workflowFn: regMyWf,
        schedule: '* * * * *',
        context: { region: 'us' },
      },
      {
        scheduleName: 'sched-b',
        workflowFn: regOtherWf,
        schedule: '0 0 * * *',
        context: null,
      },
    ]);
    let schedules = await DBOS.listSchedules();
    expect(schedules.length).toBe(2);
    let byName = Object.fromEntries(schedules.map((s) => [s.scheduleName, s]));
    expect(byName['sched-a'].schedule).toBe('* * * * *');
    expect(byName['sched-a'].context).toEqual({ region: 'us' });
    expect(byName['sched-b'].schedule).toBe('0 0 * * *');
    expect(byName['sched-b'].context).toBeNull();

    // Replace sched-a, add sched-c (sched-b is NOT removed — apply does upsert per schedule)
    await DBOS.applySchedules([
      {
        scheduleName: 'sched-a',
        workflowFn: regMyWf,
        schedule: '0 * * * *',
        context: null,
      },
      {
        scheduleName: 'sched-c',
        workflowFn: regOtherWf,
        schedule: '*/5 * * * *',
        context: [1, 2, 3],
      },
    ]);
    schedules = await DBOS.listSchedules();
    expect(schedules.length).toBe(3);
    byName = Object.fromEntries(schedules.map((s) => [s.scheduleName, s]));
    expect(byName['sched-a'].schedule).toBe('0 * * * *');
    expect(byName['sched-a'].context).toBeNull();
    expect(byName['sched-c'].schedule).toBe('*/5 * * * *');
    expect(byName['sched-c'].context).toEqual([1, 2, 3]);

    // Reject invalid cron
    await expect(
      DBOS.applySchedules([
        {
          scheduleName: 'bad',
          workflowFn: regMyWf,
          schedule: 'not a cron',
          context: null,
        },
      ]),
    ).rejects.toThrow();

    // Reject call from within a workflow
    const handle = await DBOS.startWorkflow(regBadApplyWf)();
    await expect(handle.getResult()).rejects.toThrow(/cannot be called from within a workflow/);

    // Clean up
    await DBOS.deleteSchedule('sched-a');
    await DBOS.deleteSchedule('sched-b');
    await DBOS.deleteSchedule('sched-c');
    expect((await DBOS.listSchedules()).length).toBe(0);
  });

  // ---------------------------------------------------------------------------
  // schedule-crud-from-workflow
  // ---------------------------------------------------------------------------

  async function crudWorkflow() {
    await DBOS.createSchedule({
      scheduleName: 'wf-schedule',
      workflowFn: regTargetWf,
      schedule: '* * * * *',
      context: { from: 'workflow' },
    });

    const schedules = await DBOS.listSchedules();
    expect(schedules.length).toBe(1);
    expect(schedules[0].scheduleName).toBe('wf-schedule');
    expect(schedules[0].context).toEqual({ from: 'workflow' });

    const sched = await DBOS.getSchedule('wf-schedule');
    expect(sched).not.toBeNull();
    expect(sched!.scheduleName).toBe('wf-schedule');
    expect(sched!.context).toEqual({ from: 'workflow' });

    await DBOS.deleteSchedule('wf-schedule');
    expect(await DBOS.getSchedule('wf-schedule')).toBeNull();
  }
  const regCrudWf = DBOS.registerWorkflow(crudWorkflow, { name: 'crudWorkflow' });

  // Matches Python test_schedule_crud_from_workflow
  test('schedule-crud-from-workflow', async () => {
    const handle = await DBOS.startWorkflow(regCrudWf)();
    await handle.getResult();

    const steps = await DBOS.listWorkflowSteps(handle.workflowID);
    expect(steps).toBeDefined();
    const stepNames = steps!.map((s) => s.name);
    expect(stepNames).toEqual([
      'DBOS.createSchedule',
      'DBOS.listSchedules',
      'DBOS.getSchedule',
      'DBOS.deleteSchedule',
      'DBOS.getSchedule',
    ]);

    // Fork the workflow and verify same steps
    const forkedHandle = await DBOS.forkWorkflow(handle.workflowID, steps!.length);
    await forkedHandle.getResult();
    const forkedSteps = await DBOS.listWorkflowSteps(forkedHandle.workflowID);
    expect(forkedSteps!.map((s) => s.name)).toEqual(stepNames);
  });

  // ---------------------------------------------------------------------------
  // Dynamic scheduler firing tests
  // ---------------------------------------------------------------------------

  async function retryUntilSuccess(fn: () => void, timeoutMs: number = 10000, intervalMs: number = 200): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let lastError: Error | undefined;
    while (Date.now() < deadline) {
      try {
        fn();
        return;
      } catch (e) {
        lastError = e as Error;
        await sleepms(intervalMs);
      }
    }
    throw lastError ?? new Error('retryUntilSuccess timed out');
  }

  // --- Firing test workflows ---
  const firesCounterA: { dates: Date[]; contexts: unknown[] } = { dates: [], contexts: [] };
  const firesCounterB: { dates: Date[]; contexts: unknown[] } = { dates: [], contexts: [] };
  async function firesWorkflowA(scheduledDate: Date, context: unknown) {
    firesCounterA.dates.push(scheduledDate);
    firesCounterA.contexts.push(context);
    await Promise.resolve();
  }
  async function firesWorkflowB(scheduledDate: Date, context: unknown) {
    firesCounterB.dates.push(scheduledDate);
    firesCounterB.contexts.push(context);
    await Promise.resolve();
  }
  const regFiresA = DBOS.registerWorkflow(firesWorkflowA, { name: 'firesWorkflowA' });
  const regFiresB = DBOS.registerWorkflow(firesWorkflowB, { name: 'firesWorkflowB' });

  test('dynamic-scheduler-fires', async () => {
    firesCounterA.dates = [];
    firesCounterA.contexts = [];
    firesCounterB.dates = [];
    firesCounterB.contexts = [];

    await DBOS.createSchedule({
      scheduleName: 'fire-a',
      workflowFn: regFiresA,
      schedule: '* * * * * *',
      context: { key: 'alpha' },
    });
    await DBOS.createSchedule({
      scheduleName: 'fire-b',
      workflowFn: regFiresB,
      schedule: '* * * * * *',
      context: { key: 'beta' },
    });

    await retryUntilSuccess(() => {
      expect(firesCounterA.dates.length).toBeGreaterThanOrEqual(2);
      expect(firesCounterB.dates.length).toBeGreaterThanOrEqual(2);
    });

    // Verify contexts
    for (const ctx of firesCounterA.contexts) {
      expect(ctx).toEqual({ key: 'alpha' });
    }
    for (const ctx of firesCounterB.contexts) {
      expect(ctx).toEqual({ key: 'beta' });
    }

    await DBOS.deleteSchedule('fire-a');
    await DBOS.deleteSchedule('fire-b');
  }, 30000);

  // --- Delete stops firing ---
  let deleteCounter = 0;
  async function deleteTestWorkflow(_scheduledDate: Date, _context: unknown) {
    deleteCounter++;
    await Promise.resolve();
  }
  const regDeleteWf = DBOS.registerWorkflow(deleteTestWorkflow, { name: 'deleteTestWorkflow' });

  test('dynamic-scheduler-delete-stops-firing', async () => {
    deleteCounter = 0;

    await DBOS.createSchedule({
      scheduleName: 'delete-test',
      workflowFn: regDeleteWf,
      schedule: '* * * * * *',
    });

    await retryUntilSuccess(() => {
      expect(deleteCounter).toBeGreaterThanOrEqual(1);
    });

    await DBOS.deleteSchedule('delete-test');
    await sleepms(3000);
    const snapshot = deleteCounter;
    await sleepms(3000);
    expect(deleteCounter).toBe(snapshot);
  }, 30000);

  // --- Add after launch ---
  let addAfterCounter = 0;
  async function addAfterWorkflow(_scheduledDate: Date, _context: unknown) {
    addAfterCounter++;
    await Promise.resolve();
  }
  const regAddAfterWf = DBOS.registerWorkflow(addAfterWorkflow, { name: 'addAfterWorkflow' });

  test('dynamic-scheduler-add-after-launch', async () => {
    addAfterCounter = 0;
    await sleepms(2000);
    expect(addAfterCounter).toBe(0);

    await DBOS.createSchedule({
      scheduleName: 'add-after',
      workflowFn: regAddAfterWf,
      schedule: '* * * * * *',
    });

    await retryUntilSuccess(() => {
      expect(addAfterCounter).toBeGreaterThanOrEqual(2);
    });

    await DBOS.deleteSchedule('add-after');
  }, 30000);

  // --- Replace schedule ---
  const replaceResults: { count: number; contexts: unknown[] } = { count: 0, contexts: [] };
  async function replaceWorkflow(_scheduledDate: Date, context: unknown) {
    replaceResults.count++;
    replaceResults.contexts.push(context);
    await Promise.resolve();
  }
  const regReplaceWf = DBOS.registerWorkflow(replaceWorkflow, { name: 'replaceWorkflow' });

  test('dynamic-scheduler-replace-schedule', async () => {
    replaceResults.count = 0;
    replaceResults.contexts = [];

    // Create a daily schedule that won't fire
    await DBOS.createSchedule({
      scheduleName: 'replace-test',
      workflowFn: regReplaceWf,
      schedule: '0 0 * * *',
      context: { version: 'v1' },
    });
    await sleepms(3000);
    expect(replaceResults.count).toBe(0);

    // Delete + create every-second schedule with v2 context
    await DBOS.deleteSchedule('replace-test');
    await DBOS.createSchedule({
      scheduleName: 'replace-test',
      workflowFn: regReplaceWf,
      schedule: '* * * * * *',
      context: { version: 'v2' },
    });

    await retryUntilSuccess(() => {
      expect(replaceResults.count).toBeGreaterThanOrEqual(2);
    });
    // All firings so far should have v2 context
    for (const ctx of replaceResults.contexts) {
      expect(ctx).toEqual({ version: 'v2' });
    }

    // Replace again with v3
    const snapshot = replaceResults.count;
    await DBOS.deleteSchedule('replace-test');
    // Wait for polling loop to detect deletion and stop the old loop
    await sleepms(3000);
    replaceResults.contexts = [];
    await DBOS.createSchedule({
      scheduleName: 'replace-test',
      workflowFn: regReplaceWf,
      schedule: '* * * * * *',
      context: { version: 'v3' },
    });

    await retryUntilSuccess(() => {
      expect(replaceResults.count).toBeGreaterThanOrEqual(snapshot + 2);
    });
    // New firings should have v3 context
    for (const ctx of replaceResults.contexts) {
      expect(ctx).toEqual({ version: 'v3' });
    }

    await DBOS.deleteSchedule('replace-test');
  }, 30000);

  // --- Long schedule shutdown ---
  let longCounter = 0;
  async function longScheduleWorkflow(_scheduledDate: Date, _context: unknown) {
    longCounter++;
    await Promise.resolve();
  }
  const regLongWf = DBOS.registerWorkflow(longScheduleWorkflow, { name: 'longScheduleWorkflow' });

  test('dynamic-scheduler-long-schedule-shutdown', async () => {
    longCounter = 0;

    await DBOS.createSchedule({
      scheduleName: 'long-test',
      workflowFn: regLongWf,
      schedule: '0 0 * * *',
    });

    await sleepms(3000);
    expect(longCounter).toBe(0);
    // Test passes if DBOS.shutdown() in afterEach doesn't hang/timeout
  }, 30000);

  // --- Pause/Resume ---
  let pauseCounter = 0;
  async function pauseWorkflow(_scheduledDate: Date, _context: unknown) {
    pauseCounter++;
    await Promise.resolve();
  }
  const regPauseWf = DBOS.registerWorkflow(pauseWorkflow, { name: 'pauseWorkflow' });

  test('dynamic-scheduler-pause-resume', async () => {
    pauseCounter = 0;

    await DBOS.createSchedule({
      scheduleName: 'pause-test',
      workflowFn: regPauseWf,
      schedule: '* * * * * *',
    });

    await retryUntilSuccess(() => {
      expect(pauseCounter).toBeGreaterThanOrEqual(1);
    });

    // Pause
    await DBOS.pauseSchedule('pause-test');
    const pausedSched = await DBOS.getSchedule('pause-test');
    expect(pausedSched!.status).toBe('PAUSED');

    await sleepms(3000);
    const snapshot = pauseCounter;
    await sleepms(3000);
    expect(pauseCounter).toBe(snapshot);

    // Resume
    await DBOS.resumeSchedule('pause-test');
    const resumedSched = await DBOS.getSchedule('pause-test');
    expect(resumedSched!.status).toBe('ACTIVE');

    await retryUntilSuccess(() => {
      expect(pauseCounter).toBeGreaterThan(snapshot);
    });

    await DBOS.deleteSchedule('pause-test');
  }, 30000);

  // ---------------------------------------------------------------------------
  // Trigger schedule
  // ---------------------------------------------------------------------------
  const triggerResults: { dates: Date[]; contexts: unknown[] } = { dates: [], contexts: [] };
  async function triggerWorkflow(scheduledDate: Date, context: unknown) {
    triggerResults.dates.push(scheduledDate);
    triggerResults.contexts.push(context);
    await Promise.resolve();
  }
  const regTriggerWf = DBOS.registerWorkflow(triggerWorkflow, { name: 'triggerWorkflow' });

  test('trigger-schedule', async () => {
    triggerResults.dates = [];
    triggerResults.contexts = [];

    // Create a daily schedule (won't fire on its own during test)
    await DBOS.createSchedule({
      scheduleName: 'trigger-test',
      workflowFn: regTriggerWf,
      schedule: '0 0 * * *',
      context: { source: 'trigger' },
    });

    // Manually trigger
    const before = new Date();
    const handle = await DBOS.triggerSchedule('trigger-test');
    await handle.getResult();
    const after = new Date();

    expect(triggerResults.dates.length).toBe(1);
    expect(triggerResults.dates[0].getTime()).toBeGreaterThanOrEqual(before.getTime());
    expect(triggerResults.dates[0].getTime()).toBeLessThanOrEqual(after.getTime());
    expect(triggerResults.contexts[0]).toEqual({ source: 'trigger' });

    // Trigger again — should produce a second firing
    const handle2 = await DBOS.triggerSchedule('trigger-test');
    await handle2.getResult();
    expect(triggerResults.dates.length).toBe(2);

    // Triggering a nonexistent schedule should throw
    await expect(DBOS.triggerSchedule('nonexistent')).rejects.toThrow(/not found/);

    await DBOS.deleteSchedule('trigger-test');
  }, 30000);

  // ---------------------------------------------------------------------------
  // Backfill schedule
  // ---------------------------------------------------------------------------
  const backfillResults: { dates: Date[]; contexts: unknown[] } = { dates: [], contexts: [] };
  async function backfillWorkflow(scheduledDate: Date, context: unknown) {
    backfillResults.dates.push(scheduledDate);
    backfillResults.contexts.push(context);
    await Promise.resolve();
  }
  const regBackfillWf = DBOS.registerWorkflow(backfillWorkflow, { name: 'backfillWorkflow' });

  test('backfill-schedule', async () => {
    backfillResults.dates = [];
    backfillResults.contexts = [];

    // Create an every-minute schedule
    await DBOS.createSchedule({
      scheduleName: 'backfill-test',
      workflowFn: regBackfillWf,
      schedule: '* * * * *',
      context: { source: 'backfill' },
    });

    // Backfill a 5-minute window in the past
    const end = new Date();
    end.setMilliseconds(0);
    const start = new Date(end.getTime() - 5 * 60 * 1000);

    const handles = await DBOS.backfillSchedule('backfill-test', start, end);
    // Every-minute schedule over 5 minutes should yield ~5 handles (start exclusive, end exclusive)
    expect(handles.length).toBeGreaterThanOrEqual(4);
    expect(handles.length).toBeLessThanOrEqual(5);

    // Wait for all workflows to complete
    for (const h of handles) {
      await h.getResult();
    }

    expect(backfillResults.dates.length).toBe(handles.length);
    // All contexts should match
    for (const ctx of backfillResults.contexts) {
      expect(ctx).toEqual({ source: 'backfill' });
    }
    // All dates should be within range
    for (const d of backfillResults.dates) {
      expect(d.getTime()).toBeGreaterThanOrEqual(start.getTime());
      expect(d.getTime()).toBeLessThan(end.getTime());
    }

    // Backfilling a nonexistent schedule should throw
    await expect(DBOS.backfillSchedule('nonexistent', start, end)).rejects.toThrow(/not found/);

    await DBOS.deleteSchedule('backfill-test');
  }, 30000);

  // ---------------------------------------------------------------------------
  // Class-based workflows
  // ---------------------------------------------------------------------------
  class ScheduledClass {
    static counter = 0;
    static contexts: unknown[] = [];

    @DBOS.workflow()
    static async scheduledStaticWf(scheduledDate: Date, context: unknown) {
      ScheduledClass.counter++;
      ScheduledClass.contexts.push(context);
      await Promise.resolve();
    }
  }

  test('dynamic-scheduler-class-static-method', async () => {
    ScheduledClass.counter = 0;
    ScheduledClass.contexts = [];

    await DBOS.createSchedule({
      scheduleName: 'class-static-test',
      workflowFn: ScheduledClass.scheduledStaticWf,
      schedule: '* * * * * *',
      context: { from: 'static-class' },
    });

    await retryUntilSuccess(() => {
      expect(ScheduledClass.counter).toBeGreaterThanOrEqual(2);
    });

    for (const ctx of ScheduledClass.contexts) {
      expect(ctx).toEqual({ from: 'static-class' });
    }

    // Also verify trigger works with class static method
    const snapshot = ScheduledClass.counter;
    const handle = await DBOS.triggerSchedule('class-static-test');
    await handle.getResult();
    expect(ScheduledClass.counter).toBeGreaterThan(snapshot);

    await DBOS.deleteSchedule('class-static-test');
  }, 30000);

  // Instance method on a ConfiguredInstance — should fail at createSchedule time
  class InstanceScheduleClass extends ConfiguredInstance {
    @DBOS.workflow()
    async instanceWf(_scheduledDate: Date, _context: unknown) {
      await Promise.resolve();
    }
  }

  const _inst = new InstanceScheduleClass('test-inst');

  test('dynamic-scheduler-instance-method-rejected', async () => {
    await expect(
      DBOS.createSchedule({
        scheduleName: 'instance-test',
        // eslint-disable-next-line @typescript-eslint/unbound-method
        workflowFn: _inst.instanceWf,
        schedule: '* * * * * *',
      }),
    ).rejects.toThrow(/instance method/i);
  }, 30000);
});
