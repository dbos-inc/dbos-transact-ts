import { DBOS, ConfiguredInstance, DBOSClient, WorkflowQueue } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
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
  });

  // ---------------------------------------------------------------------------
  // schedule-crud
  // ---------------------------------------------------------------------------

  async function myWorkflow(_scheduledDate: Date, _context: unknown) {}
  async function otherWorkflow(_scheduledDate: Date, _context: unknown) {}

  const regMyWf = DBOS.registerWorkflow(myWorkflow, { name: 'myWorkflow' });
  const regOtherWf = DBOS.registerWorkflow(otherWorkflow, { name: 'otherWorkflow' });

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
    // Verify defaults for new fields
    expect(schedules[0].lastFiredAt).toBeNull();
    expect(schedules[0].automaticBackfill).toBe(false);
    expect(schedules[0].cronTimezone).toBeNull();

    // Get schedule by name
    const sched = await DBOS.getSchedule('test-schedule');
    expect(sched).not.toBeNull();
    expect(sched!.scheduleName).toBe('test-schedule');
    expect(sched!.workflowName).toBe('myWorkflow');
    expect(sched!.schedule).toBe('* * * * *');
    expect(sched!.scheduleId).toBe(schedules[0].scheduleId);
    expect(sched!.context).toEqual({ env: 'test' });
    expect(sched!.lastFiredAt).toBeNull();
    expect(sched!.automaticBackfill).toBe(false);
    expect(sched!.cronTimezone).toBeNull();

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

    // Create with explicit schedule options
    await DBOS.createSchedule({
      scheduleName: 'tz-schedule',
      workflowFn: regMyWf,
      schedule: '0 0 * * *',
      options: { cronTimezone: 'America/New_York', automaticBackfill: true },
    });
    const tzSched = await DBOS.getSchedule('tz-schedule');
    expect(tzSched).not.toBeNull();
    expect(tzSched!.cronTimezone).toBe('America/New_York');
    expect(tzSched!.automaticBackfill).toBe(true);
    expect(tzSched!.lastFiredAt).toBeNull();
    await DBOS.deleteSchedule('tz-schedule');

    // Reject invalid timezone
    await expect(
      DBOS.createSchedule({
        scheduleName: 'bad-tz',
        workflowFn: regMyWf,
        schedule: '0 0 * * *',
        options: { cronTimezone: 'Not/A/Timezone' },
      }),
    ).rejects.toThrow(/Invalid timezone/);

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

    // Array filters
    expect((await DBOS.listSchedules({ status: ['ACTIVE', 'PAUSED'] })).length).toBe(2);
    expect((await DBOS.listSchedules({ status: ['PAUSED'] })).length).toBe(1);
    expect((await DBOS.listSchedules({ workflowName: ['myWorkflow', 'otherWorkflow'] })).length).toBe(2);
    expect((await DBOS.listSchedules({ workflowName: ['myWorkflow'] })).length).toBe(1);
    expect((await DBOS.listSchedules({ scheduleNamePrefix: ['test-', 'other-'] })).length).toBe(2);
    expect((await DBOS.listSchedules({ scheduleNamePrefix: ['nonexistent-', 'also-nonexistent-'] })).length).toBe(0);

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

  test('apply-schedules', async () => {
    // Apply two schedules at once, sched-b with options
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
        automaticBackfill: true,
        cronTimezone: 'Asia/Tokyo',
      },
    ]);
    let schedules = await DBOS.listSchedules();
    expect(schedules.length).toBe(2);
    let byName = Object.fromEntries(schedules.map((s) => [s.scheduleName, s]));
    expect(byName['sched-a'].schedule).toBe('* * * * *');
    expect(byName['sched-a'].context).toEqual({ region: 'us' });
    expect(byName['sched-a'].automaticBackfill).toBe(false);
    expect(byName['sched-a'].cronTimezone).toBeNull();
    expect(byName['sched-b'].schedule).toBe('0 0 * * *');
    expect(byName['sched-b'].context).toBeNull();
    expect(byName['sched-b'].automaticBackfill).toBe(true);
    expect(byName['sched-b'].cronTimezone).toBe('Asia/Tokyo');

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
  async function firesWorkflowA(scheduledDate: Date, context: object) {
    firesCounterA.dates.push(scheduledDate);
    firesCounterA.contexts.push(context);
    await Promise.resolve();
  }
  async function firesWorkflowB(scheduledDate: Date, context: object) {
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
      options: { cronTimezone: 'America/New_York' },
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

    // Verify lastFiredAt is set on both schedules as valid UTC timestamps
    const schedA = await DBOS.getSchedule('fire-a');
    expect(schedA).not.toBeNull();
    expect(schedA!.lastFiredAt).not.toBeNull();
    expect(schedA!.lastFiredAt!).toMatch(/Z$/);
    const lastFiredA = new Date(schedA!.lastFiredAt!);
    expect(lastFiredA.getTime()).not.toBeNaN();
    expect(lastFiredA.getTime()).toBeLessThanOrEqual(Date.now());

    const schedB = await DBOS.getSchedule('fire-b');
    expect(schedB).not.toBeNull();
    expect(schedB!.lastFiredAt).not.toBeNull();
    expect(schedB!.lastFiredAt!).toMatch(/Z$/);
    const lastFiredB = new Date(schedB!.lastFiredAt!);
    expect(lastFiredB.getTime()).not.toBeNaN();
    expect(lastFiredB.getTime()).toBeLessThanOrEqual(Date.now());
    expect(schedB!.cronTimezone).toBe('America/New_York');

    await DBOS.deleteSchedule('fire-a');
    await DBOS.deleteSchedule('fire-b');
  });

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
  });

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
  });

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
  });

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
  });

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
  });

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

    expect(triggerResults.dates.length).toBe(1);
    expect(triggerResults.dates[0].getTime()).toBeGreaterThanOrEqual(before.getTime());
    const after = new Date();
    expect(triggerResults.dates[0].getTime()).toBeLessThanOrEqual(after.getTime());
    expect(triggerResults.contexts[0]).toEqual({ source: 'trigger' });

    // Trigger again — should produce a second firing
    const handle2 = await DBOS.triggerSchedule('trigger-test');
    await handle2.getResult();
    expect(triggerResults.dates.length).toBe(2);

    // Triggering a nonexistent schedule should throw
    await expect(DBOS.triggerSchedule('nonexistent')).rejects.toThrow(/not found/);

    await DBOS.deleteSchedule('trigger-test');
  });

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
    // Helper: compute local-time midnight for a given date (cron uses local time)
    function localMidnight(year: number, month: number, day: number): Date {
      return new Date(year, month - 1, day, 0, 0, 0, 0);
    }
    // Helper: compute a local-time Date from components
    function localTime(year: number, month: number, day: number, hour: number, minute: number): Date {
      return new Date(year, month - 1, day, hour, minute, 0, 0);
    }

    // Helper to run a backfill and verify exact scheduled dates
    async function testBackfill(scheduleName: string, cronExpr: string, start: Date, end: Date, expectedDates: Date[]) {
      backfillResults.dates = [];
      backfillResults.contexts = [];

      await DBOS.createSchedule({
        scheduleName,
        workflowFn: regBackfillWf,
        schedule: cronExpr,
        context: { source: scheduleName },
      });
      // Pause so the live scheduler loop doesn't also invoke backfillWorkflow
      // and add extra entries to the shared backfillResults array.
      await DBOS.pauseSchedule(scheduleName);

      const handles = await DBOS.backfillSchedule(scheduleName, start, end);
      expect(handles.length).toBe(expectedDates.length);

      for (const h of handles) {
        await h.getResult();
      }

      expect(backfillResults.dates.length).toBe(expectedDates.length);
      const actualSorted = backfillResults.dates.map((d) => d.getTime()).sort((a, b) => a - b);
      const expectedSorted = expectedDates.map((d) => d.getTime()).sort((a, b) => a - b);
      expect(actualSorted).toEqual(expectedSorted);

      for (const ctx of backfillResults.contexts) {
        expect(ctx).toEqual({ source: scheduleName });
      }

      await DBOS.deleteSchedule(scheduleName);
    }

    // --- Every-minute schedule: backfill 6 minutes ---
    // "* * * * *" fires every minute. Window: 10:00 to 10:06 (local).
    // Expected fires at :01, :02, :03, :04, :05 (5 fires).
    const everyMinStart = localTime(2025, 1, 15, 10, 0);
    const everyMinEnd = localTime(2025, 1, 15, 10, 6);
    await testBackfill('backfill-every-min', '* * * * *', everyMinStart, everyMinEnd, [
      localTime(2025, 1, 15, 10, 1),
      localTime(2025, 1, 15, 10, 2),
      localTime(2025, 1, 15, 10, 3),
      localTime(2025, 1, 15, 10, 4),
      localTime(2025, 1, 15, 10, 5),
    ]);

    // --- Every-two-hours schedule: backfill 12 hours ---
    // "0 */2 * * *" fires at even hours: 00:00, 02:00, 04:00, ...
    // Window: 01:00 to 13:00 (local) on 2025-01-15.
    // Expected fires: 02:00, 04:00, 06:00, 08:00, 10:00, 12:00 (6 fires).
    const every2hStart = localTime(2025, 1, 15, 1, 0);
    const every2hEnd = localTime(2025, 1, 15, 13, 0);
    await testBackfill('backfill-every-2h', '0 */2 * * *', every2hStart, every2hEnd, [
      localTime(2025, 1, 15, 2, 0),
      localTime(2025, 1, 15, 4, 0),
      localTime(2025, 1, 15, 6, 0),
      localTime(2025, 1, 15, 8, 0),
      localTime(2025, 1, 15, 10, 0),
      localTime(2025, 1, 15, 12, 0),
    ]);

    // --- Every Thursday schedule: backfill ~6 weeks ---
    // "0 0 * * 4" fires at midnight every Thursday (local time).
    // Window: 2025-01-01 (Wednesday) to 2025-02-14 (Friday), exclusive end.
    // Thursdays in range: Jan 2, 9, 16, 23, 30, Feb 6, 13 (7 fires).
    const thuStart = localMidnight(2025, 1, 1);
    const thuEnd = localMidnight(2025, 2, 14);
    await testBackfill('backfill-every-thu', '0 0 * * 4', thuStart, thuEnd, [
      localMidnight(2025, 1, 2),
      localMidnight(2025, 1, 9),
      localMidnight(2025, 1, 16),
      localMidnight(2025, 1, 23),
      localMidnight(2025, 1, 30),
      localMidnight(2025, 2, 6),
      localMidnight(2025, 2, 13),
    ]);

    // Backfilling a nonexistent schedule should throw
    await expect(DBOS.backfillSchedule('nonexistent', everyMinStart, everyMinEnd)).rejects.toThrow(/not found/);
  });

  // ---------------------------------------------------------------------------
  // Backfill with timezone
  // ---------------------------------------------------------------------------
  const backfillTzUtcResults: { dates: Date[] } = { dates: [] };
  const backfillTzNyResults: { dates: Date[] } = { dates: [] };
  async function backfillTzUtcWf(scheduledDate: Date, _context: unknown) {
    backfillTzUtcResults.dates.push(scheduledDate);
    await Promise.resolve();
  }
  async function backfillTzNyWf(scheduledDate: Date, _context: unknown) {
    backfillTzNyResults.dates.push(scheduledDate);
    await Promise.resolve();
  }
  const regBackfillTzUtc = DBOS.registerWorkflow(backfillTzUtcWf, { name: 'backfillTzUtcWf' });
  const regBackfillTzNy = DBOS.registerWorkflow(backfillTzNyWf, { name: 'backfillTzNyWf' });

  test('backfill-with-timezone', async () => {
    backfillTzUtcResults.dates = [];
    backfillTzNyResults.dates = [];

    // Same cron (midnight daily), different timezones
    await DBOS.createSchedule({
      scheduleName: 'tz-utc',
      workflowFn: regBackfillTzUtc,
      schedule: '0 0 * * *',
      options: { cronTimezone: 'UTC' },
    });
    await DBOS.createSchedule({
      scheduleName: 'tz-ny',
      workflowFn: regBackfillTzNy,
      schedule: '0 0 * * *',
      options: { cronTimezone: 'America/New_York' },
    });
    // Pause both so the live scheduler doesn't interfere
    await DBOS.pauseSchedule('tz-utc');
    await DBOS.pauseSchedule('tz-ny');

    // Backfill a window that contains two UTC midnights and two NY midnights.
    // In winter, America/New_York is UTC-5.
    const start = new Date(Date.UTC(2024, 11, 31, 23, 0, 0)); // Dec 31 23:00 UTC
    const end = new Date(Date.UTC(2025, 0, 3, 0, 0, 0)); // Jan 3 00:00 UTC

    const handlesUtc = await DBOS.backfillSchedule('tz-utc', start, end);
    const handlesNy = await DBOS.backfillSchedule('tz-ny', start, end);

    for (const h of [...handlesUtc, ...handlesNy]) {
      await h.getResult();
    }

    // UTC schedule: midnight UTC on Jan 1 and Jan 2
    const utcTimes = backfillTzUtcResults.dates.map((d) => d.getTime()).sort((a, b) => a - b);
    expect(utcTimes.length).toBe(2);
    const utc0 = new Date(utcTimes[0]);
    const utc1 = new Date(utcTimes[1]);
    expect(utc0.getUTCDate()).toBe(1);
    expect(utc0.getUTCHours()).toBe(0);
    expect(utc1.getUTCDate()).toBe(2);
    expect(utc1.getUTCHours()).toBe(0);

    // NY schedule: midnight Eastern = 05:00 UTC (winter), so Jan 1 05:00 UTC and Jan 2 05:00 UTC
    const nyTimes = backfillTzNyResults.dates.map((d) => d.getTime()).sort((a, b) => a - b);
    expect(nyTimes.length).toBe(2);
    const ny0 = new Date(nyTimes[0]);
    const ny1 = new Date(nyTimes[1]);
    expect(ny0.getUTCHours()).toBe(5);
    expect(ny0.getUTCMinutes()).toBe(0);
    expect(ny1.getUTCHours()).toBe(5);
    expect(ny1.getUTCMinutes()).toBe(0);

    // The NY fires should be different instants from the UTC fires
    expect(nyTimes[0]).not.toBe(utcTimes[0]);
    expect(nyTimes[1]).not.toBe(utcTimes[1]);

    await DBOS.deleteSchedule('tz-utc');
    await DBOS.deleteSchedule('tz-ny');
  });

  // ---------------------------------------------------------------------------
  // Automatic backfill on restart
  // ---------------------------------------------------------------------------
  const autoBackfillResults: { dates: Date[] } = { dates: [] };
  async function autoBackfillWf(scheduledDate: Date, _context: unknown) {
    autoBackfillResults.dates.push(scheduledDate);
    await Promise.resolve();
  }
  const regAutoBackfillWf = DBOS.registerWorkflow(autoBackfillWf, { name: 'autoBackfillWf' });

  test('automatic-backfill-on-restart', async () => {
    autoBackfillResults.dates = [];

    // Create an hourly schedule with automatic_backfill enabled (won't fire naturally during test)
    await DBOS.createSchedule({
      scheduleName: 'backfill-restart',
      workflowFn: regAutoBackfillWf,
      schedule: '0 * * * *',
      options: { automaticBackfill: true },
    });

    // Set lastFiredAt to 3 hours ago, simulating that the scheduler was down
    const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000);
    threeHoursAgo.setMinutes(0, 0, 0);
    await DBOSExecutor.globalInstance!.systemDatabase.updateLastFiredAt(
      'backfill-restart',
      threeHoursAgo.toISOString(),
    );

    // Verify the schedule metadata reflects our changes
    const sched = await DBOS.getSchedule('backfill-restart');
    expect(sched).not.toBeNull();
    expect(sched!.automaticBackfill).toBe(true);
    expect(sched!.lastFiredAt).not.toBeNull();
    expect(new Date(sched!.lastFiredAt!).getTime()).toBe(threeHoursAgo.getTime());

    // Shutdown and relaunch — the scheduler should backfill on startup
    await DBOS.shutdown();
    await DBOS.launch();

    // Wait for the scheduler polling loop to pick up the schedule and backfill
    await retryUntilSuccess(() => {
      expect(autoBackfillResults.dates.length).toBeGreaterThanOrEqual(2);
    });

    // Verify the backfilled times are hourly slots between lastFiredAt and now
    const firedTimes = autoBackfillResults.dates.map((d) => d.getTime()).sort((a, b) => a - b);
    for (const t of firedTimes) {
      expect(t).toBeGreaterThan(threeHoursAgo.getTime());
      expect(t).toBeLessThanOrEqual(Date.now());
      // Hourly cron fires at minute 0
      expect(new Date(t).getUTCMinutes()).toBe(0);
    }

    await DBOS.deleteSchedule('backfill-restart');
  });

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
  });

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
  });

  // ---------------------------------------------------------------------------
  // Client schedule tests
  // ---------------------------------------------------------------------------

  test('test_client_schedule_crud', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await client.createSchedule({
        scheduleName: 'client-test',
        workflowName: 'myWorkflow',
        schedule: '* * * * *',
        context: { env: 'client' },
      });

      const schedules = await client.listSchedules();
      expect(schedules.length).toBe(1);
      expect(schedules[0].scheduleName).toBe('client-test');
      expect(schedules[0].workflowName).toBe('myWorkflow');
      expect(schedules[0].schedule).toBe('* * * * *');
      expect(schedules[0].context).toEqual({ env: 'client' });
      // Verify defaults for new fields
      expect(schedules[0].lastFiredAt).toBeNull();
      expect(schedules[0].automaticBackfill).toBe(false);
      expect(schedules[0].cronTimezone).toBeNull();

      const sched = await client.getSchedule('client-test');
      expect(sched).not.toBeNull();
      expect(sched!.scheduleName).toBe('client-test');
      expect(sched!.context).toEqual({ env: 'client' });

      expect(await client.getSchedule('nonexistent')).toBeNull();

      // Reject duplicate
      await expect(
        client.createSchedule({
          scheduleName: 'client-test',
          workflowName: 'myWorkflow',
          schedule: '0 0 * * *',
        }),
      ).rejects.toThrow(/already exists/);

      // Reject invalid cron
      await expect(
        client.createSchedule({
          scheduleName: 'bad',
          workflowName: 'myWorkflow',
          schedule: 'not a cron',
        }),
      ).rejects.toThrow();

      // Create with explicit schedule options
      await client.createSchedule({
        scheduleName: 'client-tz',
        workflowName: 'myWorkflow',
        schedule: '0 0 * * *',
        options: { cronTimezone: 'Europe/London', automaticBackfill: true },
      });
      const tzSched = await client.getSchedule('client-tz');
      expect(tzSched).not.toBeNull();
      expect(tzSched!.cronTimezone).toBe('Europe/London');
      expect(tzSched!.automaticBackfill).toBe(true);
      expect(tzSched!.lastFiredAt).toBeNull();

      // Reject invalid timezone
      await expect(
        client.createSchedule({
          scheduleName: 'bad-tz',
          workflowName: 'myWorkflow',
          schedule: '0 0 * * *',
          options: { cronTimezone: 'Not/A/Timezone' },
        }),
      ).rejects.toThrow(/Invalid timezone/);

      // List with filters
      await client.createSchedule({
        scheduleName: 'client-other',
        workflowName: 'otherWorkflow',
        schedule: '0 0 * * *',
      });
      expect((await client.listSchedules({ status: 'ACTIVE' })).length).toBe(3);
      expect((await client.listSchedules({ workflowName: 'myWorkflow' })).length).toBe(2);
      expect((await client.listSchedules({ scheduleNamePrefix: 'client-t' })).length).toBe(2);

      // Delete
      await client.deleteSchedule('client-test');
      await client.deleteSchedule('client-tz');
      await client.deleteSchedule('client-other');
      expect((await client.listSchedules()).length).toBe(0);
    } finally {
      await client.destroy();
    }
  });

  test('test_client_apply_schedules', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await client.applySchedules([
        { scheduleName: 'csched-a', workflowName: 'myWorkflow', schedule: '* * * * *', context: { region: 'us' } },
        {
          scheduleName: 'csched-b',
          workflowName: 'otherWorkflow',
          schedule: '0 0 * * *',
          automaticBackfill: true,
          cronTimezone: 'Europe/Rome',
        },
      ]);
      let schedules = await client.listSchedules();
      expect(schedules.length).toBe(2);
      let byName = Object.fromEntries(schedules.map((s) => [s.scheduleName, s]));
      expect(byName['csched-a'].automaticBackfill).toBe(false);
      expect(byName['csched-a'].cronTimezone).toBeNull();
      expect(byName['csched-b'].automaticBackfill).toBe(true);
      expect(byName['csched-b'].cronTimezone).toBe('Europe/Rome');

      // Replace csched-a, add csched-c
      await client.applySchedules([
        { scheduleName: 'csched-a', workflowName: 'myWorkflow', schedule: '0 * * * *', context: null },
        { scheduleName: 'csched-c', workflowName: 'otherWorkflow', schedule: '*/5 * * * *', context: [1, 2] },
      ]);
      schedules = await client.listSchedules();
      expect(schedules.length).toBe(3);
      byName = Object.fromEntries(schedules.map((s) => [s.scheduleName, s]));
      expect(byName['csched-a'].schedule).toBe('0 * * * *');
      expect(byName['csched-a'].context).toBeNull();
      expect(byName['csched-c'].context).toEqual([1, 2]);

      // Reject invalid cron
      await expect(
        client.applySchedules([{ scheduleName: 'bad', workflowName: 'myWorkflow', schedule: 'not a cron' }]),
      ).rejects.toThrow();

      // Clean up
      await client.deleteSchedule('csched-a');
      await client.deleteSchedule('csched-b');
      await client.deleteSchedule('csched-c');
    } finally {
      await client.destroy();
    }
  });

  // --- Client trigger/backfill workflows ---
  const clientTriggerResults: { dates: Date[]; contexts: unknown[] } = { dates: [], contexts: [] };
  async function clientTriggerWorkflow(scheduledDate: Date, context: unknown) {
    clientTriggerResults.dates.push(scheduledDate);
    clientTriggerResults.contexts.push(context);
    await Promise.resolve();
  }
  DBOS.registerWorkflow(clientTriggerWorkflow, { name: 'clientTriggerWorkflow' });

  const clientBackfillResults: { dates: Date[]; contexts: unknown[] } = { dates: [], contexts: [] };
  async function clientBackfillWorkflow(scheduledDate: Date, context: unknown) {
    clientBackfillResults.dates.push(scheduledDate);
    clientBackfillResults.contexts.push(context);
    await Promise.resolve();
  }
  DBOS.registerWorkflow(clientBackfillWorkflow, { name: 'clientBackfillWorkflow' });

  test('test_client_trigger_schedule', async () => {
    clientTriggerResults.dates = [];
    clientTriggerResults.contexts = [];

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await client.createSchedule({
        scheduleName: 'client-trigger',
        workflowName: 'clientTriggerWorkflow',
        schedule: '0 0 * * *',
        context: { source: 'client-trigger' },
      });

      const before = new Date();
      const handle = await client.triggerSchedule('client-trigger');
      await handle.getResult();

      expect(clientTriggerResults.dates.length).toBe(1);
      expect(clientTriggerResults.dates[0].getTime()).toBeGreaterThanOrEqual(before.getTime() - 1000);
      expect(clientTriggerResults.contexts[0]).toEqual({ source: 'client-trigger' });

      // Trigger again
      const handle2 = await client.triggerSchedule('client-trigger');
      await handle2.getResult();
      expect(clientTriggerResults.dates.length).toBe(2);

      // Nonexistent schedule
      await expect(client.triggerSchedule('nonexistent')).rejects.toThrow(/not found/);

      await client.deleteSchedule('client-trigger');
    } finally {
      await client.destroy();
    }
  });

  test('test_client_backfill_schedule', async () => {
    clientBackfillResults.dates = [];
    clientBackfillResults.contexts = [];

    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await client.createSchedule({
        scheduleName: 'client-backfill',
        workflowName: 'clientBackfillWorkflow',
        schedule: '* * * * *',
        context: { source: 'client-backfill' },
      });

      const end = new Date();
      end.setMilliseconds(0);
      const start = new Date(end.getTime() - 5 * 60 * 1000);

      const handles = await client.backfillSchedule('client-backfill', start, end);
      expect(handles.length).toBeGreaterThanOrEqual(4);
      expect(handles.length).toBeLessThanOrEqual(5);

      for (const h of handles) {
        await h.getResult();
      }

      expect(clientBackfillResults.dates.length).toBe(handles.length);
      for (const ctx of clientBackfillResults.contexts) {
        expect(ctx).toEqual({ source: 'client-backfill' });
      }
      for (const d of clientBackfillResults.dates) {
        expect(d.getTime()).toBeGreaterThanOrEqual(start.getTime());
        expect(d.getTime()).toBeLessThan(end.getTime());
      }

      await expect(client.backfillSchedule('nonexistent', start, end)).rejects.toThrow(/not found/);

      await client.deleteSchedule('client-backfill');
    } finally {
      await client.destroy();
    }
  });

  test('test_client_pause_resume_schedule', async () => {
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await client.createSchedule({
        scheduleName: 'client-pause',
        workflowName: 'myWorkflow',
        schedule: '* * * * *',
      });

      const sched = await client.getSchedule('client-pause');
      expect(sched!.status).toBe('ACTIVE');

      await client.pauseSchedule('client-pause');
      const paused = await client.getSchedule('client-pause');
      expect(paused!.status).toBe('PAUSED');

      await client.resumeSchedule('client-pause');
      const resumed = await client.getSchedule('client-pause');
      expect(resumed!.status).toBe('ACTIVE');

      await client.deleteSchedule('client-pause');
    } finally {
      await client.destroy();
    }
  });

  // ---------------------------------------------------------------------------
  // schedule-with-queue-name
  // ---------------------------------------------------------------------------

  const _schedulerTestQueue = new WorkflowQueue('scheduler-test-queue');
  const queuedReceived: unknown[] = [];
  async function queuedWorkflow(_scheduledDate: Date, context: unknown) {
    const status = await DBOS.getWorkflowStatus(DBOS.workflowID!);
    expect(status).toBeDefined();
    expect(status!.queueName).toBe('scheduler-test-queue');
    queuedReceived.push(context);
  }
  const regQueuedWf = DBOS.registerWorkflow(queuedWorkflow, { name: 'queuedWorkflow' });

  test('schedule-with-queue-name', async () => {
    queuedReceived.length = 0;

    // Create a schedule with a valid queue name
    await DBOS.createSchedule({
      scheduleName: 'queued-schedule',
      workflowFn: regQueuedWf,
      schedule: '* * * * * *',
      context: { queued: true },
      options: { queueName: 'scheduler-test-queue' },
    });

    // Verify queue_name is stored via get and list
    const sched = await DBOS.getSchedule('queued-schedule');
    expect(sched).not.toBeNull();
    expect(sched!.queueName).toBe('scheduler-test-queue');
    const schedules = await DBOS.listSchedules();
    expect(schedules.length).toBe(1);
    expect(schedules[0].queueName).toBe('scheduler-test-queue');

    // Verify the schedule fires and workflows land on the specified queue
    await retryUntilSuccess(() => {
      expect(queuedReceived.length).toBeGreaterThanOrEqual(2);
      expect(queuedReceived.every((c) => (c as { queued: boolean }).queued === true)).toBe(true);
    });

    // Trigger also uses the queue
    const countBefore = queuedReceived.length;
    const handle = await DBOS.triggerSchedule('queued-schedule');
    await handle.getResult();
    expect(queuedReceived.length).toBeGreaterThan(countBefore);

    await DBOS.deleteSchedule('queued-schedule');

    // Schedule without queue_name should have null
    await DBOS.createSchedule({
      scheduleName: 'no-queue-schedule',
      workflowFn: regQueuedWf,
      schedule: '0 0 * * *',
    });
    const noQueueSched = await DBOS.getSchedule('no-queue-schedule');
    expect(noQueueSched).not.toBeNull();
    expect(noQueueSched!.queueName).toBeNull();
    await DBOS.deleteSchedule('no-queue-schedule');
  });
});
