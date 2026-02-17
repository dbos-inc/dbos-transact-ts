import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import { generateDBOSTestConfig, setUpDBOSTestSysDb, dropDatabase } from './helpers';

describe('dynamic-scheduler-tests', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
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

  async function myWorkflow(_scheduledAt: string, _ctx: unknown) {}
  async function otherWorkflow(_scheduledAt: string, _ctx: unknown) {}

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

  async function targetWorkflow(_scheduledAt: string, _ctx: unknown) {}
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
});
