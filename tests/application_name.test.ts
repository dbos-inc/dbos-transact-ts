import { DBOS, DBOSClient, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { globalTimeout } from '../src/workflow_management';
import {
  connectToDBOSTestSystemDatabase,
  DBOSTestSystemDatabaseClient,
  generateDBOSTestConfig,
  setUpDBOSTestSysDb,
} from './helpers';
import { globalParams } from '../src/utils';
import type { GetWorkflowsInput } from '../src/workflow';

type NoArgWorkflow = () => Promise<unknown>;

const APP = 'appname-test-app';
const PEER = 'appname-test-peer';

async function ownerOf(
  client: DBOSTestSystemDatabaseClient,
  table: string,
  keyColumn: string,
  key: string,
): Promise<string | null> {
  const { rows } = await client.query<{ application_name: string | null }>(
    `SELECT application_name FROM dbos.${table} WHERE ${keyColumn} = $1`,
    [key],
  );
  return rows[0]?.application_name ?? null;
}

/** Insert a step row as though a peer application had recorded it. */
async function insertPeerStep(
  client: DBOSTestSystemDatabaseClient,
  workflowID: string,
  functionName: string,
  options: { applicationName?: string | null } = {},
): Promise<void> {
  const owner = 'applicationName' in options ? options.applicationName : PEER;
  await client.query(
    `INSERT INTO dbos.operation_outputs
       (workflow_uuid, function_id, function_name, output, serialization, completed_at_epoch_ms, application_name)
     VALUES ($1, 0, $2, '1', 'portable_json', $3, $4)`,
    [workflowID, functionName, Date.now(), owner],
  );
}

/** Insert a workflow row as though a peer application had enqueued it. */
async function insertPeerWorkflow(
  client: DBOSTestSystemDatabaseClient,
  id: string,
  options: { queueName?: string; status?: string; applicationName?: string | null; createdAt?: number } = {},
): Promise<void> {
  const now = options.createdAt ?? Date.now();
  // An explicit null means unclaimed, which `??` would otherwise swallow.
  const owner = 'applicationName' in options ? options.applicationName : PEER;
  await client.query(
    `INSERT INTO dbos.workflow_status
       (workflow_uuid, status, name, queue_name, executor_id, application_id, created_at, updated_at,
        recovery_attempts, priority, inputs, serialization, application_name)
     VALUES ($1, $2, 'peerWorkflow', $3, 'local', '', $4, $4, 0, 0, '{"positionalArgs":[]}', 'portable_json', $5)`,
    [id, options.status ?? StatusString.ENQUEUED, options.queueName ?? null, now, owner],
  );
}

describe('application-name', () => {
  let config: DBOSConfig;
  let client: DBOSTestSystemDatabaseClient;

  beforeEach(async () => {
    config = generateDBOSTestConfig();
    config.name = APP;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    client = await connectToDBOSTestSystemDatabase(config);
  });

  afterEach(async () => {
    await DBOS.shutdown();
    await client.end();
  });

  test('runtime stamps everything it writes with its application name', async () => {
    class StampTest {
      @DBOS.step()
      static async aStep(): Promise<number> {
        return Promise.resolve(1);
      }

      @DBOS.workflow()
      static async aWorkflow(): Promise<number> {
        return await StampTest.aStep();
      }
    }

    await DBOS.launch();
    expect(globalParams.appName).toBe(APP);

    const handle = await DBOS.startWorkflow(StampTest).aWorkflow();
    await expect(handle.getResult()).resolves.toBe(1);

    expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', handle.workflowID)).toBe(APP);

    // Steps carry the owner too, denormalized so step observability filters without a join.
    const { rows: steps } = await client.query<{ application_name: string | null }>(
      `SELECT application_name FROM dbos.operation_outputs WHERE workflow_uuid = $1`,
      [handle.workflowID],
    );
    expect(steps.length).toBeGreaterThan(0);
    for (const step of steps) {
      expect(step.application_name).toBe(APP);
    }

    // The application version this launch registered is owned as well.
    expect(await ownerOf(client, 'application_versions', 'version_name', DBOS.applicationVersion)).toBe(APP);
  });

  test('shutdown clears the application identity', async () => {
    await DBOS.launch();
    expect(globalParams.appName).toBe(APP);
    await DBOS.shutdown();
    expect(globalParams.appName).toBeUndefined();
  });

  test('a client without an identity writes unclaimed rows, and an explicit name wins', async () => {
    await DBOS.launch();
    const queue = await DBOS.registerQueue('appname-client-queue');
    expect(queue.name).toBe('appname-client-queue');

    const anonymous = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const handle = await anonymous.enqueue<NoArgWorkflow>({
        queueName: 'appname-client-queue',
        workflowName: 'someWorkflow',
      });
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', handle.workflowID)).toBeNull();
    } finally {
      await anonymous.destroy();
    }

    const named = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      applicationName: PEER,
    });
    try {
      expect(named.applicationName).toBe(PEER);
      const own = await named.enqueue<NoArgWorkflow>({
        queueName: 'appname-client-queue',
        workflowName: 'someWorkflow',
      });
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', own.workflowID)).toBe(PEER);

      // An explicit applicationName beats the client's own identity.
      const targeted = await named.enqueue<NoArgWorkflow>({
        queueName: 'appname-client-queue',
        workflowName: 'someWorkflow',
        applicationName: 'appname-third',
      });
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', targeted.workflowID)).toBe('appname-third');
    } finally {
      await named.destroy();
    }
  });

  test('reads surface the owner and IDs stay global', async () => {
    class ReadTest {
      @DBOS.workflow()
      static async aWorkflow(): Promise<string> {
        return Promise.resolve('ok');
      }
    }

    await DBOS.launch();
    const handle = await DBOS.startWorkflow(ReadTest).aWorkflow();
    await handle.getResult();

    const status = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(status?.applicationName).toBe(APP);

    // A peer's workflow keeps its own ID space: it is retrievable by ID from here.
    await insertPeerWorkflow(client, 'appname-peer-wf');
    const peerStatus = await DBOS.getWorkflowStatus('appname-peer-wf');
    expect(peerStatus?.applicationName).toBe(PEER);

    // Every public identity read reaches a peer's row too: these go through
    // listWorkflows, whose unset filter otherwise scopes to this application.
    const byID = await DBOS.listWorkflows({ workflowIDs: ['appname-peer-wf'] });
    expect(byID.map((w) => w.workflowID)).toEqual(['appname-peer-wf']);
    expect(byID[0].applicationName).toBe(PEER);
    expect(await DBOS.retrieveWorkflow('appname-peer-wf').getStatus()).toMatchObject({
      applicationName: PEER,
    });

    // An explicit filter still narrows, even for an ID-keyed read.
    expect(await DBOS.listWorkflows({ workflowIDs: ['appname-peer-wf'], applicationName: APP })).toEqual([]);

    // A Conductor request carries an omitted filter as JSON null, which the types do not
    // admit but the wire does: it must read as absent, not as an ID-keyed read.
    const fromTheWire = JSON.parse('{"workflowIDs": null, "applicationName": null}') as GetWorkflowsInput;
    const unfiltered = await DBOS.listWorkflows(fromTheWire);
    expect(unfiltered.map((w) => w.workflowID)).toContain(handle.workflowID);
    expect(unfiltered.map((w) => w.workflowID)).not.toContain('appname-peer-wf');
  });

  test('observability filters scope to this application and include unclaimed rows', async () => {
    class FilterTest {
      @DBOS.step()
      static async aStep(): Promise<number> {
        return Promise.resolve(11);
      }

      @DBOS.workflow()
      static async aWorkflow(): Promise<number> {
        return await FilterTest.aStep();
      }
    }

    await DBOS.launch();
    const mine = await DBOS.startWorkflow(FilterTest).aWorkflow();
    await mine.getResult();

    await insertPeerWorkflow(client, 'appname-filter-peer', { status: StatusString.SUCCESS });
    await insertPeerStep(client, 'appname-filter-peer', 'theirStep');
    await insertPeerWorkflow(client, 'appname-filter-unclaimed', {
      status: StatusString.SUCCESS,
      applicationName: null,
    });

    // Unset is this application's scope, so naming it changes nothing.
    const all = await DBOS.listWorkflows({});
    const allIDs = all.map((w) => w.workflowID);
    expect(allIDs).toContain(mine.workflowID);
    expect(allIDs).toContain('appname-filter-unclaimed');
    expect(allIDs).not.toContain('appname-filter-peer');

    const ours = await DBOS.listWorkflows({ applicationName: APP });
    const ourIDs = ours.map((w) => w.workflowID);
    expect(ourIDs).toContain(mine.workflowID);
    expect(ourIDs).toContain('appname-filter-unclaimed');
    expect(ourIDs).not.toContain('appname-filter-peer');

    // A peer's rows are reachable by naming it, alongside the unclaimed ones.
    const theirs = await DBOS.listWorkflows({ applicationName: PEER });
    const theirIDs = theirs.map((w) => w.workflowID);
    expect(theirIDs).toContain('appname-filter-peer');
    expect(theirIDs).toContain('appname-filter-unclaimed');
    expect(theirIDs).not.toContain(mine.workflowID);

    // A client with no application of its own has no scope to default to.
    const anon = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      const anonIDs = (await anon.listWorkflows({})).map((w) => w.workflowID);
      expect(anonIDs).toEqual(
        expect.arrayContaining([mine.workflowID, 'appname-filter-peer', 'appname-filter-unclaimed']),
      );
    } finally {
      await anon.destroy();
    }

    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    const aggregates = await sysdb.getWorkflowAggregates({
      groupByName: true,
      selectCount: true,
      applicationName: [PEER],
    });
    expect(aggregates.map((r) => r.group.name)).toEqual(['peerWorkflow']);

    // Grouping partitions where the filter deliberately overlaps.
    const grouped = await sysdb.getWorkflowAggregates({
      groupByApplicationName: true,
      selectCount: true,
      applicationName: [APP, PEER],
    });
    expect(new Set(grouped.map((r) => r.group.application_name))).toEqual(new Set([APP, PEER, null]));

    // Unset would have dropped the peer's group entirely.
    const ungrouped = await sysdb.getWorkflowAggregates({ groupByApplicationName: true, selectCount: true });
    expect(new Set(ungrouped.map((r) => r.group.application_name))).toEqual(new Set([APP, null]));

    const steps = await sysdb.getStepAggregates({
      groupByFunctionName: true,
      selectCount: true,
      applicationName: [PEER],
    });
    expect(steps.map((r) => r.group.function_name)).toEqual(['theirStep']);

    const windowStart = new Date(0).toISOString();
    const windowEnd = new Date(Date.now() + 3600_000).toISOString();
    const metrics = await sysdb.getMetrics(windowStart, windowEnd, [PEER]);
    const stepNames = metrics.filter((m) => m.metricType === 'step_count').map((m) => m.metricName);
    expect(new Set(stepNames)).toEqual(new Set(['theirStep']));
  });

  test('unclaimed schedules and queues belong to every application', async () => {
    class ScheduleTest {
      @DBOS.workflow()
      static async scheduled(_scheduledAt: Date, _startedAt: Date): Promise<void> {
        return Promise.resolve();
      }
    }

    await DBOS.launch();
    const sysdb = DBOSExecutor.globalInstance!.systemDatabase;
    await DBOS.createSchedule({
      scheduleName: 'appname-mine',
      workflowFn: ScheduleTest.scheduled,
      schedule: '0 0 1 1 *',
    });
    await DBOS.registerQueue('appname-mine-queue');

    for (const [scheduleID, name, owner] of [
      ['appname-peer-schedule-id', 'appname-theirs', PEER],
      ['appname-legacy-schedule-id', 'appname-unclaimed', null],
    ] as const) {
      await client.query(
        `INSERT INTO dbos.workflow_schedules (schedule_id, schedule_name, workflow_name, schedule, status, context, application_name)
         VALUES ($1, $2, 'scheduled', '0 0 1 1 *', 'ACTIVE', 'null', $3)`,
        [scheduleID, name, owner],
      );
    }
    for (const [name, owner] of [
      ['appname-theirs-queue', PEER],
      ['appname-unclaimed-queue', null],
    ] as const) {
      await client.query(`INSERT INTO dbos.queues (name, application_name) VALUES ($1, $2)`, [name, owner]);
    }

    const scheduleNames = async (applicationName?: string) =>
      new Set((await DBOS.listSchedules(applicationName ? { applicationName } : undefined)).map((s) => s.scheduleName));
    const queueNames = async (applicationName?: string) =>
      new Set((await DBOS.listQueues(applicationName)).map((q) => q.name));

    // Unset is this application's scope: its own rows plus unclaimed, never a peer's.
    expect(await scheduleNames()).toEqual(new Set(['appname-mine', 'appname-unclaimed']));
    expect(await queueNames()).toEqual(new Set(['appname-mine-queue', 'appname-unclaimed-queue']));

    // Naming an application adds the unclaimed rows, as the unset scope itself does.
    expect(await scheduleNames(APP)).toEqual(new Set(['appname-mine', 'appname-unclaimed']));
    expect(await queueNames(APP)).toEqual(new Set(['appname-mine-queue', 'appname-unclaimed-queue']));
    expect(await scheduleNames(PEER)).toEqual(new Set(['appname-theirs', 'appname-unclaimed']));

    // The listing is attributable: a queue carries its owner.
    const theirs = await DBOS.listQueues(PEER);
    expect(new Set(theirs.map((q) => q.name))).toEqual(new Set(['appname-theirs-queue', 'appname-unclaimed-queue']));
    expect(new Set(theirs.map((q) => q.applicationName ?? null))).toEqual(new Set([PEER, null]));

    // The client runs the same query, so it takes the same filter.
    const peerClient = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      expect(new Set((await peerClient.listQueues(PEER)).map((q) => q.name))).toEqual(
        new Set(['appname-theirs-queue', 'appname-unclaimed-queue']),
      );
    } finally {
      await peerClient.destroy();
    }

    // Name-addressed lookups stay global: a globally unique name is an identity.
    expect(await sysdb.getQueue('appname-theirs-queue')).not.toBeNull();

    // Re-registering an unclaimed row claims it, without recreating it.
    await DBOS.registerQueue('appname-unclaimed-queue');
    await DBOS.applySchedules([
      { scheduleName: 'appname-unclaimed', workflowFn: ScheduleTest.scheduled, schedule: '0 0 1 1 *' },
    ]);
    expect(await ownerOf(client, 'queues', 'name', 'appname-unclaimed-queue')).toBe(APP);
    expect(await ownerOf(client, 'workflow_schedules', 'schedule_name', 'appname-unclaimed')).toBe(APP);
    const { rows: scheduleRows } = await client.query<{ schedule_id: string }>(
      `SELECT schedule_id FROM dbos.workflow_schedules WHERE schedule_name = 'appname-unclaimed'`,
    );
    expect(scheduleRows.map((r) => r.schedule_id)).toEqual(['appname-legacy-schedule-id']);
  });

  test('dequeue skips another application and claims unclaimed workflows', async () => {
    let ran = 0;

    class DequeueTest {
      @DBOS.workflow()
      static async aWorkflow(): Promise<number> {
        ran += 1;
        return Promise.resolve(ran);
      }
    }

    await DBOS.launch();
    const queue = await DBOS.registerQueue('appname-dequeue-queue');

    // A peer's enqueued workflow on our queue must never be dequeued here.
    await insertPeerWorkflow(client, 'appname-dequeue-peer', { queueName: queue.name });

    // An unclaimed workflow belongs to every application, so this one claims it.
    const anonymous = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    let unclaimedID: string;
    try {
      const handle = await anonymous.enqueue<NoArgWorkflow>({
        queueName: queue.name,
        workflowName: 'aWorkflow',
        workflowClassName: DequeueTest.name,
      });
      unclaimedID = handle.workflowID;
    } finally {
      await anonymous.destroy();
    }

    await DBOS.retrieveWorkflow(unclaimedID).getResult();
    expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', unclaimedID)).toBe(APP);

    // The peer's row is untouched: still ENQUEUED, still theirs.
    const { rows } = await client.query<{ status: string; application_name: string }>(
      `SELECT status, application_name FROM dbos.workflow_status WHERE workflow_uuid = 'appname-dequeue-peer'`,
    );
    expect(rows[0].status).toBe(StatusString.ENQUEUED);
    expect(rows[0].application_name).toBe(PEER);
  });

  test('bulk operations spare another application', async () => {
    class BulkTest {
      @DBOS.workflow()
      static async aWorkflow(): Promise<string> {
        return Promise.resolve('ok');
      }
    }

    await DBOS.launch();
    const mine = await DBOS.startWorkflow(BulkTest).aWorkflow();
    await mine.getResult();

    const old = Date.now() - 1_000_000;
    await insertPeerWorkflow(client, 'appname-gc-peer', { status: StatusString.SUCCESS, createdAt: old });
    await insertPeerWorkflow(client, 'appname-gc-peer-pending', { status: StatusString.PENDING, createdAt: old });

    // Garbage collection deletes only what this application owns, plus unclaimed rows.
    await DBOSExecutor.globalInstance!.systemDatabase.garbageCollect(Date.now(), undefined);
    expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', 'appname-gc-peer')).toBe(PEER);

    // A global timeout leaves the peer's in-flight workflow alone.
    await globalTimeout(DBOSExecutor.globalInstance!.systemDatabase, Date.now());
    const { rows } = await client.query<{ status: string }>(
      `SELECT status FROM dbos.workflow_status WHERE workflow_uuid = 'appname-gc-peer-pending'`,
    );
    expect(rows[0].status).toBe(StatusString.PENDING);
  });

  test('versions are per application', async () => {
    await DBOS.launch();
    const ourVersion = DBOS.applicationVersion;

    // A peer registers a newer version; it must not become our latest.
    const peer = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      applicationName: PEER,
    });
    try {
      await client.query(
        `INSERT INTO dbos.application_versions (version_id, version_name, version_timestamp, application_name)
         VALUES ($1, 'peer-version', $2, $3)`,
        ['peer-version-id', Date.now() + 100_000, PEER],
      );

      const latest = await DBOS.getLatestApplicationVersion();
      expect(latest.versionName).toBe(ourVersion);
      expect(latest.applicationName).toBe(APP);

      const peerLatest = await peer.getLatestApplicationVersion();
      expect(peerLatest.versionName).toBe('peer-version');

      // listApplicationVersions is scoped to this application plus unclaimed versions.
      const ours = await DBOS.listApplicationVersions();
      expect(ours.map((v) => v.versionName)).not.toContain('peer-version');
    } finally {
      await peer.destroy();
    }
  });

  test('conflicting names across applications raise', async () => {
    await DBOS.launch();
    await DBOS.registerQueue('appname-contested-queue');
    await DBOS.createSchedule({
      scheduleName: 'appname-contested-schedule',
      workflowFn: ScheduleHolder.scheduled,
      schedule: '0 0 * * * *',
    });

    const peer = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      applicationName: PEER,
    });
    try {
      await expect(peer.registerQueue('appname-contested-queue')).rejects.toThrow(
        /already registered by application 'appname-test-app'/,
      );
      await expect(
        peer.createSchedule({
          scheduleName: 'appname-contested-schedule',
          workflowName: 'scheduled',
          schedule: '0 0 * * * *',
        }),
      ).rejects.toThrow(/already registered by application 'appname-test-app'/);
      await expect(peer.setLatestApplicationVersion(DBOS.applicationVersion)).rejects.toThrow(
        /already registered by application 'appname-test-app'/,
      );
    } finally {
      await peer.destroy();
    }
  });

  test('rename moves every owned table and validates its arguments', async () => {
    class RenameTest {
      @DBOS.workflow()
      static async aWorkflow(): Promise<string> {
        return Promise.resolve('ok');
      }
    }

    await DBOS.launch();
    await DBOS.registerQueue('appname-rename-queue');
    const handle = await DBOS.startWorkflow(RenameTest).aWorkflow();
    await handle.getResult();
    const version = DBOS.applicationVersion;
    await DBOS.shutdown();

    const admin = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await expect(admin.renameApplication(APP, APP)).rejects.toThrow(/already holds that name/);
      await expect(admin.renameApplication(undefined, 'renamed-app')).rejects.toThrow(/Nothing to re-own/);

      // A non-integer batch size (including NaN) is rejected before the first transaction commits, so nothing moves.
      await expect(admin.renameApplication(APP, 'renamed-app', { batchSize: Number('ten') })).rejects.toThrow(
        /batchSize must be a positive integer/,
      );
      await expect(admin.renameApplication(APP, 'renamed-app', { batchSize: 2.5 })).rejects.toThrow(
        /batchSize must be a positive integer/,
      );
      expect(await ownerOf(client, 'queues', 'name', 'appname-rename-queue')).toBe(APP);

      const moved = await admin.renameApplication(APP, 'renamed-app');
      expect(moved.queues).toBeGreaterThanOrEqual(1);
      expect(moved.versions).toBeGreaterThanOrEqual(1);
      expect(moved.workflows).toBeGreaterThanOrEqual(1);
      expect(moved.steps).toBeGreaterThanOrEqual(0);

      expect(await ownerOf(client, 'queues', 'name', 'appname-rename-queue')).toBe('renamed-app');
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', handle.workflowID)).toBe('renamed-app');
      expect(await ownerOf(client, 'application_versions', 'version_name', version)).toBe('renamed-app');
    } finally {
      await admin.destroy();
    }
  });

  test('rename moves only the sources it is given', async () => {
    await DBOS.launch();
    await DBOS.shutdown();

    await insertPeerWorkflow(client, 'appname-rename-peer');
    await insertPeerWorkflow(client, 'appname-rename-unclaimed', { applicationName: null });

    const admin = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      // Unclaimed rows are not implied; they move only when asked.
      await admin.renameApplication(APP, 'renamed-app');
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', 'appname-rename-unclaimed')).toBeNull();
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', 'appname-rename-peer')).toBe(PEER);

      // Adopting unclaimed rows without naming a previous application moves just those.
      await admin.renameApplication(undefined, 'renamed-app', { adoptUnclaimedRows: true });
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', 'appname-rename-unclaimed')).toBe('renamed-app');
      expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', 'appname-rename-peer')).toBe(PEER);
    } finally {
      await admin.destroy();
    }
  });

  test('rename batches terminal rows and resumes', async () => {
    await DBOS.launch();
    await DBOS.shutdown();

    for (let i = 0; i < 5; i++) {
      await insertPeerWorkflow(client, `appname-batch-${i}`, {
        status: StatusString.SUCCESS,
        applicationName: APP,
      });
    }

    const admin = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      // A batch size below the row count exercises the key-range walk.
      const moved = await admin.renameApplication(APP, 'renamed-app', { batchSize: 2 });
      expect(moved.workflows).toBeGreaterThanOrEqual(5);
      for (let i = 0; i < 5; i++) {
        expect(await ownerOf(client, 'workflow_status', 'workflow_uuid', `appname-batch-${i}`)).toBe('renamed-app');
      }
      // A re-run is a no-op, not a double count.
      const again = await admin.renameApplication(APP, 'renamed-app', { batchSize: 2 });
      expect(again.workflows).toBe(0);
    } finally {
      await admin.destroy();
    }
  });
});

class ScheduleHolder {
  @DBOS.workflow()
  static async scheduled(_date: Date, _ctx: unknown): Promise<void> {
    return Promise.resolve();
  }
}
