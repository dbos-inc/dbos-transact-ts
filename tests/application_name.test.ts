import { Client } from 'pg';
import { DBOS, DBOSClient, StatusString } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import { globalTimeout } from '../src/workflow_management';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';
import { globalParams } from '../src/utils';

type NoArgWorkflow = () => Promise<unknown>;

const APP = 'appname-test-app';
const PEER = 'appname-test-peer';

async function sysdbClient(config: DBOSConfig): Promise<Client> {
  const client = new Client({ connectionString: config.systemDatabaseUrl });
  await client.connect();
  return client;
}

async function ownerOf(client: Client, table: string, keyColumn: string, key: string): Promise<string | null> {
  const { rows } = await client.query<{ application_name: string | null }>(
    `SELECT application_name FROM dbos.${table} WHERE ${keyColumn} = $1`,
    [key],
  );
  return rows[0]?.application_name ?? null;
}

/** Insert a workflow row as though a peer application had enqueued it. */
async function insertPeerWorkflow(
  client: Client,
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
  let client: Client;

  beforeEach(async () => {
    config = generateDBOSTestConfig();
    config.name = APP;
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    client = await sysdbClient(config);
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
  });

  test('observability filters include unclaimed rows and can span applications', async () => {
    class FilterTest {
      @DBOS.workflow()
      static async aWorkflow(): Promise<string> {
        return Promise.resolve('ok');
      }
    }

    await DBOS.launch();
    const mine = await DBOS.startWorkflow(FilterTest).aWorkflow();
    await mine.getResult();

    await insertPeerWorkflow(client, 'appname-filter-peer');
    await insertPeerWorkflow(client, 'appname-filter-unclaimed', { applicationName: null });

    // Unset: every application's workflows.
    const all = await DBOS.listWorkflows({});
    const allIDs = all.map((w) => w.workflowID);
    expect(allIDs).toContain(mine.workflowID);
    expect(allIDs).toContain('appname-filter-peer');
    expect(allIDs).toContain('appname-filter-unclaimed');

    // Scoped: this application's rows plus unclaimed ones, never the peer's.
    const ours = await DBOS.listWorkflows({ applicationName: APP });
    const ourIDs = ours.map((w) => w.workflowID);
    expect(ourIDs).toContain(mine.workflowID);
    expect(ourIDs).toContain('appname-filter-unclaimed');
    expect(ourIDs).not.toContain('appname-filter-peer');

    // A peer's rows are reachable by naming it.
    const theirs = await DBOS.listWorkflows({ applicationName: PEER });
    expect(theirs.map((w) => w.workflowID)).toContain('appname-filter-peer');
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
