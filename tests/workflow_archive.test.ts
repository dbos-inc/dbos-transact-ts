// The system database's half of workflow archiving: which writes queue a workflow.

import { Client } from 'pg';
import { DBOS, DBOSClient } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import * as enterprise from '../src/enterprise';
import { DBOSError, DBOSInitializationError, DBOSWorkflowArchiveNotConfiguredError } from '../src/error';
import * as systemDatabase from '../src/system_database';
import { SystemDatabase } from '../src/system_database';
import type { WorkflowArchive } from '../src/workflow_archive';
import {
  ALL_ENTITLED,
  describeUnlessSharded,
  Event,
  expectError,
  FakeConductor,
  generateDBOSTestConfig,
  retryUntilSuccess,
  setUpDBOSTestSysDb,
} from './helpers';

const fakeArchive: WorkflowArchive = { archiveID: 'fake' };

class ArchiveWorkflows {
  static started = new Event();
  static release = new Event();

  @DBOS.workflow()
  static async ok(): Promise<number> {
    return Promise.resolve(1);
  }

  @DBOS.workflow()
  static async fails(): Promise<void> {
    await Promise.resolve();
    throw new Error('no');
  }

  @DBOS.workflow()
  static async blocks(): Promise<void> {
    ArchiveWorkflows.started.set();
    await ArchiveWorkflows.release.wait();
  }

  @DBOS.workflow()
  static async sleeps(): Promise<void> {
    await DBOS.sleep(10000);
  }

  @DBOS.workflow()
  static async importer(workflowID: string): Promise<string> {
    const handle = await DBOS.importArchivedWorkflow(workflowID, 123);
    return handle.workflowID;
  }
}

/** What an enterprise package's archiving hooks were asked to do. */
class Recorded {
  static archivers: FakeArchiver[] = [];
  static imports: unknown[][] = [];
}

class FakeArchiver implements enterprise.Archiver {
  started = false;
  stopped = false;
  constructor(
    readonly executor: DBOSExecutor,
    readonly archive: WorkflowArchive,
  ) {
    Recorded.archivers.push(this);
  }
  start() {
    this.started = true;
  }
  stop() {
    this.stopped = true;
    return Promise.resolve();
  }
}

function fakeEnterprise(): Partial<enterprise.Enterprise> {
  return {
    ConductorWebsocket: class {} as unknown as enterprise.ConductorFactory,
    AutoUpgradePass: class {} as unknown as enterprise.AutoUpgradePassFactory,
    ShardedSystemDatabase: class {} as unknown as enterprise.ShardedSystemDatabaseFactory,
    Suspend: class {} as unknown as enterprise.SuspendFactory,
    ExternalStorage: class {} as unknown as enterprise.ExternalStorageFactory,
    Archiver: FakeArchiver,
    importArchivedWorkflow: (...args: unknown[]) => {
      Recorded.imports.push(args);
      return Promise.resolve();
    },
  };
}

async function archivePending(config: DBOSConfig, workflowID: string): Promise<boolean | null> {
  const client = new Client({ connectionString: config.systemDatabaseUrl });
  await client.connect();
  try {
    const { rows } = await client.query<{ archive_pending: boolean | null }>(
      'SELECT archive_pending FROM dbos.workflow_status WHERE workflow_uuid = $1',
      [workflowID],
    );
    return rows[0].archive_pending;
  } finally {
    await client.end();
  }
}

async function runSql(config: DBOSConfig, sql: string, params: unknown[] = []) {
  const client = new Client({ connectionString: config.systemDatabaseUrl });
  await client.connect();
  try {
    return await client.query(sql, params);
  } finally {
    await client.end();
  }
}

function sysdb(): SystemDatabase {
  return DBOSExecutor.globalInstance!.systemDatabase;
}

// Workflow archiving is not supported on a sharded system database.
describeUnlessSharded('workflow-archive-marking', () => {
  let config: DBOSConfig;

  beforeAll(() => {
    config = generateDBOSTestConfig();
  });

  beforeEach(async () => {
    ArchiveWorkflows.started = new Event();
    ArchiveWorkflows.release = new Event();
    Recorded.imports = [];
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    ArchiveWorkflows.release.set();
    await DBOS.shutdown();
    jest.restoreAllMocks();
  });

  test('completions-unmarked-without-archive', async () => {
    await expect(ArchiveWorkflows.ok()).resolves.toBe(1);
    await expect(ArchiveWorkflows.fails()).rejects.toThrow('no');
    const workflows = await DBOS.listWorkflows({});
    expect(workflows).toHaveLength(2);
    for (const w of workflows) {
      expect(w.completedAt).toBeDefined();
      expect(await archivePending(config, w.workflowID)).toBeNull();
    }
  });

  test('every-terminal-transition-marks', async () => {
    sysdb().archiveCompletions = true;

    const okHandle = await DBOS.startWorkflow(ArchiveWorkflows).ok();
    await okHandle.getResult();
    expect(await archivePending(config, okHandle.workflowID)).toBe(true);

    const failHandle = await DBOS.startWorkflow(ArchiveWorkflows).fails();
    await expect(failHandle.getResult()).rejects.toThrow('no');
    expect(await archivePending(config, failHandle.workflowID)).toBe(true);

    // Cancelled, then resumed: resume clears the mark, completing sets it again.
    const blocked = await DBOS.startWorkflow(ArchiveWorkflows).blocks();
    await ArchiveWorkflows.started.wait();
    await DBOS.cancelWorkflow(blocked.workflowID);
    expect(await archivePending(config, blocked.workflowID)).toBe(true);
    ArchiveWorkflows.release.set();
    // Its own handle may see the cancel before or after it returns.
    await expect(blocked.getResult()).rejects.toThrow(/cancelled/);
    const resumed = await DBOS.resumeWorkflow(blocked.workflowID);
    await resumed.getResult();
    expect(await archivePending(config, blocked.workflowID)).toBe(true);

    // Rewinding clears it while the workflow runs again.
    await runSql(config, 'UPDATE dbos.workflow_status SET archive_pending = NULL WHERE workflow_uuid = $1', [
      okHandle.workflowID,
    ]);
    const rewound = await DBOS.rewindWorkflow(okHandle.workflowID);
    await rewound.getResult();
    expect(await archivePending(config, okHandle.workflowID)).toBe(true);

    // A timeout cancels through the deadline sweep.
    const timedOut = await DBOS.startWorkflow(ArchiveWorkflows, { timeoutMS: 100 }).sleeps();
    await expect(timedOut.getResult()).rejects.toThrow(/cancelled/);
    await retryUntilSuccess(async () => {
      expect(await archivePending(config, timedOut.workflowID)).toBe(true);
    });

    // Dead-lettering a workflow that ran out of recovery attempts.
    const dead = await DBOS.startWorkflow(ArchiveWorkflows).ok();
    await dead.getResult();
    await runSql(
      config,
      `UPDATE dbos.workflow_status SET status = 'PENDING', completed_at = NULL, archive_pending = NULL
       WHERE workflow_uuid = $1`,
      [dead.workflowID],
    );
    await sysdb().deadLetterWorkflows([dead.workflowID], 0);
    const status = await DBOS.getWorkflowStatus(dead.workflowID);
    expect(status?.status).toBe('MAX_RECOVERY_ATTEMPTS_EXCEEDED');
    expect(await archivePending(config, dead.workflowID)).toBe(true);
  });

  test('resume-clears-mark', async () => {
    sysdb().archiveCompletions = true;
    const handle = await DBOS.startWorkflow(ArchiveWorkflows).blocks();
    await ArchiveWorkflows.started.wait();
    await DBOS.cancelWorkflow(handle.workflowID);
    expect(await archivePending(config, handle.workflowID)).toBe(true);
    // Resumed while the cancelled execution still runs: ENQUEUED, unmarked.
    sysdb().archiveCompletions = false;
    const resumed = await DBOS.resumeWorkflow(handle.workflowID);
    expect(await archivePending(config, handle.workflowID)).toBeNull();
    ArchiveWorkflows.release.set();
    await resumed.getResult();
    expect(await archivePending(config, handle.workflowID)).toBeNull();
  });

  test('cancelling-again-leaves-the-completion', async () => {
    sysdb().archiveCompletions = true;
    const handle = await DBOS.startWorkflow(ArchiveWorkflows).blocks();
    await ArchiveWorkflows.started.wait();
    await DBOS.cancelWorkflow(handle.workflowID);
    const first = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(first?.completedAt).toBeDefined();
    // As the archiver leaves it once the cancellation is archived.
    await runSql(config, 'UPDATE dbos.workflow_status SET archive_pending = NULL WHERE workflow_uuid = $1', [
      handle.workflowID,
    ]);
    await DBOS.cancelWorkflow(handle.workflowID);
    const again = await DBOS.getWorkflowStatus(handle.workflowID);
    expect(again?.status).toBe('CANCELLED');
    // Not a new completion, so nothing is queued to archive twice.
    expect(again?.completedAt).toBe(first?.completedAt);
    expect(await archivePending(config, handle.workflowID)).toBeNull();
  });

  test('client-cancel-marks-only-with-an-archive', async () => {
    const plain = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    const archiving = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      workflowArchive: fakeArchive,
    });
    try {
      const first = await DBOS.startWorkflow(ArchiveWorkflows).blocks();
      await plain.cancelWorkflow(first.workflowID);
      expect(await archivePending(config, first.workflowID)).toBeNull();

      const second = await DBOS.startWorkflow(ArchiveWorkflows).blocks();
      await archiving.cancelWorkflow(second.workflowID);
      expect(await archivePending(config, second.workflowID)).toBe(true);
    } finally {
      await plain.destroy();
      await archiving.destroy();
    }
  });

  // No application owns an unclaimed workflow, so none archives it: its completion isn't marked.
  test('unclaimed-completions-unmarked', async () => {
    const archiving = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      workflowArchive: fakeArchive,
    });
    try {
      const options = { workflowName: 'never_run', queueName: 'unserved' };
      const unclaimed = await archiving.enqueue(options);
      const claimed = await archiving.enqueue({ ...options, applicationName: 'some-app' });
      for (const handle of [unclaimed, claimed]) {
        await archiving.cancelWorkflow(handle.workflowID);
        expect((await DBOS.getWorkflowStatus(handle.workflowID))?.status).toBe('CANCELLED');
      }
      expect(await archivePending(config, unclaimed.workflowID)).toBeNull();
      expect(await archivePending(config, claimed.workflowID)).toBe(true);
    } finally {
      await archiving.destroy();
    }
  });

  test('migration-adds-column-and-index', async () => {
    const columns = await runSql(
      config,
      `SELECT data_type FROM information_schema.columns
       WHERE table_schema = 'dbos' AND table_name = 'workflow_status' AND column_name = 'archive_pending'`,
    );
    expect(columns.rows).toEqual([{ data_type: 'boolean' }]);
    const indexes = await runSql(
      config,
      `SELECT indexdef FROM pg_indexes WHERE schemaname = 'dbos' AND indexname = 'idx_workflow_status_archive_pending'`,
    );
    expect(indexes.rows).toHaveLength(1);
    const indexdef = (indexes.rows[0] as { indexdef: string }).indexdef;
    expect(indexdef).toContain('(application_name, completed_at, workflow_uuid)');
    expect(indexdef).toContain('WHERE (archive_pending IS TRUE)');
  });

  test('import-archived-needs-an-archive', async () => {
    await expect(DBOS.importArchivedWorkflow('wf', 1)).rejects.toThrow(DBOSWorkflowArchiveNotConfiguredError);
    await expect(DBOS.importArchivedWorkflow('wf', 1)).rejects.toThrow(
      'No workflow archive is configured: set workflowArchive in the DBOS configuration or DBOSClient',
    );
    const client = await DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl! });
    try {
      await expect(client.importArchivedWorkflow('wf', 1)).rejects.toThrow(DBOSWorkflowArchiveNotConfiguredError);
    } finally {
      await client.destroy();
    }
  });

  test('import-archived-calls-enterprise', async () => {
    await expect(ArchiveWorkflows.ok()).resolves.toBe(1);
    const workflowID = (await DBOS.listWorkflows({}))[0].workflowID;
    jest.replaceProperty(enterprise.enterpriseLoader, 'override', fakeEnterprise);
    const executor = DBOSExecutor.globalInstance!;
    (executor as { workflowArchive: WorkflowArchive | undefined }).workflowArchive = fakeArchive;

    const handle = await DBOS.importArchivedWorkflow<number>(workflowID, 123);
    expect(Recorded.imports).toEqual([[executor.systemDatabase, fakeArchive, workflowID, 123]]);
    expect(handle.workflowID).toBe(workflowID);
    await expect(handle.getResult()).resolves.toBe(1);

    // Inside a workflow, the import is a step.
    const importer = await DBOS.startWorkflow(ArchiveWorkflows).importer(workflowID);
    await expect(importer.getResult()).resolves.toBe(workflowID);
    const steps = await DBOS.listWorkflowSteps(importer.workflowID);
    expect(steps?.map((s) => s.name)).toEqual(['DBOS.importArchivedWorkflow']);
    expect(Recorded.imports).toHaveLength(2);

    const client = await DBOSClient.create({
      systemDatabaseUrl: config.systemDatabaseUrl!,
      workflowArchive: fakeArchive,
    });
    try {
      const clientHandle = await client.importArchivedWorkflow<number>(workflowID, 7);
      expect(clientHandle.workflowID).toBe(workflowID);
      expect(Recorded.imports[2].slice(1)).toEqual([fakeArchive, workflowID, 7]);
    } finally {
      await client.destroy();
    }
  });

  test('cockroach-client-refuses-archive', async () => {
    jest.spyOn(SystemDatabase.prototype, 'isCockroach').mockResolvedValue(true);
    await expectError(
      DBOSClient.create({ systemDatabaseUrl: config.systemDatabaseUrl!, workflowArchive: fakeArchive }),
      DBOSError,
      'Workflow archiving is not supported on CockroachDB',
    );
  });
});

test('sharded-client-refuses-archive', async () => {
  const created = jest.spyOn(systemDatabase, 'createSystemDatabase');
  try {
    // Refused before connecting, or loading the package for the sharded system database.
    await expectError(
      DBOSClient.create({
        systemDatabaseUrl: 'postgresql://postgres:dbos@localhost:5432/unused',
        sharding: true,
        workflowArchive: fakeArchive,
      }),
      DBOSError,
      'Workflow archiving is not supported on a sharded system database',
    );
    expect(created).not.toHaveBeenCalled();
  } finally {
    jest.restoreAllMocks();
  }
});

// A sharded run's harness stubs the entitlement check, and archiving is refused there.
describeUnlessSharded('workflow-archive-launch', () => {
  let config: DBOSConfig;
  let conductor: FakeConductor;

  beforeAll(async () => {
    conductor = await FakeConductor.start();
  });

  afterAll(async () => {
    await conductor.stop();
  });

  beforeEach(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    Recorded.archivers = [];
    conductor.entitle({ ...ALL_ENTITLED });
    jest.replaceProperty(enterprise.enterpriseLoader, 'override', fakeEnterprise);
  });

  afterEach(async () => {
    await DBOS.shutdown();
    jest.restoreAllMocks();
  });

  test('workflow-archive-checks-its-entitlement', async () => {
    DBOS.setConfig({ ...config, workflowArchive: fakeArchive });
    await expectError(
      DBOS.launch(),
      DBOSInitializationError,
      `${enterprise.WORKFLOW_ARCHIVE.description} requires a DBOS key`,
    );
    await DBOS.shutdown();
    DBOS.setConfig({ ...config, workflowArchive: fakeArchive, transactEnterpriseKey: 'enterprise-key' });
    conductor.entitle({ ...ALL_ENTITLED, workflowArchive: false });
    await expectError(
      DBOS.launch({ conductorURL: conductor.url }),
      DBOSInitializationError,
      'not entitled to workflowArchive',
    );
    expect(Recorded.archivers).toEqual([]);
  });

  test('archiver-runs-from-launch-to-shutdown', async () => {
    DBOS.setConfig({ ...config, workflowArchive: fakeArchive, transactEnterpriseKey: 'enterprise-key' });
    await DBOS.launch({ conductorURL: conductor.url });
    expect(Recorded.archivers).toHaveLength(1);
    const [archiver] = Recorded.archivers;
    expect(archiver.executor).toBe(DBOSExecutor.globalInstance);
    expect(archiver.archive).toBe(fakeArchive);
    expect(archiver.started).toBe(true);
    expect(sysdb().archiveCompletions).toBe(true);

    const handle = await DBOS.startWorkflow(ArchiveWorkflows).ok();
    await handle.getResult();
    expect(await archivePending(config, handle.workflowID)).toBe(true);

    await DBOS.shutdown();
    expect(archiver.stopped).toBe(true);
  });

  test('cockroach-launch-refuses-archive', async () => {
    jest.spyOn(SystemDatabase.prototype, 'isCockroach').mockResolvedValue(true);
    DBOS.setConfig({ ...config, workflowArchive: fakeArchive, transactEnterpriseKey: 'enterprise-key' });
    await expectError(
      DBOS.launch({ conductorURL: conductor.url }),
      DBOSError,
      'Workflow archiving is not supported on CockroachDB',
    );
    expect(DBOS.isInitialized()).toBe(false);
    expect(Recorded.archivers).toEqual([]);
  });
});
