/**
 * Auto-upgrade wiring in core: who enrolls, when launch refuses, which key the entitlement
 * is checked with, and the loop's lifecycle. The enterprise package is stood in for and the
 * entitlement is answered by a local stand-in for Conductor, so this runs without either;
 * the pass itself (the queries) is tested in dbos-enterprise.
 */

import { DBOS } from '../src';
import { autoUpgradableWorkflows, AutoUpgradeLoop } from '../src/auto_upgrade';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import * as enterprise from '../src/enterprise';
import { DBOSInitializationError } from '../src/error';
import { GlobalLogger } from '../src/telemetry/logs';
import { sleepms } from '../src/utils';
import {
  ALL_ENTITLED,
  FakeConductor,
  generateDBOSTestConfig,
  retryUntilSuccess,
  setUpDBOSTestSysDb,
  describeUnlessSharded,
} from './helpers';

class NoopConductor implements enterprise.ConductorConnection {
  start() {}
  stop() {
    return Promise.resolve();
  }
}

/** Satisfies the AutoUpgradePass contract, recording every call. */
class RecordingPass implements enterprise.AutoUpgradePass {
  static instances: RecordingPass[] = [];
  readonly runs: AbortSignal[] = [];
  closes = 0;
  readonly events: string[] = [];
  runError: Error | undefined = undefined;
  closeError: Error | undefined = undefined;
  blockUntilStopped = false;

  constructor(
    readonly executor: DBOSExecutor,
    readonly options: enterprise.AutoUpgradeOptions,
  ) {
    RecordingPass.instances.push(this);
  }

  async run(signal: AbortSignal): Promise<number> {
    this.events.push('run-start');
    this.runs.push(signal);
    if (this.runError) throw this.runError;
    if (this.blockUntilStopped) {
      await new Promise<void>((resolve) => {
        if (signal.aborted) return resolve();
        signal.addEventListener('abort', () => resolve(), { once: true });
      });
    }
    this.events.push('run-end');
    return 0;
  }

  close(): Promise<void> {
    this.events.push('close');
    this.closes++;
    if (this.closeError) return Promise.reject(this.closeError);
    return Promise.resolve();
  }
}

async function waitForRuns(upgradePass: RecordingPass, count: number) {
  await retryUntilSuccess(() => {
    expect(upgradePass.runs.length).toBeGreaterThanOrEqual(count);
  }, 5000);
}

// A fresh function per registration: the registry is keyed by function.
const noop = () => async () => {};

// The stand-in package has no sharded system database, and auto-upgrade isn't shard-aware.
describeUnlessSharded('auto-upgrade-launch', () => {
  let config: DBOSConfig;
  let conductor: FakeConductor;

  beforeAll(async () => {
    conductor = await FakeConductor.start();
  });

  afterAll(async () => {
    await conductor.stop();
  });

  beforeEach(async () => {
    config = { ...generateDBOSTestConfig(), autoUpgradeIntervalMs: 50, autoUpgradeGraceMs: 7500 };
    await setUpDBOSTestSysDb(config);
    RecordingPass.instances = [];
    conductor.entitle({ ...ALL_ENTITLED });
    conductor.requests.length = 0;
    jest.replaceProperty(enterprise.enterpriseLoader, 'override', () => ({
      ConductorWebsocket: NoopConductor,
      AutoUpgradePass: RecordingPass,
      ShardedSystemDatabase: class {},
      ExternalStorage: class {},
    }));
  });

  afterEach(async () => {
    await DBOS.shutdown({ deregister: true });
    jest.restoreAllMocks();
  });

  // --- Gating ---

  test('auto-upgrade requires a key', async () => {
    DBOS.setConfig({ ...config, autoUpgrade: true });
    await expect(DBOS.launch()).rejects.toThrow('requires a DBOS key');
    expect(DBOS.isInitialized()).toBe(false);
    expect(conductor.requests).toEqual([]);

    // The refused launch leaves nothing behind.
    await DBOS.shutdown();
    DBOS.setConfig(config);
    await DBOS.launch();
    expect(DBOSExecutor.globalInstance?.autoUpgrader).toBeUndefined();
  });

  test('an opted-in workflow requires a key at launch', async () => {
    // The config is off and no key is given, but the workflow's opt-in needs a key.
    DBOS.setConfig(config);
    DBOS.registerWorkflow(noop(), { name: 'wants_upgrade', autoUpgrade: true });
    await expect(DBOS.launch()).rejects.toThrow('requires a DBOS key');
    expect(DBOS.isInitialized()).toBe(false);
    expect(conductor.requests).toEqual([]);
    expect(RecordingPass.instances).toEqual([]);
  });

  test('launch refuses when the config flag is not entitled', async () => {
    conductor.entitle({ ...ALL_ENTITLED, autoUpgrade: false });
    DBOS.setConfig({ ...config, autoUpgrade: true });
    await expect(DBOS.launch({ conductorKey: 'test-key', conductorURL: conductor.url })).rejects.toThrow(
      'not entitled to autoUpgrade (Auto-upgrading workflows)',
    );
    expect(DBOS.isInitialized()).toBe(false);
    expect(conductor.requests).toEqual([{ path: '/v2/entitlements', authorization: 'Bearer test-key' }]);
    expect(RecordingPass.instances).toEqual([]);

    // The refused launch leaves nothing behind.
    await DBOS.shutdown();
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  test('launch refuses when an opted-in workflow is not entitled', async () => {
    conductor.entitle({ ...ALL_ENTITLED, autoUpgrade: false });
    // The config is off, so only the workflow's opt-in is checked, and it refuses.
    DBOS.setConfig(config);
    DBOS.registerWorkflow(noop(), { name: 'wants_upgrade', autoUpgrade: true });
    await expect(DBOS.launch({ conductorKey: 'test-key', conductorURL: conductor.url })).rejects.toThrow(
      'not entitled to autoUpgrade',
    );
    expect(conductor.requests).toEqual([{ path: '/v2/entitlements', authorization: 'Bearer test-key' }]);
    expect(DBOSExecutor.globalInstance?.autoUpgrader).toBeUndefined();
    expect(RecordingPass.instances).toEqual([]);
  });

  test('the entitlement is checked with the enterprise key first', async () => {
    // Both keys given: the enterprise key is the one checked, with the self-hosted Conductor configured.
    DBOS.setConfig({ ...config, autoUpgrade: true, transactEnterpriseKey: 'enterprise-key' });
    DBOS.registerWorkflow(noop(), { name: 'inherits' });
    await DBOS.launch({ conductorKey: 'test-key', conductorURL: conductor.url });
    // Checked once: launch does not ask again for the workflows the config flag enrolls.
    expect(conductor.requests).toEqual([{ path: '/v2/entitlements', authorization: 'Bearer enterprise-key' }]);
    expect(DBOSExecutor.globalInstance?.autoUpgrader?.running).toBe(true);
    await DBOS.shutdown();

    // The enterprise key alone is enough: no Conductor connection is needed, and with no
    // Conductor configured the check goes to DBOS's hosted one.
    const fetchSpy = jest
      .spyOn(globalThis, 'fetch')
      .mockImplementation(() =>
        Promise.resolve(new Response(JSON.stringify({ entitlements: ALL_ENTITLED }), { status: 200 })),
      );
    delete process.env.DBOS_DOMAIN;
    await DBOS.launch();
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toBe('https://cloud.dbos.dev/conductor/v2/entitlements');
    expect((init?.headers as Record<string, string>).Authorization).toBe('Bearer enterprise-key');
    expect(conductor.requests).toHaveLength(1);
    expect(DBOSExecutor.globalInstance?.conductor).toBeUndefined();
    expect(DBOSExecutor.globalInstance?.autoUpgrader?.running).toBe(true);
  });

  // --- Enrollment ---

  test('workflows enroll by flag then by config', () => {
    DBOS.setConfig(config);
    DBOS.registerWorkflow(noop(), { name: 'opted_in', autoUpgrade: true });
    DBOS.registerWorkflow(noop(), { name: 'opted_out', autoUpgrade: false });
    DBOS.registerWorkflow(noop(), { name: 'inherits' });
    DBOS.registerWorkflow(noop(), { name: 'method', className: 'Jobs' });
    // Steps are not workflows.
    DBOS.registerStep(noop(), { name: 'a_step' });

    // Names are what workflow_status stores: a free function has an empty class name.
    expect(autoUpgradableWorkflows(false)).toEqual([{ className: '', name: 'opted_in' }]);
    expect(autoUpgradableWorkflows(true).sort((a, b) => a.name.localeCompare(b.name))).toEqual([
      { className: '', name: 'inherits' },
      { className: 'Jobs', name: 'method' },
      { className: '', name: 'opted_in' },
    ]);
  });

  test('no loop without enrolled workflows', async () => {
    // A key is given, but nothing enrolls: the config is off and the one workflow opts out.
    DBOS.setConfig(config);
    DBOS.registerWorkflow(noop(), { name: 'opted_out', autoUpgrade: false });
    await DBOS.launch({ conductorKey: 'test-key', conductorURL: conductor.url });
    expect(DBOSExecutor.globalInstance?.autoUpgrader).toBeUndefined();
    expect(RecordingPass.instances).toEqual([]);
    // So no entitlement is checked either.
    expect(conductor.requests).toEqual([]);
  });

  // --- Loop lifecycle through DBOS ---

  test('the loop runs the pass until shutdown', async () => {
    DBOS.setConfig({ ...config, autoUpgrade: true });
    DBOS.registerWorkflow(noop(), { name: 'inherits' });
    await DBOS.launch({ conductorKey: 'test-key', conductorURL: conductor.url });
    const executor = DBOSExecutor.globalInstance!;
    // The config's flag reaches the executor: the unflagged workflow enrolls, and the
    // Conductor key stands in for the enterprise key.
    expect(executor.autoUpgradableWorkflows()).toEqual([{ className: '', name: 'inherits' }]);
    expect(conductor.requests).toEqual([{ path: '/v2/entitlements', authorization: 'Bearer test-key' }]);
    const upgrader = executor.autoUpgrader!;
    expect(upgrader.running).toBe(true);
    expect(upgrader.intervalMs).toBe(50);

    // The pass was built from this executor with the configured grace, and ticks on the configured interval.
    expect(RecordingPass.instances).toHaveLength(1);
    const [upgradePass] = RecordingPass.instances;
    expect(upgradePass.executor).toBe(executor);
    expect(upgradePass.options.graceMs).toBe(7500);
    await waitForRuns(upgradePass, 3);
    expect(upgradePass.runs.every((signal) => signal === upgradePass.runs[0])).toBe(true);
    expect(upgradePass.runs[0].aborted).toBe(false);

    await DBOS.shutdown();
    expect(upgrader.running).toBe(false);
    expect(executor.autoUpgrader).toBeUndefined();
    expect(upgradePass.closes).toBe(1);
    expect(upgradePass.runs[0].aborted).toBe(true);
    // Nothing ticks after shutdown.
    const runs = upgradePass.runs.length;
    await sleepms(200);
    expect(upgradePass.runs.length).toBe(runs);
  });

  test('default interval and grace', async () => {
    DBOS.setConfig({ ...generateDBOSTestConfig(), autoUpgrade: true });
    DBOS.registerWorkflow(noop(), { name: 'inherits' });
    await DBOS.launch({ conductorKey: 'test-key', conductorURL: conductor.url });
    expect(DBOSExecutor.globalInstance?.autoUpgrader?.intervalMs).toBe(30_000);
    const [upgradePass] = RecordingPass.instances;
    expect(upgradePass.options.graceMs).toBe(0);
  });
});

// --- Loop lifecycle in isolation ---

// The stand-in package has no sharded system database, and auto-upgrade isn't shard-aware.
describeUnlessSharded('auto-upgrade-loop', () => {
  const logger = new GlobalLogger();
  const executor = undefined as unknown as DBOSExecutor;

  test('a pass failure does not stop the loop', async () => {
    const upgradePass = new RecordingPass(executor, { graceMs: 0 });
    upgradePass.runError = new Error('boom');
    const warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});
    const loop = new AutoUpgradeLoop(upgradePass, 10, logger);
    loop.start();
    try {
      await waitForRuns(upgradePass, 3);
    } finally {
      await loop.stop();
    }
    expect(loop.running).toBe(false);
    expect(warnSpy).toHaveBeenCalledWith('Auto-upgrade pass failed: boom');
    expect(upgradePass.closes).toBe(1);
  });

  test('stop lets a pass in flight finish before closing', async () => {
    const upgradePass = new RecordingPass(executor, { graceMs: 0 });
    upgradePass.blockUntilStopped = true;
    const loop = new AutoUpgradeLoop(upgradePass, 10, logger);
    loop.start();
    await waitForRuns(upgradePass, 1);

    await loop.stop();
    expect(loop.running).toBe(false);
    // The pass saw the abort, returned, and only then was closed.
    expect(upgradePass.events).toEqual(['run-start', 'run-end', 'close']);
  });

  test('a close failure is reported, not thrown', async () => {
    const upgradePass = new RecordingPass(executor, { graceMs: 0 });
    upgradePass.closeError = new Error('lock gone');
    const warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {});
    const loop = new AutoUpgradeLoop(upgradePass, 10, logger);
    loop.start();
    await waitForRuns(upgradePass, 1);
    await loop.stop();
    expect(warnSpy).toHaveBeenCalledWith('Auto-upgrade pass did not close cleanly: lock gone');
  });
});

test('the loaded package must provide the pass', () => {
  jest.replaceProperty(enterprise.enterpriseLoader, 'override', () => ({ ConductorWebsocket: NoopConductor }));
  expect(() => enterprise.load(enterprise.AUTO_UPGRADE.description)).toThrow(DBOSInitializationError);
  expect(() => enterprise.load(enterprise.AUTO_UPGRADE.description)).toThrow(
    'Auto-upgrading workflows requires @dbos-inc/dbos-enterprise',
  );
  expect(() => enterprise.load()).toThrow('it does not export AutoUpgradePass');
  jest.restoreAllMocks();
});
