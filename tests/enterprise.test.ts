import { DBOS } from '../src';
import { DBOSConfig, DBOSExecutor } from '../src/dbos-executor';
import * as enterprise from '../src/enterprise';
import { DBOSInitializationError } from '../src/error';
import { globalParams } from '../src/utils';
import { GlobalLogger } from '../src/telemetry/logs';
import {
  ALL_ENTITLED,
  expectError,
  FakeConductor,
  generateDBOSTestConfig,
  setUpDBOSTestSysDb,
  testUnlessSharded,
} from './helpers';

// The real check, taken before a sharded run's harness stubs it for every launch.
const requireEnterprise = enterprise.requireEnterprise;

// The package is an optional peer that core's own install never pulls in, so it is really absent here.
describe('enterprise-loader', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  // A sharded run always has the package.
  testUnlessSharded('absent package gets the install hint', () => {
    expect(() => enterprise.load()).toThrow(DBOSInitializationError);
    expect(() => enterprise.load()).toThrow('npm install @dbos-inc/dbos-enterprise');
    expect(() => enterprise.load()).toThrow("Cannot find module '@dbos-inc/dbos-enterprise'");
    // The message names what needed the package.
    expect(() => enterprise.load()).toThrow('Connecting to DBOS Conductor requires');
    expect(() => enterprise.load(enterprise.AUTO_UPGRADE.description)).toThrow(
      'Auto-upgrading workflows requires @dbos-inc/dbos-enterprise',
    );
  });

  test.each([
    'Package subpath \'./internal/utils\' is not defined by "exports"',
    'Cannot require module @dbos-inc/dbos-enterprise',
  ])('a load failure names its cause (%s)', (message) => {
    jest.replaceProperty(enterprise.enterpriseLoader, 'override', () => {
      throw new Error(message);
    });
    expect(() => enterprise.load()).toThrow('npm install @dbos-inc/dbos-enterprise');
    expect(() => enterprise.load()).toThrow(`Cause: ${message}`);
  });

  test('a package without the Conductor client names that cause', () => {
    jest.replaceProperty(enterprise.enterpriseLoader, 'override', () => ({}));
    expect(() => enterprise.load()).toThrow('it does not export ConductorWebsocket');
  });
});

describe('enterprise-launch', () => {
  let config: DBOSConfig;

  beforeAll(() => {
    config = generateDBOSTestConfig();
  });

  beforeEach(async () => {
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  afterEach(async () => {
    await DBOS.shutdown();
    jest.restoreAllMocks();
  });

  // A sharded run always has the package.
  testUnlessSharded('refused launch leaves nothing behind', async () => {
    await expect(DBOS.launch({ conductorKey: 'test-key' })).rejects.toThrow(DBOSInitializationError);
    expect(DBOS.isInitialized()).toBe(false);

    // Neither teardown nor a fresh launch trips over the refused one.
    await DBOS.shutdown();
    await DBOS.launch();
    expect(DBOS.executorID).toBe('local');
  });

  // A sharded run always has the package.
  testUnlessSharded('DBOS Cloud always requires the package', async () => {
    const originalCloud = globalParams.dbosCloud;
    const originalSysDbUrl = process.env.DBOS_SYSTEM_DATABASE_URL;
    process.env.DBOS_SYSTEM_DATABASE_URL = config.systemDatabaseUrl;
    globalParams.dbosCloud = true;
    try {
      // DBOS Cloud always connects to Conductor, whether or not a key is passed in code.
      await expect(DBOS.launch()).rejects.toThrow('npm install @dbos-inc/dbos-enterprise');
      expect(DBOS.isInitialized()).toBe(false);
    } finally {
      globalParams.dbosCloud = originalCloud;
      if (originalSysDbUrl === undefined) {
        delete process.env.DBOS_SYSTEM_DATABASE_URL;
      } else {
        process.env.DBOS_SYSTEM_DATABASE_URL = originalSysDbUrl;
      }
    }
  });

  // The stand-in package has no sharded system database.
  testUnlessSharded('skips self recovery when Conductor is enabled', async () => {
    class FakeConductor implements enterprise.ConductorConnection {
      start() {}
      stop() {
        return Promise.resolve();
      }
    }
    const recoverSpy = jest.spyOn(DBOSExecutor.prototype, 'recoverPendingWorkflows');
    class FakePass implements enterprise.AutoUpgradePass {
      run() {
        return Promise.resolve(0);
      }
      close() {
        return Promise.resolve();
      }
    }
    jest.replaceProperty(enterprise.enterpriseLoader, 'override', () => ({
      ConductorWebsocket: FakeConductor,
      AutoUpgradePass: FakePass,
      ShardedSystemDatabase: class {},
    }));

    await DBOS.launch({ conductorKey: 'test-key', conductorURL: 'ws://127.0.0.1:1' });
    expect(recoverSpy).not.toHaveBeenCalled();
    await DBOS.shutdown();

    await DBOS.launch();
    expect(recoverSpy).toHaveBeenCalled();
  });
});

// --- Entitlements ---

// A feature for exercising the check itself, entitled under a name Conductor may not know.
const SOME_FEATURE: enterprise.Feature = { entitlement: 'someFeature', description: 'Some feature' };

describe('entitlements', () => {
  let conductor: FakeConductor;
  let originalCloud: boolean;
  let originalDomain: string | undefined;

  beforeAll(async () => {
    conductor = await FakeConductor.start();
  });

  afterAll(async () => {
    await conductor.stop();
  });

  beforeEach(() => {
    originalCloud = globalParams.dbosCloud;
    globalParams.dbosCloud = false;
    originalDomain = process.env.DBOS_DOMAIN;
    delete process.env.DBOS_DOMAIN;
    conductor.entitle({ ...ALL_ENTITLED, someFeature: true });
    conductor.requests.length = 0;
  });

  afterEach(() => {
    globalParams.dbosCloud = originalCloud;
    if (originalDomain === undefined) {
      delete process.env.DBOS_DOMAIN;
    } else {
      process.env.DBOS_DOMAIN = originalDomain;
    }
    jest.restoreAllMocks();
  });

  function check(options: { enterpriseKey?: string; conductorKey?: string; features?: enterprise.Feature[] } = {}) {
    return requireEnterprise(options.features ?? [SOME_FEATURE], {
      enterpriseKey: options.enterpriseKey,
      conductorKey: options.conductorKey,
      conductorURL: conductor.url,
    });
  }

  test('the entitlement URL sits beside the Conductor URL', () => {
    const url = enterprise.entitlementURL;
    expect(url('wss://cloud.dbos.dev/conductor/v1alpha1')).toBe('https://cloud.dbos.dev/conductor/v2/entitlements');
    expect(url('wss://cloud.dbos.dev/conductor/v1alpha1/')).toBe('https://cloud.dbos.dev/conductor/v2/entitlements');
    // Credentials in the URL are dropped, so they are never sent or quoted in errors.
    expect(url('wss://user:secret@conductor.example.com:8443/conductor/v1alpha1')).toBe(
      'https://conductor.example.com:8443/conductor/v2/entitlements',
    );
    expect(url('ws://user:secret@[::1]:8090')).toBe('http://[::1]:8090/v2/entitlements');
    // A self-hosted Conductor serves its routes from the root.
    expect(url('ws://localhost:8090')).toBe('http://localhost:8090/v2/entitlements');
    expect(url('https://conductor.example.com/')).toBe('https://conductor.example.com/v2/entitlements');
  });

  test('the default Conductor follows the DBOS domain', () => {
    expect(enterprise.defaultConductorURL()).toBe(enterprise.HOSTED_CONDUCTOR_URL);
    process.env.DBOS_DOMAIN = 'staging.dbos.dev';
    expect(enterprise.defaultConductorURL()).toBe('wss://staging.dbos.dev/conductor/v1alpha1');
  });

  test('an entitled key unlocks enterprise features', async () => {
    await check({ enterpriseKey: 'enterprise-key' });
    await check({ conductorKey: 'conductor-key' });
    // The enterprise key is checked when both are set.
    await check({ enterpriseKey: 'enterprise-key', conductorKey: 'conductor-key' });
    expect(conductor.requests).toEqual([
      { path: '/v2/entitlements', authorization: 'Bearer enterprise-key' },
      { path: '/v2/entitlements', authorization: 'Bearer conductor-key' },
      { path: '/v2/entitlements', authorization: 'Bearer enterprise-key' },
    ]);
  });

  test('each feature needs its own entitlement', async () => {
    conductor.entitle({ sharding: true, autoUpgrade: false, someFeature: true });
    await check({ enterpriseKey: 'enterprise-key' });
    let message = '';
    await check({ enterpriseKey: 'enterprise-key', features: [SOME_FEATURE, enterprise.AUTO_UPGRADE] }).catch(
      (e: Error) => (message = e.message),
    );
    // Names only what the org lacks, by entitlement and by what it does.
    expect(message).toContain('not entitled to autoUpgrade (Auto-upgrading workflows)');
    expect(message).toContain('plan does not include it');
    expect(message).not.toContain('someFeature');
    expect(message).not.toContain('enterprise-key');
    // One request answers for every feature.
    expect(conductor.requests).toHaveLength(2);
  });

  test('a past-limit org is told to upgrade', async () => {
    // Conductor refuses a past-limit org outright, tagging the 403 with a code.
    conductor.reject(403, 'your DBOS account is past the free trial limit', { 'X-DBOS-Error': 'past_limit' });
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow(
      'past its plan limit, so it is not entitled to someFeature (Some feature). Upgrade its plan',
    );
    // The code, not the status or the prose, marks it: a bare 403 is another refusal.
    conductor.reject(403, 'your DBOS account is past the free trial limit');
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('refused the check (HTTP 403)');
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.not.toThrow('Upgrade its plan');
  });

  test('an entitlement Conductor does not report is not granted', async () => {
    // A Conductor older than the feature doesn't list it, which grants nothing.
    conductor.entitle({ ...ALL_ENTITLED });
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('not entitled to someFeature');
    // Anything but true grants nothing either.
    conductor.entitle({ someFeature: 'yes' });
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('not entitled to someFeature');
  });

  test('enterprise features require a key', async () => {
    // DBOS's hosted Conductor, implicit or named, is never asked without a key.
    for (const conductorURL of [undefined, enterprise.HOSTED_CONDUCTOR_URL]) {
      await expect(requireEnterprise([SOME_FEATURE], { conductorURL })).rejects.toThrow(
        'Some feature requires a DBOS key. Set transactEnterpriseKey',
      );
    }
    expect(conductor.requests).toEqual([]);
  });

  test('a self-hosted Conductor is asked without a key', async () => {
    // Without OAuth, a self-hosted Conductor grants every feature to any caller.
    await check();
    expect(conductor.requests).toEqual([{ path: '/v2/entitlements', authorization: undefined }]);
    // With OAuth, it wants a key.
    conductor.reject(401, 'this endpoint requires a DBOS API key');
    let message = '';
    await check().catch((e: Error) => (message = e.message));
    expect(message).toContain('requires a DBOS key: Conductor at');
    expect(message).toContain('(HTTP 401): this endpoint requires a DBOS API key');
    expect(message).toContain('Set transactEnterpriseKey');
  });

  test('Conductor credentials never reach errors', async () => {
    conductor.reject(401, 'invalid API key');
    const withCredentials = conductor.url.replace('ws://', 'ws://user:secret@');
    let message = '';
    await requireEnterprise([SOME_FEATURE], { enterpriseKey: 'enterprise-key', conductorURL: withCredentials }).catch(
      (e: Error) => (message = e.message),
    );
    expect(message).toContain('rejected the key (HTTP 401)');
    expect(message).not.toContain('secret');
    expect(message).not.toContain('user');
  });

  test.each([
    'invalid API key',
    // A user credential, which the route refuses in favor of an API key.
    'this endpoint requires a DBOS API key',
  ])('a rejected key is refused (%s)', async (detail) => {
    conductor.reject(401, detail);
    let message = '';
    await check({ enterpriseKey: 'enterprise-key' }).catch((e: Error) => (message = e.message));
    expect(message).toContain('requires a valid DBOS key.');
    expect(message).toContain(`rejected the key (HTTP 401): ${detail}`);
    expect(message).not.toContain('enterprise-key');
  });

  test.each([
    [403, 'forbidden'],
    [404, 'organization not found'],
    [409, 'conflict'],
  ])('any other refusal quotes Conductor (%s)', async (status, detail) => {
    conductor.reject(status, detail);
    let message = '';
    await check({ enterpriseKey: 'enterprise-key' }).catch((e: Error) => (message = e.message));
    expect(message).toContain(`refused the check (HTTP ${status}): ${detail}`);
    expect(message).not.toContain('rejected the key');
    expect(message).not.toContain('enterprise-key');
  });

  test('a Conductor without the route is refused', async () => {
    // An older Conductor's router answers an unknown route with no problem detail.
    conductor.status = 404;
    conductor.body = '404 page not found';
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('has no entitlements route (HTTP 404)');
  });

  test('a redirect is refused and never carries the key', async () => {
    // Following a redirect would re-send the request, headers included, to the host the
    // Location names. That would hand the key to whoever answered.
    const elsewhere = await FakeConductor.start();
    try {
      conductor.status = 302;
      conductor.headers = { Location: `${elsewhere.url.replace('ws://', 'http://')}/v2/entitlements` };
      await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('refused the check (HTTP 302)');
      // The redirect target never saw the key, or anything at all.
      expect(elsewhere.requests).toEqual([]);
    } finally {
      await elsewhere.stop();
    }
  });

  test.each([[[]], [{ entitlements: null }], [{ entitled: true }], ['not json']])(
    'a malformed answer counts as an outage (%j)',
    async (body) => {
      conductor.body = body;
      await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('Retry once Conductor');
    },
  );

  test('an unreachable Conductor blocks startup', async () => {
    conductor.reject(503, 'unavailable');
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow(
      'checking it with Conductor at http://127.0.0.1:',
    );
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('failed (HTTP 503: unavailable)');
    // A failure without a problem detail is reported by status alone.
    conductor.body = 'Bad Gateway';
    conductor.status = 502;
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('failed (HTTP 502).');
    // A port nothing listens on.
    let message = '';
    await requireEnterprise([SOME_FEATURE], {
      enterpriseKey: 'enterprise-key',
      conductorURL: 'ws://127.0.0.1:1',
    }).catch((e: Error) => (message = e.message));
    expect(message).toContain('checking it with Conductor');
    expect(message).toContain('failed');
    expect(message).not.toContain('enterprise-key');
  });

  test('a hosted Conductor outage does not block startup', async () => {
    const fetchSpy = jest.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('connection refused'));
    const warnSpy = jest.spyOn(GlobalLogger.prototype, 'warn').mockImplementation(() => {});
    // No Conductor URL means DBOS's hosted one, whose outage only warns.
    await requireEnterprise([SOME_FEATURE], { enterpriseKey: 'enterprise-key' });
    expect(fetchSpy.mock.calls.map(([url]) => url)).toEqual(['https://cloud.dbos.dev/conductor/v2/entitlements']);
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(String(warnSpy.mock.calls[0][0])).toContain('Could not verify the DBOS key');
    expect(String(warnSpy.mock.calls[0][0])).not.toContain('enterprise-key');
    // Another domain is not DBOS's hosted Conductor, so its outage still refuses.
    process.env.DBOS_DOMAIN = 'unreachable.example.com';
    await expect(requireEnterprise([SOME_FEATURE], { enterpriseKey: 'enterprise-key' })).rejects.toThrow(
      'Retry once Conductor is reachable',
    );
  });

  // Make the hosted Conductor answer with this error.
  function hostedAnswers(status: number, headers: Record<string, string> = {}) {
    jest.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('', { status, headers }));
    return jest.spyOn(GlobalLogger.prototype, 'warn').mockImplementation(() => {});
  }

  function checkHosted() {
    return requireEnterprise([SOME_FEATURE], { enterpriseKey: 'enterprise-key' });
  }

  test.each([403, 407, 408, 425, 429])('hosted throttling and proxies count as outages (%s)', async (status) => {
    // From the hosted Conductor these mean it couldn't answer, not that it refused the key.
    const warnSpy = hostedAnswers(status);
    await checkHosted();
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(String(warnSpy.mock.calls[0][0])).toContain(`(HTTP ${status})`);
  });

  test('hosted refusals still refuse', async () => {
    hostedAnswers(403, { 'X-DBOS-Error': 'past_limit' });
    await expect(checkHosted()).rejects.toThrow('past its plan limit');
    jest.restoreAllMocks();
    hostedAnswers(401);
    await expect(checkHosted()).rejects.toThrow('rejected the key (HTTP 401)');
  });

  test("another Conductor's throttling still refuses", async () => {
    conductor.reject(429, 'slow down');
    await expect(check({ enterpriseKey: 'enterprise-key' })).rejects.toThrow('refused the check (HTTP 429)');
  });

  test('DBOS Cloud passes without a check', async () => {
    globalParams.dbosCloud = true;
    await check();
    expect(conductor.requests).toEqual([]);
  });

  test('config turns on enterprise features', () => {
    expect(enterprise.enabledFeatures({})).toEqual([]);
    expect(enterprise.enabledFeatures({ autoUpgrade: false })).toEqual([]);
    expect(enterprise.enabledFeatures({ autoUpgrade: true })).toEqual([enterprise.AUTO_UPGRADE]);
    expect(enterprise.enabledFeatures({ sharding: false })).toEqual([]);
    expect(enterprise.enabledFeatures({ sharding: true })).toEqual([enterprise.SHARDING]);
    // Every registered feature has a distinct entitlement name.
    const names = enterprise.FEATURES.map((f) => f.entitlement);
    expect(new Set(names).size).toBe(names.length);
  });

  // A sharded run's harness stubs the entitlement check this exercises.
  testUnlessSharded('an auto-upgrading executor checks its entitlement', async () => {
    const config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    try {
      // Refused before the package is even loaded, so this holds without dbos-enterprise.
      DBOS.setConfig({ ...config, autoUpgrade: true });
      await expect(DBOS.launch()).rejects.toThrow('Auto-upgrading workflows requires a DBOS key');
      await DBOS.shutdown();
      DBOS.setConfig({ ...config, autoUpgrade: true, transactEnterpriseKey: 'enterprise-key' });
      conductor.entitle({ ...ALL_ENTITLED, autoUpgrade: false });
      await expect(DBOS.launch({ conductorURL: conductor.url })).rejects.toThrow('not entitled to autoUpgrade');
      expect(conductor.requests).toEqual([{ path: '/v2/entitlements', authorization: 'Bearer enterprise-key' }]);
      // Entitled, the launch still needs the package.
      conductor.entitle({ ...ALL_ENTITLED });
      await expect(DBOS.launch({ conductorURL: conductor.url })).rejects.toThrow(
        'Auto-upgrading workflows requires @dbos-inc/dbos-enterprise',
      );
      expect(DBOS.isInitialized()).toBe(false);
    } finally {
      await DBOS.shutdown();
    }
  });

  // A sharded run's harness stubs the entitlement check this exercises.
  testUnlessSharded('a sharded executor checks its entitlement', async () => {
    const config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    try {
      // Refused before the package is even loaded, so this holds without dbos-enterprise.
      DBOS.setConfig({ ...config, sharding: true });
      await expectError(
        DBOS.launch(),
        DBOSInitializationError,
        `${enterprise.SHARDING.description} requires a DBOS key`,
      );
      await DBOS.shutdown();
      DBOS.setConfig({ ...config, sharding: true, transactEnterpriseKey: 'enterprise-key' });
      conductor.entitle({ ...ALL_ENTITLED, sharding: false });
      await expectError(
        DBOS.launch({ conductorURL: conductor.url }),
        DBOSInitializationError,
        'not entitled to sharding',
      );
      expect(conductor.requests).toEqual([{ path: '/v2/entitlements', authorization: 'Bearer enterprise-key' }]);
      expect(DBOS.isInitialized()).toBe(false);
    } finally {
      await DBOS.shutdown();
    }
  });
});
