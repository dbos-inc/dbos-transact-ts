import { DBOS } from '../src';
import { DBOSConfig } from '../src/dbos-executor';
import * as enterprise from '../src/enterprise';
import { DBOSInitializationError } from '../src/error';
import { globalParams } from '../src/utils';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

// The package is an optional peer that core's own install never pulls in, so it is really absent here.
describe('enterprise-loader', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('absent package gets the install hint', () => {
    expect(() => enterprise.load()).toThrow(DBOSInitializationError);
    expect(() => enterprise.load()).toThrow('npm install @dbos-inc/dbos-enterprise');
    expect(() => enterprise.load()).toThrow("Cannot find module '@dbos-inc/dbos-enterprise'");
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
  });

  test('refused launch leaves nothing behind', async () => {
    await expect(DBOS.launch({ conductorKey: 'test-key' })).rejects.toThrow(DBOSInitializationError);
    expect(DBOS.isInitialized()).toBe(false);

    // Neither teardown nor a fresh launch trips over the refused one.
    await DBOS.shutdown();
    await DBOS.launch();
    expect(DBOS.executorID).toBe('local');
  });

  test('DBOS Cloud always requires the package', async () => {
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
});
