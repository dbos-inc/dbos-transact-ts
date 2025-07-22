import { DBOS, DBOSConfig } from '../src';
import { DBOSLocalCtx, runWithTopContext } from '../src/context';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

class TempWorkflowTest {
  @DBOS.transaction()
  static async tx_GetWorkflowID() {
    return Promise.resolve(DBOS.workflowID);
  }

  @DBOS.step()
  static async st_GetWorkflowID() {
    return Promise.resolve(DBOS.workflowID);
  }

  @DBOS.transaction()
  static async tx_GetAuth() {
    return Promise.resolve({
      user: DBOS.authenticatedUser,
      roles: DBOS.authenticatedRoles,
    });
  }

  @DBOS.step()
  static async st_GetAuth() {
    return Promise.resolve({
      user: DBOS.authenticatedUser,
      roles: DBOS.authenticatedRoles,
    });
  }

  @DBOS.transaction()
  static async tx_GetRequest() {
    return Promise.resolve(DBOS.request);
  }

  @DBOS.step()
  static async st_GetRequest() {
    return Promise.resolve(DBOS.request);
  }
}

describe('v2api-temp-wf', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig('pg-node');
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('tx_GetWorkflowID', async () => {
    const wfUUID = `tx-${Date.now()}`;
    const actual = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await TempWorkflowTest.tx_GetWorkflowID();
    });
    expect(actual).toBe(wfUUID);
  });

  test('st_GetWorkflowID', async () => {
    const wfUUID = `st-${Date.now()}`;
    const actual = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await TempWorkflowTest.st_GetWorkflowID();
    });
    expect(actual).toBe(wfUUID);
  });

  test('tx_GetAuth', async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await TempWorkflowTest.tx_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

  test('st_GetAuth', async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await TempWorkflowTest.st_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

  test('tx_GetRequest', async () => {
    const ctx: DBOSLocalCtx = {
      request: { requestID: `requestID-${Date.now()}` },
    };
    const actual = await runWithTopContext(ctx, async () => {
      return await TempWorkflowTest.tx_GetRequest();
    });
    expect(actual).toEqual(ctx.request);
  });

  test('st_GetRequest', async () => {
    const ctx: DBOSLocalCtx = {
      request: { requestID: `requestID-${Date.now()}` },
    };
    const actual = await runWithTopContext(ctx, async () => {
      return await TempWorkflowTest.st_GetRequest();
    });
    expect(actual).toEqual(ctx.request);
  });
});
