import { DBOS, DBOSConfig } from "../src";
import { generateDBOSTestConfig, setUpDBOSTestDb } from "./helpers";

class TempWorkflowTest {
  @DBOS.transaction()
  static async tx_GetWorkflowID() {
    return Promise.resolve(DBOS.workflowID);
  }

  @DBOS.step()
  static async st_GetWorkflowID() {
    return Promise.resolve(DBOS.workflowID);
  }

  @DBOS.workflow()
  static async wf_GetWorkflowID() {
    return Promise.resolve(DBOS.workflowID);
  }

  @DBOS.workflow()
  static async wrap_tx_GetWorkflowID() {
    return TempWorkflowTest.tx_GetWorkflowID();
  }

  @DBOS.workflow()
  static async wrap_st_GetWorkflowID() {
    return TempWorkflowTest.st_GetWorkflowID();
  }

  @DBOS.workflow()
  static async wf_GetAuth() {
    return {
      user: DBOS.authenticatedUser,
      roles: DBOS.authenticatedRoles,
    }
  }

  @DBOS.transaction()
  static async tx_GetAuth() {
    return {
      user: DBOS.authenticatedUser,
      roles: DBOS.authenticatedRoles,
    }
  }

  @DBOS.step()
  static async st_GetAuth() {
    return {
      user: DBOS.authenticatedUser,
      roles: DBOS.authenticatedRoles,
    }
  }

  @DBOS.workflow()
  static async wrap_tx_GetAuth() {
    return TempWorkflowTest.tx_GetAuth();
  }

  @DBOS.workflow()
  static async wrap_st_GetAuth() {
    return TempWorkflowTest.st_GetAuth();
  }
}

describe("v2api-temp-wf", () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestDb(config);
  });

  beforeEach(async () => {
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test("wf_GetWorkflowID", async () => {
    const wfUUID = `wf-${Date.now()}`;
    const actual = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await TempWorkflowTest.wf_GetWorkflowID();
    });
    expect(actual).toBe(wfUUID);
  });

  test("tx_GetWorkflowID", async () => {
    const wfUUID = `tx-${Date.now()}`;
    const actual = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await TempWorkflowTest.tx_GetWorkflowID();
    });
    expect(actual).toBe(wfUUID);
  });

  test("st_GetWorkflowID", async () => {
    const wfUUID = `st-${Date.now()}`;
    const actual = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await TempWorkflowTest.st_GetWorkflowID();
    });
    expect(actual).toBe(wfUUID);
  });

  test("wrap_tx_GetWorkflowID", async () => {
    const wfUUID = `wtx-${Date.now()}`;
    const actual = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await TempWorkflowTest.wrap_tx_GetWorkflowID();
    });
    expect(actual).toBe(wfUUID);
  });

  test("wrap_st_GetWorkflowID", async () => {
    const wfUUID = `wst-${Date.now()}`;

    const actual = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await TempWorkflowTest.wrap_st_GetWorkflowID();
    });
    expect(actual).toBe(wfUUID);
  });

  test("wf_GetAuth", async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await TempWorkflowTest.wf_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

  test("tx_GetAuth", async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await TempWorkflowTest.tx_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

  test("st_GetAuth", async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await TempWorkflowTest.st_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

  test("wrap_tx_GetAuth", async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await TempWorkflowTest.wrap_tx_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

  test("wrap_st_GetAuth", async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await TempWorkflowTest.wrap_st_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

});
