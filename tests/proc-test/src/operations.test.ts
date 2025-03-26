import { DBOS, parseConfigFile } from '@dbos-inc/dbos-sdk';
import { StoredProcTest } from './operations';
import { v1 as uuidv1 } from 'uuid';

import { workflow_status } from '@dbos-inc/dbos-sdk/schemas/system_db_schema';
import { transaction_outputs } from '@dbos-inc/dbos-sdk/schemas/user_db_schema';
import { DBOSConfig } from '@dbos-inc/dbos-sdk';
import { DBOSConfigInternal } from '@dbos-inc/dbos-sdk/dist/src/dbos-executor';
import { Client, ClientConfig } from 'pg';
import { runWithTopContext } from '../../../dist/src/context';
import { DebugMode } from '../../../src/dbos-executor';

async function runSql<T>(config: ClientConfig, func: (client: Client) => Promise<T>) {
  const client = new Client(config);
  try {
    await client.connect();
    return await func(client);
  } finally {
    await client.end();
  }
}

async function dropLocalProcs() {
  const localProcs = ['getGreetCountLocal', 'helloProcedureLocal', 'helloProcedure_v2_local'];
  const sqlDropLocalProcs = localProcs
    .map(
      (proc) => `DROP ROUTINE IF EXISTS "StoredProcTest_${proc}_p"; DROP ROUTINE IF EXISTS "StoredProcTest_${proc}_f";`,
    )
    .join('\n');
  await DBOS.queryUserDB(sqlDropLocalProcs);
}

describe('stored-proc-v2-test', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    [config] = parseConfigFile();
    DBOS.setConfig(config);
    await DBOS.launch();
    try {
      await dropLocalProcs();
    } finally {
      await DBOS.shutdown();
    }
  });

  beforeEach(async () => {
    DBOS.setConfig(config);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('wf_GetWorkflowID', async () => {
    const wfUUID = `wf-${Date.now()}`;
    const result = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await StoredProcTest.wf_GetWorkflowID();
    });

    expect(result).toBe(wfUUID);
  });

  test('sp_GetWorkflowID', async () => {
    const wfUUID = `sp-${Date.now()}`;
    const result = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await StoredProcTest.sp_GetWorkflowID();
    });

    expect(result).toBe(wfUUID);
  });

  test('sp_GetAuth', async () => {
    const now = `${Date.now()}`;
    const user = `user-${now}`;
    const roles = [`role-1-${now}`, `role-2-${now}`, `role-3-${now}`];
    const actual = await DBOS.withAuthedContext(user, roles, async () => {
      return await StoredProcTest.sp_GetAuth();
    });
    expect(actual).toEqual({ user, roles });
  });

  test('sp_GetRequest', async () => {
    const ctx = {
      request: { requestID: `requestID-${Date.now()}` },
    };
    const actual = await runWithTopContext(ctx, async () => {
      return await StoredProcTest.sp_GetRequest();
    });
    expect(actual.requestID).toEqual(ctx.request.requestID);
  });

  test('test-txAndProcGreetingWorkflow_v2', async () => {
    const wfUUID = uuidv1();
    const user = `txAndProcWFv2_${Date.now()}`;
    const res = await DBOS.withNextWorkflowID(wfUUID, async () => {
      return await StoredProcTest.txAndProcGreetingWorkflow_v2(user);
    });

    expect(res.count).toBe(0);
    expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expect(res.local).toMatch(`Hello, ${user}_local! You have been greeted 1 times.`);

    const dbClient = new Client(config.poolConfig);
    try {
      await dbClient.connect();
      const { rows: txRows } = await dbClient.query<transaction_outputs>(
        'SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1',
        [wfUUID],
      );
      expect(txRows.length).toBe(3);
      expect(txRows[0].function_id).toBe(0);
      expect(txRows[0].output).toBe('0');
      expectNullResult(txRows[0].error);

      expect(txRows[1].function_id).toBe(1);
      expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
      expectNullResult(txRows[1].error);

      expect(txRows[2].function_id).toBe(2);
      expect(txRows[2].output).toMatch(`Hello, ${user}_local! You have been greeted 1 times.`);
      expectNullResult(txRows[2].error);
    } finally {
      await dbClient.end();
    }
  });
});

describe('stored-proc-test', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    [config] = parseConfigFile();
    DBOS.setConfig(config);
    await DBOS.launch();
    await dropLocalProcs();
  });

  afterAll(async () => {
    await DBOS.shutdown();
  });

  test('test-procReplay', async () => {
    const now = Date.now();
    const user = `procReplay-${now}`;
    const res = await StoredProcTest.helloProcedure(user);
    expect(res).toMatch(`Hello, ${user}! You have been greeted 1 times.`);

    const wfUUID = `procReplay-wfid-${now}`;
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      expect(await StoredProcTest.getGreetCount(user)).toBe(1);
    });

    await DBOS.retrieveWorkflow(wfUUID).getResult();

    const statuses = await runSql({ ...config.poolConfig, database: config.system_database }, async (client) => {
      const { rows } = await client.query<workflow_status>(
        'SELECT * FROM dbos.workflow_status WHERE workflow_uuid = $1',
        [wfUUID],
      );
      return rows;
    });

    expect(statuses.length).toBe(1);
    expect(statuses[0].status).toBe('SUCCESS');
    expect(statuses[0].output).toBe(JSON.stringify(1));
    expectNullResult(statuses[0].error);

    await DBOS.withNextWorkflowID(wfUUID, async () => {
      expect(await StoredProcTest.getGreetCount(user)).toBe(1);
    });

    const txRows = await DBOS.queryUserDB<transaction_outputs>(
      'SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1',
      [wfUUID],
    );
    expect(txRows.length).toBe(1);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe('1');
    expectNullResult(txRows[0].error);
  });

  test('test-procGreetingWorkflow', async () => {
    const wfUUID = uuidv1();
    const user = `procWF_${Date.now()}`;
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await StoredProcTest.procGreetingWorkflow(user);
      expect(res.count).toBe(0);
      expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
      expect(res.rowCount).toBeGreaterThan(0);

      const txRows = await DBOS.queryUserDB<transaction_outputs>(
        'SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1',
        [wfUUID],
      );
      expect(txRows.length).toBe(3);
      expect(txRows[0].function_id).toBe(0);
      expect(txRows[0].output).toBe('0');
      expectNullResult(txRows[0].error);

      expect(txRows[1].function_id).toBe(1);
      expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
      expectNullResult(txRows[1].error);

      expect(txRows[2].function_id).toBe(2);
      expect(txRows[2].output).toEqual(`${res.rowCount}`);
      expectNullResult(txRows[1].error);
    });
  });

  test('test-debug-procGreetingWorkflow', async () => {
    const wfUUID = uuidv1();
    const user = `debugProcWF_${Date.now()}`;
    let res: { count: number; greeting: string; rowCount: number } | undefined = undefined;
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      res = await StoredProcTest.procGreetingWorkflow(user);
      expect(res.count).toBe(0);
      expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
      expect(res.rowCount).toBeGreaterThan(0);
    });
    await DBOS.shutdown();

    // Temporarily use debug
    const debugConfig = { ...config, debugMode: true };
    DBOS.setConfig(debugConfig);
    await DBOS.launch({ debugMode: DebugMode.ENABLED });
    try {
      await DBOS.withNextWorkflowID(wfUUID, async () => {
        await expect(StoredProcTest.procGreetingWorkflow(user)).resolves.toEqual(res);
      });
      await expect(DBOS.executeWorkflowById(wfUUID).then((x) => x.getResult())).resolves.toEqual(res);
    } finally {
      await DBOS.shutdown();
    }

    DBOS.setConfig(config);
    await DBOS.launch();
  });

  test('test-txGreetingWorkflow', async () => {
    const wfUUID = uuidv1();

    const user = `txWF_${Date.now()}`;
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await StoredProcTest.txAndProcGreetingWorkflow(user);
      expect(res.count).toBe(0);
      expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    });

    const txRows = await DBOS.queryUserDB<transaction_outputs>(
      'SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1',
      [wfUUID],
    );
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe('0');
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);
  });

  test('test-procLocalGreetingWorkflow', async () => {
    const wfUUID = uuidv1();
    const user = `procLocalWF_${Date.now()}`;
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await StoredProcTest.procLocalGreetingWorkflow(user);
      expect(res.count).toBe(0);
      expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
      expect(res.rowCount).toBeGreaterThan(0);

      const txRows = await DBOS.queryUserDB<transaction_outputs>(
        'SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1',
        [wfUUID],
      );
      expect(txRows.length).toBe(3);
      expect(txRows[0].function_id).toBe(0);
      expect(txRows[0].output).toBe('0');
      expectNullResult(txRows[0].error);

      expect(txRows[1].function_id).toBe(1);
      expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
      expectNullResult(txRows[1].error);

      expect(txRows[2].function_id).toBe(2);
      expect(txRows[2].output).toEqual(`${res.rowCount}`);
      expectNullResult(txRows[1].error);
    });
  });

  test('test-txAndProcGreetingWorkflow', async () => {
    const wfUUID = uuidv1();
    const user = `txAndProcWF_${Date.now()}`;
    await DBOS.withNextWorkflowID(wfUUID, async () => {
      const res = await StoredProcTest.txAndProcGreetingWorkflow(user);
      expect(res.count).toBe(0);
      expect(res.greeting).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    });

    const txRows = await DBOS.queryUserDB<transaction_outputs>(
      'SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1',
      [wfUUID],
    );
    expect(txRows.length).toBe(2);
    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toBe('0');
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[1].error);
  });

  test('test-procErrorWorkflow', async () => {
    const wfid = uuidv1();
    const user = `procErrorWF_${Date.now()}`;

    await DBOS.withNextWorkflowID(wfid, async () => {
      await expect(StoredProcTest.procErrorWorkflow(user)).rejects.toThrow('This is a test error');
    });

    const txRows = await DBOS.queryUserDB<transaction_outputs>(
      'SELECT * FROM dbos.transaction_outputs WHERE workflow_uuid=$1',
      [wfid],
    );

    expect(txRows.length).toBe(3);

    expect(txRows[0].function_id).toBe(0);
    expect(txRows[0].output).toMatch(`Hello, ${user}! You have been greeted 1 times.`);
    expectNullResult(txRows[0].error);

    expect(txRows[1].function_id).toBe(1);
    expect(txRows[1].output).toMatch(`1`);
    expectNullResult(txRows[1].error);

    expect(txRows[2].function_id).toBe(2);
    expectNullResult(txRows[2].output);
    expect(txRows[2].error).not.toBeNull();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    expect(JSON.parse(txRows[2].error!).message).toMatch('This is a test error');
  });
});

function expectNullResult(result: string | null) {
  if (typeof result === 'string') {
    expect(JSON.parse(result)).toBeNull();
  } else {
    expect(result).toBeNull();
  }
}
