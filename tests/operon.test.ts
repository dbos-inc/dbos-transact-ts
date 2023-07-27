import { Operon, WorkflowContext, TransactionContext, CommunicatorContext } from "src/";
import { v1 as uuidv1 } from 'uuid';
import axios, { AxiosResponse } from 'axios';

interface OperonKv {
  id: number,
  value: string,
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

describe('operon-tests', () => {
  let operon: Operon;
  const username: string = process.env.DB_USER || 'postgres';

  beforeEach(async () => {
    operon = new Operon();
    await operon.resetOperonTables();
    await operon.pool.query("DROP TABLE IF EXISTS OperonKv;");
    await operon.pool.query("CREATE TABLE IF NOT EXISTS OperonKv (id SERIAL PRIMARY KEY, value TEXT);");
  });

  afterEach(async () => {
    await operon.destroy();
  });


  test('simple-function', async() => {
    const testFunction = async (txnCtxt: TransactionContext, name: string) => {
      const { rows } = await txnCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
      return JSON.stringify(rows[0]);
    };

    const testWorkflow = async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: string = await workflowCtxt.transaction(testFunction, name);
      return funcResult;
    };

    const workflowResult: string = await operon.workflow(testWorkflow, {}, username);

    expect(JSON.parse(workflowResult)).toEqual({"current_user": username});
  });


  test('return-void', async() => {
    const testFunction = async (txnCtxt: TransactionContext) => {
      void txnCtxt;
      await sleep(10);
      return;
    };
    const workflowUUID = uuidv1();
    await operon.transaction(testFunction, {workflowUUID: workflowUUID});
    await operon.transaction(testFunction, {workflowUUID: workflowUUID});
    await operon.transaction(testFunction, {workflowUUID: workflowUUID});
  });

  test('tight-loop', async() => {
    const testFunction = async (txnCtxt: TransactionContext, name: string) => {
      const { rows }= await txnCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
      return JSON.stringify(rows[0]);
    };

    const testWorkflow = async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: string = await workflowCtxt.transaction(testFunction, name);
      return funcResult;
    };

    for (let i = 0; i < 100; i++) {
      const workflowResult: string = await operon.workflow(testWorkflow, {}, username);
      expect(JSON.parse(workflowResult)).toEqual({"current_user": username});
    }
  });
  

  test('abort-function', async() => {
    const testFunction = async (txnCtxt: TransactionContext, name: string) => {
      const { rows }= await txnCtxt.client.query<OperonKv>("INSERT INTO OperonKv(value) VALUES ($1) RETURNING id", [name]);
      if (name === "fail") {
        await txnCtxt.rollback();
      }
      return Number(rows[0].id);
    };

    const testFunctionRead = async (txnCtxt: TransactionContext, id: number) => {
      const { rows }= await txnCtxt.client.query<OperonKv>("SELECT id FROM OperonKv WHERE id=$1", [id]);
      if (rows.length > 0) {
        return Number(rows[0].id);
      } else {
        // Cannot find, return a negative number.
        return -1;
      }
    };

    const testWorkflow = async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: number = await workflowCtxt.transaction(testFunction, name);
      const checkResult: number = await workflowCtxt.transaction(testFunctionRead, funcResult);
      return checkResult;
    };

    for (let i = 0; i < 10; i++) {
      const workflowResult: number = await operon.workflow(testWorkflow, {}, username);
      expect(workflowResult).toEqual(i + 1);
    }
    
    // Should not appear in the database.
    const workflowResult: number = await operon.workflow(testWorkflow, {}, "fail");
    expect(workflowResult).toEqual(-1);
  });

  test('multiple-aborts', async() => {
    const testFunction = async (txnCtxt: TransactionContext, name: string) => {
      const { rows }= await txnCtxt.client.query<OperonKv>("INSERT INTO OperonKv(value) VALUES ($1) RETURNING id", [name]);
      if (name !== "fail") {
        // Recursively call itself so we have multiple rollbacks.
        await testFunction(txnCtxt, "fail");
      }
      await txnCtxt.rollback();
      return Number(rows[0].id);
    };

    const testFunctionRead = async (txnCtxt: TransactionContext, id: number) => {
      const { rows }= await txnCtxt.client.query<OperonKv>("SELECT id FROM OperonKv WHERE id=$1", [id]);
      if (rows.length > 0) {
        return Number(rows[0].id);
      } else {
        // Cannot find, return a negative number.
        return -1;
      }
    };

    const testWorkflow = async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: number = await workflowCtxt.transaction(testFunction, name);
      const checkResult: number = await workflowCtxt.transaction(testFunctionRead, funcResult);
      return checkResult;
    };

    // Should not appear in the database.
    const workflowResult: number = await operon.workflow(testWorkflow, {}, "test");
    expect(workflowResult).toEqual(-1);
  });


  test('oaoo-simple', async() => {
    const testFunction = async (txnCtxt: TransactionContext, name: string) => {
      const { rows }= await txnCtxt.client.query<OperonKv>("INSERT INTO OperonKv(value) VALUES ($1) RETURNING id", [name]);
      if (name === "fail") {
        await txnCtxt.rollback();
      }
      return Number(rows[0].id);
    };

    const testFunctionRead = async (txnCtxt: TransactionContext, id: number) => {
      const { rows }= await txnCtxt.client.query<OperonKv>("SELECT id FROM OperonKv WHERE id=$1", [id]);
      if (rows.length > 0) {
        return Number(rows[0].id);
      } else {
        // Cannot find, return a negative number.
        return -1;
      }
    };

    const testWorkflow = async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: number = await workflowCtxt.transaction(testFunction, name);
      const checkResult: number = await workflowCtxt.transaction(testFunctionRead, funcResult);
      return checkResult;
    };

    let workflowResult: number;
    const uuidArray: string[] = [];
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidv1();
      uuidArray.push(workflowUUID);
      workflowResult = await operon.workflow(testWorkflow, {workflowUUID: workflowUUID}, username);
      expect(workflowResult).toEqual(i + 1);
    }
    // Should not appear in the database.
    const failUUID: string = uuidv1();
    workflowResult = await operon.workflow(testWorkflow, {workflowUUID: failUUID}, "fail");
    expect(workflowResult).toEqual(-1);

    // Rerunning with the same workflow UUID should return the same output.
    for (let i = 0; i < 10; i++) {
      const workflowUUID: string = uuidArray[i];
      const workflowResult: number = await operon.workflow(testWorkflow, {workflowUUID: workflowUUID}, username);
      expect(workflowResult).toEqual(i + 1);
    }
    // Given the same workflow UUID but different input, should return the original execution.
    workflowResult = await operon.workflow(testWorkflow, {workflowUUID: failUUID}, "hello");
    expect(workflowResult).toEqual(-1);
  });


  test('simple-communicator', async() => {
    const testCommunicator = async (commCtxt: CommunicatorContext, name: string) => {
      const response1 = await axios.post<AxiosResponse>('https://postman-echo.com/post', {"name": name});
      const response2 = await axios.post<AxiosResponse>('https://postman-echo.com/post', response1.data.data);
      return JSON.stringify(response2.data);
    };

    const testWorkflow = async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult = await workflowCtxt.external(testCommunicator, {}, name);
      return funcResult ?? "error";
    };

    const workflowUUID: string = uuidv1();

    let result: string = await operon.workflow(testWorkflow, {workflowUUID: workflowUUID}, 'qianl15');
    expect(JSON.parse(result)).toMatchObject({data: { "name" : "qianl15"}});

    // Test OAOO. Should return the original result.
    result = await operon.workflow(testWorkflow, {workflowUUID: workflowUUID}, 'peter');
    expect(JSON.parse(result)).toMatchObject({data: { "name" : "qianl15"}});
  });

  test('simple-workflow-notifications', async() => {

    const receiveWorkflow = async(ctxt: WorkflowContext) => {
      const test = await ctxt.recv("test", 2) as number;
      const fail = await ctxt.recv("fail", 0) ;
      return test === 0 && fail === null;
    }

    const sendWorkflow = async(ctxt: WorkflowContext) => {
      return await ctxt.send("test", 0);
    }

    const workflowUUID = uuidv1();
    const promise = operon.workflow(receiveWorkflow, {workflowUUID: workflowUUID});
    const send = await operon.workflow(sendWorkflow, {});
    expect(send).toBe(true);
    expect(await promise).toBe(true);
    const retry = await operon.workflow(receiveWorkflow, {workflowUUID: workflowUUID});
    expect(retry).toBe(true);
  });

  test('simple-operon-notifications', async() => {
    // Send and have a receiver waiting.
    const promise = operon.recv({}, "test", 2);
    const send = await operon.send({}, "test", 123);
    expect(send).toBe(true);
    expect(await promise).toBe(123);

    // Send and then receive.
    await expect(operon.send({}, "test2", 456)).resolves.toBe(true);
    await sleep(10);
    await expect(operon.recv({}, "test2", 1)).resolves.toBe(456);
  });

  test('notification-oaoo',async () => {
    const sendWorkflowUUID = uuidv1();
    const recvWorkflowUUID = uuidv1();
    const promise = operon.recv({workflowUUID: recvWorkflowUUID}, "test", 1);
    const send = await operon.send({workflowUUID: sendWorkflowUUID}, "test", 123);
    expect(send).toBe(true);

    expect(await promise).toBe(123);

    // Send again with the same UUID but different input.
    // Even we sent it twice, it should still be 123.
    await expect(operon.send({workflowUUID: sendWorkflowUUID}, "test", 123)).resolves.toBe(true);

    await expect(operon.recv({workflowUUID: recvWorkflowUUID}, "test", 1)).resolves.toBe(123);

    // Receive again with the same workflowUUID, should get the same result.
    await expect(operon.recv({workflowUUID: recvWorkflowUUID}, "test", 1)).resolves.toBe(123);

    // Receive again with the different workflowUUID.
    await expect(operon.recv({}, "test", 2)).resolves.toBeNull();
  });
});

