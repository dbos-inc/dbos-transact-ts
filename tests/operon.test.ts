/* eslint-disable @typescript-eslint/no-unsafe-member-access */

import { Operon, FunctionContext, registerFunction,registerWorkflow, WorkflowContext } from "src/";

describe('testing operon', () => {
  test('should run a simple workflow', async() => {
    const username = process.env.DB_USER || 'postgres';
    const operon: Operon = new Operon({
      user: username,
      password: process.env.DB_PASSWORD || 'dbos',
      connectionTimeoutMillis:  3000
    });
    await operon.resetOperonTables();

    const testFunction = registerFunction(async (functionCtxt: FunctionContext, name: string) => {
      const { rows } = await functionCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
      return JSON.stringify(rows[0]);
    });

    const testWorkflow = registerWorkflow(async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: string = await testFunction(workflowCtxt, name);
      return funcResult;
    });

    const workflowResult: string = await testWorkflow(operon, {}, username);

    expect(JSON.parse(workflowResult)).toEqual({"current_user": username});
    await operon.pool.end();
  });


  test('tight loop function calls', async() => {
    const username = process.env.DB_USER || 'postgres';
    const operon: Operon = new Operon({
      user: username,
      password: process.env.DB_PASSWORD || 'dbos',
      connectionTimeoutMillis:  3000,
      max: 10
    });
    await operon.resetOperonTables();

    const testFunction = registerFunction(async (functionCtxt: FunctionContext, name: string) => {
      const { rows }= await functionCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
      return JSON.stringify(rows[0]);
    });

    const testWorkflow = registerWorkflow(async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: string = await testFunction(workflowCtxt, name);
      return funcResult;
    });

    for (let i = 0; i < 100; i++) {
      const workflowResult: string = await testWorkflow(operon, {idempotencyKey: String(i)}, username);
      expect(JSON.parse(workflowResult)).toEqual({"current_user": username});
    }

    await operon.pool.end();
  });
  

  test('should abort function properly', async() => {
    const username = process.env.DB_USER || 'postgres';
    const operon: Operon = new Operon({
      user: username,
      password: process.env.DB_PASSWORD || 'dbos',
      connectionTimeoutMillis:  3000,
      max: 2
    });
    await operon.resetOperonTables();

    await operon.pool.query("DROP TABLE IF EXISTS OperonKv;");
    await operon.pool.query("CREATE TABLE IF NOT EXISTS OperonKv (id SERIAL PRIMARY KEY, value TEXT);");

    const testFunction = registerFunction(async (functionCtxt: FunctionContext, name: string) => {
      const { rows }= await functionCtxt.client.query("INSERT INTO OperonKv(value) VALUES ($1) RETURNING id", [name]);
      if (name === "fail") {
        await functionCtxt.rollback();
      }
      return Number(rows[0].id);
    });

    const testFunctionRead = registerFunction(async (functionCtxt: FunctionContext, id: number) => {
      const { rows }= await functionCtxt.client.query("SELECT id FROM OperonKv WHERE id=$1", [id]);
      if (rows.length > 0) {
        return Number(rows[0].id);
      } else {
        // Cannot find, return a negative number.
        return -1;
      }
    });

    const testWorkflow = registerWorkflow(async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: number = await testFunction(workflowCtxt, name);
      const checkResult: number = await testFunctionRead(workflowCtxt, funcResult);
      return checkResult;
    });

    for (let i = 0; i < 10; i++) {
      const workflowResult: number = await testWorkflow(operon, {idempotencyKey: String(i)}, username);
      expect(workflowResult).toEqual(i + 1);
    }
    
    // Should not appear in the database.
    const workflowResult: number = await testWorkflow(operon, {}, "fail");
    expect(workflowResult).toEqual(-1);

    await operon.pool.end();
  });
});
