import { Operon, FunctionContext, registerFunction,registerWorkflow, WorkflowContext } from "src/";

describe('testing operon', () => {
  test('should run a simple workflow', async() => {
    const username = process.env.DB_USER || 'postgres';
    const operon: Operon = new Operon({
      user: username,
      password: process.env.DB_PASSWORD || 'dbos',
      connectionTimeoutMillis:  3000
    });

    const testFunction = registerFunction(async (functionCtxt: FunctionContext, name: string) => {
      const { rows }= await functionCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
      return JSON.stringify(rows[0]);
    });

    const testWorkflow = registerWorkflow(async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: string = await testFunction(workflowCtxt, name);
      return funcResult;
    });

    const workflowResult: string = await testWorkflow(operon, username);

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

    const testFunction = registerFunction(async (functionCtxt: FunctionContext, name: string) => {
      const { rows }= await functionCtxt.client.query(`select current_user from current_user where current_user=$1;`, [name]);
      return JSON.stringify(rows[0]);
    });

    const testWorkflow = registerWorkflow(async (workflowCtxt: WorkflowContext, name: string) => {
      const funcResult: string = await testFunction(workflowCtxt, name);
      return funcResult;
    });

    for (let i = 0; i < 100; i++) {
      const workflowResult: string = await testWorkflow(operon, username);
      expect(JSON.parse(workflowResult)).toEqual({"current_user": username});
    }
    await operon.pool.end();
  });
})
