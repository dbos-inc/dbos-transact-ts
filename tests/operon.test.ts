import { Operon, FunctionContext, registerFunction,registerWorkflow, WorkflowContext } from "src/";

describe('testing operon', () => {
  test('should run a simple workflow', async() => {
    const operon: Operon = new Operon({
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'dbos',
      connectionTimeoutMillis:  3000
    });

    const testFunction = registerFunction(async (functionCtxt: FunctionContext, name: string) => {
      console.log("hello from function: " + name);
      const { rows }= await functionCtxt.client.query('select current_user;');
      return JSON.stringify(rows[0]);
    });

    const testWorkflow = registerWorkflow(async (workflowCtxt: WorkflowContext, name: string) => {
      console.log("hello from workflow: " + name);
      const funcResult: string = await testFunction(workflowCtxt, name);
      console.log("workflow got function output: " + funcResult);
      return funcResult;
    });

    const workflowResult: string = await testWorkflow(operon, 'dbos');
    console.log(workflowResult);

    expect(JSON.parse(workflowResult)).toEqual({"current_user": process.env.DB_USER || 'postgres'});
    
    await operon.pool.end();
  });
})
