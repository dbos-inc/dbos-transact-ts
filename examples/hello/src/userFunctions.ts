import { TransactionContext, WorkflowContext, OperonTransaction, OperonWorkflow, GetApi } from 'operon'

export class Hello {

  @OperonTransaction()
  static async helloFunction (txnCtxt: TransactionContext, name: string)  {
    const greeting = `Hello, ${name}!`
    const { rows } = await txnCtxt.pgClient.query("INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id", [greeting])
    return `Greeting ${rows[0].greeting_id}: ${greeting}`;
  }

  @OperonWorkflow()
  @GetApi('/greeting/:name')
  static async helloWorkflow(workflowCtxt: WorkflowContext, name: string) {
    return await workflowCtxt.transaction(Hello.helloFunction, name);
  }

}




