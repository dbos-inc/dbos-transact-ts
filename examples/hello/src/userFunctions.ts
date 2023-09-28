import { TransactionContext, WorkflowContext, OperonTransaction, OperonWorkflow, GetApi, OperonContext, OperonTransactionFunction } from 'operon'
import { getRegisteredOperations } from '../../../dist/src/decorators';

export class Hello {

  @OperonTransaction()
  static async helloFunction(ctxt: TransactionContext, name: string) {
    const greeting = `Hello, ${name}!`
    const { rows } = await ctxt.pgClient.query<{ greeting_id: number }>("INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id", [greeting])
    return `Greeting ${rows[0].greeting_id}: ${greeting}`;
  }

  @OperonWorkflow()
  @GetApi('/greeting/:name')
  static async helloWorkflow(ctxt: WorkflowContext, name: string) {
    
    // Added a new 'proxy' method to WorkflowContext that returns a developer-friendly object that enables direct invocation
    // proxy object only includes methods decorated with @OperonTransaction + converts the direct call to an indirect call thru context.transaction
    // Plan to add @OperonCommunicator/external support before exiting draft

    // TS mapping types are used create a developer friendly type signature for proxy's return value. See the top of workflow.ts for more info

    return await ctxt.proxy(Hello).helloFunction(name);
  }
}

