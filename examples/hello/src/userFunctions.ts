import { TransactionContext, WorkflowContext, OperonTransaction, OperonWorkflow, GetApi, OperonCommunicator, CommunicatorContext } from 'operon'

export class Hello {

  @OperonCommunicator()
  static async helloExternal(commCtxt: CommunicatorContext, encodedName: string) {
    commCtxt.log(`decoding ${encodedName} via httpbin`)
    try {
      const url = `https://httpbin.org/base64/${encodeURIComponent(encodedName)}`;
      const res = await fetch(url);
      return await res.text();
    } catch {
      commCtxt.warn("httpbin/base64 failed, decoding locally");
      return atob(encodedName);
    }
  }
  
  @OperonTransaction()
  static async helloFunction(txnCtxt: TransactionContext, name: string) {
    const greeting = `Hello, ${name}!`
    const { rows } = await txnCtxt.pgClient.query<{greeting_id: number}>("INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id", [greeting])
    txnCtxt.log(`Inserted greeting ${rows[0].greeting_id}: ${greeting}`)
    return `Greeting ${rows[0].greeting_id}: ${greeting}`;
  }

  @OperonWorkflow()
  @GetApi('/greeting/:name')
  static async helloWorkflow(wfCtxt: WorkflowContext, name: string) {
    
    // Added a new 'proxy' method to WorkflowContext that returns a developer-friendly object that enables direct invocation
    // proxy object only includes methods decorated with @OperonTransaction + converts the direct call to an indirect call thru context.transaction
    // Plan to add @OperonCommunicator/external support before exiting draft

    // TS mapping types are used create a developer friendly type signature for proxy's return value. See the top of workflow.ts for more info

    wfCtxt.log("Hello, workflow!");
    const encodedName = btoa(name);
    const decodedName = await wfCtxt.external(Hello.helloExternal, encodedName);
    return await wfCtxt.proxy(Hello).helloFunction(decodedName);
  }
}

