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
    
    // WorkflowContext has a new 'proxy' method that returns a developer-friendly object that enables direct invocation of transaction and communicator method;
    // The proxy method takes a class of static Operon operations and returns an object that includes *only* transaction and communicator methods 
    // that can be invoked directly: proxy.someMethod(param1, param2) instead of wfCtxt.transaction(Class.someMethod, param1, param2);

    // TS mapping types are used to filter out non-transaction and communicator methods as well as to remove the Transaction/CommunicatorContext parameter
    // from the method signature.

    const hello = wfCtxt.proxy(Hello);
    wfCtxt.log("Hello, workflow!");
    const encodedName = btoa(name);
    const decodedName = await hello.helloExternal(encodedName);
    return await hello.helloFunction(decodedName);
  }
}

