import { TransactionContext, WorkflowContext, OperonTransaction, OperonWorkflow, GetApi, OperonCommunicator, CommunicatorContext, HandlerContext } from 'operon'

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
    const { rows } = await txnCtxt.pgClient.query<{ greeting_id: number }>("INSERT INTO OperonHello(greeting) VALUES ($1) RETURNING greeting_id", [greeting])
    txnCtxt.log(`Inserted greeting ${rows[0].greeting_id}: ${greeting}`)
    return `Greeting ${rows[0].greeting_id}: ${greeting}`;
  }

  @OperonWorkflow()
  static async helloWorkflow(wfCtxt: WorkflowContext, name: string) {
    wfCtxt.log("Hello, workflow!");
    const encodedName = btoa(name);
    const decodedName = await wfCtxt.invoke(Hello).helloExternal(encodedName);
    return await wfCtxt.invoke(Hello).helloFunction(decodedName);
  }

  @GetApi('/greeting/:name')
  static async helloEndpoint(ctx: HandlerContext, name: string) {
    ctx.log("helloEndpoint");
    return await ctx.invoke(Hello).helloWorkflow(name).then(x => x.getResult());
  }
}

