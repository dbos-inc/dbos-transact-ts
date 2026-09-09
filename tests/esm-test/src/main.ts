import { DBOS, WorkflowQueue } from '@dbos-inc/dbos-sdk';
import { serializeError } from 'serialize-error';
import { CjsOps } from './cjs-ops.cjs';

const queue = new WorkflowQueue('esm_test_queue');

class EsmOps {
  @DBOS.step()
  static esmStep(input: string): Promise<string> {
    return Promise.resolve(`step:${input}`);
  }

  // Calling into the CommonJS module proves both module systems share one DBOS registry.
  @DBOS.workflow()
  static async esmWorkflow(input: string): Promise<string> {
    const fromEsm = await EsmOps.esmStep(input);
    const fromCjs = await CjsOps.describeError();
    return `${fromEsm}|${fromCjs}`;
  }
}

DBOS.setConfig({ name: 'esm-test', systemDatabaseUrl: process.env.DBOS_SYSTEM_DATABASE_URL });

// Top-level await, which only an ESM entry point can use.
await DBOS.launch();

const handle = await DBOS.startWorkflow(EsmOps, { queueName: queue.name }).esmWorkflow('queued');
const result = await handle.getResult();
const steps = await DBOS.listWorkflowSteps(handle.workflowID);

await DBOS.shutdown();

console.log(`workflow result: ${result}`);
console.log(`recorded steps: ${steps?.map((step) => step.name).join(',')}`);
console.log(`esm-only dependency: ${String(serializeError(new Error('from esm')).message)}`);
console.log('ESM test completed successfully!');
