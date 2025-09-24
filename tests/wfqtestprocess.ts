import { DBOS, WorkflowQueue } from '../src';
import { generateDBOSTestConfig } from './helpers';

const q = new WorkflowQueue('testq', { concurrency: 1, rateLimit: { limitPerPeriod: 1, periodSec: 1 } });

export class WF {
  @DBOS.workflow()
  static async queuedTask() {
    await DBOS.sleepms(100);
    return 1;
  }

  @DBOS.workflow()
  static async enqueue5Tasks() {
    for (let i = 0; i < 5; ++i) {
      console.log(`Iteration ${i + 1}`);
      const wfh = await DBOS.startWorkflow(WF, { queueName: q.name }).queuedTask();
      await wfh.getResult();
      await DBOS.sleepms(900);

      if (i === 3 && process.env['DIE_ON_PURPOSE']) {
        console.log('CRASH');
        process.exit();
      }
    }
    return 5;
  }

  static x = 5;
}

async function main() {
  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);
  await DBOS.launch();

  await DBOS.withNextWorkflowID('testqueuedwfcrash', async () => {
    await WF.enqueue5Tasks();
  });
  await DBOS.shutdown();
}

if (require.main === module) {
  main()
    .then(() => {})
    .catch((e) => {
      console.log(e);
    });
}
