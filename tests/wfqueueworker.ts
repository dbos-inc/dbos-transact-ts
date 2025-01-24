import { DBOS, WorkflowQueue } from '../src';
import { generateDBOSTestConfig } from './helpers';
import { sleepms } from "../src/utils";

const workerConcurrencyQueue = new WorkflowQueue("workerQ", { worker_concurrency: 1 });

class TestWFs
{
      @DBOS.workflow()
    static async noop() {
        return Promise.resolve();
    }
}

async function main() {
  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);
  await DBOS.launch();

  // Sleep for several poll intervals
  await sleepms(5000);

  await DBOS.shutdown();
}

if (require.main === module) {
  main().then(()=>{}).catch((e)=>{console.log(e)});
}
