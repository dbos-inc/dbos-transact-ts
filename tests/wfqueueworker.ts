import { DBOS, WorkflowQueue } from '../src';
import { generateDBOSTestConfig } from './helpers';
import { sleepms } from "../src/utils";

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const workerConcurrencyQueue = new WorkflowQueue("workerQ", { workerConcurrency: 1 });

// This declaration is just for registration in DBOS internal operations registry
// eslint-disable-next-line @typescript-eslint/no-unused-vars
class TestWFs {
  @DBOS.workflow()
  static async noop() {
    return Promise.resolve();
  }
}

async function queueEntriesAreCleanedUp() {
  let maxTries = 10;
  let success = false;
  while (maxTries > 0) {
      const r = await DBOS.getWorkflowQueue({});
      if (r.workflows.length === 0) {
          success = true;
          break;
      }
      await sleepms(1000);
      --maxTries;
  }
  return success;
}

async function main() {
  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);
  await DBOS.launch();

  const success = await queueEntriesAreCleanedUp();
  if (!success) {
    throw new Error(`${process.env['DBOS__VMID']} worker detected unhandled workflow queue entries`);
  }

  await DBOS.shutdown();

  process.exit(0);
}

if (require.main === module) {
  main()
    .then(() => {
      process.exit(0);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
