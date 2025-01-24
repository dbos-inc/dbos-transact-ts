import { DBOS } from '../src';
import { generateDBOSTestConfig } from './helpers';
import { sleepms } from "../src/utils";

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
