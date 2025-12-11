import { DBOS } from '@dbos-inc/dbos-sdk';
import { generateDBOSTestConfig } from './helpers';

@DBOS.className('TestClass')
class TestClass2 {
  @DBOS.workflow()
  static async workflow2() {
    return Promise.resolve();
  }
}

DBOS.registerWorkflow(async () => Promise.resolve('WF'), { className: 'TestClass', name: 'wf' });

async function main() {
  new TestClass2();

  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);
  await DBOS.launch();
  await DBOS.shutdown();
}

main()
  .then()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
