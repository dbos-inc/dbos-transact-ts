import { DBOS } from '@dbos-inc/dbos-sdk';
import { generateDBOSTestConfig } from './helpers';

class TestClass {
  @DBOS.workflow()
  static async workflow() {
    return Promise.resolve();
  }
}

@DBOS.className('TestClass')
class TestClass2 {
  @DBOS.workflow()
  static async workflow2() {
    return Promise.resolve();
  }
}

async function main() {
  new TestClass();
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
