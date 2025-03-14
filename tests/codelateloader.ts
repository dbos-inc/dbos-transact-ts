import { DBOS } from '@dbos-inc/dbos-sdk';
import { generateDBOSTestConfig } from './helpers';

async function main() {
  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);
  await DBOS.launch();
  const modulePath = require.resolve('./codereload');
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  require(modulePath);
  await DBOS.shutdown();
}

main()
  .then()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
