import { DBOS } from '@dbos-inc/dbos-sdk';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

class WF {
  @DBOS.step()
  static async loggingStep() {
    DBOS.logger.info(`Info: Step should be logged`);
    return Promise.resolve(1);
  }

  @DBOS.transaction()
  static async loggingTransaction() {
    DBOS.logger.info(`Info: Transaction should be logged`);
    return Promise.resolve(2);
  }

  @DBOS.workflow()
  static async loggingWorkflow() {
    DBOS.logger.info(`Info: WFID should be logged`);
    return (await WF.loggingStep()) + (await WF.loggingTransaction());
  }
}

async function main() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb({ ...config, logLevel: 'debug', addContextMetadata: true });

  DBOS.setConfig({ ...config, addContextMetadata: true });
  await DBOS.launch();
  await DBOS.withNextWorkflowID('loggerWorkflowId', async () => {
    DBOS.logger.info(`The computed answer is ${await WF.loggingWorkflow()}`);
  });
  await DBOS.shutdown();
}

main()
  .then()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
