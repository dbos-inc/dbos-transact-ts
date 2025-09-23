import { DBOS } from '@dbos-inc/dbos-sdk';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

class WF {
  @DBOS.step()
  static async loggingStep() {
    DBOS.logger.info(`Info: Step should be logged`);
    return Promise.resolve(1);
  }

  @DBOS.workflow()
  static async loggingWorkflow() {
    DBOS.logger.info(`Info: WFID should be logged`);
    return await WF.loggingStep();
  }
}

async function main() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestSysDb({ ...config, logLevel: 'debug', addContextMetadata: true });

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
