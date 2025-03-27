import { exit } from 'process';
import { DBOS, WorkflowQueue } from '../src';
import { generateDBOSTestConfig } from './helpers';

class ClientTest {
  @DBOS.workflow()
  static async enqueueTest(
    numVal: number,
    strVal: string,
    objVal: { first: string; last: string; age: number },
  ): Promise<string> {
    return Promise.resolve(`${numVal}-${strVal}-${JSON.stringify(objVal)}`);
  }

  @DBOS.workflow()
  static async sendTest(topic?: string) {
    return await DBOS.recv<string>(topic);
  }

  @DBOS.workflow()
  static async eventTest(key: string, value: string, update: boolean = false) {
    await DBOS.setEvent(key, value);
    await DBOS.sleepSeconds(5);
    if (update) {
      await DBOS.setEvent(key, `updated-${value}`);
    }
    return `${key}-${value}`;
  }
}

async function main() {
  console.log(`app version ${process.env.DBOS__APPVERSION}`);
  const config = generateDBOSTestConfig();
  DBOS.setConfig(config);
  await DBOS.launch();

  const workflowID = process.argv[2];
  const topic = process.argv[3];

  if (!workflowID) {
    console.error('workflowID not provided');
    process.exit(1);
  }

  if (!topic) {
    console.error('topic not provided');
    process.exit(1);
  }

  await DBOS.startWorkflow(ClientTest, { workflowID }).sendTest(topic);
  console.log(`Workflow ${workflowID} started`);
  exit(0);
}

if (require.main === module) {
  main()
    .then(() => {})
    .catch((e) => {
      console.log(e);
      exit(1);
    });
}
