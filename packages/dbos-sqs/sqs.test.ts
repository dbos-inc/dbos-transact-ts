import { Message, SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Logger } from "@aws-sdk/types";
import { SQSCommunicator, SQSMessageConsumer } from "./index";
export { SQSCommunicator };
import { TestingRuntime, createTestingRuntime, configureInstance, WorkflowContext, Workflow } from "@dbos-inc/dbos-sdk";

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface ValueObj {
  val: number,
}

class SQSReceiver
{
  static msgRcvCount: number = 0;
  static msgValueSum: number = 0;
  @SQSMessageConsumer({queueUrl: process.env['SQS_QUEUE_URL']})
  @Workflow()
  static async recvMessage(_ctx: WorkflowContext, msg: Message) {
    const ms = msg.Body!;
    const res = JSON.parse(ms) as ValueObj;
    SQSReceiver.msgRcvCount++;
    SQSReceiver.msgValueSum += res.val;
    return Promise.resolve();
  }
}

describe("sqs-tests", () => {
  let testRuntime: TestingRuntime | undefined = undefined;
  let sqsIsAvailable = true;
  let sqsCfg: SQSCommunicator | undefined = undefined;

  beforeAll(() => {
    // Check if SES is available and update app config, skip the test if it's not
    if (!process.env['AWS_REGION'] || !process.env['SQS_QUEUE_URL']) {
      sqsIsAvailable = false;
    }
    else {
      // This would normally be a global or static or something
      sqsCfg = configureInstance(SQSCommunicator, 'default', {awscfgname: 'aws_config', queueUrl: process.env['SQS_QUEUE_URL']});
    }
  });

  beforeEach(async () => {
    if (sqsIsAvailable) {
      testRuntime = await createTestingRuntime(undefined,'sqs-test-dbos-config.yaml');
    }
    else {
      console.log("SQS Test is not configured.  To run, set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and SQS_QUEUE_URL");
    }
  });

  afterEach(async () => {
    if (sqsIsAvailable) {
      await testRuntime?.destroy();
    }
  }, 10000);

  // This tests receive also; which is already wired up
  test("sqs-send", async () => {
    if (!sqsIsAvailable || !testRuntime || !sqsCfg) {
      console.log("SQS unavailable, skipping SQS tests");
      return;
    }
    const sv: ValueObj = {
      val: 10,
    }

    /*
    const ser = await testRuntime.invoke(sqsCfg).sendMessage(
        {
            MessageBody: JSON.stringify(sv),
        },
    );
    expect(ser.MessageId).toBeDefined();
    */
    const logger: Logger = {
      // Output log message to the console
      trace: (message: string) => {
        console.log(message);
      },
      debug: (message: string) => {
        console.log(message);
      },
      info: (message: string) => {
        console.log(message);
      },
      warn: (message: string) => {
        console.log(message);
      },
      error: (message: string) => {
        console.log(message);
      },
    };

    const sqs = new SQSClient({
      endpoint: 'https://sqs.us-east-2.amazonaws.com/',
      //: 'https://sqs.us-east-2.amazonaws.com/300133265267/dbos-sqs-test',
      region: process.env.AWS_REGION!,
      credentials:{
        accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
      },
      logger: logger,
    });
    await sqs.send(new SendMessageCommand({MessageBody: JSON.stringify(sv), QueueUrl: '/300133265267/dbos-sqs-test'}));

    // Wait for receipt
    for (let i = 0; i < 100; ++i) {
      if (SQSReceiver.msgRcvCount === 1) break;
      await sleepms(100);
    }
    expect(SQSReceiver.msgRcvCount).toBe(1);
    expect(SQSReceiver.msgValueSum).toBe(10);
  });
});
