import { Message } from "@aws-sdk/client-sqs";
import { SQSCommunicator, SQSMessageConsumer } from "./index";
export { SQSCommunicator };
import { TestingRuntime, createTestingRuntime, configureInstance, WorkflowContext, Workflow } from "@dbos-inc/dbos-sdk";

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

class SQSReceiver
{
  static msgRcvCount: number = 0;
  @SQSMessageConsumer({queueURL: process.env['SQS_QUEUE_URL']})
  @Workflow()
  static async recvMessage(_ctx: WorkflowContext, _msg: Message) {
    ++SQSReceiver.msgRcvCount;
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
      sqsCfg = configureInstance(SQSCommunicator, 'default', {awscfgname: 'aws_config', queueURL: process.env['SQS_QUEUE_URL']});
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
    const ser = await testRuntime.invoke(sqsCfg).sendMessage(
        {
            MessageBody: "{}",
        },
    );
    expect(ser.MessageId).toBeDefined();

    // Wait for receipt
    for (let i = 0; i < 100; ++i) {
      if (SQSReceiver.msgRcvCount === 1) break;
      await sleepms(100);
    }
    expect(SQSReceiver.msgRcvCount).toBe(1);
  });
});
