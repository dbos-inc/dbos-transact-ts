import { Message } from '@aws-sdk/client-sqs';
import { DBOS_SQS, SQSMessageConsumer } from './index';
import { DBOS, parseConfigFile } from '@dbos-inc/dbos-sdk';

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface ValueObj {
  val: number;
}

class SQSReceiver {
  static msgRcvCount: number = 0;
  static msgValueSum: number = 0;
  @SQSMessageConsumer({ queueUrl: process.env['SQS_QUEUE_URL'] })
  @DBOS.workflow()
  static async recvMessage(msg: Message) {
    const ms = msg.Body!;
    const res = JSON.parse(ms) as ValueObj;
    SQSReceiver.msgRcvCount++;
    SQSReceiver.msgValueSum += res.val;
    return Promise.resolve();
  }
}

describe('sqs-tests', () => {
  let sqsIsAvailable = true;
  let sqsCfg: DBOS_SQS | undefined = undefined;

  beforeAll(() => {
    // Check if SES is available and update app config, skip the test if it's not
    if (!process.env['AWS_REGION'] || !process.env['SQS_QUEUE_URL']) {
      sqsIsAvailable = false;
    } else {
      // This would normally be a global or static or something
      const [cfg, rtCfg] = parseConfigFile({ configfile: 'sqs-test-dbos-config.yaml' });
      DBOS.setConfig(cfg, rtCfg);
      sqsCfg = new DBOS_SQS('default', {
        awscfgname: 'aws_config',
        queueUrl: process.env['SQS_QUEUE_URL'],
      });
    }
  });

  beforeEach(async () => {
    if (sqsIsAvailable) {
      await DBOS.launch();
    } else {
      console.log(
        'SQS Test is not configured.  To run, set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and SQS_QUEUE_URL',
      );
    }
  });

  afterEach(async () => {
    if (sqsIsAvailable) {
      await DBOS.shutdown();
    }
  }, 10000);

  // This tests receive also; which is already wired up
  test('sqs-send', async () => {
    if (!sqsIsAvailable || !sqsCfg) {
      console.log('SQS unavailable, skipping SQS tests');
      return;
    }
    const sv: ValueObj = {
      val: 10,
    };
    const ser = await sqsCfg.sendMessage({
      MessageBody: JSON.stringify(sv),
    });
    expect(ser.MessageId).toBeDefined();

    // Wait for receipt
    for (let i = 0; i < 100; ++i) {
      if (SQSReceiver.msgRcvCount === 1) break;
      await sleepms(100);
    }
    expect(SQSReceiver.msgRcvCount).toBe(1);
    expect(SQSReceiver.msgValueSum).toBe(10);
  });
});
