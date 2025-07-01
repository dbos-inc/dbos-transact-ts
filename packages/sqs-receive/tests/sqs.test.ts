import { afterEach, beforeEach, suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { Message, SendMessageCommand, SendMessageCommandInput, SQSClient } from '@aws-sdk/client-sqs';
import { SQSReceiver } from '..';
import { DBOS } from '@dbos-inc/dbos-sdk';

const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface ValueObj {
  val: number;
}

function createSQS() {
  return new SQSClient({
    region: process.env['AWS_REGION'] ?? '',
    endpoint: process.env['AWS_ENDPOINT_URL_SQS'],
    credentials: {
      accessKeyId: process.env['AWS_ACCESS_KEY_ID'] ?? '',
      secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY'] ?? '',
    },
    //logger: console,
  });
}

// Create a new type that omits the QueueUrl property
type MessageWithoutQueueUrl = Omit<SendMessageCommandInput, 'QueueUrl'>;

// Create a new type that allows QueueUrl to be added later
type MessageWithOptionalQueueUrl = MessageWithoutQueueUrl & { QueueUrl?: string };

async function sendMessageInternal(msg: MessageWithOptionalQueueUrl) {
  try {
    const smsg = { ...msg, QueueUrl: msg.QueueUrl || process.env['SQS_QUEUE_URL']! };
    return await createSQS().send(new SendMessageCommand(smsg));
  } catch (e) {
    DBOS.logger.error(e);
    throw e;
  }
}

const sendMessageStep = DBOS.registerStep(sendMessageInternal, {
  name: 'Send SQS Message',
});

const sqsReceiver = new SQSReceiver({
  client: createSQS,
});

class SQSRcv {
  static msgRcvCount: number = 0;
  static msgValueSum: number = 0;
  @sqsReceiver.messageConsumer({ queueUrl: process.env['SQS_QUEUE_URL'] })
  @DBOS.workflow()
  static async recvMessage(msg: Message) {
    const ms = msg.Body!;
    const res = JSON.parse(ms) as ValueObj;
    SQSRcv.msgRcvCount++;
    SQSRcv.msgValueSum += res.val;
    return Promise.resolve();
  }
}

suite('sqs-tests', async () => {
  const sqsAvailable = process.env['AWS_REGION'] && process.env['SQS_QUEUE_URL'];

  beforeEach(async () => {
    if (sqsAvailable) {
      DBOS.setConfig({ name: 'dbossqstest' });
      await DBOS.launch();
    }
  });

  afterEach(async () => {
    if (sqsAvailable) {
      await DBOS.shutdown();
    }
  });

  // This tests receive also; which is already wired up
  await test('sqs-send', { skip: !sqsAvailable }, async () => {
    const sv: ValueObj = {
      val: 10,
    };
    const ser = await sendMessageStep({
      MessageBody: JSON.stringify(sv),
    });
    expect(ser.MessageId).toBeDefined();

    // Wait for receipt
    for (let i = 0; i < 100; ++i) {
      if (SQSRcv.msgRcvCount === 1) break;
      await sleepms(100);
    }
    expect(SQSRcv.msgRcvCount).toBe(1);
    expect(SQSRcv.msgValueSum).toBe(10);
  });
}).catch(assert.fail);
