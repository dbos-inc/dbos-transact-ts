# DBOS AWS Simple Queue Service (SQS) Receiver

Message queues are a common building block for distributed systems. Message queues allow processing to occur at a different place or time, perhaps in another programming environment. Due to its flexibility, robustness, integration, and low cost, [Amazon Simple Queue Service](https://aws.amazon.com/sqs/) is the most popular message queuing service underpinning distributed systems in AWS.

This package includes a [DBOS](https://docs.dbos.dev/) receiver for SQS messages, which invokes a workflow for each message received.

The test in this package also shows wrapping SQS send in a [DBOS step](https://docs.dbos.dev/typescript/tutorials/step-tutorial).

## Getting Started

In order to send and receive messages with SQS, it is necessary to register with AWS, create a queue, and create access keys for the queue. (See [Send Messages Between Distributed Applications](https://aws.amazon.com/getting-started/hands-on/send-messages-distributed-applications/) in AWS documentation.)

## Configuring a DBOS Application with AWS SQS

First, ensure that the DBOS SQS package is installed into the application:

```
npm install @dbos-inc/sqs-receive
```

## Receiving Messages

The DBOS SQS receiver provides the capability of running DBOS workflows exactly once per SQS message, even on standard "at-least-once" SQS queues.

The package uses decorators to configure message receipt and identify the functions that will be invoked during message dispatch.

### Imports

First, ensure that the SQS receiver class is imported:

```typescript
import { SQSReceiver } from '@dbos-inc/dbos-sqs';
```

### Receiver Configuration

Receiving messages requires:

- Creating an `SQSReceiver` object
- Providing configuration, so that the receiver can connect to AWS and locate the SQS queues
- Associating your workflow code with SQS message queues
- Registering your receiver with DBOS

The SQS Receiver can be configured in 3 ways:

- When constructing the `SQSReceiver` object
- At the class level, with `@<receiver>.configuration`
- At the method level, with `@<receiver>.messageConsumer`

```typescript
// Note the configuration interface is:
interface SQSConfig {
  client?: SQSClient | (()=>SQSClient);
  queueUrl?: string;
  getWorkflowKey?: (m: Message) => string;
  workflowQueueName?: string;
}

// Create a receiver (can configure now, or later...)
const sqsReceiver = new SQSReceiver();

// Optionally, configure the receiver at the class level
@sqsReceiver.configure({client: .../*client or function to retrieve client goes here*/})
class SQSEventProcessor {
  ...
}
```

Then, within the class, one or more `static` workflow methods should be decorated with `@sqsReceiver.messageConsumer` to handle SQS messages:

```typescript
@sqsReceiver.configure(...)
class SQSEventProcessor {
  @sqsReceiver.messageConsumer({ queueUrl: process.env['SQS_QUEUE_URL'] })
  @DBOS.workflow()
  static async recvMessage(msg: Message) {
    // Workflow code goes here...
  }
}
```

Finally, register your SQS receiver, and launch DBOS:

```typescript
DBOS.registerLifecycleCallback(sqsReceiver);
await DBOS.launch();
```

_NOTE:_ The DBOS `@messageConsumer` decorator should be applied to a method decorated with `@DBOS.workflow`.

### Concurrency and Rate Limiting

By default, `@messageConsumer` workflows are started immediately after message receipt. If `workflowQueueName` is specified in the `SQSConfig` at either the method, class, or receiver level, then the workflow will be enqueued in a [workflow queue](https://docs.dbos.dev/typescript/reference/transactapi/workflow-queues).

### Once-And-Only-Once (OAOO) Semantics

Typical application processing for standard SQS queues implements "at least once" processing of the message:

- Receive the message from the SQS queue
- If necessary, extend the visibility timeout of the message during the course of processing
- After all processing is complete, delete the message from the queue. If there are any failures,
  the message will remain in the queue and be redelivered to another consumer.

The DBOS receiver proceeds differently:

- Receive the message from the SQS queue
- Start a workflow (using an OAOO key computed from the message)
- Quickly delete the message

This means that, instead of the SQS service redelivering the message in the case of a transient failure, it is up to DBOS to restart any interrupted workflows. Also, since DBOS workflows execute to completion exactly once, it is not necessary to use a SQS FIFO queue for exactly-once processing.

## Simple Testing

The `sqs.test.ts` file included in the source repository demonstrates sending and processing SQS messages. Before running, set the following environment variables:

- `SQS_QUEUE_URL`: SQS queue URL with access for sending and receiving messages
- `AWS_ENDPOINT_URL_SQS`: SQS endpoint URL
- `AWS_REGION`: AWS region to use
- `AWS_ACCESS_KEY_ID`: The access key with permission to use the SQS service
- `AWS_SECRET_ACCESS_KEY`: The secret access key corresponding to `AWS_ACCESS_KEY_ID`

## Sending Messages

Sending messages in DBOS is done using the AWS libraries directly, with the send call wrapped in a DBOS step to make execution durable.

### Imports

First, ensure that the AWS libraries are imported:

```typescript
import { Message, SendMessageCommand, SendMessageCommandInput, SQSClient } from '@aws-sdk/client-sqs';
```

### Sending Code

The code below is just an example. You may choose to create your SQS client and message sending code differently; the key here is that it is registered with DBOS:

```typescript
// Your preferred method for connecting to SQS
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

// Maybe you have the URL in each message, or maybe set globally...
// Create a new type that omits the QueueUrl property
type MessageWithoutQueueUrl = Omit<SendMessageCommandInput, 'QueueUrl'>;

// Create a new type that allows QueueUrl to be added later
type MessageWithOptionalQueueUrl = MessageWithoutQueueUrl & { QueueUrl?: string };

// This is the send code, not to be called directly...
async function sendMessageInternal(msg: MessageWithOptionalQueueUrl) {
  try {
    const smsg = { ...msg, QueueUrl: msg.QueueUrl || process.env['SQS_QUEUE_URL']! };
    return await createSQS().send(new SendMessageCommand(smsg));
  } catch (e) {
    DBOS.logger.error(e);
    throw e;
  }
}

// `sendMessageStep(msg)` will be what your workflows actually call.
const sendMessageStep = DBOS.registerStep(sendMessageInternal, { name: 'Send SQS Message' });
```

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact-ts).
