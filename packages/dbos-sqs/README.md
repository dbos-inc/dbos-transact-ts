# DBOS AWS Simple Queue Service (SQS) Receiver

Message queues are a common building block for distributed systems. Message queues allow processing to occur at a different place or time, perhaps in another programming environment. Due to its flexibility, robustness, integration, and low cost, [Amazon Simple Queue Service](https://aws.amazon.com/sqs/) is the most popular message queuing service underpinning distributed systems in AWS.

This package includes a [DBOS](https://docs.dbos.dev/).

The test in this package also shows wrapping SQS send in a [DBOS step](https://docs.dbos.dev/typescript/tutorials/step-tutorial).

## Getting Started

In order to send and receive messages with SQS, it is necessary to register with AWS, create a queue, and create access keys for the queue. (See [Send Messages Between Distributed Applications](https://aws.amazon.com/getting-started/hands-on/send-messages-distributed-applications/) in AWS documentation.)

## Configuring a DBOS Application with AWS SQS

First, ensure that the DBOS SQS package is installed into the application:

```
npm install --save @dbos-inc/dbos-sqs
```

## Sending Messages

### Imports

First, ensure that the communicator is imported:

```typescript
import { DBOS_SQS } from '@dbos-inc/dbos-sqs';
```

### Selecting A Configuration

`DBOS_SQS` is a configured class. This means that the configuration (or config file key name) must be provided when a class instance is created, for example:

```typescript
const sqsCfg = new DBOS_SQS('default', { awscfgname: 'aws_config' });
```

### Sending With Standard Queues

Within a [DBOS Workflow](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial), call the `DBOS_SQS` function from the workflow context:

```typescript
const sendRes = await sqsCfg.sendMessage({
  MessageBody: '{/*app data goes here*/}',
});
```

### FIFO Queues

Sending to [SQS FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html) is the same as with standard queues, except that FIFO queues need a `MessageDeduplicationId` (or content-based deduplication) and can be sharded by a `MessageGroupId`.

```typescript
const sendRes = await sqsCfg.sendMessage({
  MessageBody: '{/*app data goes here*/}',
  MessageDeduplicationId: 'Message key goes here',
  MessageGroupId: 'Message grouping key goes here',
});
```

## Receiving Messages

The DBOS SQS receiver provides the capability of running DBOS workflows exactly once per SQS message, even on standard "at-least-once" SQS queues.

The package uses decorators to configure message receipt and identify the functions that will be invoked during message dispatch.

### Imports

First, ensure that the method decorators are imported:

```typescript
import { SQSMessageConsumer, SQSConfigure } from '@dbos-inc/dbos-sqs';
```

### Receiver Configuration

The `@SQSConfigure` decorator should be applied at the class level to identify the credentials used by receiver functions in the class:

```typescript
interface SQSConfig {
  client?: SQSClient | (()=>SQSClient);
  queueUrl?: string;
  getWFKey?: (m: Message) => string; // Calculate workflow OAOO key for each message
  workflowQueueName?: string;
}

@SQSConfigure({awscfgname: 'sqs_receiver'})
class SQSEventProcessor {
  ...
}
```

Then, within the class, one or more methods should be decorated with `SQSMessageConsumer` to handle SQS messages:

```typescript
@SQSConfigure({ awscfgname: 'sqs_receiver' })
class SQSEventProcessor {
  @SQSMessageConsumer({ queueUrl: process.env['SQS_QUEUE_URL'] })
  @DBOS.workflow()
  static async recvMessage(msg: Message) {
    // Workflow code goes here...
  }
}
```

_NOTE:_ The DBOS `@SQSMessageConsumer` decorator should be applied to a method decorated with `@DBOS.workflow`. It is also possible to decorate an old-style DBOS workflow, which is also decorated with `@Workflow` and requires a first argument of type `WorkflowContext`.

### Concurrency and Rate Limiting

By default, `@SQSMessageConsumer` workflows are started immediately after message receipt. If `workflowQueueName` is specified in the `SQSConfig` at either the method or class level, then the workflow will be enqueued in a [workflow queue](https://docs.dbos.dev/typescript/reference/transactapi/workflow-queues).

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
- `AWS_REGION`: AWS region to use
- `AWS_ACCESS_KEY_ID`: The access key with permission to use the SQS service
- `AWS_SECRET_ACCESS_KEY`: The secret access key corresponding to `AWS_ACCESS_KEY_ID`

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact-ts).
