# DBOS Confluent Kafka Receiver

Publish/subscribe message queues are a common building block for distributed systems.
Message queues allow processing to occur at a different place or time, perhaps in multiple client programming environments.
Due to its performance, flexibility, and simple, scalable design, [Kafka](https://www.confluent.io/cloud-kafka) is a popular choice for publish/subscribe.

This package includes a [DBOS](https://docs.dbos.dev/) receiver for Kafka messages, which reliably invokes a
[DBOS workflow](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial) for every Kafka message received.

This package is based on the [Confluent JavaScript Client for Apache Kafka](https://github.com/confluentinc/confluent-kafka-javascript/).

## Configuring a Confluent Kafka Receiver

First, ensure that the DBOS Confluent Kafka Receiver package is installed into the application:

```
npm install --save @dbos-inc/confluent-kafka-receive
```

Then, create a `ConfluentKafkaReceiver` instance, providing the Kafka configuration information to the constructor.

```ts
const kafkaConfig = {
  clientId: 'example-dbos-kafka-client',
  brokers: ['kafka-host:9092'],
};

const kafkaReceiver = new ConfluentKafkaReceiver(kafkaConfig);
```

Finally, register a DBOS workflow as a Kafka topic consumer via the `KafkaReceiver` instance.
This can be done with the `KafkaReceiver.consumer` decorator or the `KafkaReceiver.registerConsumer` function.

```ts
class KafkaExample {
  @kafkaReceiver.consumer('example-topic')
  @DBOS.workflow()
  static async consumerWorkflow(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    DBOS.logger.info(`Message received: ${message.value}`);
  }

  static async registeredConsumerWorkflow(topic: string, partition: number, message: ConfluentKafkaJS.Message) {
    DBOS.logger.info(`Message received: ${message.value}`);
  }
}

KafkaExample.registeredConsumerWorkflow = DBOS.registerWorkflow(KafkaExample.registeredConsumerWorkflow, {
  name: 'registeredConsumerWorkflow',
});
kafkaReceiver.registerConsumer(KafkaExample.registeredConsumerWorkflow, 'another-example-topic');
```

When registering a Kafka consumer workflow, you can specify a single topic or an array of topics.
Topics are specified as strings or regular expressions.

### Kafka Consumer Configuration

If you need more control, you can pass consumer configuration into the decorator or `register` function.
Additionally, if you need managed concurrency, you can specify the [DBOS Queue](https://docs.dbos.dev/typescript/tutorials/queue-tutorial)
to use when executing the workflow.

```ts
class KafkaExample {
  @kafkaReceiver.consumer('example-topic', {
    config: { groupId: 'custom-group-id' },
  })
  @DBOS.workflow()
  static async consumerWorkflow(topic: string, partition: number, message: KafkaMessage) {
    DBOS.logger.info(`Message received: ${message.value}`);
  }

  static async registeredConsumerWorkflow(topic: string, partition: number, message: KafkaMessage) {
    DBOS.logger.info(`Message received: ${message.value}`);
  }
}

KafkaExample.registeredConsumerWorkflow = DBOS.registerWorkflow(KafkaExample.registeredConsumerWorkflow, {
  name: 'registeredConsumerWorkflow',
});
kafkaReceiver.registerConsumer(KafkaExample.registeredConsumerWorkflow, 'another-example-topic', {
  config: { groupId: 'custom-group-id' },
});
```

### Concurrency and Rate Limiting

By default, Kafka `eventConsumer` workflows are started immediately after message receipt.
If `queueName` is specified in `eventConsumer` options, then the workflow will be enqueued in a [workflow queue](https://docs.dbos.dev/typescript/reference/transactapi/workflow-queues).

```ts
class KafkaExample {
  @kafkaReceiver.consumer('example-topic', { queueName: 'example-queue' })
  @DBOS.workflow()
  static async consumerWorkflow(topic: string, partition: number, message: KafkaMessage) {
    DBOS.logger.info(`Message received: ${message.value}`);
  }

  static async registeredConsumerWorkflow(topic: string, partition: number, message: KafkaMessage) {
    DBOS.logger.info(`Message received: ${message.value}`);
  }
}

KafkaExample.registeredConsumerWorkflow = DBOS.registerWorkflow(KafkaExample.registeredConsumerWorkflow, {
  name: 'registeredConsumerWorkflow',
});
kafkaReceiver.registerConsumer(KafkaExample.registeredConsumerWorkflow, 'another-example-topic', {
  queueName: 'example-queue',
});
```

## Sending Messages

Sending Kafka messages is done directly using the KafkaJS library.
You can wrap the message send call in a DBOS Step to make it reliable.

```ts
import { KafkaJS } from '@confluentinc/kafka-javascript';

class KafkaTestClass {
  @DBOS.workflow()
  static async kafkaSendWorkflow(name: string, value: number) {
    const kafka = new KafkaJS.Kafka({ kafkaJS: kafkaConfig });

    const producer = kafka.producer();
    await producer.connect();

    try {
      DBOS.runStep(
        async () => {
          const message = JSON.stringify({ name, value });
          await producer.send({
            topic: 'example-topic',
            messages: [{ value: message }],
          });
        },
        { name: 'send-kafka-message' },
      );
    } finally {
      await producer.disconnect();
    }
  }
}
```

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact-ts).
